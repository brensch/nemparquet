package orchestrator

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
)

// ExtractInnerZips takes the byte data of a downloaded outer archive, extracts
// any inner .zip files, and saves them to cfg.InputDir.
// It logs extraction events for the outer archive and individual inner files.
// Returns a slice of absolute paths to successfully extracted inner zip files and any aggregated error.
func ExtractInnerZips(ctx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, archiveURL string, archiveData []byte) ([]string, error) {
	l := logger.With(slog.String("archive_url", archiveURL)) // Logger already has archive_url context from orchestrator
	l.Info("Starting extraction of inner zip files.")
	extractStartTime := time.Now() // Overall start time for this archive's extraction

	// Log start of processing for the outer archive (using a suitable event type)
	sourceFeed := "" // Determine source feed if possible for logging
	if len(cfg.ArchiveFeedURLs) > 0 {
		sourceFeed = cfg.ArchiveFeedURLs[0] // Approximation
	}
	// Using EventProcessStart for the outer archive extraction phase
	db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventProcessStart, sourceFeed, "", "Starting inner zip extraction", "", nil)

	zipReader, err := zip.NewReader(bytes.NewReader(archiveData), int64(len(archiveData)))
	if err != nil {
		errMsg := fmt.Sprintf("failed create zip reader for %s: %v", archiveURL, err)
		l.Error(errMsg)
		// Log failure to read archive
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", errMsg, "", nil)
		return nil, fmt.Errorf(errMsg)
	}

	extractedFilePaths := []string{}
	var extractionErrors []error

	// Ensure the target directory exists
	if err := os.MkdirAll(cfg.InputDir, 0755); err != nil {
		errMsg := fmt.Sprintf("failed to create input directory %s: %v", cfg.InputDir, err)
		l.Error(errMsg)
		// Log this critical error associated with the outer archive
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", errMsg, "", nil)
		return nil, fmt.Errorf(errMsg)
	}

	filesToExtract := []*zip.File{}
	for _, f := range zipReader.File {
		// Identify potential files first
		if !f.FileInfo().IsDir() && strings.EqualFold(filepath.Ext(f.Name), ".zip") {
			filesToExtract = append(filesToExtract, f)
		}
	}

	l.Info("Identified inner zip files to extract.", slog.Int("count", len(filesToExtract)))

	for _, f := range filesToExtract {
		// Check context cancellation before processing each inner file
		select {
		case <-ctx.Done():
			l.Warn("Extraction cancelled by context during inner file processing.", "error", ctx.Err())
			extractionErrors = append(extractionErrors, ctx.Err())
			// Combine collected errors and return immediately
			finalErr := errors.Join(extractionErrors...)
			// Log cancellation for the outer archive
			db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", fmt.Sprintf("Extraction cancelled: %v", ctx.Err()), "", nil)
			return extractedFilePaths, finalErr
		default:
		}

		innerZipName := filepath.Base(f.Name) // Use just the base name for the output file
		outputZipPath := filepath.Join(cfg.InputDir, innerZipName)
		absOutputZipPath, absErr := filepath.Abs(outputZipPath)
		if absErr != nil {
			l.Error("Cannot get absolute path for saving inner zip, skipping.", "inner_zip", innerZipName, "error", absErr)
			extractionErrors = append(extractionErrors, fmt.Errorf("abs path %s: %w", innerZipName, absErr))
			continue // Skip to next file
		}

		innerLogger := l.With(slog.String("inner_zip", innerZipName), slog.String("output_path", absOutputZipPath))
		innerLogger.Debug("Extracting inner zip file.") // More detailed logging can be Debug

		// Log start of extraction for the inner file (using DownloadStart as before)
		innerStartTime := time.Now()
		db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventDownloadStart, archiveURL, absOutputZipPath, "", "", nil)

		rc, openErr := f.Open()
		if openErr != nil {
			extractErr := fmt.Errorf("open inner %s: %w", innerZipName, openErr)
			innerLogger.Error("Failed open inner stream.", "error", extractErr)
			extractionErrors = append(extractionErrors, extractErr)
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, archiveURL, absOutputZipPath, extractErr.Error(), "", nil)
			continue // Skip to next file
		}

		outFile, createErr := os.Create(absOutputZipPath)
		if createErr != nil {
			extractErr := fmt.Errorf("create file %s: %w", absOutputZipPath, createErr)
			innerLogger.Error("Failed create output file.", "error", extractErr)
			extractionErrors = append(extractionErrors, extractErr)
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, archiveURL, absOutputZipPath, extractErr.Error(), "", nil)
			rc.Close() // Close reader since we are continuing
			continue   // Skip to next file
		}

		_, copyErr := io.Copy(outFile, rc)
		// --- Explicitly close files right after use ---
		closeOutErr := outFile.Close()
		closeRcErr := rc.Close()
		// --- End explicit close ---

		innerExtractDuration := time.Since(innerStartTime) // Duration for this specific file extraction

		// Check for errors during copy or close
		currentFileError := errors.Join(copyErr, closeOutErr, closeRcErr)

		if currentFileError != nil {
			extractErr := fmt.Errorf("extract/close inner %s: %w", innerZipName, currentFileError)
			innerLogger.Error("Failed extract/close inner zip.", "error", extractErr)
			extractionErrors = append(extractionErrors, extractErr)
			os.Remove(absOutputZipPath) // Attempt to clean up failed file
			// Log specific error for the inner file
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, archiveURL, absOutputZipPath, extractErr.Error(), "", &innerExtractDuration)
			continue // Skip to next file
		}

		// If we reach here, extraction for this inner file was successful
		innerLogger.Info("Inner zip extracted successfully.")
		extractedFilePaths = append(extractedFilePaths, absOutputZipPath)
		// Log successful extraction for the inner file (using DownloadEnd as before)
		db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventDownloadEnd, archiveURL, absOutputZipPath, "", "", &innerExtractDuration)

	} // End loop through inner zip files

	totalExtractDuration := time.Since(extractStartTime)
	l.Info("Finished extraction phase for archive.",
		slog.Int("inner_zips_extracted", len(extractedFilePaths)),
		slog.Int("extraction_errors", len(extractionErrors)),
		slog.Duration("duration", totalExtractDuration.Round(time.Millisecond)))

	// Combine any extraction errors collected during the loop
	finalErr := errors.Join(extractionErrors...)

	// Log overall outcome for the outer archive processing
	outcomeMsg := fmt.Sprintf("Inner zips extracted: %d", len(extractedFilePaths))
	if finalErr != nil {
		outcomeMsg = fmt.Sprintf("%s, Errors: %d", outcomeMsg, len(extractionErrors))
		// Log outer archive error if any inner extraction failed
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", fmt.Sprintf("Extraction finished with errors: %v", finalErr), "", &totalExtractDuration)
	} else {
		// Log outer archive success (using EventProcessEnd)
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventProcessEnd, sourceFeed, "", outcomeMsg, "", &totalExtractDuration)
	}

	return extractedFilePaths, finalErr
}
