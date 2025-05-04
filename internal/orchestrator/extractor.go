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
	"strings" // Keep sync if needed elsewhere, not used directly here
	"time"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
)

// ExtractInnerZips takes the byte data of a downloaded outer archive, extracts
// any inner .zip files, and saves them to cfg.InputDir.
// It logs extraction events for the outer archive and only logs errors for individual inner files.
// Returns a slice of absolute paths to successfully extracted inner zip files and any aggregated error.
func ExtractInnerZips(ctx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, archiveURL string, archiveData []byte) ([]string, error) {
	l := logger.With(slog.String("archive_url", archiveURL))
	l.Info("Starting extraction of inner zip files.")
	extractStartTime := time.Now() // Overall start time for this archive's extraction

	sourceFeed := "" // Determine source feed if possible for logging
	if len(cfg.ArchiveFeedURLs) > 0 {
		sourceFeed = cfg.ArchiveFeedURLs[0] // Approximation
	}
	// Log start of processing for the outer archive
	db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventProcessStart, sourceFeed, "", "Starting inner zip extraction", "", nil)

	zipReader, err := zip.NewReader(bytes.NewReader(archiveData), int64(len(archiveData)))
	if err != nil {
		errMsg := fmt.Sprintf("failed create zip reader for %s: %v", archiveURL, err)
		l.Error(errMsg)
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", errMsg, "", nil)
		return nil, fmt.Errorf(errMsg)
	}

	extractedFilePaths := []string{}
	var extractionErrors []error

	// Ensure the target directory exists (moved outside the loop)
	if err := os.MkdirAll(cfg.InputDir, 0755); err != nil {
		errMsg := fmt.Sprintf("failed to create input directory %s: %v", cfg.InputDir, err)
		l.Error(errMsg)
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", errMsg, "", nil)
		return nil, fmt.Errorf(errMsg)
	}

	filesToExtract := []*zip.File{}
	for _, f := range zipReader.File {
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
			finalErr := errors.Join(extractionErrors...)
			db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", fmt.Sprintf("Extraction cancelled: %v", ctx.Err()), "", nil)
			return extractedFilePaths, finalErr
		default:
		}

		innerZipName := filepath.Base(f.Name)
		outputZipPath := filepath.Join(cfg.InputDir, innerZipName)
		absOutputZipPath, absErr := filepath.Abs(outputZipPath)
		if absErr != nil {
			l.Error("Cannot get absolute path for saving inner zip, skipping.", "inner_zip", innerZipName, "error", absErr)
			extractionErrors = append(extractionErrors, fmt.Errorf("abs path %s: %w", innerZipName, absErr))
			// Log error for this specific inner file? Optional, depends on desired granularity.
			// db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, archiveURL, outputZipPath, absErr.Error(), "", nil)
			continue
		}

		innerLogger := l.With(slog.String("inner_zip", innerZipName), slog.String("output_path", absOutputZipPath))
		innerLogger.Debug("Extracting inner zip file.")

		// --- No DB Log Start Here ---
		innerStartTime := time.Now() // Still useful for duration calculation on error

		rc, openErr := f.Open()
		if openErr != nil {
			extractErr := fmt.Errorf("open inner %s: %w", innerZipName, openErr)
			innerLogger.Error("Failed open inner stream.", "error", extractErr)
			extractionErrors = append(extractionErrors, extractErr)
			// Log error for this inner file
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, archiveURL, absOutputZipPath, extractErr.Error(), "", nil)
			continue
		}

		outFile, createErr := os.Create(absOutputZipPath)
		if createErr != nil {
			extractErr := fmt.Errorf("create file %s: %w", absOutputZipPath, createErr)
			innerLogger.Error("Failed create output file.", "error", extractErr)
			extractionErrors = append(extractionErrors, extractErr)
			// Log error for this inner file
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, archiveURL, absOutputZipPath, extractErr.Error(), "", nil)
			rc.Close()
			continue
		}

		_, copyErr := io.Copy(outFile, rc)
		closeOutErr := outFile.Close()
		closeRcErr := rc.Close()
		innerExtractDuration := time.Since(innerStartTime)

		currentFileError := errors.Join(copyErr, closeOutErr, closeRcErr)

		if currentFileError != nil {
			extractErr := fmt.Errorf("extract/close inner %s: %w", innerZipName, currentFileError)
			innerLogger.Error("Failed extract/close inner zip.", "error", extractErr)
			extractionErrors = append(extractionErrors, extractErr)
			os.Remove(absOutputZipPath)
			// Log error for this inner file
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, archiveURL, absOutputZipPath, extractErr.Error(), "", &innerExtractDuration)
			continue
		}

		// --- No DB Log End Here for Success ---
		innerLogger.Debug("Inner zip extracted successfully.") // Keep debug log
		extractedFilePaths = append(extractedFilePaths, absOutputZipPath)

	} // End loop through inner zip files

	totalExtractDuration := time.Since(extractStartTime)
	l.Info("Finished extraction phase for archive.",
		slog.Int("inner_zips_extracted", len(extractedFilePaths)),
		slog.Int("extraction_errors", len(extractionErrors)), // Count errors collected in the slice
		slog.Duration("duration", totalExtractDuration.Round(time.Millisecond)))

	finalErr := errors.Join(extractionErrors...)

	// Log overall outcome for the outer archive processing
	outcomeMsg := fmt.Sprintf("Inner zips extracted: %d", len(extractedFilePaths))
	if finalErr != nil {
		// Filter out only context cancellation errors for the final message if desired
		loggableErr := finalErr
		if errors.Is(finalErr, context.Canceled) || errors.Is(finalErr, context.DeadlineExceeded) {
			// Check if there are other errors besides cancellation
			nonCancelErr := false
			tempErr := finalErr
			for {
				unwrapped := errors.Unwrap(tempErr)
				if unwrapped == nil {
					break
				}
				if !errors.Is(unwrapped, context.Canceled) && !errors.Is(unwrapped, context.DeadlineExceeded) {
					nonCancelErr = true
					break
				}
				tempErr = unwrapped
			}
			if !nonCancelErr && len(extractionErrors) == 1 { // Only log cancellation if it's the *only* error
				loggableErr = ctx.Err() // Use the context error directly for cleaner logging
			}
		}

		outcomeMsg = fmt.Sprintf("%s, Errors: %d", outcomeMsg, len(extractionErrors))
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventError, sourceFeed, "", fmt.Sprintf("Extraction finished with errors: %v", loggableErr), "", &totalExtractDuration)
	} else {
		// Log outer archive success
		db.LogFileEvent(ctx, dbConnPool, archiveURL, db.FileTypeOuterArchive, db.EventProcessEnd, sourceFeed, "", outcomeMsg, "", &totalExtractDuration)
	}

	return extractedFilePaths, finalErr
}
