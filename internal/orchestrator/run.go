package orchestrator

import (
	"archive/zip" // Need zip package
	"bytes"       // Need bytes for zip reader
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http" // Need http for downloader client

	// Need url for downloader discovery
	"os" // Need os for Stat check
	"path/filepath"
	"strings"
	"sync"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/downloader" // Keep downloader for discovery/single download
	"github.com/brensch/nemparquet/internal/processor"
	"github.com/brensch/nemparquet/internal/util" // Keep util for http client
)

// RunCombinedWorkflow orchestrates the download and process sequence.
func RunCombinedWorkflow(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting combined workflow...")
	client := util.DefaultHTTPClient() // Create HTTP client once
	var finalErr error                 // Accumulate errors across phases

	// --- Phase 1: Process Archive URLs ---
	logger.Info("Phase 1: Processing Archive Feed URLs...")
	archiveURLsDiscoveredMap, archiveDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.ArchiveFeedURLs, logger) // Use ArchiveFeedURLs
	if archiveDiscoveryErr != nil {
		logger.Error("Errors during archive discovery phase.", "error", archiveDiscoveryErr)
		finalErr = errors.Join(finalErr, archiveDiscoveryErr)
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	} // Check context

	archiveURLsToProcess := make([]string, 0, len(archiveURLsDiscoveredMap))
	for url := range archiveURLsDiscoveredMap {
		archiveURLsToProcess = append(archiveURLsToProcess, url)
	}

	logger.Info("Checking DB status for outer archive files...", slog.Int("count", len(archiveURLsToProcess)))
	processedOuterArchives, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, archiveURLsToProcess, db.FileTypeOuterArchive, db.EventProcessEnd)
	if dbErr != nil {
		logger.Error("DB error checking outer archive status.", "error", dbErr)
		finalErr = errors.Join(finalErr, dbErr)
		processedOuterArchives = make(map[string]bool)
	} // Proceed cautiously

	logger.Info("Processing outer archives sequentially...")
	processedArchiveCount := 0
	archiveErrorCount := 0
	newlyExtractedPaths := []string{}

	for _, outerArchiveURL := range archiveURLsToProcess {
		select {
		case <-ctx.Done():
			logger.Warn("Workflow cancelled during archive processing.")
			return errors.Join(finalErr, ctx.Err())
		default:
		}
		processedArchiveCount++
		l := logger.With(slog.String("outer_archive_url", outerArchiveURL), slog.Int("archive_num", processedArchiveCount), slog.Int("total_archives", len(archiveURLsToProcess)))
		if _, processed := processedOuterArchives[outerArchiveURL]; processed && !forceProcess && !forceDownload {
			l.Info("Skipping outer archive, already marked as processed.")
			db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventSkipProcess, "", "", "Already processed", "", nil)
			continue
		}

		l.Info("Downloading outer archive.")
		outerStartTime := time.Now()
		db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventDownloadStart, "", "", "", "", nil)
		req, err := http.NewRequestWithContext(ctx, "GET", outerArchiveURL, nil)
		if err != nil {
			l.Error("Failed create request for outer archive.", "error", err)
			finalErr = errors.Join(finalErr, err)
			archiveErrorCount++
			continue
		}
		req.Header.Set("User-Agent", downloader.GetRandomUserAgent())
		req.Header.Set("Accept", "*/*")
		outerZipData, err := util.DownloadFile(client, req)
		outerDownloadDuration := time.Since(outerStartTime)
		if err != nil {
			l.Error("Failed to download outer archive.", "error", err, slog.Duration("duration", outerDownloadDuration))
			db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventError, "", "", fmt.Sprintf("download failed: %v", err), "", &outerDownloadDuration)
			finalErr = errors.Join(finalErr, err)
			archiveErrorCount++
			continue
		}
		db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventDownloadEnd, "", "", "", "", &outerDownloadDuration)
		l.Info("Outer archive downloaded.", slog.Int("bytes", len(outerZipData)), slog.Duration("duration", outerDownloadDuration))

		l.Info("Processing inner files within outer archive.")
		extractedPathsFromThisArchive, err := processInnerArchiveFiles(ctx, cfg, dbConn, l, outerArchiveURL, outerZipData, forceDownload)
		processDuration := time.Since(outerStartTime)
		if err != nil {
			l.Error("Errors occurred while processing inner files.", "error", err, slog.Duration("duration", processDuration))
			db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventError, "", "", fmt.Sprintf("inner processing error: %v", err), "", &processDuration)
			finalErr = errors.Join(finalErr, err)
			archiveErrorCount++
		} else {
			l.Info("Successfully processed inner files for outer archive.")
			db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventProcessEnd, "", "", "", "", &processDuration)
			newlyExtractedPaths = append(newlyExtractedPaths, extractedPathsFromThisArchive...)
		}
	}
	logger.Info("Phase 1: Archive Feed URL processing complete.", slog.Int("archives_processed", processedArchiveCount), slog.Int("errors", archiveErrorCount))

	// --- Phase 2: Process Current URLs (Download individual data zips) ---
	logger.Info("Phase 2: Processing Current Feed URLs...")
	currentURLsDiscoveredMap, currentDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.FeedURLs, logger)
	if currentDiscoveryErr != nil {
		logger.Error("Errors during current URL discovery phase.", "error", currentDiscoveryErr)
		finalErr = errors.Join(finalErr, currentDiscoveryErr)
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	}

	currentURLsToProcess := make([]string, 0, len(currentURLsDiscoveredMap))
	for url := range currentURLsDiscoveredMap {
		currentURLsToProcess = append(currentURLsToProcess, url)
	}
	logger.Info("Checking DB status for current data zip files...", slog.Int("count", len(currentURLsToProcess)))
	currentCompletedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, currentURLsToProcess, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		logger.Error("DB error checking current zip status.", "error", dbErr)
		finalErr = errors.Join(finalErr, dbErr)
		currentCompletedMap = make(map[string]bool)
	}
	currentURLsToDownload := []string{}
	skippedCount := 0
	for _, url := range currentURLsToProcess {
		if _, completed := currentCompletedMap[url]; completed && !forceDownload {
			skippedCount++
		} else {
			currentURLsToDownload = append(currentURLsToDownload, url)
		}
	}
	if skippedCount > 0 {
		logger.Info("Skipping already downloaded current files.", slog.Int("skipped_count", skippedCount), slog.Int("total_discovered", len(currentURLsToProcess)))
	}

	logger.Info("Downloading current data zips sequentially...", slog.Int("files_to_download", len(currentURLsToDownload)))
	processedCurrentCount := 0
	currentDownloadErrorCount := 0
	pathsDownloadedThisRun := append([]string{}, newlyExtractedPaths...)
	for _, zipURL := range currentURLsToDownload {
		select {
		case <-ctx.Done():
			logger.Warn("Workflow cancelled during current URL download.")
			return errors.Join(finalErr, ctx.Err())
		default:
		}
		processedCurrentCount++
		l := logger.With(slog.String("zip_url", zipURL), slog.Int("current_zip_num", processedCurrentCount), slog.Int("total_to_download", len(currentURLsToDownload)))
		l.Info("Processing current zip download.")
		sourceURL := currentURLsDiscoveredMap[zipURL]
		savedPath, err := downloader.DownloadSingleZipAndSave(ctx, cfg, dbConn, l, client, zipURL, sourceURL)
		if err != nil {
			finalErr = errors.Join(finalErr, err)
			currentDownloadErrorCount++
		} else if savedPath != "" {
			pathsDownloadedThisRun = append(pathsDownloadedThisRun, savedPath)
		}
	}
	logger.Info("Phase 2: Current Feed URL download complete.", slog.Int("downloads_attempted", processedCurrentCount), slog.Int("errors", currentDownloadErrorCount))

	// --- Phase 3: Process All Relevant Data Zips (Concurrently) ---
	logger.Info("Phase 3: Processing all relevant data zips...")
	pathsToProcess, dbErr := db.GetPathsToProcess(ctx, dbConn)
	if dbErr != nil {
		return errors.Join(finalErr, fmt.Errorf("failed get final paths to process: %w", dbErr))
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	}
	finalPathMap := make(map[string]struct{})
	for _, p := range pathsDownloadedThisRun {
		absP, err := filepath.Abs(p)
		if err == nil {
			finalPathMap[absP] = struct{}{}
		} else {
			logger.Warn("Could not get absolute path for newly downloaded/extracted file", "path", p, "error", err)
		}
	}
	if !forceProcess {
		for _, p := range pathsToProcess {
			absP, err := filepath.Abs(p)
			if err == nil {
				finalPathMap[absP] = struct{}{}
			} else {
				logger.Warn("Could not get absolute path for previously downloaded file", "path", p, "error", err)
			}
		}
	} else {
		logger.Info("Force process enabled: Only newly downloaded/extracted zips from this run will be processed.")
	}
	finalPathsToProcessList := make([]string, 0, len(finalPathMap))
	for p := range finalPathMap {
		finalPathsToProcessList = append(finalPathsToProcessList, p)
	}
	if len(finalPathsToProcessList) == 0 {
		logger.Info("No data zips require processing. Workflow finished.")
		return finalErr
	}

	logger.Info("Queueing data zips for processing.", slog.Int("count", len(finalPathsToProcessList)), slog.Int("workers", cfg.NumWorkers))
	processingJobsChan := make(chan string, len(finalPathsToProcessList))
	var processorWg sync.WaitGroup
	var processorErrors sync.Map
	processor.StartProcessorWorkers(ctx, cfg, dbConn, logger, cfg.NumWorkers, processingJobsChan, &processorWg, &processorErrors, forceProcess)
	queueLoopCtx, cancelQueueLoop := context.WithCancel(ctx)
	queueDone := make(chan struct{})
	go func() {
		defer close(queueDone)
		defer cancelQueueLoop()
		for _, path := range finalPathsToProcessList {
			select {
			case <-queueLoopCtx.Done():
				logger.Warn("Processing queueing cancelled.")
				return
			case processingJobsChan <- path:
			}
		}
		logger.Debug("Finished queueing all paths for processing.")
	}()
	<-queueDone
	close(processingJobsChan)

	logger.Info("Waiting for processor workers to complete...")
	processorWg.Wait()
	logger.Info("All processor workers finished.")
	processorErrorCount := 0
	processorErrors.Range(func(key, value interface{}) bool {
		path := key.(string)
		err := value.(error)
		finalErr = errors.Join(finalErr, fmt.Errorf("process %s: %w", filepath.Base(path), err))
		processorErrorCount++
		return true
	})
	if processorErrorCount > 0 {
		logger.Warn("Processing finished with errors.", slog.Int("error_count", processorErrorCount))
	} else if finalErr == nil {
		logger.Info("Processing finished successfully.")
	}

	logger.Info("Combined workflow finished.")
	return finalErr
}

// processInnerArchiveFiles handles extracting and saving needed inner zip files.
func processInnerArchiveFiles(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, outerArchiveURL string, outerZipData []byte, forceDownload bool) ([]string, error) {
	l := logger.With(slog.String("outer_archive_url", outerArchiveURL))
	l.Debug("Opening outer archive from memory.")
	zipReader, err := zip.NewReader(bytes.NewReader(outerZipData), int64(len(outerZipData)))
	if err != nil {
		l.Error("Failed create zip reader for outer archive.", "error", err)
		return nil, fmt.Errorf("zip.NewReader for %s: %w", outerArchiveURL, err)
	}
	innerZipFiles := []*zip.File{}
	innerZipIdentifiers := []string{}
	for _, f := range zipReader.File {
		select {
		case <-ctx.Done():
			l.Warn("Cancelled during inner file scan.")
			return nil, ctx.Err()
		default:
		}
		if !f.FileInfo().IsDir() && strings.EqualFold(filepath.Ext(f.Name), ".zip") {
			innerZipFiles = append(innerZipFiles, f)
			innerZipIdentifiers = append(innerZipIdentifiers, filepath.Base(f.Name))
		}
	}
	if len(innerZipFiles) == 0 {
		l.Info("No inner zip files found within outer archive.")
		return nil, nil
	}
	l.Info("Found inner zip files.", slog.Int("count", len(innerZipFiles)))
	l.Debug("Checking DB status for inner zip files...")
	innerCompletedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, innerZipIdentifiers, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		l.Error("DB error checking inner zip status, attempting to extract all.", "error", dbErr)
		innerCompletedMap = make(map[string]bool)
	}
	var innerErrors error
	extractedPaths := []string{}
	extractedCount := 0
	skippedCount := 0
	for _, f := range innerZipFiles {
		select {
		case <-ctx.Done():
			l.Warn("Cancelled during inner file extraction.")
			return nil, errors.Join(innerErrors, ctx.Err())
		default:
		}
		innerZipName := filepath.Base(f.Name)
		innerLogger := l.With(slog.String("inner_zip", innerZipName))
		if _, completed := innerCompletedMap[innerZipName]; completed && !forceDownload {
			innerLogger.Debug("Skipping inner zip extraction, already downloaded.")
			skippedCount++
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventSkipDownload, outerArchiveURL, "", "Already downloaded", "", nil)
			continue
		}
		innerLogger.Info("Extracting and saving inner zip.")
		extractStartTime := time.Now()
		outputZipPath := filepath.Join(cfg.InputDir, innerZipName)
		absOutputZipPath, _ := filepath.Abs(outputZipPath)
		db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventDownloadStart, outerArchiveURL, absOutputZipPath, "", "", nil)
		rc, err := f.Open()
		if err != nil {
			extractErr := fmt.Errorf("open inner %s: %w", innerZipName, err)
			innerLogger.Error("Failed open inner zip stream.", "error", extractErr)
			innerErrors = errors.Join(innerErrors, extractErr)
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, absOutputZipPath, extractErr.Error(), "", nil)
			continue
		}
		outFile, err := os.Create(outputZipPath)
		if err != nil {
			rc.Close()
			extractErr := fmt.Errorf("create file %s: %w", outputZipPath, err)
			innerLogger.Error("Failed create output file.", "error", extractErr)
			innerErrors = errors.Join(innerErrors, extractErr)
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, absOutputZipPath, extractErr.Error(), "", nil)
			continue
		}
		_, err = io.Copy(outFile, rc)
		rc.Close()
		closeErr := outFile.Close()
		extractDuration := time.Since(extractStartTime)
		if err != nil {
			extractErr := fmt.Errorf("copy inner %s: %w", innerZipName, err)
			innerLogger.Error("Failed copy inner zip.", "error", extractErr)
			os.Remove(outputZipPath)
			innerErrors = errors.Join(innerErrors, extractErr)
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, absOutputZipPath, extractErr.Error(), "", &extractDuration)
			continue
		}
		if closeErr != nil {
			innerLogger.Warn("Error closing output file.", "error", closeErr)
			innerErrors = errors.Join(innerErrors, fmt.Errorf("close %s: %w", outputZipPath, closeErr))
		}
		innerLogger.Debug("Inner zip extracted.", slog.Duration("duration", extractDuration.Round(time.Millisecond)))
		extractedCount++
		extractedPaths = append(extractedPaths, absOutputZipPath)
		db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventDownloadEnd, outerArchiveURL, absOutputZipPath, "", "", &extractDuration)
	}
	l.Info("Finished processing inner files.", slog.Int("extracted", extractedCount), slog.Int("skipped", skippedCount), slog.Int("errors", errorCount(innerErrors)))
	return extractedPaths, innerErrors
}

// ** CORRECTED ** Helper to count joined errors
func errorCount(err error) int {
	if err == nil {
		return 0
	}
	// Use errors.As to safely check for the multi-error interface
	var multiErr interface{ Unwrap() []error }
	if errors.As(err, &multiErr) {
		return len(multiErr.Unwrap())
	}
	// If it's not nil and doesn't unwrap into multiple errors, it's one error
	return 1
}
