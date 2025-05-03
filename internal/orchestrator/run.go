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
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/downloader"
	"github.com/brensch/nemparquet/internal/processor"
	"github.com/brensch/nemparquet/internal/util" // Use updated util
)

// RunCombinedWorkflow orchestrates the download and process sequence.
// Flow: Start processors -> Process Archives (Download->Extract->Queue Inner) -> Process Current (Download->Queue) -> Wait
func RunCombinedWorkflow(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting combined workflow (Strict Phases)...")
	client := util.DefaultHTTPClient() // Uses client with long timeout
	var finalErr error

	// Channel for paths to be processed
	processingJobsChan := make(chan string, cfg.NumWorkers*4) // Larger buffer maybe?
	var processorWg sync.WaitGroup
	var processorErrors sync.Map // Collect errors from processor workers

	// --- Phase 1: Start Processing Workers ---
	logger.Info("Phase 1: Starting processor workers.", slog.Int("workers", cfg.NumWorkers))
	processor.StartProcessorWorkers(
		ctx, cfg, dbConn, logger,
		cfg.NumWorkers, processingJobsChan, // Read from this channel
		&processorWg, &processorErrors,
		forceProcess, // Pass force flag
	)

	// --- Ensure processor channel is closed and workers are waited for on exit ---
	// Use a defer function to handle cleanup regardless of how the function exits (success, error, cancellation)
	defer func() {
		logger.Info("Closing processor channel and waiting for workers...")
		// Close the channel first to signal no more jobs
		close(processingJobsChan)
		// Wait for all processor workers to finish processing remaining items
		processorWg.Wait()
		logger.Info("All processor workers finished.")

		processorErrorCount := 0
		processorErrors.Range(func(key, value interface{}) bool {
			path := key.(string)
			err := value.(error)
			// Use errors.Join with finalErr *after* the main logic finishes
			// For now, just log them here. The final return will handle joining.
			logger.Warn("Processor worker error recorded", slog.String("path", path), slog.Any("error", err))
			processorErrorCount++
			return true
		})
		if processorErrorCount > 0 {
			logger.Warn("Processing phase completed with errors.", slog.Int("error_count", processorErrorCount))
		} else if finalErr == nil { // Log overall success only if no prior errors
			logger.Info("Processing phase completed successfully.")
		}
	}() // End defer

	// --- Phase 2: Discover All Potential Zip URLs ---
	logger.Info("Phase 2: Discovering all potential ZIP URLs...")
	// Discover from both sets of URLs
	currentURLsMap, currentDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.FeedURLs, logger)        // Pass full cfg
	archiveURLsMap, archiveDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.ArchiveFeedURLs, logger) // Pass full cfg - DiscoverZipURLs needs update to take specific list
	// TODO: Update DiscoverZipURLs signature to accept []string instead of config.Config
	// For now, assuming it reads both lists from cfg internally (needs fix in downloader.go)
	discoveryErr := errors.Join(currentDiscoveryErr, archiveDiscoveryErr) // Combine discovery errors
	if discoveryErr != nil {
		logger.Error("Errors during discovery phase.", "error", discoveryErr)
		finalErr = errors.Join(finalErr, discoveryErr)
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	} // Check context

	// Combine discovered URLs, keeping track of source for logging/type
	allDiscoveredURLs := make([]string, 0, len(currentURLsMap)+len(archiveURLsMap))
	urlToSourceMap := make(map[string]string)
	urlIsArchive := make(map[string]bool) // Track which URLs point to outer archives

	for url, source := range currentURLsMap {
		if _, exists := urlToSourceMap[url]; !exists { // Avoid duplicates if somehow present in both lists
			allDiscoveredURLs = append(allDiscoveredURLs, url)
			urlToSourceMap[url] = source
			urlIsArchive[url] = false // Mark as NOT an outer archive
		}
	}
	for url, source := range archiveURLsMap {
		if _, exists := urlToSourceMap[url]; !exists {
			allDiscoveredURLs = append(allDiscoveredURLs, url)
			urlToSourceMap[url] = source
			urlIsArchive[url] = true // Mark as an outer archive
		} else {
			// If already found in current, keep it marked as non-archive
			logger.Warn("URL discovered in both current and archive feeds, treating as current.", "url", url)
		}
	}
	sort.Strings(allDiscoveredURLs) // Process in a consistent order

	if len(allDiscoveredURLs) == 0 {
		logger.Info("No zip URLs discovered. Workflow finished.")
		return discoveryErr
	}
	logger.Info("Discovery complete.", slog.Int("total_unique_zips", len(allDiscoveredURLs)))

	// --- Phase 3: Determine Initial Work Items from DB ---
	logger.Info("Phase 3: Determining initial work items from database...")
	initialPathsToProcess, dbErr := db.GetPathsToProcess(ctx, dbConn)
	if dbErr != nil {
		return errors.Join(finalErr, fmt.Errorf("failed get initial paths to process: %w", dbErr))
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	}
	logger.Info("Initial processing check complete.", slog.Int("paths_to_process_initially", len(initialPathsToProcess)))

	// --- Phase 4: Queue Initial Processing Jobs ---
	if !forceProcess { // Only queue initial jobs if not forcing process
		logger.Info("Phase 4: Queueing initially identified paths for processing.", slog.Int("count", len(initialPathsToProcess)))
		initialJobsSent := 0
		queueLoopCtx, cancelQueueLoop := context.WithCancel(ctx)
		queueDone := make(chan struct{})
		go func() {
			defer close(queueDone)
			defer cancelQueueLoop()
			for _, path := range initialPathsToProcess {
				if _, statErr := os.Stat(path); statErr != nil {
					logger.Warn("Initial process path missing on disk, skipping queue.", "path", path, "error", statErr)
					continue
				}
				select {
				case <-queueLoopCtx.Done():
					logger.Warn("Initial queuing cancelled.")
					return
				case processingJobsChan <- path:
					initialJobsSent++
				}
			}
			logger.Debug("Finished queuing all initial paths naturally.")
		}()
		<-queueDone // Wait for initial queuing goroutine to finish
		logger.Info("Initial processing jobs queued.", slog.Int("queued_count", initialJobsSent))
	} else {
		logger.Info("Phase 4: Skipping initial path queueing because force-process is enabled.")
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	} // Check context again

	// --- Phase 5: Sequential Download/Extract, Feeding Processors ---
	logger.Info("Phase 5: Starting sequential download/extract process for all discovered URLs...", slog.Int("total_urls", len(allDiscoveredURLs)))

	// Batch check download status for *all* discovered URLs (both types)
	downloadCompletedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, allDiscoveredURLs, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		logger.Error("DB error checking download status batch.", "error", dbErr)
		finalErr = errors.Join(finalErr, dbErr)
		downloadCompletedMap = make(map[string]bool)
	}
	outerArchiveCompletedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, allDiscoveredURLs, db.FileTypeOuterArchive, db.EventProcessEnd) // Check for outer archive process end
	if dbErr != nil {
		logger.Error("DB error checking outer archive process status.", "error", dbErr)
		finalErr = errors.Join(finalErr, dbErr)
		outerArchiveCompletedMap = make(map[string]bool)
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	} // Check context

	processedURLCount := 0
	downloadErrorCount := 0
	skippedDownloadCount := 0

	for _, currentURL := range allDiscoveredURLs {
		select {
		case <-ctx.Done():
			logger.Warn("Workflow cancelled during download/extract loop.")
			return errors.Join(finalErr, ctx.Err())
		default:
		} // Check context at start of loop

		processedURLCount++
		isOuterArchive := urlIsArchive[currentURL]
		fileTypeForLog := db.FileTypeZip
		if isOuterArchive {
			fileTypeForLog = db.FileTypeOuterArchive
		}
		l := logger.With(slog.String("url", currentURL), slog.String("type", fileTypeForLog), slog.Int("url_num", processedURLCount), slog.Int("total_urls", len(allDiscoveredURLs)))

		// --- Check if download/extraction can be skipped ---
		shouldSkip := false
		if isOuterArchive {
			if _, processed := outerArchiveCompletedMap[currentURL]; processed && !forceProcess && !forceDownload {
				l.Info("Skipping outer archive, already processed.")
				db.LogFileEvent(ctx, dbConn, currentURL, fileTypeForLog, db.EventSkipProcess, "", "", "Already processed", "", nil)
				shouldSkip = true
			}
		} else {
			if _, downloaded := downloadCompletedMap[currentURL]; downloaded && !forceDownload {
				l.Info("Skipping regular zip download, already completed.")
				db.LogFileEvent(ctx, dbConn, currentURL, fileTypeForLog, db.EventSkipDownload, urlToSourceMap[currentURL], "", "Already downloaded", "", nil)
				shouldSkip = true
				// Queue for processing check if skipped download
				absPath, err := filepath.Abs(filepath.Join(cfg.InputDir, filepath.Base(currentURL)))
				if err != nil {
					l.Warn("Cannot get absolute path for skipped download, cannot queue.", "error", err)
				} else if _, statErr := os.Stat(absPath); statErr == nil {
					l.Info("Queueing previously downloaded zip for processing check.", slog.String("path", absPath))
					select {
					case processingJobsChan <- absPath:
					case <-ctx.Done():
						return errors.Join(finalErr, ctx.Err())
					}
				} else {
					l.Warn("Skipped download file missing locally, cannot queue.", "path", absPath, "error", statErr)
				}
			}
		}
		if shouldSkip {
			skippedDownloadCount++
			continue
		}

		// --- Perform Download ---
		l.Info("Starting download.")
		startTime := time.Now()
		db.LogFileEvent(ctx, dbConn, currentURL, fileTypeForLog, db.EventDownloadStart, urlToSourceMap[currentURL], "", "", "", nil)
		req, err := http.NewRequestWithContext(ctx, "GET", currentURL, nil)
		if err != nil {
			l.Error("Failed create request.", "error", err)
			finalErr = errors.Join(finalErr, err)
			downloadErrorCount++
			continue
		}
		req.Header.Set("User-Agent", downloader.GetRandomUserAgent())
		req.Header.Set("Accept", "*/*")

		var downloadedData []byte
		var downloadErr error

		// Use streaming download with progress for outer archives
		if isOuterArchive {
			l.Debug("Using streaming download for outer archive.")
			progressCallback := func(downloadedBytes int64, totalBytes int64) {
				// Log progress every 100MB (logic is inside the progressReader now)
				// Ensure logger passed here has the right context (it does via 'l')
				if totalBytes > 0 {
					l.Info("Download progress", slog.Int64("downloaded_mb", downloadedBytes/(1024*1024)), slog.Int64("total_mb", totalBytes/(1024*1024)), slog.Float64("percent", float64(downloadedBytes)*100.0/float64(totalBytes)))
				} else {
					l.Info("Download progress", slog.Int64("downloaded_mb", downloadedBytes/(1024*1024)))
				}
			}
			downloadedData, downloadErr = util.DownloadFileWithProgress(client, req, progressCallback)
		} else {
			// Use regular download for current zips (can switch if needed)
			l.Debug("Using standard download for current zip.")
			// Need to recreate request as DownloadFile doesn't take it anymore
			// Or better: modify DownloadSingleZipAndSave to handle both cases
			// Let's stick to calling DownloadSingleZipAndSave for consistency
			// This means the logic below for saving/queuing needs adjustment
			// --- Reverting to call DownloadSingleZipAndSave ---
			savedPath, singleDownloadErr := downloader.DownloadSingleZipAndSave(ctx, cfg, dbConn, l, client, currentURL, urlToSourceMap[currentURL])
			downloadErr = singleDownloadErr // Assign error
			if downloadErr == nil && savedPath != "" {
				// Queue the saved path for processing
				select {
				case processingJobsChan <- savedPath:
					l.Debug("Sent path to processor channel.", slog.String("path", savedPath))
				case <-ctx.Done():
					logger.Warn("Cancelled while sending downloaded path.")
					return errors.Join(finalErr, ctx.Err())
				}
				// Skip the outer archive handling logic below for this iteration
				continue // Move to next URL
			} else if downloadErr != nil {
				// Error handled within DownloadSingleZipAndSave, just join and continue
				finalErr = errors.Join(finalErr, downloadErr)
				downloadErrorCount++
				continue // Move to next URL
			} else {
				// Should not happen if download succeeded but path is empty
				l.Error("Download reported success but no path returned.")
				finalErr = errors.Join(finalErr, fmt.Errorf("empty path returned for successful download: %s", currentURL))
				downloadErrorCount++
				continue
			}
			// --- End Reverting ---
		}

		// This part is now only reached if it was an outer archive download
		downloadDuration := time.Since(startTime)
		if downloadErr != nil {
			l.Error("Download failed.", "error", downloadErr)
			db.LogFileEvent(ctx, dbConn, currentURL, fileTypeForLog, db.EventError, urlToSourceMap[currentURL], "", fmt.Sprintf("download failed: %v", downloadErr), "", &downloadDuration)
			finalErr = errors.Join(finalErr, downloadErr)
			downloadErrorCount++
			continue
		}
		db.LogFileEvent(ctx, dbConn, currentURL, fileTypeForLog, db.EventDownloadEnd, urlToSourceMap[currentURL], "", "", "", &downloadDuration)
		l.Info("Download complete.", slog.Int("bytes", len(downloadedData)))

		// --- Handle downloaded outer archive data (Extract/Queue) ---
		if isOuterArchive {
			l.Info("Processing inner files within outer archive.")
			extractErr := processInnerArchiveFilesAndQueue(ctx, cfg, dbConn, l, currentURL, downloadedData, forceDownload, forceProcess, processingJobsChan)
			processDuration := time.Since(startTime) // Includes download + extract/queue
			if extractErr != nil {
				l.Error("Errors processing inner files.", "error", extractErr)
				db.LogFileEvent(ctx, dbConn, currentURL, db.FileTypeOuterArchive, db.EventError, "", "", fmt.Sprintf("inner processing error: %v", extractErr), "", &processDuration)
				finalErr = errors.Join(finalErr, extractErr) /* Don't increment downloadErrorCount */
			} else {
				l.Info("Successfully processed inner files (extracted/queued).")
				db.LogFileEvent(ctx, dbConn, currentURL, db.FileTypeOuterArchive, db.EventProcessEnd, "", "", "Inner files extracted/queued", "", &processDuration)
			}
		}
		// Note: Regular zips are handled and queued within the reverted block above

	} // End download loop

	// --- Phase 6: Shutdown and Wait ---
	logger.Info("Phase 6: All download/queueing phases complete.")
	// The defer function handles closing the channel and waiting for workers.

	return finalErr
}

// processInnerArchiveFilesAndQueue handles extracting, saving, and queueing needed inner zip files.
// Returns any aggregated error encountered during the process.
func processInnerArchiveFilesAndQueue(
	ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger,
	outerArchiveURL string, outerZipData []byte, forceDownload bool, forceProcess bool,
	processingJobsChan chan<- string,
) error { // Return only error now

	l := logger.With(slog.String("outer_archive_url", outerArchiveURL))
	l.Debug("Opening outer archive from memory.")
	zipReader, err := zip.NewReader(bytes.NewReader(outerZipData), int64(len(outerZipData)))
	if err != nil {
		l.Error("Failed create zip reader.", "error", err)
		return fmt.Errorf("zip.NewReader %s: %w", outerArchiveURL, err)
	}

	innerZipFiles := []*zip.File{}
	innerZipIdentifiers := []string{}
	for _, f := range zipReader.File { /* ... find inner zips ... */
		select {
		case <-ctx.Done():
			l.Warn("Cancelled inner scan.")
			return ctx.Err()
		default:
		}
		if !f.FileInfo().IsDir() && strings.EqualFold(filepath.Ext(f.Name), ".zip") {
			innerZipFiles = append(innerZipFiles, f)
			innerZipIdentifiers = append(innerZipIdentifiers, filepath.Base(f.Name))
		}
	}
	if len(innerZipFiles) == 0 {
		l.Info("No inner zip files found.")
		return nil
	}
	l.Info("Found inner zip files.", slog.Int("count", len(innerZipFiles)))

	l.Debug("Checking DB status for inner zip files...")
	innerCompletedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, innerZipIdentifiers, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		l.Error("DB error checking inner status, extracting all.", "error", dbErr)
		innerCompletedMap = make(map[string]bool)
	} // Proceed cautiously, log error

	var innerErrors error
	extractedCount := 0
	skippedCount := 0
	queuedCount := 0

	for _, f := range innerZipFiles {
		select {
		case <-ctx.Done():
			l.Warn("Cancelled inner extraction.")
			return errors.Join(innerErrors, ctx.Err())
		default:
		}
		innerZipName := filepath.Base(f.Name)
		innerLogger := l.With(slog.String("inner_zip", innerZipName))
		outputZipPath := filepath.Join(cfg.InputDir, innerZipName)
		absOutputZipPath, absErr := filepath.Abs(outputZipPath)
		if absErr != nil {
			innerLogger.Error("Cannot get absolute path, skipping inner zip.", "error", absErr)
			innerErrors = errors.Join(innerErrors, absErr)
			continue
		}

		// Check if inner zip download needs to be skipped
		if _, completed := innerCompletedMap[innerZipName]; completed && !forceDownload {
			innerLogger.Debug("Skipping inner zip extraction, already downloaded.")
			skippedCount++
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventSkipDownload, outerArchiveURL, absOutputZipPath, "Already downloaded", "", nil)
			// Queue for processing check if file exists locally
			if _, statErr := os.Stat(absOutputZipPath); statErr == nil {
				innerLogger.Info("Queueing previously downloaded inner zip for processing check.", slog.String("path", absOutputZipPath))
				select {
				case processingJobsChan <- absOutputZipPath:
					queuedCount++
				case <-ctx.Done():
					return errors.Join(innerErrors, ctx.Err())
				}
			} else {
				innerLogger.Warn("Skipped inner zip missing locally, cannot queue.", "path", absOutputZipPath, "error", statErr)
			}
			continue
		}

		// --- Extract and Save inner zip ---
		innerLogger.Info("Extracting and saving inner zip.")
		extractStartTime := time.Now()
		db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventDownloadStart, outerArchiveURL, absOutputZipPath, "", "", nil)
		rc, err := f.Open()
		if err != nil { /* ... handle open error ... */
			extractErr := fmt.Errorf("open inner %s: %w", innerZipName, err)
			innerLogger.Error("Failed open inner stream.", "error", extractErr)
			innerErrors = errors.Join(innerErrors, extractErr)
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, absOutputZipPath, extractErr.Error(), "", nil)
			continue
		}
		outFile, err := os.Create(outputZipPath)
		if err != nil { /* ... handle create error ... */
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
		if err != nil { /* ... handle copy error ... */
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
		db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventDownloadEnd, outerArchiveURL, absOutputZipPath, "", "", &extractDuration)

		// Queue the newly saved path for processing
		select {
		case processingJobsChan <- absOutputZipPath:
			innerLogger.Debug("Sent extracted path to processor channel.", slog.String("path", absOutputZipPath))
			queuedCount++
		case <-ctx.Done():
			logger.Warn("Cancelled while sending extracted path.")
			innerErrors = errors.Join(innerErrors, ctx.Err())
			return errors.Join(innerErrors, ctx.Err())
		}
	} // End loop inner zips

	l.Info("Finished processing inner files.", slog.Int("extracted", extractedCount), slog.Int("skipped", skippedCount), slog.Int("queued_for_processing", queuedCount), slog.Int("errors", errorCount(innerErrors)))
	return innerErrors // Return only errors encountered during this stage
}

// errorCount helper
func errorCount(err error) int {
	if err == nil {
		return 0
	}
	var multiErr interface{ Unwrap() []error }
	if errors.As(err, &multiErr) {
		return len(multiErr.Unwrap())
	}
	return 1
}
