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
	"sync/atomic"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/downloader"
	"github.com/brensch/nemparquet/internal/processor"
	"github.com/brensch/nemparquet/internal/util"

	"golang.org/x/sync/semaphore" // Import semaphore for inner extraction
)

// RunCombinedWorkflow orchestrates the download and process sequence.
// Flow: Start processors -> Process Archives (Download->Extract->Queue Inner) -> Process Current (Download->Queue) -> Wait
func RunCombinedWorkflow(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting combined workflow (Strict Phases)...")
	client := util.DefaultHTTPClient()
	var finalErr error

	// Channel for paths to be processed
	processingJobsChan := make(chan string, cfg.NumWorkers*4)
	var processorWg sync.WaitGroup
	var processorErrors sync.Map

	// --- Phase 1: Start Processing Workers ---
	logger.Info("Phase 1: Starting processor workers.", slog.Int("workers", cfg.NumWorkers))
	processor.StartProcessorWorkers(
		ctx, cfg, dbConn, logger,
		cfg.NumWorkers, processingJobsChan,
		&processorWg, &processorErrors,
		forceProcess,
	)

	// --- Ensure processor channel is closed and workers are waited for on exit ---
	defer func() {
		logger.Info("Closing processor channel and waiting for workers...")
		close(processingJobsChan)
		processorWg.Wait()
		logger.Info("All processor workers finished.")
		processorErrorCount := 0
		processorErrors.Range(func(key, value interface{}) bool {
			path := key.(string)
			err := value.(error)
			logger.Warn("Processor worker error recorded", slog.String("path", path), slog.Any("error", err))
			// Join processor errors into finalErr *after* main logic returns
			// For now, just log them here.
			processorErrorCount++
			return true
		})
		if processorErrorCount > 0 {
			logger.Warn("Processing phase completed with errors.", slog.Int("error_count", processorErrorCount))
		}
		// Final success log depends on finalErr state after this defer potentially modifies it
	}() // End defer

	// --- Phase 2: Discover All Potential Zip URLs ---
	logger.Info("Phase 2: Discovering all potential ZIP URLs...")
	currentURLsMap, currentDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.FeedURLs, logger)
	archiveURLsMap, archiveDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.ArchiveFeedURLs, logger)
	discoveryErr := errors.Join(currentDiscoveryErr, archiveDiscoveryErr)
	if discoveryErr != nil {
		logger.Error("Errors during discovery phase.", "error", discoveryErr)
		finalErr = errors.Join(finalErr, discoveryErr)
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	} // Check context after discovery

	allDiscoveredURLs := make([]string, 0, len(currentURLsMap)+len(archiveURLsMap))
	urlToSourceMap := make(map[string]string)
	urlIsArchive := make(map[string]bool)
	for url, source := range currentURLsMap {
		if _, exists := urlToSourceMap[url]; !exists {
			allDiscoveredURLs = append(allDiscoveredURLs, url)
			urlToSourceMap[url] = source
			urlIsArchive[url] = false
		}
	}
	for url, source := range archiveURLsMap {
		if _, exists := urlToSourceMap[url]; !exists {
			allDiscoveredURLs = append(allDiscoveredURLs, url)
			urlToSourceMap[url] = source
			urlIsArchive[url] = true
		} else {
			logger.Warn("URL discovered in both current and archive feeds, treating as current.", "url", url)
		}
	}
	sort.Strings(allDiscoveredURLs)
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
	if !forceProcess {
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
		<-queueDone
		logger.Info("Initial processing jobs queued.", slog.Int("queued_count", initialJobsSent))
	} else {
		logger.Info("Phase 4: Skipping initial path queueing because force-process is enabled.")
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	}

	// --- Phase 5: Sequential Download/Extract, Feeding Processors ---
	logger.Info("Phase 5: Starting sequential download/extract process for all discovered URLs...", slog.Int("total_urls", len(allDiscoveredURLs)))
	downloadCompletedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, allDiscoveredURLs, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		logger.Error("DB error checking download status batch.", "error", dbErr)
		finalErr = errors.Join(finalErr, dbErr)
		downloadCompletedMap = make(map[string]bool)
	}
	outerArchiveCompletedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, allDiscoveredURLs, db.FileTypeOuterArchive, db.EventProcessEnd)
	if dbErr != nil {
		logger.Error("DB error checking outer archive process status.", "error", dbErr)
		finalErr = errors.Join(finalErr, dbErr)
		outerArchiveCompletedMap = make(map[string]bool)
	}
	if ctx.Err() != nil {
		return errors.Join(finalErr, ctx.Err())
	}

	processedURLCount := 0
	downloadErrorCount := 0
	skippedDownloadCount := 0

downloadLoop: // Label for breaking outer loop on context cancellation
	for _, currentURL := range allDiscoveredURLs {
		select {
		case <-ctx.Done():
			logger.Warn("Workflow cancelled during download/extract loop.")
			finalErr = errors.Join(finalErr, ctx.Err())
			break downloadLoop
		default:
		} // Break outer loop
		processedURLCount++
		isOuterArchive := urlIsArchive[currentURL]
		fileTypeForLog := db.FileTypeZip
		if isOuterArchive {
			fileTypeForLog = db.FileTypeOuterArchive
		}
		l := logger.With(slog.String("url", currentURL), slog.String("type", fileTypeForLog), slog.Int("url_num", processedURLCount), slog.Int("total_urls", len(allDiscoveredURLs)))

		// Check skips
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
				absPath, err := filepath.Abs(filepath.Join(cfg.InputDir, filepath.Base(currentURL)))
				if err != nil {
					l.Warn("Cannot get absolute path for skipped download, cannot queue.", "error", err)
				} else if _, statErr := os.Stat(absPath); statErr == nil {
					l.Info("Queueing previously downloaded zip for processing check.", slog.String("path", absPath))
					select {
					case processingJobsChan <- absPath:
					case <-ctx.Done():
						finalErr = errors.Join(finalErr, ctx.Err())
						break downloadLoop
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

		// Perform Download
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
		if isOuterArchive {
			l.Debug("Using streaming download for outer archive.")
			progressCallback := func(dlBytes int64, totalBytes int64) {
				if totalBytes > 0 {
					l.Info("Download progress", slog.Int64("dl_mb", dlBytes/(1<<20)), slog.Int64("tot_mb", totalBytes/(1<<20)), slog.Float64("pct", float64(dlBytes)*100.0/float64(totalBytes)))
				} else {
					l.Info("Download progress", slog.Int64("dl_mb", dlBytes/(1<<20)))
				}
			}
			downloadedData, downloadErr = util.DownloadFileWithProgress(client, req, progressCallback)
		} else {
			downloadedData, downloadErr = util.DownloadFile(client, req)
		}
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

		// Handle downloaded data
		if isOuterArchive {
			l.Info("Processing inner files within outer archive.")
			extractErr := processInnerArchiveFiles(ctx, cfg, dbConn, l, currentURL, downloadedData, forceDownload, processingJobsChan) // Pass channel
			processDuration := time.Since(startTime)
			if extractErr != nil {
				if errors.Is(extractErr, context.Canceled) || errors.Is(extractErr, context.DeadlineExceeded) {
					l.Warn("Inner archive processing cancelled.")
					finalErr = errors.Join(finalErr, extractErr)
					break downloadLoop
				} // Break outer loop
				l.Error("Errors processing inner files.", "error", extractErr)
				db.LogFileEvent(ctx, dbConn, currentURL, db.FileTypeOuterArchive, db.EventError, "", "", fmt.Sprintf("inner processing error: %v", extractErr), "", &processDuration)
				finalErr = errors.Join(finalErr, extractErr)
			} else {
				l.Info("Successfully processed inner files (extracted/queued).")
				db.LogFileEvent(ctx, dbConn, currentURL, db.FileTypeOuterArchive, db.EventProcessEnd, "", "", "Inner files extracted/queued", "", &processDuration)
			}
		} else {
			zipFilename := filepath.Base(currentURL)
			outputZipPath := filepath.Join(cfg.InputDir, zipFilename)
			absOutputZipPath, absErr := filepath.Abs(outputZipPath)
			if absErr != nil {
				l.Error("Cannot get absolute path for saving zip", "error", absErr)
				finalErr = errors.Join(finalErr, absErr)
				downloadErrorCount++
				continue
			}
			l.Debug("Saving downloaded data zip.", slog.String("path", absOutputZipPath))
			err = os.WriteFile(absOutputZipPath, downloadedData, 0644)
			saveDuration := time.Since(startTime)
			if err != nil {
				saveErr := fmt.Errorf("failed save zip %s: %w", absOutputZipPath, err)
				db.LogFileEvent(ctx, dbConn, currentURL, db.FileTypeZip, db.EventError, urlToSourceMap[currentURL], absOutputZipPath, saveErr.Error(), "", &saveDuration)
				logger.Error("Failed saving zip.", "error", saveErr)
				finalErr = errors.Join(finalErr, saveErr)
				downloadErrorCount++
				continue
			}
			db.LogFileEvent(ctx, dbConn, currentURL, db.FileTypeZip, "save_end", urlToSourceMap[currentURL], absOutputZipPath, "", "", &saveDuration)
			l.Info("Successfully saved zip.", slog.String("path", absOutputZipPath))
			select {
			case processingJobsChan <- absOutputZipPath:
				l.Debug("Sent path to processor channel.", slog.String("path", absOutputZipPath))
			case <-ctx.Done():
				logger.Warn("Cancelled while sending downloaded path.")
				finalErr = errors.Join(finalErr, ctx.Err())
				break downloadLoop
			} // Break outer loop
		}
	} // End download loop

	// --- Phase 6: Shutdown and Wait ---
	logger.Info("Phase 6: Download/queueing loop finished.")
	// The defer function handles closing the channel and waiting for workers.

	// Consolidate final errors *after* defer runs
	// Need to capture processor errors from the map *before* returning finalErr
	processorErrorCount := 0
	processorErrors.Range(func(key, value interface{}) bool {
		path := key.(string)
		err := value.(error)
		finalErr = errors.Join(finalErr, fmt.Errorf("process %s: %w", filepath.Base(path), err))
		processorErrorCount++
		return true
	})
	if processorErrorCount > 0 {
		logger.Warn("Processing completed with errors.", slog.Int("error_count", processorErrorCount))
	} else if finalErr == nil {
		logger.Info("Processing completed successfully.")
	}

	logger.Info("Combined workflow finished.")
	return finalErr // Return final accumulated error state
}

// processInnerArchiveFiles handles extracting, saving, and queueing needed inner zip files concurrently.
func processInnerArchiveFiles(
	ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger,
	outerArchiveURL string, outerZipData []byte, forceDownload bool,
	processingJobsChan chan<- string,
) error { // Return only aggregated error

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
	innerDownloadedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, innerZipIdentifiers, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		l.Error("DB error checking inner status, proceeding cautiously.", "error", dbErr)
		innerDownloadedMap = make(map[string]bool)
	} // Log error, proceed

	var innerErrors sync.Map // Use sync.Map for concurrent error storage
	var extractedCount atomic.Int32
	var skippedCount atomic.Int32
	var queuedCount atomic.Int32
	var innerWg sync.WaitGroup
	concurrencyLimit := int64(cfg.NumWorkers / 2)
	if concurrencyLimit < 1 {
		concurrencyLimit = 1
	}
	if concurrencyLimit > 8 {
		concurrencyLimit = 8
	}
	innerSem := semaphore.NewWeighted(concurrencyLimit)
	l.Debug("Starting concurrent extraction/save.", slog.Int64("concurrency", concurrencyLimit))

	innerLoopCtx, cancelInnerLoop := context.WithCancel(ctx) // Context to cancel inner goroutines early
	defer cancelInnerLoop()                                  // Ensure cancellation signal propagates if outer context isn't cancelled first

	for _, fPtr := range innerZipFiles {
		f := fPtr // Capture loop variable

		// Check context before potentially blocking on semaphore or dispatching
		select {
		case <-innerLoopCtx.Done():
			l.Warn("Cancelled before processing next inner file.")
			break
		default:
		} // Break loop if cancelled

		innerZipName := filepath.Base(f.Name)
		innerLogger := l.With(slog.String("inner_zip", innerZipName))
		outputZipPath := filepath.Join(cfg.InputDir, innerZipName)
		absOutputZipPath, absErr := filepath.Abs(outputZipPath)
		if absErr != nil {
			innerLogger.Error("Cannot get absolute path, skipping inner zip.", "error", absErr)
			innerErrors.Store(innerZipName, absErr)
			continue
		}

		// Check if inner zip download needs to be skipped
		if _, completed := innerDownloadedMap[innerZipName]; completed && !forceDownload {
			innerLogger.Debug("Skipping inner zip extraction, already downloaded.")
			skippedCount.Add(1)
			if _, statErr := os.Stat(absOutputZipPath); statErr == nil {
				innerLogger.Info("Queueing previously downloaded inner zip.", slog.String("path", absOutputZipPath))
				select {
				case processingJobsChan <- absOutputZipPath:
					queuedCount.Add(1)
				case <-innerLoopCtx.Done():
					break
				} // Break loop if cancelled
			} else {
				innerLogger.Warn("Skipped inner zip missing locally, cannot queue.", "path", absOutputZipPath, "error", statErr)
			}
			continue
		}

		// Acquire semaphore
		if err := innerSem.Acquire(innerLoopCtx, 1); err != nil {
			innerLogger.Error("Failed acquire semaphore (context cancelled?)", "error", err)
			innerErrors.Store(innerZipName, err)
			break
		} // Break loop

		// Launch goroutine
		innerWg.Add(1)
		go func(file *zip.File, log *slog.Logger, outPathAbs string) {
			defer innerWg.Done()
			defer innerSem.Release(1)
			// Check context at start of goroutine too
			if innerLoopCtx.Err() != nil {
				return
			}

			log.Info("Extracting and saving inner zip.")
			extractStartTime := time.Now()
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventDownloadStart, outerArchiveURL, outPathAbs, "", "", nil)
			rc, err := file.Open()
			if err != nil {
				extractErr := fmt.Errorf("open inner %s: %w", innerZipName, err)
				log.Error("Failed open inner stream.", "error", extractErr)
				innerErrors.Store(innerZipName, extractErr)
				db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, outPathAbs, extractErr.Error(), "", nil)
				return
			}
			defer rc.Close()
			outFile, err := os.Create(outPathAbs)
			if err != nil {
				extractErr := fmt.Errorf("create file %s: %w", outPathAbs, err)
				log.Error("Failed create output file.", "error", extractErr)
				innerErrors.Store(innerZipName, extractErr)
				db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, outPathAbs, extractErr.Error(), "", nil)
				return
			}
			defer func() {
				if cerr := outFile.Close(); cerr != nil {
					log.Warn("Error closing output file.", "error", cerr)
					innerErrors.Store(innerZipName+"_close", cerr)
				}
			}()
			_, err = io.Copy(outFile, rc)
			extractDuration := time.Since(extractStartTime)
			if err != nil {
				extractErr := fmt.Errorf("copy inner %s: %w", innerZipName, err)
				log.Error("Failed copy inner zip.", "error", extractErr)
				os.Remove(outPathAbs)
				innerErrors.Store(innerZipName, extractErr)
				db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, outPathAbs, extractErr.Error(), "", &extractDuration)
				return
			}

			log.Debug("Inner zip extracted.", slog.Duration("duration", extractDuration.Round(time.Millisecond)))
			extractedCount.Add(1)
			db.LogFileEvent(ctx, dbConn, innerZipName, db.FileTypeZip, db.EventDownloadEnd, outerArchiveURL, outPathAbs, "", "", &extractDuration)

			select {
			case processingJobsChan <- outPathAbs:
				log.Debug("Sent extracted path to processor channel.", slog.String("path", outPathAbs))
				queuedCount.Add(1)
			case <-innerLoopCtx.Done():
				logger.Warn("Cancelled while sending extracted path.")
				innerErrors.Store(innerZipName+"_queue_cancel", innerLoopCtx.Err())
			}
		}(f, innerLogger, absOutputZipPath)

	} // End loop dispatching inner zips

	// Wait for all extraction goroutines for *this specific outer archive* to complete
	innerWg.Wait()
	l.Info("Finished processing inner files.", slog.Int("extracted", int(extractedCount.Load())), slog.Int("skipped", int(skippedCount.Load())), slog.Int("queued_for_processing", int(queuedCount.Load())))

	// Consolidate errors from the sync.Map for *this archive*
	var aggregatedInnerError error
	innerErrors.Range(func(key, value interface{}) bool {
		filename := key.(string)
		err := value.(error)
		aggregatedInnerError = errors.Join(aggregatedInnerError, fmt.Errorf("%s: %w", filename, err))
		return true
	})

	// Log a single summary event for the outer archive's extraction phase
	finalMessage := fmt.Sprintf("Inner file processing summary: Extracted=%d, Skipped=%d, Queued=%d", extractedCount.Load(), skippedCount.Load(), queuedCount.Load())
	if aggregatedInnerError != nil {
		finalMessage += fmt.Sprintf(", Errors Encountered: %v", aggregatedInnerError)
		db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventError, "", "", finalMessage, "", nil)
	} else { /* Log outer archive process end event only if NO inner errors? Or log it anyway? Let's log it anyway to signify this phase finished. */
		db.LogFileEvent(ctx, dbConn, outerArchiveURL, db.FileTypeOuterArchive, db.EventProcessEnd, "", "", finalMessage, "", nil)
	} // Log outer archive process end here

	return aggregatedInnerError // Return aggregated errors from this archive's inner processing
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
