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
	"os/signal" // Added for graceful shutdown
	"path/filepath"
	"runtime" // Added for semaphore calculation
	"sort"
	"strings"
	"sync"    // Keep for inner archive counts
	"syscall" // Added for graceful shutdown
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/downloader"
	"github.com/brensch/nemparquet/internal/processor" // Will use the refactored processor
	"github.com/brensch/nemparquet/internal/util"

	"golang.org/x/sync/semaphore" // Keep for inner extraction limiting
)

// RunCombinedWorkflow orchestrates the download and process sequence.
// Workers now handle DB interaction per section. Includes graceful shutdown.
func RunCombinedWorkflow(appCtx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting combined workflow (Worker Connection per Section)...")

	// --- Graceful Shutdown Setup ---
	ctx, stop := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Ensure stop is called

	// --- Initialize Components ---
	client := util.DefaultHTTPClient()
	var finalErr error

	// Channel for paths to be processed by workers
	processingJobsChan := make(chan string, cfg.NumWorkers*4) // Tunable buffer
	var processorWg sync.WaitGroup
	var processorErrors sync.Map // Collect errors from workers

	// --- Phase 1: Start Processing Workers ---
	logger.Info("Phase 1: Starting processor workers.", slog.Int("workers", cfg.NumWorkers))
	// Pass the main DB pool (*sql.DB) to workers.
	// Workers will use this pool for checks, but create short-lived connections for writing sections.
	processor.StartProcessorWorkers(
		ctx, cfg, dbConnPool, logger, // <<< Pass Pool
		cfg.NumWorkers, processingJobsChan,
		&processorWg, &processorErrors,
		forceProcess,
	)

	// --- Ensure processor channel is closed and workers are waited for on exit ---
	// This defer runs *after* the main function body returns, but *before* RunCombinedWorkflow returns.
	// It ensures we wait for workers even if the download/discovery loop exits early.
	defer func() {
		logger.Info("Waiting for processor workers to finish...")
		processorWg.Wait() // Wait for all workers launched by StartProcessorWorkers
		logger.Info("All processor workers finished.")

		// Aggregate errors AFTER workers are done
		processorErrorCount := 0
		processorErrors.Range(func(key, value interface{}) bool {
			path := key.(string)
			err := value.(error)
			// Avoid double-logging context cancelled if the main context was cancelled
			if !(errors.Is(err, context.Canceled) && ctx.Err() != nil) {
				logger.Warn("Processor worker error recorded", slog.String("path", path), slog.Any("error", err))
				finalErr = errors.Join(finalErr, fmt.Errorf("process %s: %w", filepath.Base(path), err))
				processorErrorCount++
			}
			return true
		})
		if processorErrorCount > 0 {
			logger.Warn("Processing phase completed with errors.", slog.Int("error_count", processorErrorCount))
		}

		// Final status log depends on aggregated finalErr
		finalCtxErr := ctx.Err()
		if finalCtxErr != nil && !errors.Is(finalCtxErr, context.Canceled) {
			logger.Error("Workflow finished due to context error.", "error", finalCtxErr)
			finalErr = errors.Join(finalErr, finalCtxErr) // Ensure context error is included
		}

		if finalErr == nil {
			logger.Info("Combined workflow finished successfully.")
		} else {
			logger.Warn("Combined workflow finished with errors.", "error", finalErr)
		}
	}() // End defer

	// --- Phase 2 & 3: Discover URLs and Feed Paths ---
	// Run discovery and feeding in a separate goroutine to allow main thread to respond to cancellation
	discoveryDone := make(chan struct{})
	go func() {
		defer close(discoveryDone)      // Signal discovery is finished
		defer close(processingJobsChan) // IMPORTANT: Close channel when feeding is done

		logger.Info("Phase 2: Discovering all potential ZIP URLs...")
		currentURLsMap, currentDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.FeedURLs, logger)
		archiveURLsMap, archiveDiscoveryErr := downloader.DiscoverZipURLs(ctx, cfg.ArchiveFeedURLs, logger)
		discoveryErr := errors.Join(currentDiscoveryErr, archiveDiscoveryErr)
		if discoveryErr != nil {
			logger.Error("Errors during discovery phase.", "error", discoveryErr)
			finalErr = errors.Join(finalErr, discoveryErr)
		}
		if ctx.Err() != nil {
			logger.Warn("Discovery cancelled.")
			finalErr = errors.Join(finalErr, ctx.Err())
			return
		}

		allDiscoveredURLs, urlToSourceMap, urlIsArchive := combineDiscoveredURLs(logger, currentURLsMap, archiveURLsMap)
		if len(allDiscoveredURLs) == 0 {
			logger.Info("No zip URLs discovered. Exiting discovery.")
			return
		}
		logger.Info("Discovery complete.", slog.Int("total_unique_zips", len(allDiscoveredURLs)))

		logger.Info("Phase 3: Determining initial work items and download status from DB...")
		downloadCompletedMap, dbErrDl := db.GetCompletionStatusBatch(ctx, dbConnPool, allDiscoveredURLs, db.FileTypeZip, db.EventDownloadEnd)
		outerArchiveCompletedMap, dbErrArc := db.GetCompletionStatusBatch(ctx, dbConnPool, allDiscoveredURLs, db.FileTypeOuterArchive, db.EventProcessEnd)
		dbCheckErr := errors.Join(dbErrDl, dbErrArc)
		if dbCheckErr != nil {
			logger.Error("DB error checking batch completion status.", "error", dbCheckErr)
			finalErr = errors.Join(finalErr, dbCheckErr)
			downloadCompletedMap = make(map[string]bool)
			outerArchiveCompletedMap = make(map[string]bool)
		}
		if ctx.Err() != nil {
			logger.Warn("Discovery/DB Check cancelled.")
			finalErr = errors.Join(finalErr, ctx.Err())
			return
		}

		initialPathsToProcess, dbErrInit := db.GetPathsToProcess(ctx, dbConnPool) // Check for files needing processing
		if dbErrInit != nil {
			logger.Error("Failed get initial paths to process", "error", dbErrInit)
			finalErr = errors.Join(finalErr, dbErrInit)
		}
		if ctx.Err() != nil {
			logger.Warn("Initial path check cancelled.")
			finalErr = errors.Join(finalErr, ctx.Err())
			return
		}
		logger.Info("Initial DB checks complete.", slog.Int("paths_to_process_initially", len(initialPathsToProcess)))

		// --- Phase 4: Queue Initial Processing Jobs ---
		if !forceProcess {
			logger.Info("Phase 4: Queueing initially identified paths for processing.", slog.Int("count", len(initialPathsToProcess)))
			initialJobsSent := 0
			for _, path := range initialPathsToProcess {
				if _, statErr := os.Stat(path); statErr != nil {
					logger.Warn("Initial process path missing on disk, skipping queue.", "path", path, "error", statErr)
					continue
				}
				select {
				case <-ctx.Done():
					logger.Warn("Initial queuing cancelled.")
					finalErr = errors.Join(finalErr, ctx.Err())
					return
				case processingJobsChan <- path:
					initialJobsSent++
				}
			}
			logger.Info("Initial processing jobs queued.", slog.Int("queued_count", initialJobsSent))
		} else {
			logger.Info("Phase 4: Skipping initial path queueing because force-process is enabled.")
		}
		if ctx.Err() != nil {
			logger.Warn("Context cancelled after initial queueing.")
			finalErr = errors.Join(finalErr, ctx.Err())
			return
		}

		// --- Phase 5: Sequential Download/Extract, Feeding Processors ---
		logger.Info("Phase 5: Starting sequential download/extract process...", slog.Int("total_urls", len(allDiscoveredURLs)))
		processedURLCount := 0
		downloadErrorCount := 0
		skippedDownloadCount := 0
		extractedCount := 0
		skippedExtractCount := 0

	downloadLoop:
		for _, currentURL := range allDiscoveredURLs {
			select {
			case <-ctx.Done():
				logger.Warn("Workflow cancelled during download/extract loop.")
				finalErr = errors.Join(finalErr, ctx.Err())
				break downloadLoop
			default:
			}
			processedURLCount++
			isOuterArchive := urlIsArchive[currentURL]
			fileTypeForLog := db.FileTypeZip
			if isOuterArchive {
				fileTypeForLog = db.FileTypeOuterArchive
			}
			l := logger.With(slog.String("url", currentURL), slog.String("type", fileTypeForLog), slog.Int("url_num", processedURLCount), slog.Int("total_urls", len(allDiscoveredURLs)))

			// --- Skip Logic ---
			shouldSkip := false
			if isOuterArchive {
				if _, processed := outerArchiveCompletedMap[currentURL]; processed && !forceProcess && !forceDownload {
					l.Info("Skipping outer archive, already processed.")
					db.LogFileEvent(ctx, dbConnPool, currentURL, fileTypeForLog, db.EventSkipProcess, "", "", "Already processed", "", nil)
					shouldSkip = true
				}
			} else {
				if _, downloaded := downloadCompletedMap[currentURL]; downloaded && !forceDownload {
					l.Info("Skipping regular zip download, already completed.")
					db.LogFileEvent(ctx, dbConnPool, currentURL, fileTypeForLog, db.EventSkipDownload, urlToSourceMap[currentURL], "", "Already downloaded", "", nil)
					shouldSkip = true
					absPath, err := filepath.Abs(filepath.Join(cfg.InputDir, filepath.Base(currentURL)))
					if err == nil {
						if _, statErr := os.Stat(absPath); statErr == nil {
							l.Info("Queueing previously downloaded zip for processing check.", slog.String("path", absPath))
							select {
							case processingJobsChan <- absPath:
							case <-ctx.Done():
								break downloadLoop
							}
						} else {
							l.Warn("Previously downloaded file missing locally, cannot queue.", "path", absPath, "error", statErr)
						}
					} else {
						l.Warn("Cannot get absolute path for skipped download, cannot queue.", "error", err)
					}
				}
			}
			if shouldSkip {
				skippedDownloadCount++
				continue
			}
			// --- End Skip Logic ---

			// --- Perform Download ---
			l.Info("Starting download.")
			startTime := time.Now()
			db.LogFileEvent(ctx, dbConnPool, currentURL, fileTypeForLog, db.EventDownloadStart, urlToSourceMap[currentURL], "", "", "", nil)
			req, err := http.NewRequestWithContext(ctx, "GET", currentURL, nil)
			if err != nil {
				l.Error("Failed create request.", "error", err)
				finalErr = errors.Join(finalErr, err)
				downloadErrorCount++
				db.LogFileEvent(ctx, dbConnPool, currentURL, fileTypeForLog, db.EventError, urlToSourceMap[currentURL], "", fmt.Sprintf("create request failed: %v", err), "", nil)
				continue
			}
			req.Header.Set("User-Agent", downloader.GetRandomUserAgent())
			req.Header.Set("Accept", "*/*")
			var downloadedData []byte
			var downloadErr error
			progressCallback := func(dlBytes int64, totalBytes int64) {
				if totalBytes > 0 {
					l.Info("Download progress", slog.Int64("dl_mb", dlBytes>>20), slog.Int64("tot_mb", totalBytes>>20), slog.Float64("pct", float64(dlBytes)*100.0/float64(totalBytes)))
				} else {
					l.Info("Download progress", slog.Int64("dl_mb", dlBytes>>20))
				}
			}
			downloadedData, downloadErr = util.DownloadFileWithProgress(client, req, progressCallback)
			downloadDuration := time.Since(startTime)
			if downloadErr != nil {
				if errors.Is(downloadErr, context.Canceled) || errors.Is(downloadErr, context.DeadlineExceeded) {
					l.Warn("Download cancelled.", "error", downloadErr)
					finalErr = errors.Join(finalErr, downloadErr)
					break downloadLoop
				}
				l.Error("Download failed.", "error", downloadErr)
				finalErr = errors.Join(finalErr, downloadErr)
				downloadErrorCount++
				db.LogFileEvent(ctx, dbConnPool, currentURL, fileTypeForLog, db.EventError, urlToSourceMap[currentURL], "", fmt.Sprintf("download failed: %v", downloadErr), "", &downloadDuration)
				continue
			}
			db.LogFileEvent(ctx, dbConnPool, currentURL, fileTypeForLog, db.EventDownloadEnd, urlToSourceMap[currentURL], "", "", "", &downloadDuration)
			l.Info("Download complete.", slog.Int("bytes", len(downloadedData)))
			// --- End Download ---

			// --- Handle Downloaded Data ---
			if isOuterArchive {
				l.Info("Processing inner files within outer archive.")
				innerExtracted, innerSkipped, extractErr := processInnerArchiveFiles(ctx, cfg, dbConnPool, l, currentURL, downloadedData, forceDownload, processingJobsChan)
				processDuration := time.Since(startTime)
				extractedCount += innerExtracted
				skippedExtractCount += innerSkipped
				if extractErr != nil {
					if errors.Is(extractErr, context.Canceled) || errors.Is(extractErr, context.DeadlineExceeded) {
						l.Warn("Inner archive processing cancelled.")
						finalErr = errors.Join(finalErr, extractErr)
						break downloadLoop
					}
					l.Error("Errors processing inner files.", "error", extractErr)
					db.LogFileEvent(ctx, dbConnPool, currentURL, db.FileTypeOuterArchive, db.EventError, "", "", fmt.Sprintf("inner processing error: %v", extractErr), "", &processDuration)
					finalErr = errors.Join(finalErr, extractErr)
				} else {
					l.Info("Successfully processed inner files (extracted/skipped/queued).")
					db.LogFileEvent(ctx, dbConnPool, currentURL, db.FileTypeOuterArchive, db.EventProcessEnd, "", "", fmt.Sprintf("Inner files extracted=%d, skipped=%d", innerExtracted, innerSkipped), "", &processDuration)
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
					db.LogFileEvent(ctx, dbConnPool, currentURL, db.FileTypeZip, db.EventError, urlToSourceMap[currentURL], absOutputZipPath, saveErr.Error(), "", &saveDuration)
					logger.Error("Failed saving zip.", "error", saveErr)
					finalErr = errors.Join(finalErr, saveErr)
					downloadErrorCount++
					continue
				}
				// Log save event? Maybe just rely on download end + path being queued.
				// db.LogFileEvent(ctx, dbConnPool, currentURL, db.FileTypeZip, "save_end", urlToSourceMap[currentURL], absOutputZipPath, "", "", &saveDuration)
				l.Info("Successfully saved zip.", slog.String("path", absOutputZipPath))
				select {
				case processingJobsChan <- absOutputZipPath:
					l.Debug("Sent path to processor channel.", slog.String("path", absOutputZipPath))
				case <-ctx.Done():
					logger.Warn("Cancelled while sending downloaded path.")
					finalErr = errors.Join(finalErr, ctx.Err())
					break downloadLoop
				}
			}
			// --- End Handle Downloaded Data ---
		} // End download loop
		logger.Info("Discovery/Download/Queueing loop finished.", slog.Int("urls_processed", processedURLCount), slog.Int("downloads_skipped", skippedDownloadCount), slog.Int("download_errors", downloadErrorCount), slog.Int("inner_extracted", extractedCount), slog.Int("inner_skipped", skippedExtractCount))
	}() // End discovery goroutine

	// --- Phase 6: Wait for Completion or Cancellation ---
	select {
	case <-ctx.Done(): // Wait for context cancellation (e.g., Ctrl+C)
		logger.Warn("Shutdown signal received or context cancelled externally. Waiting for components to finish...")
		// Context is already cancelled, workers and discovery should stop gracefully
	case <-discoveryDone: // Wait for discovery/feeding goroutine to finish normally
		logger.Info("Discovery and feeding goroutine finished normally.")
		// pathsChan is now closed by the discovery goroutine's defer
	}

	// --- Phase 7: Shutdown and Wait ---
	// Defer function handles waiting for workers and logging final status.

	// Return the aggregated error state captured by the defer function.
	// The defer function modifies `finalErr` before this return happens.
	return finalErr
}

// combineDiscoveredURLs helper function
func combineDiscoveredURLs(logger *slog.Logger, maps ...map[string]string) ([]string, map[string]string, map[string]bool) {
	allDiscoveredURLs := make([]string, 0)
	urlToSourceMap := make(map[string]string)
	urlIsArchive := make(map[string]bool)
	isArchiveFeed := false
	for i, urlMap := range maps {
		if i > 0 {
			isArchiveFeed = true
		}
		for url, source := range urlMap {
			if _, exists := urlToSourceMap[url]; !exists {
				allDiscoveredURLs = append(allDiscoveredURLs, url)
				urlToSourceMap[url] = source
				urlIsArchive[url] = isArchiveFeed
			} else if isArchiveFeed {
				logger.Warn("URL discovered in both current and archive feeds, treating as current.", "url", url)
			}
		}
	}
	sort.Strings(allDiscoveredURLs)
	return allDiscoveredURLs, urlToSourceMap, urlIsArchive
}

// processInnerArchiveFiles handles extracting, saving, and queueing needed inner zip files concurrently.
// Returns counts of extracted/skipped files and any aggregated error.
func processInnerArchiveFiles(
	ctx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger,
	outerArchiveURL string, outerZipData []byte, forceDownload bool,
	pathsChan chan<- string, // Send extracted paths here
) (extractedCount int, skippedCount int, err error) {

	l := logger.With(slog.String("outer_archive_url", outerArchiveURL))
	l.Debug("Opening outer archive from memory.")
	zipReader, err := zip.NewReader(bytes.NewReader(outerZipData), int64(len(outerZipData)))
	if err != nil {
		l.Error("Failed create zip reader.", "error", err)
		return 0, 0, fmt.Errorf("zip.NewReader %s: %w", outerArchiveURL, err)
	}

	innerZipFiles := []*zip.File{}
	innerZipIdentifiers := []string{}
	for _, f := range zipReader.File {
		select {
		case <-ctx.Done():
			l.Warn("Cancelled inner scan.")
			return extractedCount, skippedCount, ctx.Err()
		default:
		}
		if !f.FileInfo().IsDir() && strings.EqualFold(filepath.Ext(f.Name), ".zip") {
			innerZipFiles = append(innerZipFiles, f)
			innerZipIdentifiers = append(innerZipIdentifiers, filepath.Base(f.Name))
		}
	}
	if len(innerZipFiles) == 0 {
		l.Info("No inner zip files found.")
		return 0, 0, nil
	}
	l.Info("Found inner zip files.", slog.Int("count", len(innerZipFiles)))

	l.Debug("Checking DB status for inner zip files...")
	innerDownloadedMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConnPool, innerZipIdentifiers, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		l.Error("DB error checking inner status, proceeding cautiously.", "error", dbErr)
		innerDownloadedMap = make(map[string]bool)
		err = errors.Join(err, dbErr)
	}

	var innerErrors sync.Map
	var wg sync.WaitGroup
	// Limit concurrency for extraction
	concurrencyLimit := runtime.GOMAXPROCS(0) / 2
	if concurrencyLimit < 1 {
		concurrencyLimit = 1
	}
	if concurrencyLimit > 8 {
		concurrencyLimit = 8
	} // Cap concurrency
	innerSem := semaphore.NewWeighted(int64(concurrencyLimit))
	l.Debug("Starting concurrent extraction/save.", slog.Int("concurrency", concurrencyLimit))

	innerLoopCtx, cancelInnerLoop := context.WithCancel(ctx)
	defer cancelInnerLoop()

	for _, fPtr := range innerZipFiles {
		f := fPtr
		select {
		case <-innerLoopCtx.Done():
			l.Warn("Cancelled before processing next inner file.")
			err = errors.Join(err, innerLoopCtx.Err())
			break
		default:
		}

		innerZipName := filepath.Base(f.Name)
		innerLogger := l.With(slog.String("inner_zip", innerZipName))
		outputZipPath := filepath.Join(cfg.InputDir, innerZipName)
		absOutputZipPath, absErr := filepath.Abs(outputZipPath)
		if absErr != nil {
			innerLogger.Error("Cannot get absolute path, skipping inner zip.", "error", absErr)
			innerErrors.Store(innerZipName, absErr)
			continue
		}

		if _, completed := innerDownloadedMap[innerZipName]; completed && !forceDownload {
			innerLogger.Debug("Skipping inner zip extraction, already downloaded.")
			skippedCount++
			if _, statErr := os.Stat(absOutputZipPath); statErr == nil {
				innerLogger.Info("Queueing previously downloaded inner zip for processing.", slog.String("path", absOutputZipPath))
				select {
				case pathsChan <- absOutputZipPath:
				case <-innerLoopCtx.Done():
					err = errors.Join(err, innerLoopCtx.Err())
					break
				}
			} else {
				innerLogger.Warn("Skipped inner zip missing locally, cannot queue.", "path", absOutputZipPath, "error", statErr)
			}
			continue
		}

		// Acquire semaphore respecting context
		if acquireErr := innerSem.Acquire(innerLoopCtx, 1); acquireErr != nil {
			innerLogger.Warn("Cancelled while waiting for extraction semaphore.", "error", acquireErr)
			err = errors.Join(err, acquireErr)
			break // Exit outer loop
		}

		wg.Add(1)
		go func(file *zip.File, log *slog.Logger, outPathAbs string) {
			defer wg.Done()
			defer innerSem.Release(1)
			select {
			case <-innerLoopCtx.Done():
				return
			default:
			} // Check context again

			log.Info("Extracting and saving inner zip.")
			extractStartTime := time.Now()
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventDownloadStart, outerArchiveURL, outPathAbs, "", "", nil)

			rc, openErr := file.Open()
			if openErr != nil {
				extractErr := fmt.Errorf("open inner %s: %w", innerZipName, openErr)
				log.Error("Failed open inner stream.", "error", extractErr)
				innerErrors.Store(innerZipName, extractErr)
				db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, outPathAbs, extractErr.Error(), "", nil)
				return
			}
			defer rc.Close()

			outFile, createErr := os.Create(outPathAbs)
			if createErr != nil {
				extractErr := fmt.Errorf("create file %s: %w", outPathAbs, createErr)
				log.Error("Failed create output file.", "error", extractErr)
				innerErrors.Store(innerZipName, extractErr)
				db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, outPathAbs, extractErr.Error(), "", nil)
				return
			}
			defer func() {
				if cerr := outFile.Close(); cerr != nil {
					log.Warn("Error closing output file.", "error", cerr)
					innerErrors.Store(innerZipName+"_close", cerr)
				}
			}()

			_, copyErr := io.Copy(outFile, rc)
			extractDuration := time.Since(extractStartTime)
			if copyErr != nil {
				extractErr := fmt.Errorf("copy inner %s: %w", innerZipName, copyErr)
				log.Error("Failed copy inner zip.", "error", extractErr)
				os.Remove(outPathAbs)
				innerErrors.Store(innerZipName, extractErr)
				db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventError, outerArchiveURL, outPathAbs, extractErr.Error(), "", &extractDuration)
				return
			}

			log.Debug("Inner zip extracted.", slog.Duration("duration", extractDuration.Round(time.Millisecond)))
			db.LogFileEvent(ctx, dbConnPool, innerZipName, db.FileTypeZip, db.EventDownloadEnd, outerArchiveURL, outPathAbs, "", "", &extractDuration)

			select {
			case pathsChan <- outPathAbs:
				log.Debug("Sent extracted path to processor channel.", slog.String("path", outPathAbs))
			case <-innerLoopCtx.Done():
				logger.Warn("Cancelled while sending extracted path.")
				innerErrors.Store(innerZipName+"_queue_cancel", innerLoopCtx.Err())
			}
		}(f, innerLogger, absOutputZipPath)
	} // End loop dispatching inner zips

	wg.Wait()
	processedInnerCount := len(innerZipFiles) - skippedCount
	errorCount := 0
	innerErrors.Range(func(key, value interface{}) bool { errorCount++; return true })
	extractedCount = processedInnerCount - errorCount // Approximate extracted count

	l.Info("Finished processing inner files.", slog.Int("attempted_extract", processedInnerCount), slog.Int("errors", errorCount), slog.Int("extracted_approx", extractedCount), slog.Int("skipped_download", skippedCount))

	var aggregatedInnerError error = err
	innerErrors.Range(func(key, value interface{}) bool {
		filename := key.(string)
		errVal := value.(error)
		aggregatedInnerError = errors.Join(aggregatedInnerError, fmt.Errorf("%s: %w", filename, errVal))
		return true
	})

	return extractedCount, skippedCount, aggregatedInnerError
}
