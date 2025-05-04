package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt" // Import fmt for error formatting
	"log/slog"
	"os/signal"
	"runtime" // For NumCPU
	"sync"
	"syscall"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"
	"golang.org/x/sync/semaphore" // For limiting concurrency
)

// RunCombinedWorkflow orchestrates the discovery, download, and concurrent extraction of archive files.
// 1. Discovers archive URLs.
// 2. Checks DB for already completed archives.
// 3. Downloads new/incomplete archives sequentially.
// 4. Dispatches extraction tasks to a concurrent worker pool as downloads complete.
// Parameters forceDownload and forceProcess control skipping behavior.
func RunCombinedWorkflow(appCtx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting main orchestration workflow (download + concurrent extract)...")

	// --- Graceful Shutdown Setup ---
	// The main context passed down will handle cancellation in sub-functions.
	ctx, stop := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- Initialize HTTP Client ---
	// Create it once here and pass it down.
	client := util.DefaultHTTPClient()

	// --- Phase 0: Get Completed Archives from DB ---
	completedArchives := make(map[string]bool)
	var initialDbErr error
	if !forceProcess { // Only check DB if not forcing processing
		completedArchives, initialDbErr = db.GetCompletedArchiveURLs(ctx, dbConnPool, logger)
		if initialDbErr != nil {
			logger.Error("Failed to get completed archives from DB, proceeding without skip check.", "error", initialDbErr)
			// Continue, but log the error. Don't overwrite completedArchives map.
		}
	} else {
		logger.Info("Force process enabled, skipping DB check for completed archives.")
	}
	// Handle context cancellation during DB check
	if ctx.Err() != nil {
		logger.Warn("Context cancelled during initial DB check.", "error", ctx.Err())
		return errors.Join(initialDbErr, ctx.Err())
	}

	// --- Phase 1: Discover Archive URLs ---
	archiveURLs, discoveryErr := DiscoverArchiveURLs(ctx, cfg, logger)
	// Combine potential DB error with discovery error
	discoveryErr = errors.Join(initialDbErr, discoveryErr)
	if discoveryErr != nil {
		if len(archiveURLs) == 0 {
			logger.Error("Archive discovery failed critically or found no URLs. Exiting.", "error", discoveryErr)
			return discoveryErr
		}
		logger.Warn("Archive discovery completed with non-fatal errors. Proceeding with discovered URLs.", "error", discoveryErr)
	}
	if ctx.Err() != nil {
		logger.Warn("Context cancelled after discovery phase.", "error", ctx.Err())
		return errors.Join(discoveryErr, ctx.Err())
	}
	if len(archiveURLs) == 0 {
		logger.Info("No archive URLs discovered or remaining after initial checks.")
		return discoveryErr // Return discovery errors if any
	}

	// --- Phase 2: Download Sequentially, Dispatch Extraction Concurrently ---
	logger.Info("Starting sequential download and concurrent extraction.", slog.Int("url_count", len(archiveURLs)))

	var processingErrors []error
	var allExtractedFilesMu sync.Mutex // Mutex to protect the shared slice
	var allExtractedFiles []string     // Collect all successfully extracted file paths

	var extractWg sync.WaitGroup // WaitGroup for extraction goroutines

	// Determine concurrency limit for extraction (e.g., number of CPU cores)
	// Allow overriding via config in the future if needed.
	extractorConcurrency := runtime.NumCPU()
	if extractorConcurrency < 1 {
		extractorConcurrency = 1
	}
	// Consider adding a config option for this limit
	// if cfg.ExtractorConcurrency > 0 { extractorConcurrency = cfg.ExtractorConcurrency }
	logger.Info("Extraction concurrency limit set.", slog.Int("limit", extractorConcurrency))
	// Use a semaphore to limit the number of concurrent extractions
	extractSem := semaphore.NewWeighted(int64(extractorConcurrency))

	totalDownloadAttempts := 0
	totalSkippedCount := 0
	totalDownloadErrors := 0
	totalExtractionDispatched := 0
	totalExtractionErrors := 0 // Track extraction errors separately

downloadLoop: // Label for breaking out of the loop specifically
	for i, url := range archiveURLs {
		logEntry := logger.With(slog.String("archive_url", url), slog.Int("index", i+1), slog.Int("total_urls", len(archiveURLs)))

		// Check for context cancellation before each download attempt
		select {
		case <-ctx.Done():
			logEntry.Warn("Orchestration cancelled during download loop.", "error", ctx.Err())
			processingErrors = append(processingErrors, ctx.Err())
			break downloadLoop // Exit the download loop cleanly
		default:
		}

		// --- Skip Check ---
		// Use forceProcess to override DB check, forceDownload might be used inside DownloadArchive if needed later
		if completed, found := completedArchives[url]; found && completed && !forceProcess {
			logEntry.Info("Skipping archive, already marked as completed in DB.")
			totalSkippedCount++
			continue // Skip to the next URL
		}

		// --- Download ---
		totalDownloadAttempts++
		logEntry.Info("Attempting download.")
		// Pass cfg down in case DownloadArchive needs config values (like ArchiveFeedURLs for source approx)
		archiveData, downloadErr := DownloadArchive(ctx, cfg, dbConnPool, logEntry, client, url)

		if downloadErr != nil {
			// Log error already happens inside DownloadArchive
			// logEntry.Error("Failed to download archive.", "error", downloadErr)
			processingErrors = append(processingErrors, fmt.Errorf("download %s: %w", url, downloadErr))
			totalDownloadErrors++
			// If download fails (e.g., network error, 404), continue to the next URL
			continue
		}

		// --- Dispatch Extraction ---
		logEntry.Info("Download successful, dispatching for extraction.")
		totalExtractionDispatched++

		// Acquire semaphore before launching goroutine to limit concurrency
		if err := extractSem.Acquire(ctx, 1); err != nil {
			// This likely means the context was cancelled while waiting
			logEntry.Error("Failed to acquire extraction semaphore.", "error", err)
			processingErrors = append(processingErrors, fmt.Errorf("acquire semaphore for %s: %w", url, err))
			// If we can't acquire due to cancellation, stop dispatching
			break downloadLoop // Exit the download loop cleanly
		}

		extractWg.Add(1)
		go func(archiveURL string, data []byte, workerLog *slog.Logger) {
			defer extractWg.Done()
			defer extractSem.Release(1) // Release semaphore when done

			// Create a new context for this specific extraction job?
			// jobCtx, cancelJob := context.WithCancel(ctx) // Or just use the main ctx
			// defer cancelJob()

			workerLog.Info("Extraction worker started.")
			// Pass cfg down as ExtractInnerZips needs cfg.InputDir
			extractedPaths, extractErr := ExtractInnerZips(ctx, cfg, dbConnPool, workerLog, archiveURL, data)

			// Protect access to shared slices when recording results/errors
			allExtractedFilesMu.Lock()
			if extractErr != nil {
				// Log error already happens inside ExtractInnerZips
				// workerLog.Error("Extraction worker failed.", "error", extractErr)
				processingErrors = append(processingErrors, fmt.Errorf("extract %s: %w", archiveURL, extractErr))
				totalExtractionErrors++ // Increment specific extraction error count
			} else {
				// Log success already happens inside ExtractInnerZips
				// workerLog.Info("Extraction worker finished successfully.", slog.Int("extracted_count", len(extractedPaths)))
				allExtractedFiles = append(allExtractedFiles, extractedPaths...)
			}
			allExtractedFilesMu.Unlock()

		}(url, archiveData, logEntry) // Pass necessary variables to the goroutine

	} // End downloadLoop

	// --- Wait for Extraction Workers ---
	// Program flow naturally reaches here after the downloadLoop finishes (either normally or via break)
	logger.Info("Download loop finished. Waiting for extraction workers to complete...")
	extractWg.Wait()
	logger.Info("All extraction workers finished.")

	// --- Final Summary ---
	// Combine collected processing errors
	finalError := errors.Join(processingErrors...)
	// Combine with initial discovery/DB error
	if discoveryErr != nil {
		finalError = errors.Join(discoveryErr, finalError)
	}

	// Log detailed summary counts
	logger.Info("Orchestration finished.",
		slog.Int("urls_discovered", len(archiveURLs)),
		slog.Int("urls_skipped_db", totalSkippedCount),
		slog.Int("download_attempts", totalDownloadAttempts),
		slog.Int("download_errors", totalDownloadErrors),
		slog.Int("extractions_dispatched", totalExtractionDispatched),
		slog.Int("extraction_errors", totalExtractionErrors),           // Specific count for extraction failures
		slog.Int("total_inner_zips_extracted", len(allExtractedFiles)), // Note: Accessing len outside lock is safe after WaitGroup finishes
	)

	if finalError != nil {
		// Simplified check for cancellation errors vs other errors
		isCancellation := errors.Is(finalError, context.Canceled) || errors.Is(finalError, context.DeadlineExceeded)
		containsOtherErrors := false

		// Check if the joined error contains something other than cancellation
		// This is a basic check; a more robust solution might involve iterating through joined errors if Go version allows.
		if !isCancellation {
			containsOtherErrors = true
		} else {
			// If the top level IS cancellation, check if there's an underlying non-cancellation error
			unwrapped := errors.Unwrap(finalError)
			if unwrapped != nil && !errors.Is(unwrapped, context.Canceled) && !errors.Is(unwrapped, context.DeadlineExceeded) {
				// Check if the unwrapped error is different from the main error before declaring it "other"
				if finalError.Error() != unwrapped.Error() {
					containsOtherErrors = true
				}
			}
			// Also consider the case where multiple errors were joined, and one might not be cancellation
			if len(processingErrors) > 1 && isCancellation {
				// This is heuristic: if multiple errors occurred and the final one is cancellation,
				// it's likely other errors happened before cancellation.
				containsOtherErrors = true
			}
		}

		if isCancellation && !containsOtherErrors {
			logger.Warn("Workflow cancelled.", "error", finalError)
		} else {
			logger.Error("Workflow completed with errors.", "error", finalError)
		}
		return finalError
	}

	logger.Info("Workflow completed successfully.")
	return nil
}
