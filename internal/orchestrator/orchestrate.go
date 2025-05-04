package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"path/filepath" // Import filepath
	"runtime"
	"sync"
	"syscall"

	// Import time
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"
	_ "github.com/marcboeker/go-duckdb" // Ensure driver is registered
)

// extractionJob holds the data needed for an extraction task.
type extractionJob struct {
	archiveURL string
	data       []byte
	logEntry   *slog.Logger
}

// processingJob holds the path to a zip file ready for processing.
type processingJob struct {
	zipFilePath string
	logEntry    *slog.Logger // Logger context specific to this job's origin (optional)
}

// RunCombinedWorkflow orchestrates discovery, download, extraction, and processing.
// Uses dedicated workers for schema creation, data writing, extraction, and processing,
// drawing connections from a shared sql.DB pool.
func RunCombinedWorkflow(appCtx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	// Note: The input dbConnPool is now the primary pool used throughout.
	// We assume it's already initialized and passed correctly.
	// If dbConnPool was *only* for logging before, we might need to initialize
	// the main operational pool here instead. Assuming dbConnPool is the intended operational pool.

	logger.Info("Starting main orchestration workflow (shared pool)...")

	ctx, stop := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := util.DefaultHTTPClient()

	// --- Phase 0: Get Completed Archives/Zips from DB (using the shared pool) ---
	completedOuterArchives := make(map[string]bool)
	var initialDbErr error
	if !forceProcess && !forceDownload {
		completedOuterArchives, initialDbErr = db.GetCompletedArchiveURLs(ctx, dbConnPool, logger)
		if initialDbErr != nil {
			logger.Error("Failed to get completed outer archives from DB, proceeding without skip check.", "error", initialDbErr)
		}
	} else {
		logger.Info("Forcing download/process, skipping DB check for completed archives.")
	}
	if ctx.Err() != nil {
		logger.Warn("Context cancelled during initial DB check.", "error", ctx.Err())
		return errors.Join(initialDbErr, ctx.Err())
	}

	// --- Phase 1: Discover Archive URLs ---
	archiveURLs, discoveryErr := DiscoverArchiveURLs(ctx, cfg, logger)
	discoveryErr = errors.Join(initialDbErr, discoveryErr) // Combine potential DB error
	if discoveryErr != nil {
		if len(archiveURLs) == 0 {
			logger.Error("Archive discovery failed critically or found no URLs. Exiting.", "error", discoveryErr)
			return discoveryErr
		}
		logger.Warn("Archive discovery completed with non-fatal errors. Proceeding.", "error", discoveryErr)
	}
	if ctx.Err() != nil {
		logger.Warn("Context cancelled after discovery phase.", "error", ctx.Err())
		return errors.Join(discoveryErr, ctx.Err())
	}
	if len(archiveURLs) == 0 {
		logger.Info("No archive URLs discovered or remaining after initial checks.")
		return discoveryErr
	}

	// --- Setup Worker Pools and Channels ---
	var processingErrorsMu sync.Mutex
	var processingErrors []error

	var extractWg sync.WaitGroup
	var processWg sync.WaitGroup
	var writerWg sync.WaitGroup
	var creatorWg sync.WaitGroup

	numExtractWorkers := runtime.NumCPU()
	numProcessWorkers := runtime.NumCPU()
	if numExtractWorkers < 1 {
		numExtractWorkers = 1
	}
	if numProcessWorkers < 1 {
		numProcessWorkers = 1
	}

	extractJobsChan := make(chan extractionJob, numExtractWorkers*2)
	processingJobsChan := make(chan processingJob, numProcessWorkers*2)
	writeDataChan := make(chan WriteOperation, numProcessWorkers*10)
	schemaCreateChan := make(chan SchemaCreateRequest, numProcessWorkers)

	logger.Info("Initializing workers.",
		slog.Int("extract_workers", numExtractWorkers),
		slog.Int("process_workers", numProcessWorkers),
	)

	// Start DuckDB Writer Goroutine (passing the shared pool)
	writerWg.Add(1)
	// Pass dbConnPool (the *sql.DB pool)
	go runDuckDBWriter(ctx, dbConnPool, logger.With(slog.String("component", "writer")), writeDataChan, &writerWg)

	// Start Schema Creator Goroutine (passing the shared pool)
	creatorWg.Add(1)
	// Pass dbConnPool (the *sql.DB pool)
	go runSchemaCreator(ctx, dbConnPool, logger.With(slog.String("component", "schema_creator")), schemaCreateChan, &creatorWg)

	// Start Extraction Workers
	for i := 0; i < numExtractWorkers; i++ {
		go extractionWorker(ctx, cfg, dbConnPool, logger, &extractWg, &processingErrorsMu, &processingErrors, processingJobsChan, extractJobsChan, i)
	}

	// Start Processing Workers
	for i := 0; i < numProcessWorkers; i++ {
		processWg.Add(1)
		go processingWorker(ctx, cfg, dbConnPool, logger, &processWg, &processingErrorsMu, &processingErrors, schemaCreateChan, writeDataChan, processingJobsChan, i)
	}

	// --- Phase 2: Download Outer Archives and Dispatch Extraction Jobs ---
	logger.Info("Starting sequential download and dispatching extraction jobs.", slog.Int("url_count", len(archiveURLs)))

	totalDownloadAttempts := 0
	totalSkippedCount := 0
	totalDownloadErrors := 0
	totalExtractionDispatched := 0

	// Download Loop
	for i, url := range archiveURLs {
		logEntry := logger.With(slog.String("archive_url", url), slog.Int("index", i+1), slog.Int("total_urls", len(archiveURLs)))

		select {
		case <-ctx.Done():
			logEntry.Warn("Orchestration cancelled during download loop.", "error", ctx.Err())
			goto cleanup
		default:
		}

		// Skip Check
		if completed, found := completedOuterArchives[url]; found && completed && !forceProcess && !forceDownload {
			logEntry.Info("Skipping outer archive, already marked as completed in DB.")
			totalSkippedCount++
			continue
		}

		// Download
		totalDownloadAttempts++
		logEntry.Info("Attempting download.")
		archiveData, downloadErr := DownloadArchive(ctx, cfg, dbConnPool, logEntry, client, url)

		if downloadErr != nil {
			processingErrorsMu.Lock()
			processingErrors = append(processingErrors, fmt.Errorf("download %s: %w", url, downloadErr))
			processingErrorsMu.Unlock()
			totalDownloadErrors++
			continue
		}

		// Dispatch Extraction Job
		logEntry.Info("Download successful, dispatching job to extraction workers.")
		totalExtractionDispatched++
		extractWg.Add(1)
		select {
		case extractJobsChan <- extractionJob{archiveURL: url, data: archiveData, logEntry: logEntry}:
			// Job sent
		case <-ctx.Done():
			logEntry.Warn("Context cancelled while dispatching extraction job.", "error", ctx.Err())
			extractWg.Done()
			goto cleanup
		}
	} // End download loop

cleanup:
	// --- Shutdown Sequence ---
	logger.Info("Download loop finished. Closing extraction job channel.")
	close(extractJobsChan)

	logger.Info("Waiting for extraction workers to finish...")
	extractWg.Wait()
	logger.Info("Extraction workers finished. Closing processing job channel.")
	close(processingJobsChan)

	logger.Info("Waiting for processing workers to finish...")
	processWg.Wait()
	logger.Info("Processing workers finished. Closing data write channel.")
	close(writeDataChan)

	logger.Info("Waiting for DuckDB writer to finish...")
	writerWg.Wait()
	logger.Info("DuckDB writer finished. Closing schema create channel.")
	close(schemaCreateChan)

	logger.Info("Waiting for Schema creator to finish...")
	creatorWg.Wait()
	logger.Info("Schema creator finished.")

	// --- Final Summary ---
	processingErrorsMu.Lock()
	numProcessingErrors := len(processingErrors)
	finalError := errors.Join(processingErrors...)
	processingErrorsMu.Unlock()

	if discoveryErr != nil {
		finalError = errors.Join(discoveryErr, finalError)
	}
	if ctx.Err() != nil && !errors.Is(finalError, ctx.Err()) {
		finalError = errors.Join(finalError, ctx.Err())
	}

	logger.Info("Orchestration finished.",
		slog.Int("urls_discovered", len(archiveURLs)),
		slog.Int("urls_skipped_db", totalSkippedCount),
		slog.Int("download_attempts", totalDownloadAttempts),
		slog.Int("download_errors", totalDownloadErrors),
		slog.Int("extractions_dispatched", totalExtractionDispatched),
		slog.Int("total_processing_errors", numProcessingErrors),
	)

	if finalError != nil {
		// Final error check (same as before)
		isCancellation := errors.Is(finalError, context.Canceled) || errors.Is(finalError, context.DeadlineExceeded)
		containsOtherErrors := false
		if !isCancellation {
			containsOtherErrors = true
		} else {
			unwrapped := errors.Unwrap(finalError)
			if unwrapped != nil && !errors.Is(unwrapped, context.Canceled) && !errors.Is(unwrapped, context.DeadlineExceeded) {
				if finalError.Error() != unwrapped.Error() {
					containsOtherErrors = true
				}
			}
			processingErrorsMu.Lock() // Lock needed if accessing processingErrors slice
			if len(processingErrors) > 0 && isCancellation {
				// Check if the only error IS the cancellation error added at the end
				if !(len(processingErrors) == 1 && errors.Is(processingErrors[0], ctx.Err())) {
					containsOtherErrors = true
				}
			}
			processingErrorsMu.Unlock()
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

// extractionWorker (Modified): Listens for extraction jobs, performs extraction,
// and sends the *path* of the extracted inner zip to the processingJobsChan.
func extractionWorker(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB, // Used for logging within ExtractInnerZips
	baseLogger *slog.Logger,
	wg *sync.WaitGroup,
	mu *sync.Mutex, // Mutex for shared error slice
	processingErrors *[]error, // Pointer to the shared error slice
	processingJobsChan chan<- processingJob, // Channel to send paths for processing
	jobs <-chan extractionJob, // Channel to receive extraction jobs
	workerID int,
) {
	logger := baseLogger.With(slog.Int("worker_id", workerID), slog.String("component", "extractor"))
	logger.Info("Extraction worker started.")

	for job := range jobs {
		select {
		case <-ctx.Done():
			logger.Warn("Context cancelled before starting extraction job.", "archive_url", job.archiveURL, "error", ctx.Err())
			wg.Done() // Must call Done as wg.Add happened before send
			continue
		default:
		}

		workerLog := job.logEntry.With(slog.Int("worker_id", workerID), slog.String("component", "extractor"))
		workerLog.Info("Processing extraction job.")

		// Perform the extraction
		extractedPaths, extractErr := ExtractInnerZips(ctx, cfg, dbConnPool, workerLog, job.archiveURL, job.data)

		if extractErr != nil {
			mu.Lock()
			*processingErrors = append(*processingErrors, fmt.Errorf("extract %s: %w", job.archiveURL, extractErr))
			mu.Unlock()
			// Don't dispatch failed extractions for processing
		} else {
			workerLog.Info("Extraction successful, dispatching inner zips for processing.", slog.Int("count", len(extractedPaths)))
			// Dispatch each successfully extracted file path for processing
		dispatchLoop: // Label needed for breaking out of inner loop on context cancellation
			for _, path := range extractedPaths {
				procJob := processingJob{
					zipFilePath: path,
					logEntry:    workerLog.With(slog.String("inner_zip_path", filepath.Base(path))), // Add inner zip context
				}
				select {
				case processingJobsChan <- procJob:
					// Sent processing job
				case <-ctx.Done():
					workerLog.Warn("Context cancelled while dispatching processing job.", "path", path, "error", ctx.Err())
					// No need to add error here, main loop/final check handles ctx.Err()
					// Need to break out of this inner loop if cancelled
					break dispatchLoop // Break out of the path dispatching loop for this job
				}
			}
			// Removed endExtractionJob label as break dispatchLoop achieves the same
		}

		// Decrement WaitGroup *after* processing (and dispatching) is complete for this *extraction* job
		wg.Done()
	} // End range jobs

	logger.Info("Extraction worker finished (jobs channel closed).")
}

// processingWorker listens on the processingJobsChan and processes individual zip files.
func processingWorker(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB, // Used for logging within processZip
	baseLogger *slog.Logger,
	wg *sync.WaitGroup,
	mu *sync.Mutex, // Mutex for shared error slice
	processingErrors *[]error, // Pointer to the shared error slice
	schemaChan chan<- SchemaCreateRequest, // Channel for schema creation
	writeChan chan<- WriteOperation, // Channel for data writing
	jobs <-chan processingJob, // Channel to receive jobs
	workerID int,
) {
	defer wg.Done() // Decrement WaitGroup when worker exits
	logger := baseLogger.With(slog.Int("worker_id", workerID), slog.String("component", "processor"))
	logger.Info("Processing worker started.")

	for job := range jobs {
		select {
		case <-ctx.Done():
			logger.Warn("Context cancelled before starting processing job.", "zip_file", job.zipFilePath, "error", ctx.Err())
			// Don't add error, final check handles ctx.Err()
			continue // Continue to drain channel? Or return? Let's continue.
		default:
		}

		// Use the logger passed with the job if available, otherwise fallback
		workerLog := job.logEntry
		if workerLog == nil {
			workerLog = logger.With(slog.String("zip_file", filepath.Base(job.zipFilePath)))
		} else {
			workerLog = workerLog.With(slog.Int("worker_id", workerID), slog.String("component", "processor")) // Add worker context
		}

		workerLog.Info("Processing zip file job.")

		// Call the main processing function for the zip file
		// Pass dbConnPool for logging purposes inside processZip
		err := processZip(ctx, cfg, dbConnPool, workerLog, job.zipFilePath, schemaChan, writeChan)

		if err != nil {
			// Log error already happens inside processZip
			mu.Lock()
			*processingErrors = append(*processingErrors, fmt.Errorf("process %s: %w", job.zipFilePath, err))
			mu.Unlock()
		}
		// No wg.Done() here, it's handled by the defer at the start
	} // End range jobs

	logger.Info("Processing worker finished (jobs channel closed).")
}
