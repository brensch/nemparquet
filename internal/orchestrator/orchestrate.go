package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"
	// No longer needs semaphore
)

// extractionJob defined previously (can be kept or removed if not used elsewhere)
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
// Uses dedicated workers for schema creation, data writing, extraction, and processing.
func RunCombinedWorkflow(appCtx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting main orchestration workflow (full pipeline)...")

	ctx, stop := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := util.DefaultHTTPClient()

	// --- Phase 0: Get Completed Archives/Zips from DB ---
	// Get completed *outer* archives for skipping download/extraction
	completedOuterArchives := make(map[string]bool)
	var initialDbErr error
	if !forceProcess && !forceDownload { // Skip DB check if forcing either download or process
		completedOuterArchives, initialDbErr = db.GetCompletedArchiveURLs(ctx, dbConnPool, logger) // Uses db package func
		if initialDbErr != nil {
			logger.Error("Failed to get completed outer archives from DB, proceeding without skip check.", "error", initialDbErr)
		}
	} else {
		logger.Info("Forcing download/process, skipping DB check for completed archives.")
	}
	// Get completed *inner* zips for potentially skipping processing step later (optional optimization)
	// completedInnerZips, dbErrInner := db.GetCompletedInnerZips(ctx, dbConnPool, logger) // Requires a new DB function
	// initialDbErr = errors.Join(initialDbErr, dbErrInner)

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
	var processingErrorsMu sync.Mutex // Mutex for the shared error slice
	var processingErrors []error      // Collect errors from all stages

	var extractWg sync.WaitGroup
	var processWg sync.WaitGroup
	var writerWg sync.WaitGroup  // For DuckDB writer
	var creatorWg sync.WaitGroup // For Schema creator

	// Determine worker counts
	numExtractWorkers := runtime.NumCPU()
	numProcessWorkers := runtime.NumCPU() // Can be tuned separately
	if numExtractWorkers < 1 {
		numExtractWorkers = 1
	}
	if numProcessWorkers < 1 {
		numProcessWorkers = 1
	}
	// Add config options? cfg.NumExtractWorkers, cfg.NumProcessWorkers

	// Channels
	// Increase buffer size? Depends on typical number of inner zips per outer archive.
	extractJobsChan := make(chan extractionJob, numExtractWorkers*2)      // Jobs for extraction workers
	processingJobsChan := make(chan processingJob, numProcessWorkers*2)   // Jobs for processing workers
	writeDataChan := make(chan WriteOperation, numProcessWorkers*10)      // Data rows for DuckDB writer (buffer size depends on row rate)
	schemaCreateChan := make(chan SchemaCreateRequest, numProcessWorkers) // Schema creation requests

	logger.Info("Initializing workers.",
		slog.Int("extract_workers", numExtractWorkers),
		slog.Int("process_workers", numProcessWorkers),
	)

	// Start DuckDB Writer Goroutine (Single)
	writerWg.Add(1)
	go runDuckDBWriter(ctx, cfg.DbPath, logger.With(slog.String("component", "writer")), writeDataChan, &writerWg)

	// Start Schema Creator Goroutine (Single)
	creatorWg.Add(1)
	go runSchemaCreator(ctx, cfg.DbPath, logger.With(slog.String("component", "schema_creator")), schemaCreateChan, &creatorWg)

	// Start Extraction Workers
	for i := 0; i < numExtractWorkers; i++ {
		// Pass processingJobsChan to the extraction worker
		go extractionWorker(ctx, cfg, dbConnPool, logger, &extractWg, &processingErrorsMu, &processingErrors, processingJobsChan, extractJobsChan, i)
	}

	// Start Processing Workers
	for i := 0; i < numProcessWorkers; i++ {
		processWg.Add(1)
		// Pass schemaCreateChan and writeDataChan to the processing worker
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
			// No need to append error here, final check will catch ctx.Err()
			goto cleanup // Use goto for cleanup phase
		default:
		}

		// Skip Check (Outer Archive)
		if completed, found := completedOuterArchives[url]; found && completed && !forceProcess && !forceDownload {
			logEntry.Info("Skipping outer archive, already marked as completed in DB.")
			totalSkippedCount++
			continue
		}

		// Download Outer Archive
		totalDownloadAttempts++
		logEntry.Info("Attempting download.")
		archiveData, downloadErr := DownloadArchive(ctx, cfg, dbConnPool, logEntry, client, url)

		if downloadErr != nil {
			processingErrorsMu.Lock()
			processingErrors = append(processingErrors, fmt.Errorf("download %s: %w", url, downloadErr))
			processingErrorsMu.Unlock()
			totalDownloadErrors++
			continue // Continue to next URL
		}

		// Dispatch Extraction Job
		logEntry.Info("Download successful, dispatching job to extraction workers.")
		totalExtractionDispatched++
		extractWg.Add(1) // Increment WaitGroup *before* sending job
		select {
		case extractJobsChan <- extractionJob{archiveURL: url, data: archiveData, logEntry: logEntry}:
			// Job sent
		case <-ctx.Done():
			logEntry.Warn("Context cancelled while dispatching extraction job.", "error", ctx.Err())
			extractWg.Done() // Decrement because job wasn't processed
			// No need to append error here
			goto cleanup // Use goto for cleanup phase
		}
	} // End download loop

cleanup:
	// --- Shutdown Sequence ---
	logger.Info("Download loop finished. Closing extraction job channel.")
	close(extractJobsChan) // Signal extraction workers: no more downloads

	logger.Info("Waiting for extraction workers to finish...")
	extractWg.Wait() // Wait for all extractions to complete (and potentially dispatch processing jobs)
	logger.Info("Extraction workers finished. Closing processing job channel.")
	close(processingJobsChan) // Signal processing workers: no more zip files to process

	logger.Info("Waiting for processing workers to finish...")
	processWg.Wait() // Wait for all processing jobs to complete
	logger.Info("Processing workers finished. Closing data write channel.")
	close(writeDataChan) // Signal DuckDB writer: no more data rows

	logger.Info("Waiting for DuckDB writer to finish...")
	writerWg.Wait() // Wait for writer to flush and exit
	logger.Info("DuckDB writer finished. Closing schema create channel.")
	close(schemaCreateChan) // Signal Schema creator: no more requests

	logger.Info("Waiting for Schema creator to finish...")
	creatorWg.Wait() // Wait for schema creator to exit
	logger.Info("Schema creator finished.")

	// --- Final Summary ---
	processingErrorsMu.Lock() // Lock to read final error count
	numProcessingErrors := len(processingErrors)
	finalError := errors.Join(processingErrors...)
	processingErrorsMu.Unlock() // Unlock

	// Combine with initial discovery/DB error
	if discoveryErr != nil {
		finalError = errors.Join(discoveryErr, finalError)
	}
	// Add context error if cancellation happened
	if ctx.Err() != nil && !errors.Is(finalError, ctx.Err()) {
		finalError = errors.Join(finalError, ctx.Err())
	}

	// Log detailed summary counts
	logger.Info("Orchestration finished.",
		slog.Int("urls_discovered", len(archiveURLs)),
		slog.Int("urls_skipped_db", totalSkippedCount),
		slog.Int("download_attempts", totalDownloadAttempts),
		slog.Int("download_errors", totalDownloadErrors),
		slog.Int("extractions_dispatched", totalExtractionDispatched),
		// Add counts for processing if needed (e.g., atomic counters in workers)
		slog.Int("total_processing_errors", numProcessingErrors), // Combined errors from all stages
		// slog.Int("total_inner_zips_extracted", len(allExtractedFiles)), // Need to collect this from extract workers if required
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
			if numProcessingErrors > 0 && isCancellation {
				// Check if the only error IS the cancellation error added at the end
				processingErrorsMu.Lock() // Lock needed if accessing processingErrors slice
				if !(len(processingErrors) == 1 && errors.Is(processingErrors[0], ctx.Err())) {
					containsOtherErrors = true
				}
				processingErrorsMu.Unlock()
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

// extractionWorker (Modified): Listens for extraction jobs, performs extraction,
// and sends the *path* of the extracted inner zip to the processingJobsChan.
func extractionWorker(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB,
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
					goto endExtractionJob // Use goto to jump past the rest of the path dispatching for this job
				}
			}
		endExtractionJob: // Label to jump to after cancellation during path dispatch
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
	dbConnPool *sql.DB,
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
