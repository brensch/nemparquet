package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog" // Import http
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/downloader"
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

// RunCombinedWorkflow orchestrates discovery, download, extraction, and processing using predefined schemas.
// Handles fatal schema validation errors by stopping the workflow.
func RunCombinedWorkflow(appCtx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting main orchestration workflow (predefined schemas)...")

	// Use context with cancel for graceful shutdown AND fatal error propagation
	ctx, stopFunc := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	// It's crucial to call stopFunc eventually to release resources associated with NotifyContext
	defer stopFunc()

	client := util.DefaultHTTPClient()

	// --- Phase 0: Get Completed Files from DB ---
	completedOuterArchives := make(map[string]bool)
	completedZips := make(map[string]bool)
	var initialDbErr error
	if !forceProcess {
		if !forceDownload {
			completedOuterArchives, initialDbErr = db.GetCompletedArchiveURLs(ctx, dbConnPool, logger)
			if initialDbErr != nil {
				logger.Error("Failed to get completed outer archives from DB.", "error", initialDbErr)
			}
		} else {
			logger.Info("Force download enabled, skipping DB check for completed outer archives.")
		}
		var zipDbErr error
		completedZips, zipDbErr = db.GetCompletedZipIdentifiers(ctx, dbConnPool, logger)
		if zipDbErr != nil {
			logger.Error("Failed to get completed zip identifiers from DB.", "error", zipDbErr)
		}
		initialDbErr = errors.Join(initialDbErr, zipDbErr)
	} else {
		logger.Info("Force process enabled, skipping DB checks.")
	}
	if ctx.Err() != nil {
		logger.Warn("Context cancelled during initial DB check.", "error", ctx.Err())
		return errors.Join(initialDbErr, ctx.Err())
	}

	// --- Phase 1: Discover URLs ---
	var discoveryErr error
	var archiveURLs []string
	var regularZipMap map[string]string
	var regularZipURLs []string
	var archiveDiscErr error
	archiveURLs, archiveDiscErr = DiscoverArchiveURLs(ctx, cfg, logger) // Assumes this function exists in orchestrator package
	discoveryErr = errors.Join(discoveryErr, archiveDiscErr)
	if ctx.Err() != nil {
		return errors.Join(initialDbErr, discoveryErr, ctx.Err())
	}
	var regularDiscErr error
	regularZipMap, regularDiscErr = downloader.DiscoverZipURLs(ctx, cfg.FeedURLs, logger.With(slog.String("feed_type", "regular")))
	discoveryErr = errors.Join(discoveryErr, regularDiscErr)
	for url := range regularZipMap {
		regularZipURLs = append(regularZipURLs, url)
	}
	sort.Strings(regularZipURLs)
	if ctx.Err() != nil {
		return errors.Join(initialDbErr, discoveryErr, ctx.Err())
	}
	discoveryErr = errors.Join(initialDbErr, discoveryErr)
	if discoveryErr != nil {
		if len(archiveURLs) == 0 && len(regularZipURLs) == 0 {
			logger.Error("Discovery failed critically or found no URLs. Exiting.", "error", discoveryErr)
			return discoveryErr
		}
		logger.Warn("Discovery completed with non-fatal errors. Proceeding.", "error", discoveryErr)
	}
	if len(archiveURLs) == 0 && len(regularZipURLs) == 0 {
		logger.Info("No archive or regular zip URLs discovered or remaining.")
		return discoveryErr
	}

	// --- Setup Worker Pools and Channels ---
	var processingErrorsMu sync.Mutex
	var processingErrors []error
	var extractWg, processWg, writerWg, regularDownloadWg sync.WaitGroup
	// Removed creatorWg

	numExtractWorkers := runtime.NumCPU()
	numProcessWorkers := runtime.NumCPU()
	if numExtractWorkers < 1 {
		numExtractWorkers = 1
	}
	if numProcessWorkers < 1 {
		numProcessWorkers = 1
	}

	extractJobsChan := make(chan extractionJob, numExtractWorkers*2)
	processingJobsChan := make(chan processingJob, numProcessWorkers*2+len(regularZipURLs))
	writeDataChan := make(chan WriteOperation, numProcessWorkers*10)
	// Removed schemaCreateChan
	fatalErrChan := make(chan error, 1) // Channel for workers to report fatal errors

	logger.Info("Initializing workers.",
		slog.Int("extract_workers", numExtractWorkers),
		slog.Int("process_workers", numProcessWorkers),
	)

	// Start Shared Workers (Writer only)
	writerWg.Add(1)
	go runDuckDBWriter(ctx, dbConnPool, logger.With(slog.String("component", "writer")), writeDataChan, &writerWg)
	// Removed Schema Creator Goroutine start

	// Start Extraction Workers
	for i := 0; i < numExtractWorkers; i++ {
		go extractionWorker(ctx, cfg, dbConnPool, logger, &extractWg, &processingErrorsMu, &processingErrors, processingJobsChan, extractJobsChan, completedZips, forceProcess, i)
	}

	// Start Processing Workers (Pass fatalErrChan, remove schemaChan)
	for i := 0; i < numProcessWorkers; i++ {
		processWg.Add(1)
		go processingWorker(ctx, cfg, dbConnPool, logger, &processWg, &processingErrorsMu, &processingErrors /* Removed schemaChan */, writeDataChan, processingJobsChan, fatalErrChan, i)
	}

	// --- Goroutine to listen for fatal errors and trigger shutdown ---
	var fatalError error // Variable to store the fatal error
	shutdownWg := sync.WaitGroup{}
	shutdownWg.Add(1)
	go func() {
		defer shutdownWg.Done()
		select {
		case err, ok := <-fatalErrChan:
			// Check 'ok' in case channel was closed before error sent
			if ok && err != nil {
				logger.Error("Fatal error received from worker, initiating shutdown.", "error", err)
				fatalError = err // Store the error
				stopFunc()       // Trigger context cancellation
			} else {
				logger.Debug("Fatal error listener exiting: channel closed.")
			}
		case <-ctx.Done():
			// Context cancelled normally or by signal, just exit listener
			logger.Debug("Fatal error listener exiting: context cancelled.")
		}
	}()

	// --- Phase 2a: Run Archive Downloads ---
	totalExtractionDispatched, archiveDownloadErrors, archiveSkippedCount := downloadArchivesAndDispatch(
		ctx, cfg, dbConnPool, logger, client, archiveURLs, completedOuterArchives,
		extractJobsChan, &extractWg, &processingErrorsMu, &processingErrors,
		forceDownload, forceProcess,
	)

	// --- Phase 2b: Run Regular Zip Downloads (in parallel goroutine) ---
	regularDownloadWg.Add(1)
	var totalProcessingDispatched, regularDownloadErrors, regularSkippedCount int
	go func() {
		totalProcessingDispatched, regularDownloadErrors, regularSkippedCount = downloadRegularZipsAndDispatch(
			ctx, cfg, dbConnPool, logger, client, regularZipURLs, regularZipMap,
			completedZips, processingJobsChan, &regularDownloadWg,
			&processingErrorsMu, &processingErrors, forceProcess,
		)
	}()

	// --- Shutdown Sequence ---
	logger.Info("Waiting for downloads and extraction dispatch to complete...")
	extractWg.Wait()
	logger.Debug("Extraction workers finished dispatching.")
	regularDownloadWg.Wait()
	logger.Debug("Regular downloads finished dispatching.")
	logger.Info("All dispatching complete. Closing processing job channel.")
	close(processingJobsChan) // Close channel *after* all producers are done

	// Wait for the fatal error listener goroutine to exit.
	// Close the fatalErrChan *after* closing processingJobsChan and waiting for processWg
	// to ensure any potential late fatal error from a processor is caught.
	// However, closing it earlier allows shutdownWg.Wait() to proceed sooner if no error occurred.
	// Let's close it after waiting for processors.

	logger.Info("Waiting for processing workers to finish...")
	processWg.Wait()
	logger.Info("Processing workers finished. Closing fatal error channel.")
	close(fatalErrChan) // Now safe to close fatal chan
	shutdownWg.Wait()   // Wait for listener to exit
	logger.Debug("Fatal error listener goroutine finished.")

	logger.Info("Closing data write channel.")
	close(writeDataChan) // Close write channel after processors are done

	logger.Info("Waiting for DuckDB writer to finish...")
	writerWg.Wait()
	logger.Info("DuckDB writer finished.")
	// Removed waiting for creatorWg

	// --- Final Summary ---
	processingErrorsMu.Lock()
	numProcessingErrors := len(processingErrors)
	finalError := errors.Join(processingErrors...)
	processingErrorsMu.Unlock()
	if discoveryErr != nil {
		finalError = errors.Join(discoveryErr, finalError)
	}
	// Prioritize fatal error if it occurred
	if fatalError != nil {
		finalError = errors.Join(fatalError, finalError)
	} else if ctx.Err() != nil && !errors.Is(finalError, ctx.Err()) {
		finalError = errors.Join(finalError, ctx.Err())
	}

	logger.Info("Orchestration finished.",
		slog.Int("archive_urls_discovered", len(archiveURLs)), slog.Int("regular_urls_discovered", len(regularZipURLs)),
		slog.Int("archives_skipped_db", archiveSkippedCount), slog.Int("regular_zips_skipped_db", regularSkippedCount),
		slog.Int("archive_download_errors", archiveDownloadErrors), slog.Int("regular_download_errors", regularDownloadErrors),
		slog.Int("extractions_dispatched", totalExtractionDispatched), slog.Int("processing_jobs_dispatched", totalProcessingDispatched),
		slog.Int("total_processing_errors", numProcessingErrors),
	)

	if finalError != nil {
		if errors.Is(finalError, ErrSchemaValidationFailed) {
			logger.Error("Workflow stopped due to fatal schema validation error.", "error", finalError)
		} else {
			isCancellation := errors.Is(finalError, context.Canceled) || errors.Is(finalError, context.DeadlineExceeded)
			containsOtherErrors := false
			if !isCancellation {
				containsOtherErrors = true
			} else {
				var tempErrs []error
				if jErr, ok := finalError.(interface{ Unwrap() []error }); ok {
					tempErrs = jErr.Unwrap()
				} else {
					unwrapped := errors.Unwrap(finalError)
					if unwrapped != nil {
						tempErrs = []error{unwrapped}
					}
				}
				for _, e := range tempErrs {
					if !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
						containsOtherErrors = true
						break
					}
				}
				processingErrorsMu.Lock()
				if len(processingErrors) > 0 && isCancellation && !containsOtherErrors {
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
		}
		return finalError
	}

	logger.Info("Workflow completed successfully.")
	return nil
}

// extractionWorker (No changes needed)
func extractionWorker(
	ctx context.Context, cfg config.Config, dbConnPool *sql.DB, baseLogger *slog.Logger, wg *sync.WaitGroup, mu *sync.Mutex, processingErrors *[]error, processingJobsChan chan<- processingJob, jobs <-chan extractionJob, completedZips map[string]bool, forceProcess bool, workerID int,
) {
	logger := baseLogger.With(slog.Int("worker_id", workerID), slog.String("component", "extractor"))
	logger.Info("Extraction worker started.")
	for job := range jobs {
		select {
		case <-ctx.Done():
			logger.Warn("Context cancelled before starting extraction job.", "archive_url", job.archiveURL, "error", ctx.Err())
			wg.Done()
			continue
		default:
		}
		workerLog := job.logEntry.With(slog.Int("worker_id", workerID), slog.String("component", "extractor"))
		workerLog.Info("Processing extraction job.")
		extractedPaths, extractErr := ExtractInnerZips(ctx, cfg, dbConnPool, workerLog, job.archiveURL, job.data)
		if extractErr != nil {
			mu.Lock()
			*processingErrors = append(*processingErrors, fmt.Errorf("extract %s: %w", job.archiveURL, extractErr))
			mu.Unlock()
		} else {
			workerLog.Info("Extraction successful, dispatching inner zips for processing.", slog.Int("count", len(extractedPaths)))
		dispatchLoop:
			for _, path := range extractedPaths {
				innerZipName := filepath.Base(path)
				if completed, found := completedZips[innerZipName]; found && completed && !forceProcess {
					workerLog.Info("Skipping processing for inner zip, already completed.", "inner_zip", innerZipName)
					continue
				}
				procJob := processingJob{zipFilePath: path, logEntry: workerLog.With(slog.String("inner_zip_path", innerZipName))}
				select {
				case processingJobsChan <- procJob:
				case <-ctx.Done():
					workerLog.Warn("Context cancelled while dispatching processing job.", "path", path, "error", ctx.Err())
					break dispatchLoop
				}
			}
		}
		wg.Done()
	}
	logger.Info("Extraction worker finished (jobs channel closed).")
}

// processingWorker (Modified): Removed schemaChan parameter.
func processingWorker(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB,
	baseLogger *slog.Logger,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	processingErrors *[]error,
	// Removed schemaChan chan<- SchemaCreateRequest,
	writeChan chan<- WriteOperation,
	jobs <-chan processingJob,
	fatalErrChan chan<- error,
	workerID int,
) {
	defer wg.Done()
	logger := baseLogger.With(slog.Int("worker_id", workerID), slog.String("component", "processor"))
	logger.Info("Processing worker started.")

	for job := range jobs {
		// Check context *before* starting job processing
		// This prevents starting a job if shutdown has already been triggered
		select {
		case <-ctx.Done():
			logger.Warn("Context cancelled before starting processing job.", "zip_file", job.zipFilePath, "error", ctx.Err())
			continue // Skip job if context already cancelled
		default:
		}

		workerLog := job.logEntry
		if workerLog == nil {
			workerLog = logger.With(slog.String("zip_file", filepath.Base(job.zipFilePath)))
		} else {
			workerLog = workerLog.With(slog.Int("worker_id", workerID), slog.String("component", "processor"))
		}

		workerLog.Info("Processing zip file job.")

		// Call processZip without schemaChan
		err := processZip(ctx, cfg, dbConnPool, workerLog, job.zipFilePath /* Removed schemaChan */, writeChan)

		if err != nil {
			// Check if it's the fatal schema error
			if errors.Is(err, ErrSchemaValidationFailed) {
				workerLog.Error("Fatal schema validation error encountered.", "zip_file", job.zipFilePath, "error", err)
				// Send non-blocking to fatal channel
				select {
				case fatalErrChan <- err:
					logger.Info("Sent fatal error notification.")
				default:
					// If channel is full or closed, log but don't block worker
					logger.Warn("Fatal error channel full or closed, unable to send notification.")
				}
				// Add fatal error to regular errors as well for final summary
				mu.Lock()
				*processingErrors = append(*processingErrors, fmt.Errorf("process %s: %w", job.zipFilePath, err))
				mu.Unlock()
				// Worker will exit naturally on next loop iteration due to context cancellation triggered by the listener
			} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				// Log and collect non-fatal, non-cancellation errors
				mu.Lock()
				*processingErrors = append(*processingErrors, fmt.Errorf("process %s: %w", job.zipFilePath, err))
				mu.Unlock()
			}
			// Cancellation errors are handled by the main orchestrator's final check
		}
	} // End range jobs

	logger.Info("Processing worker finished (jobs channel closed).")
}

// --- Ensure helper functions are defined (or imported) ---
// DiscoverArchiveURLs(...)
// DownloadArchive(...)
// ExtractInnerZips(...)
// runDuckDBWriter(...)
// processZip(...) - defined in processor.go
// CreateAllPredefinedSchemas(...) - defined in schema_preload.go
