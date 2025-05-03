package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath" // Import needed for error message formatting
	"sync"

	// "time" // No longer needed directly here

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/downloader"
	"github.com/brensch/nemparquet/internal/processor"
)

// RunCombinedWorkflow orchestrates the download and process sequence.
func RunCombinedWorkflow(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, forceDownload, forceProcess bool) error {
	logger.Info("Starting combined workflow...")

	// --- Phase 1: Discover All Potential Zip URLs ---
	logger.Info("Phase 1: Discovering all potential ZIP URLs...")
	allDiscoveredURLs, discoveryErr := downloader.DiscoverZipURLs(ctx, cfg, logger)
	if discoveryErr != nil {
		// Log full error but potentially return a cleaner message if needed
		logger.Error("Failed during discovery phase.", "error", discoveryErr)
	}
	// Check context after discovery even if there were non-fatal errors
	if ctx.Err() != nil {
		logger.Warn("Workflow cancelled after discovery phase.")
		return errors.Join(discoveryErr, ctx.Err()) // Combine discovery errors with context error
	}
	if len(allDiscoveredURLs) == 0 {
		logger.Info("No zip URLs discovered. Workflow finished.")
		return discoveryErr // Return discovery errors if any
	}
	logger.Info("Discovery complete.", slog.Int("total_unique_zips", len(allDiscoveredURLs)))

	// --- Phase 2: Determine Work Items from DB ---
	logger.Info("Phase 2: Determining work items from database...")
	urlsToDownload, initialPathsToProcess, err := db.GetWorkItems(ctx, dbConn, allDiscoveredURLs)
	if err != nil {
		// Combine potential discovery errors with this DB error
		return errors.Join(discoveryErr, fmt.Errorf("failed to get work items from DB: %w", err))
	}
	// Check context after DB query
	if ctx.Err() != nil {
		logger.Warn("Workflow cancelled after determining work items.")
		return errors.Join(discoveryErr, ctx.Err())
	}
	logger.Info("Work items determined.",
		slog.Int("urls_to_download", len(urlsToDownload)),
		slog.Int("paths_to_process_initially", len(initialPathsToProcess)),
	)

	// --- Apply force flags ---
	if forceDownload {
		logger.Warn("Force download enabled: All discovered URLs will be targeted for download.")
		urlsToDownload = allDiscoveredURLs // Overwrite with all discovered URLs
		initialPathsToProcess = []string{} // If re-downloading, don't process potentially old paths
		logger.Info("Work items updated for force download.",
			slog.Int("urls_to_download", len(urlsToDownload)),
			slog.Int("paths_to_process_initially", len(initialPathsToProcess)),
		)
	}
	if forceProcess {
		if !forceDownload {
			logger.Warn("Force process enabled: All previously downloaded zips (found via DB) will be added to processing queue.")
			// Note: Current GetWorkItems logic correctly identifies these when forceProcess is handled by the worker.
			// We just need to make sure the worker respects the forceProcess flag.
			logger.Warn("Note: force-process without force-download currently forces initially identified items and newly downloaded items.")
		} else {
			logger.Warn("Force download & process enabled: All discovered items will be downloaded and then processed.")
		}
		// The main effect is that the processor workers won't skip based on DB state check later.
	}

	// Early exit if nothing to do after filtering/forcing
	if len(urlsToDownload) == 0 && len(initialPathsToProcess) == 0 {
		logger.Info("No files need downloading or processing. Workflow finished.")
		return discoveryErr // Return discovery errors if any
	}

	// --- Phase 3: Setup Processing Workers and Download Channel ---
	// Buffered channel to allow downloader to run slightly ahead of processors
	downloadedPathsChan := make(chan string, cfg.NumWorkers*2)
	var processorWg sync.WaitGroup
	var processorErrors sync.Map // Collect errors from processor workers

	logger.Info("Phase 3: Starting processor workers.", slog.Int("workers", cfg.NumWorkers))
	processor.StartProcessorWorkers(
		ctx, cfg, dbConn, logger,
		cfg.NumWorkers, downloadedPathsChan, // Pass the channel
		&processorWg, &processorErrors, // Pass WG and error map
		forceProcess, // Pass force flag to workers
	)

	// --- Phase 4: Feed Initial Processing Jobs ---
	logger.Info("Phase 4: Queueing initially identified paths for processing.", slog.Int("count", len(initialPathsToProcess)))
	initialJobsSent := 0
	// Use a separate context for this loop to allow breaking early if main context cancels
	queueLoopCtx, cancelQueueLoop := context.WithCancel(ctx)
	queueDone := make(chan struct{}) // Signal when queuing is done

	go func() {
		defer close(queueDone)  // Signal completion
		defer cancelQueueLoop() // Ensure context is cancelled on natural exit
		for _, path := range initialPathsToProcess {
			select {
			case <-queueLoopCtx.Done(): // Check if main context cancelled
				logger.Warn("Initial queuing cancelled.")
				return
			case downloadedPathsChan <- path:
				initialJobsSent++
				// logger.Debug("Initial job queued.", slog.String("path", path)) // Optional: verbose log
			}
		}
		logger.Debug("Finished queuing all initial paths naturally.")
	}()

	// --- Phase 5: Start Sequential Download (feeds paths to channel) ---
	var downloaderWg sync.WaitGroup // Wait for the downloader goroutine
	var downloaderErr error         // Error from the downloader goroutine

	// Only start downloader if there are URLs to download
	if len(urlsToDownload) > 0 {
		logger.Info("Phase 5: Starting sequential download process.", slog.Int("files_to_download", len(urlsToDownload)))
		downloaderWg.Add(1)
		go func() {
			defer downloaderWg.Done()
			// Close channel only if downloader runs and finishes
			defer close(downloadedPathsChan)
			downloaderErr = downloader.RunSequentialDownloads(
				ctx, cfg, dbConn, logger,
				urlsToDownload, downloadedPathsChan,
			)
			logger.Info("Downloader goroutine finished.")
		}()
	} else {
		logger.Info("Phase 5: Skipping downloads (no files need downloading).")
		// Wait for initial queuing to finish before closing channel
		<-queueDone
		// Close the channel immediately if no downloader is started,
		// allowing processors to finish with initial jobs.
		close(downloadedPathsChan)
	}

	// --- Phase 6: Wait for Completion ---
	logger.Info("Phase 6: Waiting for downloader (if started) and processor workers to complete...")

	// Wait for downloader (if it was started)
	if len(urlsToDownload) > 0 {
		downloaderWg.Wait()
		if downloaderErr != nil {
			logger.Error("Downloader finished with error.", "error", downloaderErr)
			// Don't return yet, let processors finish with remaining items
		} else {
			logger.Info("Downloader completed.")
		}
	}

	// Wait for all processor workers to finish
	// They will exit naturally when downloadedPathsChan is closed
	processorWg.Wait()
	logger.Info("All processor workers finished.")

	// Consolidate processor errors
	var combinedErr error = errors.Join(discoveryErr, downloaderErr) // Start with errors from previous phases
	processorErrorCount := 0
	processorErrors.Range(func(key, value interface{}) bool {
		path := key.(string)
		err := value.(error)
		combinedErr = errors.Join(combinedErr, fmt.Errorf("process %s: %w", filepath.Base(path), err))
		processorErrorCount++
		return true
	})

	if processorErrorCount > 0 {
		logger.Warn("Processing finished with errors.", slog.Int("error_count", processorErrorCount))
	} else if combinedErr == nil { // Only log success if no prior errors
		logger.Info("Processing finished successfully.")
	}

	logger.Info("Combined workflow finished.")
	return combinedErr
}
