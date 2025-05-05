package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http" // Import http
	"sync"

	"github.com/brensch/nemparquet/internal/config"
	// db package needed if DownloadArchive logs directly, otherwise remove
)

// downloadArchivesAndDispatch loops through archive URLs, downloads them,
// and dispatches extraction jobs to the provided channel.
// It stops early if the context is cancelled.
func downloadArchivesAndDispatch(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB,
	logger *slog.Logger,
	client *http.Client,
	archiveURLs []string,
	completedOuterArchives map[string]bool,
	extractJobsChan chan<- extractionJob,
	extractWg *sync.WaitGroup,
	processingErrorsMu *sync.Mutex,
	processingErrors *[]error,
	forceDownload bool,
	forceProcess bool,
) (int, int, int) { // Returns dispatched count, error count, skipped count

	logger.Info("Starting archive download and dispatching extraction jobs.", slog.Int("url_count", len(archiveURLs)))
	totalExtractionDispatched := 0
	archiveDownloadErrors := 0
	archiveSkippedCount := 0

	for i, url := range archiveURLs {
		logEntry := logger.With(slog.String("archive_url", url), slog.Int("index", i+1), slog.Int("total_archive_urls", len(archiveURLs)))

		// Check for cancellation before processing each URL
		select {
		case <-ctx.Done():
			logEntry.Warn("Context cancelled during archive download loop.", "error", ctx.Err())
			// Return current counts, the cancellation error will be handled by the caller
			return totalExtractionDispatched, archiveDownloadErrors, archiveSkippedCount
		default:
		}

		// Skip Check
		if completed, found := completedOuterArchives[url]; found && completed && !forceProcess && !forceDownload {
			logEntry.Info("Skipping outer archive, already marked as completed in DB.")
			archiveSkippedCount++
			continue
		}

		// Download
		logEntry.Info("Attempting archive download.")
		archiveData, downloadErr := DownloadArchive(ctx, cfg, dbConnPool, logEntry, client, url) // Assumes DownloadArchive exists in this package

		if downloadErr != nil {
			// Check if error is due to context cancellation
			if errors.Is(downloadErr, context.Canceled) || errors.Is(downloadErr, context.DeadlineExceeded) {
				logEntry.Warn("Archive download cancelled.", "error", downloadErr)
				// Return current counts, cancellation error handled by caller
				return totalExtractionDispatched, archiveDownloadErrors, archiveSkippedCount
			}
			// Record other download errors
			processingErrorsMu.Lock()
			*processingErrors = append(*processingErrors, fmt.Errorf("download archive %s: %w", url, downloadErr))
			processingErrorsMu.Unlock()
			archiveDownloadErrors++
			continue // Continue to next archive URL
		}

		// Dispatch Extraction Job
		logEntry.Info("Archive download successful, dispatching job to extraction workers.")
		totalExtractionDispatched++
		extractWg.Add(1) // Increment WaitGroup *before* sending job
		select {
		case extractJobsChan <- extractionJob{archiveURL: url, data: archiveData, logEntry: logEntry}:
			// Job sent successfully
		case <-ctx.Done():
			logEntry.Warn("Context cancelled while dispatching extraction job.", "error", ctx.Err())
			extractWg.Done() // Decrement because job wasn't processed
			// Return current counts, cancellation error handled by caller
			return totalExtractionDispatched, archiveDownloadErrors, archiveSkippedCount
		}
	} // End archive download loop

	logger.Info("Archive download loop finished.")
	return totalExtractionDispatched, archiveDownloadErrors, archiveSkippedCount
}
