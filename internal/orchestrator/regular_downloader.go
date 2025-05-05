package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http" // Import http
	"path/filepath"
	"sync"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/downloader" // Needs downloader package for DownloadSingleZipAndSave
	// db package needed if DownloadSingleZipAndSave logs directly, otherwise remove
)

// downloadRegularZipsAndDispatch loops through regular zip URLs sequentially, downloads them,
// checks completion status, and dispatches processing jobs.
// Runs within its own goroutine managed by the caller via regularDownloadWg.
func downloadRegularZipsAndDispatch(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB,
	logger *slog.Logger,
	client *http.Client,
	regularZipURLs []string,
	regularZipMap map[string]string, // Map: URL -> SourceFeed
	completedZips map[string]bool,
	processingJobsChan chan<- processingJob,
	regularDownloadWg *sync.WaitGroup,
	processingErrorsMu *sync.Mutex,
	processingErrors *[]error,
	forceProcess bool,
) (int, int, int) { // Returns dispatched count, error count, skipped count
	defer regularDownloadWg.Done() // Signal completion when function exits

	logger.Info("Starting regular zip download loop.", slog.Int("url_count", len(regularZipURLs)))
	totalProcessingDispatched := 0
	regularDownloadErrors := 0
	regularSkippedCount := 0

	for i, url := range regularZipURLs {
		logEntry := logger.With(slog.String("regular_zip_url", url), slog.Int("index", i+1), slog.Int("total_regular_urls", len(regularZipURLs)))

		// Check for cancellation before processing each URL
		select {
		case <-ctx.Done():
			logEntry.Warn("Orchestration cancelled during regular zip download loop.", "error", ctx.Err())
			// Return current counts
			return totalProcessingDispatched, regularDownloadErrors, regularSkippedCount
		default:
		}

		// Skip Check
		if completed, found := completedZips[url]; found && completed && !forceProcess {
			logEntry.Info("Skipping regular zip, already marked as completed in DB.")
			regularSkippedCount++
			continue
		}

		// Download
		totalProcessingDispatched++ // Increment attempts here, adjust later if download fails
		logEntry.Info("Attempting regular zip download.")
		sourceFeedURL := regularZipMap[url]
		// Call the function from the actual downloader package
		savedPath, downloadErr := downloader.DownloadSingleZipAndSave(ctx, cfg, dbConnPool, logEntry, client, url, sourceFeedURL)

		if downloadErr != nil {
			totalProcessingDispatched-- // Decrement as job won't be dispatched
			// Check if error is due to context cancellation
			if errors.Is(downloadErr, context.Canceled) || errors.Is(downloadErr, context.DeadlineExceeded) {
				logEntry.Warn("Regular zip download cancelled.", "error", downloadErr)
				// Return current counts
				return totalProcessingDispatched, regularDownloadErrors, regularSkippedCount
			}
			// Record other download errors
			processingErrorsMu.Lock()
			*processingErrors = append(*processingErrors, fmt.Errorf("download regular zip %s: %w", url, downloadErr))
			processingErrorsMu.Unlock()
			regularDownloadErrors++
			continue // Continue to next URL
		}

		// Dispatch Processing Job
		if savedPath != "" {
			logEntry.Info("Regular zip download successful, dispatching job for processing.", slog.String("path", savedPath))
			// totalProcessingDispatched count remains incremented
			processJob := processingJob{
				zipFilePath: savedPath,
				logEntry:    logEntry.With(slog.String("zip_file", filepath.Base(savedPath))),
			}
			select {
			case processingJobsChan <- processJob:
				// Job sent successfully
			case <-ctx.Done():
				logEntry.Warn("Context cancelled while dispatching processing job for regular zip.", "error", ctx.Err())
				// Return current counts
				return totalProcessingDispatched, regularDownloadErrors, regularSkippedCount
			}
		} else {
			// Should not happen if downloadErr was nil, but handle defensively
			totalProcessingDispatched-- // Decrement as job won't be dispatched
			logEntry.Warn("Download reported success but savedPath is empty, skipping dispatch.")
		}
	} // End regular zip download loop

	logger.Info("Regular zip download loop finished.")
	return totalProcessingDispatched, regularDownloadErrors, regularSkippedCount
}
