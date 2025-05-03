package downloader

import (
	"bytes" // Needed for html.Parse
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand" // Import math/rand
	"net/http"
	"net/url"
	"os"
	"path/filepath" // Needed for Abs and Base
	"strings"
	"sync"

	// "sync/atomic" // No longer needed for sequential
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"

	// Needed for GetCompletionStatusBatch if kept in db package
	"golang.org/x/net/html"
	// "golang.org/x/sync/semaphore" // No longer needed for sequential
)

// --- List of Realistic User Agents ---
var commonUserAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.1 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
}

// Seed random number generator once
func init() {
	rand.Seed(time.Now().UnixNano())
}

// Function to get a random user agent
func getRandomUserAgent() string {
	if len(commonUserAgents) == 0 {
		// Fallback if list is somehow empty
		return "NEMParquetConverter/1.2 (Go-client)" // Increment version maybe
	}
	return commonUserAgents[rand.Intn(len(commonUserAgents))]
}

// DiscoverZipURLs performs only the discovery phase.
func DiscoverZipURLs(ctx context.Context, cfg config.Config, logger *slog.Logger) ([]string, error) {
	client := util.DefaultHTTPClient()
	var discoveryErr error
	allZipLinks := make(map[string]string) // Map absolute URL -> source feed URL
	processedFeeds := 0
	var discoveryMu sync.Mutex // Still useful if feed discovery itself was parallel (it's not currently)

	logger.Debug("Starting discovery across feed URLs", slog.Int("feed_count", len(cfg.FeedURLs)))
	for _, baseURL := range cfg.FeedURLs {
		// Check for context cancellation before processing each feed
		select {
		case <-ctx.Done():
			logger.Warn("Discovery cancelled by context.")
			return nil, errors.Join(discoveryErr, ctx.Err())
		default:
			// Continue processing feed
		}

		processedFeeds++
		l := logger.With(slog.String("feed_url", baseURL), slog.Int("feed_num", processedFeeds), slog.Int("total_feeds", len(cfg.FeedURLs)))
		l.Debug("Checking feed for discovery")

		base, err := url.Parse(baseURL)
		if err != nil {
			l.Warn("Skip: parse base URL failed.", "error", err)
			discoveryErr = errors.Join(discoveryErr, fmt.Errorf("parse base %s: %w", baseURL, err))
			continue
		}
		req, err := http.NewRequestWithContext(ctx, "GET", baseURL, nil)
		if err != nil {
			l.Warn("Skip: create request failed.", "error", err)
			discoveryErr = errors.Join(discoveryErr, fmt.Errorf("create request %s: %w", baseURL, err))
			continue
		}
		// Set a user agent for discovery requests too
		req.Header.Set("User-Agent", getRandomUserAgent())

		resp, err := client.Do(req)
		if err != nil {
			l.Warn("Skip: GET failed.", "error", err)
			discoveryErr = errors.Join(discoveryErr, fmt.Errorf("discover GET %s: %w", baseURL, err))
			continue
		}
		// Ensure body is read and closed even on non-200 status codes to free resources
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close() // Close body immediately

		if resp.StatusCode != http.StatusOK {
			l.Warn("Skip: Bad status.", "status", resp.Status)
			discoveryErr = errors.Join(discoveryErr, fmt.Errorf("discover status %s: %s", resp.Status, baseURL))
			continue
		}
		if readErr != nil {
			l.Warn("Skip: read body failed.", "error", readErr)
			discoveryErr = errors.Join(discoveryErr, fmt.Errorf("discover read %s: %w", baseURL, readErr))
			continue
		}

		root, err := html.Parse(bytes.NewReader(bodyBytes))
		if err != nil {
			l.Warn("Skip: parse HTML failed.", "error", err)
			discoveryErr = errors.Join(discoveryErr, fmt.Errorf("discover parse HTML %s: %w", baseURL, err))
			continue
		}

		links := util.ParseLinks(root, ".zip")
		discoveryMu.Lock() // Lock map access
		newLinksFound := 0
		for _, relativeLink := range links {
			zipURLAbs, err := base.Parse(relativeLink)
			if err == nil {
				absURL := zipURLAbs.String()
				if _, exists := allZipLinks[absURL]; !exists {
					allZipLinks[absURL] = baseURL // Store source feed
					newLinksFound++
				}
			} else {
				l.Warn("Failed to resolve relative link", "link", relativeLink, "error", err)
			}
		}
		discoveryMu.Unlock() // Unlock map access
		l.Debug("Feed check complete", slog.Int("new_links", newLinksFound), slog.Int("total_unique_links", len(allZipLinks)))
	} // End feed discovery loop

	discoveredURLs := make([]string, 0, len(allZipLinks))
	for url := range allZipLinks {
		discoveredURLs = append(discoveredURLs, url)
	}
	logger.Debug("Finished discovery phase.", slog.Int("total_unique_zips", len(discoveredURLs)))
	return discoveredURLs, discoveryErr
}

// DownloadArchives discovers zip URLs, checks DB, and downloads missing ones sequentially.
func DownloadArchives(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, forceDownload bool) error {
	var discoveryErr error // Keep track of discovery errors separately

	// --- Phase 1: Discover all unique zip URLs ---
	logger.Info("Starting Phase 1: Discovering ZIP files...")
	allDiscoveredURLs, discoveryErr := DiscoverZipURLs(ctx, cfg, logger) // Call separated discovery function
	if discoveryErr != nil {
		logger.Error("Failed during discovery phase.", "error", discoveryErr)
		// Decide whether to stop or continue based on severity
	}
	if ctx.Err() != nil { // Check context after discovery
		return errors.Join(discoveryErr, ctx.Err())
	}
	archivesFound := len(allDiscoveredURLs)
	if archivesFound == 0 {
		logger.Info("Phase 1 Complete: No unique ZIP files found.")
		return discoveryErr // Return discovery errors if any
	}
	logger.Info("Phase 1 Complete", slog.Int("total_unique_zips", archivesFound))

	// --- Phase 1.5: Batch Check DB Status ---
	logger.Info("Checking database for previously downloaded files...")
	completedStatusMap, dbErr := db.GetCompletionStatusBatch(ctx, dbConn, allDiscoveredURLs, db.FileTypeZip, db.EventDownloadEnd)
	if dbErr != nil {
		logger.Error("Database check failed, attempting to download all.", "error", dbErr)
		discoveryErr = errors.Join(discoveryErr, fmt.Errorf("db batch check: %w", dbErr)) // Combine errors
		completedStatusMap = make(map[string]bool)                                        // Assume none completed if check fails
	} else {
		logger.Debug("Database check complete.", slog.Int("already_downloaded_count", len(completedStatusMap)))
	}

	// --- Phase 1.6: Filter URLs and Log Skip Summary ---
	urlsToDownload := make([]string, 0, archivesFound)
	skippedCount := 0
	for _, zipURL := range allDiscoveredURLs { // Use the slice from DiscoverZipURLs
		if _, found := completedStatusMap[zipURL]; found && !forceDownload {
			skippedCount++
		} else {
			urlsToDownload = append(urlsToDownload, zipURL)
		}
	}
	if skippedCount > 0 {
		logger.Info("Skipping already downloaded files.", slog.Int("skipped_count", skippedCount), slog.Int("total_discovered", archivesFound))
	}
	if len(urlsToDownload) == 0 {
		logger.Info("No new zip files require downloading.")
		return discoveryErr // Return only discovery/db errors if any
	}

	// --- Phase 2: Download Sequentially (Filtered List) ---
	logger.Info("Starting Phase 2: Downloading new/forced zips sequentially...", slog.Int("files_to_process", len(urlsToDownload)))

	var finalErr error = discoveryErr // Start with potential discovery/db errors
	processedZipCount := 0
	errorCount := 0

	// Simple sequential loop
	for _, zipURL := range urlsToDownload {
		// Check context cancellation before each download
		select {
		case <-ctx.Done():
			logger.Warn("Download cancelled.")
			finalErr = errors.Join(finalErr, ctx.Err())
			return finalErr // Exit loop early
		default:
			// Continue with download
		}

		processedZipCount++
		l := logger.With(
			slog.String("zip_url", zipURL),
			slog.Int("zip_num", processedZipCount),
			slog.Int("total_to_process", len(urlsToDownload)),
		)
		l.Info("Processing zip file.")

		// Call downloadSingleZipAndSave
		// Source URL isn't strictly needed by downloadSingleZipAndSave itself, but good practice to have if available
		sourceURL := "" // Default to empty if map lookup fails (shouldn't happen if discovered)
		// if src, ok := allZipLinks[zipURL]; ok { sourceURL = src } // Get source URL if needed later

		_, err := downloadSingleZipAndSave(ctx, cfg, dbConn, l, util.DefaultHTTPClient(), zipURL, sourceURL) // Ignore saved path return here
		if err != nil {
			// Error is already logged within downloadSingleZipAndSave
			finalErr = errors.Join(finalErr, fmt.Errorf("zip %s: %w", zipURL, err))
			errorCount++
			// Optional: Introduce a small delay here *after* an error before proceeding?
			// time.Sleep(500 * time.Millisecond)
		} else {
			l.Info("Successfully processed zip file.")
		}

		// Optional: Add a small delay between *all* downloads to be extra cautious?
		// time.Sleep(100 * time.Millisecond)

	} // End sequential loop

	logger.Info("Phase 2 Complete: Finished processing all required zips.")
	if errorCount > 0 {
		logger.Warn("Download phase completed with errors", slog.Int("error_count", errorCount), "error", finalErr)
	} else if finalErr == nil { // Check if only discovery/db errors occurred
		logger.Info("Download phase completed successfully.")
	}
	return finalErr
}

// RunSequentialDownloads is used by the orchestrator to download a specific list sequentially
// and feed paths to a channel.
func RunSequentialDownloads(
	ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger,
	urlsToDownload []string, downloadedPathsChan chan<- string,
) error {
	client := util.DefaultHTTPClient()
	var finalErr error
	processedCount := 0

	logger.Info("Sequential downloader started for orchestrator.", slog.Int("count", len(urlsToDownload)))

	for _, zipURL := range urlsToDownload {
		select {
		case <-ctx.Done():
			logger.Warn("Download sequence cancelled.")
			finalErr = errors.Join(finalErr, ctx.Err())
			return finalErr // Exit loop early
		default:
			// Continue with download
		}

		processedCount++
		l := logger.With(
			slog.String("zip_url", zipURL),
			slog.Int("zip_num", processedCount),
			slog.Int("total_to_process", len(urlsToDownload)),
		)
		l.Info("Processing download.")

		// Call downloadSingleZipAndSave
		// Source URL isn't available here unless passed in, default to empty
		savedPath, err := downloadSingleZipAndSave(ctx, cfg, dbConn, l, client, zipURL, "")

		if err != nil {
			l.Error("Failed to download zip", "error", err) // Error logged within downloadSingleZipAndSave too
			finalErr = errors.Join(finalErr, fmt.Errorf("download %s: %w", zipURL, err))
			// Continue to the next file even if one fails
		} else if savedPath != "" {
			l.Info("Successfully downloaded zip.", slog.String("saved_path", savedPath))
			// Send the *path* of the successfully downloaded file to the processor channel
			select {
			case downloadedPathsChan <- savedPath:
				l.Debug("Sent path to processor channel.", slog.String("path", savedPath))
			case <-ctx.Done():
				logger.Warn("Download sequence cancelled while sending path to processor.")
				finalErr = errors.Join(finalErr, ctx.Err())
				return finalErr // Exit loop if cancelled while sending
			}
		}
	}

	logger.Info("Sequential downloader finished processing all URLs for orchestrator.")
	return finalErr
}

// downloadSingleZipAndSave handles downloading one zip and saving it.
// Returns the saved absolute path on success, or error on failure.
func downloadSingleZipAndSave(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, client *http.Client, zipURL, sourceFeedURL string) (string, error) {
	startTime := time.Now()
	zipFilename := filepath.Base(zipURL)
	relativeOutputPath := filepath.Join(cfg.InputDir, zipFilename) // Relative path first

	// Calculate Absolute Path
	outputZipPath, absErr := filepath.Abs(relativeOutputPath)
	if absErr != nil {
		pathErr := fmt.Errorf("failed to get absolute path for %s: %w", relativeOutputPath, absErr)
		logger.Error("Cannot determine absolute output path.", "relative_path", relativeOutputPath, "error", pathErr)
		db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventError, sourceFeedURL, relativeOutputPath, pathErr.Error(), "", nil)
		return "", pathErr
	}
	l := logger.With(slog.String("output_path", outputZipPath)) // Add abs path to logger context

	l.Info("Starting download.")
	// Log start event with the absolute path
	db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventDownloadStart, sourceFeedURL, outputZipPath, "", "", nil)

	// Create request object
	req, err := http.NewRequestWithContext(ctx, "GET", zipURL, nil)
	if err != nil {
		reqErr := fmt.Errorf("create request failed: %w", err)
		db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventError, sourceFeedURL, outputZipPath, reqErr.Error(), "", nil)
		logger.Error("Failed creating request.", "error", reqErr)
		return "", reqErr
	}
	req.Header.Set("User-Agent", getRandomUserAgent())
	req.Header.Set("Accept", "application/zip,application/octet-stream,*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	// Download File content
	data, err := util.DownloadFile(client, req)
	downloadDuration := time.Since(startTime)

	// --- Error Handling ---
	if err != nil {
		dlErr := fmt.Errorf("download http failed: %w", err)
		isRateLimitError := false
		errString := strings.ToLower(dlErr.Error())
		if strings.Contains(errString, "429") || strings.Contains(errString, "503") || strings.Contains(errString, "403") || strings.Contains(errString, "forbidden") || strings.Contains(errString, "too many") {
			isRateLimitError = true
		}
		// Log event with absolute path
		db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventError, sourceFeedURL, outputZipPath, dlErr.Error(), "", &downloadDuration)
		if isRateLimitError {
			logger.Warn("Download failed (Rate limit/block suspected).", "error", dlErr, slog.Duration("duration", downloadDuration.Round(time.Millisecond)))
		} else {
			logger.Error("Download failed.", "error", dlErr, slog.Duration("duration", downloadDuration.Round(time.Millisecond)))
		}
		return "", dlErr
	}

	// --- Success Path ---
	l.Debug("Download complete.", slog.Int("bytes", len(data)), slog.Duration("duration", downloadDuration.Round(time.Millisecond)))

	// Save the downloaded data to disk (using absolute path)
	err = os.WriteFile(outputZipPath, data, 0644)
	saveDuration := time.Since(startTime)
	if err != nil {
		saveErr := fmt.Errorf("failed to save zip file %s: %w", outputZipPath, err)
		// Log event with absolute path
		db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventError, sourceFeedURL, outputZipPath, saveErr.Error(), "", &saveDuration)
		logger.Error("Failed saving downloaded zip.", "error", err, slog.Duration("total_duration", saveDuration.Round(time.Millisecond)))
		return "", saveErr
	}
	l.Debug("Saved zip file successfully.", slog.Duration("total_duration", saveDuration.Round(time.Millisecond)))

	// Log download end event with the absolute path
	db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventDownloadEnd, sourceFeedURL, outputZipPath, "", "", &saveDuration)

	return outputZipPath, nil // Return absolute path on success
}
