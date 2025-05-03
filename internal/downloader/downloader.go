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

	// "sync/atomic" // No longer needed
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"

	"golang.org/x/net/html"
	// Removed semaphore, atomic
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
func GetRandomUserAgent() string { // Exported function
	if len(commonUserAgents) == 0 {
		// Fallback if list is somehow empty
		return "NEMParquetConverter/1.2 (Go-client)" // Increment version maybe
	}
	return commonUserAgents[rand.Intn(len(commonUserAgents))]
}

// DiscoverZipURLs performs the discovery phase for a given list of feed URLs.
// Returns a map of discovered ZipURL -> SourceFeedURL and any non-fatal discovery error.
func DiscoverZipURLs(ctx context.Context, repos []string, logger *slog.Logger) (map[string]string, error) {
	client := util.DefaultHTTPClient()
	var discoveryErr error
	// Combine both feed URL lists for discovery
	discoveredLinks := make(map[string]string) // Map absolute URL -> source feed URL
	processedFeeds := 0
	var discoveryMu sync.Mutex // Protect map access

	logger.Debug("Starting discovery across feed URLs", slog.Int("feed_count", len(repos)))
	for _, baseURL := range repos {
		// Check for context cancellation before processing each feed
		select {
		case <-ctx.Done():
			logger.Warn("Discovery cancelled by context.")
			return nil, errors.Join(discoveryErr, ctx.Err())
		default:
			// Continue processing feed
		}

		processedFeeds++
		l := logger.With(slog.String("feed_url", baseURL), slog.Int("feed_num", processedFeeds), slog.Int("total_feeds", len(repos)))
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
		req.Header.Set("User-Agent", GetRandomUserAgent()) // Use exported function

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
			continue // Continue even if body read failed on non-200
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
				if _, exists := discoveredLinks[absURL]; !exists {
					discoveredLinks[absURL] = baseURL // Store source feed
					newLinksFound++
				}
			} else {
				l.Warn("Failed to resolve relative link", "link", relativeLink, "error", err)
			}
		}
		discoveryMu.Unlock() // Unlock map access
		l.Debug("Feed check complete", slog.Int("new_links", newLinksFound), slog.Int("total_unique_links", len(discoveredLinks)))
	} // End feed discovery loop

	logger.Debug("Finished discovery phase.", slog.Int("total_unique_zips", len(discoveredLinks)))
	return discoveredLinks, discoveryErr
}

// RunSequentialDownloads iterates through URLs, downloads them sequentially,
// and sends the *local path* of successfully downloaded files to the output channel.
// This is called by the orchestrator.
func RunSequentialDownloads(
	ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger,
	urlsToDownload []string, downloadedPathsChan chan<- string, // Takes list of URLs to download
	urlToSourceMap map[string]string, // Need source URL for logging
) error {
	client := util.DefaultHTTPClient()
	var finalErr error
	processedCount := 0

	logger.Info("Sequential downloader started.", slog.Int("count", len(urlsToDownload)))

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

		sourceURL := urlToSourceMap[zipURL] // Get source URL from map passed by orchestrator

		// Call DownloadSingleZipAndSave (renamed from downloadSingleZipAndSave)
		savedPath, err := DownloadSingleZipAndSave(ctx, cfg, dbConn, l, client, zipURL, sourceURL)

		if err != nil {
			l.Error("Failed to download zip", "error", err) // Error logged within DownloadSingleZipAndSave too
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
				// Need to ensure channel is closed eventually if we exit here
				// The defer close in the calling goroutine (orchestrator) handles this.
				return finalErr // Exit loop if cancelled while sending
			}
		}
		// Optional small delay between downloads
		// time.Sleep(50 * time.Millisecond)
	}

	logger.Info("Sequential downloader finished processing all URLs.")
	return finalErr
}

// DownloadSingleZipAndSave handles downloading one zip and saving it.
// Returns the saved absolute path on success, or error on failure.
// Logs events using the original zipURL as the filename key.
func DownloadSingleZipAndSave(ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger, client *http.Client, zipURL, sourceFeedURL string) (string, error) {
	startTime := time.Now()
	zipFilename := filepath.Base(zipURL)
	relativeOutputPath := filepath.Join(cfg.InputDir, zipFilename) // Relative path first

	// Calculate Absolute Path
	outputZipPath, absErr := filepath.Abs(relativeOutputPath)
	if absErr != nil {
		pathErr := fmt.Errorf("failed to get absolute path for %s: %w", relativeOutputPath, absErr)
		logger.Error("Cannot determine absolute output path.", "relative_path", relativeOutputPath, "error", pathErr)
		// Log an event without a valid path? Maybe log with relative path?
		db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventError, sourceFeedURL, relativeOutputPath, pathErr.Error(), "", nil)
		return "", pathErr
	}
	l := logger.With(slog.String("output_path", outputZipPath)) // Add abs path to logger context

	l.Info("Starting download.")
	// Log start event with the absolute path in output_path, URL as filename
	db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventDownloadStart, sourceFeedURL, outputZipPath, "", "", nil)

	// Create request object
	req, err := http.NewRequestWithContext(ctx, "GET", zipURL, nil)
	if err != nil {
		reqErr := fmt.Errorf("create request failed: %w", err)
		// Log event with absolute path
		db.LogFileEvent(ctx, dbConn, zipURL, db.FileTypeZip, db.EventError, sourceFeedURL, outputZipPath, reqErr.Error(), "", nil)
		logger.Error("Failed creating request.", "error", reqErr)
		return "", reqErr
	}
	req.Header.Set("User-Agent", GetRandomUserAgent()) // Use exported function
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
		return "", dlErr // Return the error
	}
	// --- End Error Handling ---

	// --- Success Path ---
	l.Debug("Download complete.", slog.Int("bytes", len(data)), slog.Duration("duration", downloadDuration.Round(time.Millisecond)))

	// Save the downloaded data to disk (using absolute path)
	err = os.WriteFile(outputZipPath, data, 0644)
	saveDuration := time.Since(startTime) // Update duration to include save time
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
