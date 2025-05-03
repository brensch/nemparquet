package util

import (
	"bytes" // Needed for bytes.Buffer
	"fmt"
	"io"
	"net/http"
	"time"
)

// ProgressCallback is a function type for reporting download progress.
// downloadedBytes: bytes downloaded so far.
// totalBytes: total size of the file (-1 if unknown).
type ProgressCallback func(downloadedBytes int64, totalBytes int64)

// DownloadFileWithProgress executes a pre-built HTTP request, streams the body,
// reports progress via callback, and returns the full body bytes.
// It handles response closing and non-200 status codes.
// The callback is invoked periodically during the download.
func DownloadFileWithProgress(client *http.Client, req *http.Request, callback ProgressCallback) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do request for %s: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limitReader := io.LimitReader(resp.Body, 512)
		bodyBytes, _ := io.ReadAll(limitReader)
		return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, req.URL.String(), string(bodyBytes))
	}

	// Get total size from header if available
	totalSize := resp.ContentLength // Will be -1 if unknown

	// Use a buffer to store the downloaded data
	var buf bytes.Buffer
	buf.Grow(int(totalSize)) // Pre-allocate if size is known

	// Create a progress tracking reader
	progressReader := &progressReader{
		Reader:         resp.Body,
		Total:          totalSize,
		Callback:       callback,
		BytesRead:      0,
		NextReportAt:   100 * 1024 * 1024, // Report every 100MB
		ReportInterval: 100 * 1024 * 1024,
	}

	// Copy from the progress reader to the buffer
	_, err = io.Copy(&buf, progressReader)
	if err != nil {
		return nil, fmt.Errorf("failed reading/copying body from %s: %w", req.URL.String(), err)
	}

	// Final progress report if callback exists and total size known
	if callback != nil && totalSize > 0 {
		callback(totalSize, totalSize)
	}

	return buf.Bytes(), nil
}

// progressReader wraps an io.Reader to track read progress and invoke a callback.
type progressReader struct {
	io.Reader
	Callback       ProgressCallback
	Total          int64
	BytesRead      int64
	NextReportAt   int64
	ReportInterval int64
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.Reader.Read(p)
	if n > 0 {
		pr.BytesRead += int64(n)
		if pr.Callback != nil && pr.BytesRead >= pr.NextReportAt {
			pr.Callback(pr.BytesRead, pr.Total)
			// Set next report point, ensuring it increments even if callback was delayed
			pr.NextReportAt = ((pr.BytesRead / pr.ReportInterval) + 1) * pr.ReportInterval
		}
	}
	// Ensure final callback happens if EOF is reached and not exactly on interval
	if err == io.EOF && pr.Callback != nil && pr.BytesRead < pr.Total {
		pr.Callback(pr.BytesRead, pr.Total)
	}
	return
}

// DefaultHTTPClient creates a default http.Client with a very long timeout
// suitable for potentially large file downloads.
func DefaultHTTPClient() *http.Client {
	// Set a long timeout (e.g., 1 hour) instead of infinite (0)
	// Adjust as needed based on expected max download times.
	return &http.Client{Timeout: 1 * time.Hour}
}

// --- Old DownloadFile (kept for reference or if needed elsewhere without progress) ---
func DownloadFile(client *http.Client, req *http.Request) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do request for %s: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limitReader := io.LimitReader(resp.Body, 512)
		bodyBytes, _ := io.ReadAll(limitReader)
		return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, req.URL.String(), string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading body from %s: %w", req.URL.String(), err)
	}
	return bodyBytes, nil
}
