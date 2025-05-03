package util

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// DownloadFile executes a pre-built HTTP request and returns the body bytes.
// It handles response closing and non-200 status codes.
// The caller is responsible for creating the request (including context and headers).
func DownloadFile(client *http.Client, req *http.Request) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		// Include URL from request in error message
		return nil, fmt.Errorf("http do request for %s: %w", req.URL.String(), err)
	}
	defer resp.Body.Close() // Ensure body is closed

	if resp.StatusCode != http.StatusOK {
		// Read some of the body for context on error
		limitReader := io.LimitReader(resp.Body, 512) // Read up to 512 bytes
		bodyBytes, _ := io.ReadAll(limitReader)
		// Include URL and status in the error
		return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, req.URL.String(), string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		// Include URL in error message
		return nil, fmt.Errorf("failed reading body from %s: %w", req.URL.String(), err)
	}
	return bodyBytes, nil
}

// DefaultHTTPClient creates a default http.Client with a reasonable timeout.
func DefaultHTTPClient() *http.Client {
	// Consider customizing transport settings (MaxIdleConns, etc.) if needed
	return &http.Client{Timeout: 120 * time.Second}
}
