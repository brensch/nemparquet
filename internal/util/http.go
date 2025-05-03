package util

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// DownloadFile fetches a file from a URL.
func DownloadFile(client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request for %s: %w", url, err)
	}
	req.Header.Set("User-Agent", "NEMParquetConverter/1.0 (Go-client)")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limitReader := io.LimitReader(resp.Body, 512)
		bodyBytes, _ := io.ReadAll(limitReader)
		return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, url, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body from %s: %w", url, err)
	}
	return bodyBytes, nil
}

// DefaultHTTPClient creates a default client with a timeout.
func DefaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 120 * time.Second}
}
