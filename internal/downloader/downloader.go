package downloader

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/util" // Use your module name

	"golang.org/x/net/html"
)

// DownloadProgress tracks the progress of downloads and extractions.
type DownloadProgress struct {
	TotalArchives     int    // Total number of zip archives found across all feeds
	ArchivesProcessed int    // Number of archives attempted/processed so far
	CurrentArchive    string // Name of the archive being processed
	CurrentFile       string // Name of the CSV file being extracted (if applicable)
	BytesTotal        int64  // Total bytes for the current archive download
	BytesCurrent      int64  // Bytes downloaded for the current archive
	Extracted         bool   // True if the CSV from the current archive was extracted
	Skipped           bool   // True if the archive/file was skipped (exists, no CSV, etc.)
	Err               error  // Error for the current step
}

// DownloadAndExtract downloads zips from feed URLs and extracts CSVs.
// It sends progress updates via the progressChan.
func DownloadAndExtract(cfg config.Config, progressChan chan<- DownloadProgress) error {
	client := util.DefaultHTTPClient()
	var overallErr error
	totalFilesExtractedCount := 0
	archivesFound := 0
	archivesProcessed := 0

	// --- Phase 1: Find all archives first to get a total count ---
	allZipLinks := make(map[string]string) // Map absolute URL to original base URL
	log.Println("Phase 1: Discovering ZIP files...")
	for _, baseURL := range cfg.FeedURLs {
		log.Printf(" Checking feed for discovery: %s", baseURL)
		base, err := url.Parse(baseURL)
		if err != nil {
			log.Printf(" WARN: Failed parse base URL %s: %v. Skipping discovery.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse base %s: %w", baseURL, err))
			continue
		}

		resp, err := client.Get(baseURL)
		// Basic error handling for discovery phase
		if err != nil {
			log.Printf(" WARN: Discovery GET %s failed: %v.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("discover GET %s: %w", baseURL, err))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf(" WARN: Discovery bad status '%s' for %s.", resp.Status, baseURL)
			overallErr = errors.Join(overallErr, fmt.Errorf("discover status %s: %s", resp.Status, baseURL))
			resp.Body.Close()
			continue
		}

		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Printf(" WARN: Discovery read body %s failed: %v.", baseURL, readErr)
			overallErr = errors.Join(overallErr, fmt.Errorf("discover read %s: %w", baseURL, readErr))
			continue
		}

		root, err := html.Parse(bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf(" WARN: Discovery parse HTML %s failed: %v.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("discover parse HTML %s: %w", baseURL, err))
			continue
		}

		links := util.ParseLinks(root, ".zip")
		for _, relativeLink := range links {
			zipURLAbs, err := base.Parse(relativeLink)
			if err == nil {
				absURL := zipURLAbs.String()
				if _, exists := allZipLinks[absURL]; !exists {
					allZipLinks[absURL] = baseURL // Store it
					archivesFound++
				}
			}
		}
		log.Printf(" Discovered %d unique zip links so far...", archivesFound)
	}
	log.Printf("Phase 1 Complete: Found %d total unique ZIP files.", archivesFound)

	if archivesFound == 0 {
		log.Println("WARN: No zip files found in any feed URL.")
		// Send a final progress update? Or just return nil?
		progressChan <- DownloadProgress{TotalArchives: 0, ArchivesProcessed: 0}
		close(progressChan) // Close channel as we are done
		return overallErr   // Return any discovery errors
	}

	// --- Phase 2: Process each discovered archive ---
	log.Println("Phase 2: Downloading and extracting...")
	for zipURL := range allZipLinks { // Iterate through the unique links
		archivesProcessed++
		progress := DownloadProgress{
			TotalArchives:     archivesFound,
			ArchivesProcessed: archivesProcessed,
			CurrentArchive:    filepath.Base(zipURL), // Use filename as identifier
		}
		log.Printf(" Processing archive %d/%d: %s", archivesProcessed, archivesFound, zipURL)

		// --- Download ---
		progress.BytesCurrent = 0
		progress.BytesTotal = -1 // Indicate unknown total initially
		progressChan <- progress // Update status: Starting download

		data, err := util.DownloadFile(client, zipURL) // Assumes DownloadFile doesn't stream progress yet
		if err != nil {
			log.Printf("  WARN: Failed download %s: %v. Skipping ZIP.", zipURL, err)
			progress.Err = err
			progress.Skipped = true
			progressChan <- progress
			overallErr = errors.Join(overallErr, fmt.Errorf("download %s: %w", zipURL, err))
			continue // Skip to next archive
		}
		progress.BytesTotal = int64(len(data))
		progress.BytesCurrent = progress.BytesTotal // Mark download as complete
		log.Printf("  Downloaded %d bytes.", len(data))
		progressChan <- progress // Update status: Download complete

		// --- Extract ---
		progress.Extracted = false // Reset for extraction phase
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			log.Printf("  WARN: Cannot open downloaded data from %s as ZIP: %v. Skipping ZIP.", zipURL, err)
			progress.Err = fmt.Errorf("open zip: %w", err)
			progress.Skipped = true
			progressChan <- progress
			overallErr = errors.Join(overallErr, fmt.Errorf("open zip %s: %w", zipURL, err))
			continue
		}

		extractedSomething := false
		var extractionErr error
		for _, f := range zr.File {
			if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
				continue
			}
			cleanBaseName := filepath.Base(f.Name)
			if cleanBaseName == "" || cleanBaseName == "." || cleanBaseName == ".." || strings.HasPrefix(cleanBaseName, ".") {
				log.Printf("  WARN: Skipping potentially unsafe file in zip: %s", f.Name)
				continue
			}
			outPath := filepath.Join(cfg.InputDir, cleanBaseName)
			progress.CurrentFile = cleanBaseName // Update current file being extracted

			// Check existence
			if _, err := os.Stat(outPath); err == nil {
				log.Printf("  INFO: Skipping extraction, file already exists: %s", outPath)
				progress.Skipped = true    // Mark as skipped for this file
				progress.Extracted = false // It wasn't extracted *now*
				progressChan <- progress
				progress.Skipped = false // Reset for next potential file in zip
				continue                 // Skip this CSV
			}

			// Extract
			log.Printf("  Extracting: %s", f.Name)
			in, err := f.Open()
			if err != nil {
				log.Printf("  ERROR: Failed open '%s' within zip: %v", f.Name, err)
				extractionErr = errors.Join(extractionErr, fmt.Errorf("open zip entry %s: %w", f.Name, err))
				progress.Err = extractionErr
				progressChan <- progress
				continue // Try next file in zip
			}
			out, err := os.Create(outPath)
			if err != nil {
				in.Close()
				log.Printf("  ERROR: Failed create output file %s: %v", outPath, err)
				extractionErr = errors.Join(extractionErr, fmt.Errorf("create output %s: %w", outPath, err))
				progress.Err = extractionErr
				progressChan <- progress
				continue
			}
			copiedBytes, err := io.Copy(out, in)
			in.Close()
			out.Close()
			if err != nil {
				log.Printf("  ERROR: Failed copy data for '%s' to %s: %v", f.Name, outPath, err)
				extractionErr = errors.Join(extractionErr, fmt.Errorf("copy %s: %w", f.Name, err))
				os.Remove(outPath) // Clean up incomplete file
				progress.Err = extractionErr
				progressChan <- progress
				continue
			}

			log.Printf("    Extracted '%s' (%d bytes) to %s", f.Name, copiedBytes, outPath)
			extractedSomething = true
			totalFilesExtractedCount++
			progress.Extracted = true
			progress.Err = nil         // Clear error if this file was successful
			progressChan <- progress   // Send update: Extraction successful for this file
			progress.Extracted = false // Reset for next file
		} // End loop zip entries

		if !extractedSomething {
			log.Printf("  INFO: No *.csv files found or extracted within %s", zipURL)
			if extractionErr == nil { // If no CSV was found, but no error occurred during search
				progress.Skipped = true      // Mark archive as skipped if no CSV was extracted
				progress.CurrentFile = ""    // Clear current file
				progress.Err = extractionErr // Report any errors encountered
				progressChan <- progress
			}
		}
		overallErr = errors.Join(overallErr, extractionErr) // Add extraction errors to overall

	} // End loop archives

	log.Printf("Phase 2 Complete: Extracted %d CSV files total.", totalFilesExtractedCount)
	if totalFilesExtractedCount == 0 && overallErr == nil {
		log.Println("WARN: No new CSV files were extracted from any source feeds.")
	}

	close(progressChan) // Signal that no more progress updates are coming
	return overallErr
}
