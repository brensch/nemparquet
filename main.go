// main.go
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time" // Added for rate limiting/delay if needed

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
)

// Directories hosting the CSV feeds on NEMWEB
// Add more URLs here as needed
var feedURLs = []string{
	"https://nemweb.com.au/Reports/Current/FPP/",
	"https://nemweb.com.au/Reports/Current/FPPDAILY/",
	"https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	"https://nemweb.com.au/Reports/Current/PD7Day/",
	"https://nemweb.com.au/Reports/Current/P5_Reports/",
	// Add archive URLs if needed, e.g.:
	// "https://nemweb.com.au/Reports/Archive/FPP/",
}

// --- Main Application Logic ---

func main() {
	inputDir := flag.String("input", "./input_csv", "Directory to download & extract latest CSVs into")
	outputDir := flag.String("output", "./output_parquet", "Directory to write Parquet files to")
	flag.Parse()

	log.Printf("Starting NEM Parquet Converter...")
	log.Printf("Input directory: %s", *inputDir)
	log.Printf("Output directory: %s", *outputDir)

	// Ensure directories exist
	for _, d := range []string{*inputDir, *outputDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			log.Fatalf("FATAL: Failed to create directory %s: %v", d, err)
		}
	}

	// --- Download and Extract ---
	log.Println("--- Downloading and Extracting CSVs ---")
	if err := downloadAndExtractLatest(*inputDir); err != nil {
		// Log as warning or fatal depending on desired behavior
		log.Printf("WARN: Download/extraction process encountered errors: %v", err)
		// Decide if program should exit if downloads fail
		// os.Exit(1)
	}

	// --- Process CSVs ---
	log.Println("--- Processing CSV Files ---")
	csvFiles, err := filepath.Glob(filepath.Join(*inputDir, "*.CSV")) // Case-insensitive glob might be needed on some OS
	if err != nil {
		log.Fatalf("FATAL: Failed to search for CSV files in %s: %v", *inputDir, err)
	}
	if len(csvFiles) == 0 {
		log.Printf("INFO: No *.CSV files found in %s to process.", *inputDir)
		log.Println("NEM Parquet Converter finished.")
		return
	}

	log.Printf("Found %d CSV files to process: %v", len(csvFiles), csvFiles)

	processedCount := 0
	errorCount := 0
	for _, csvPath := range csvFiles {
		log.Printf("Processing %s...", csvPath)
		if err := processCSVSections(csvPath, *outputDir); err != nil {
			log.Printf("ERROR processing %s: %v", csvPath, err)
			errorCount++
		} else {
			processedCount++
		}
	}

	log.Println("--- Processing Summary ---")
	log.Printf("Successfully processed %d CSV files.", processedCount)
	if errorCount > 0 {
		log.Printf("Encountered errors processing %d CSV files.", errorCount)
	}
	log.Println("NEM Parquet Converter finished.")
}

// --- Core CSV to Parquet Conversion ---

// processCSVSections reads a NEM-formatted CSV, splits it into sections based on 'I' records,
// infers schema from the first data row, and writes each section to a Parquet file using WriteString.
func processCSVSections(csvPath, outDir string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open CSV %s: %w", csvPath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Consider increasing buffer size if necessary
	// const maxBuf = 2 * 1024 * 1024 // 2MB
	// buf := make([]byte, maxBuf)
	// scanner.Buffer(buf, maxBuf)

	var pw *writer.CSVWriter
	var fw source.ParquetFile
	var currentHeaders []string
	var currentComp, currentVer string
	var currentMeta []string         // Holds schema for the active section
	var isFirstDataRowInSection bool // Flag to track if we need to initialize the writer

	baseName := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}

		rec := strings.Split(trimmedLine, ",")
		if len(rec) == 0 {
			continue
		}
		recordType := strings.TrimSpace(rec[0])

		switch recordType {
		case "I":
			// --- Finalize Previous Section (if any) ---
			if pw != nil {
				log.Printf("   Finalizing section %s v%s...", currentComp, currentVer)
				// Close synchronously for reliable error handling
				if err := pw.WriteStop(); err != nil {
					log.Printf("   WARN: Error stopping Parquet writer for section %s v%s: %v", currentComp, currentVer, err)
					// Attempt to close file anyway, but error is noted
				} else {
					log.Printf("   Successfully stopped writer for %s v%s", currentComp, currentVer)
				}
				if err := fw.Close(); err != nil {
					log.Printf("   WARN: Error closing Parquet file for section %s v%s: %v", currentComp, currentVer, err)
				}
				pw = nil // Reset state
				fw = nil
				currentMeta = nil
			}

			// --- Start New Section ---
			if len(rec) < 5 {
				log.Printf("WARN line %d: Malformed 'I' record in %s (less than 5 fields): %s. Skipping section.", lineNumber, csvPath, line)
				currentHeaders = nil // Invalidate headers
				continue
			}
			currentComp, currentVer = strings.TrimSpace(rec[2]), strings.TrimSpace(rec[3])
			currentHeaders = make([]string, len(rec)-4)
			for i, h := range rec[4:] {
				currentHeaders[i] = strings.TrimSpace(h)
			}
			if len(currentHeaders) == 0 {
				log.Printf("WARN line %d: 'I' record in %s has no header columns. Skipping section %s v%s.", lineNumber, csvPath, currentComp, currentVer)
				continue
			}
			isFirstDataRowInSection = true // Signal that the next 'D' row initializes schema/writer
			log.Printf(" Found section header %s v%s at line %d", currentComp, currentVer, lineNumber)

		case "D":
			// --- Validate State ---
			if len(currentHeaders) == 0 {
				// log.Printf("DEBUG line %d: Skipping 'D' record, no active 'I' section headers.", lineNumber)
				continue // No active section headers
			}
			if len(rec) < 4 { // Need at least I,D,Comp,Ver - data can be empty
				log.Printf("WARN line %d: Malformed 'D' record in %s (less than 4 fields): %s. Skipping row.", lineNumber, csvPath, line)
				continue
			}

			// --- Extract String Values ---
			// Handle case where there are fewer data columns than header columns (e.g., trailing commas omitted)
			values := make([]string, len(currentHeaders)) // Initialize with empty strings
			numDataCols := len(rec) - 4
			for i := 0; i < len(currentHeaders); i++ {
				if i < numDataCols {
					values[i] = strings.TrimSpace(rec[i+4])
				} else {
					values[i] = "" // Assign empty string if data column is missing
				}
			}

			// --- Initialize Writer and Schema on First Data Row ---
			if isFirstDataRowInSection {
				currentMeta = make([]string, len(currentHeaders))
				log.Printf("   Inferring schema for %s v%s from first data row at line %d", currentComp, currentVer, lineNumber)

				for i := range currentHeaders {
					var typ string // Parquet type name
					val := strings.Trim(values[i], `"`)

					// Simple Type Inference (can be expanded)
					if val == "TRUE" || val == "FALSE" || strings.EqualFold(val, "true") || strings.EqualFold(val, "false") {
						typ = "BOOLEAN"
					} else if _, err := strconv.ParseInt(val, 10, 32); err == nil {
						typ = "INT32"
					} else if _, err := strconv.ParseInt(val, 10, 64); err == nil {
						typ = "INT64"
					} else if _, err := strconv.ParseFloat(val, 64); err == nil {
						// Could check for float32 range here if needed
						typ = "DOUBLE" // Use DOUBLE for safety
					} else {
						// Default to String for dates, empty strings, or anything else
						typ = "BYTE_ARRAY"
					}

					// Clean Header Name (basic example)
					cleanHeader := strings.ReplaceAll(currentHeaders[i], " ", "_")
					cleanHeader = strings.ReplaceAll(cleanHeader, ".", "_") // Replace dots
					cleanHeader = strings.ReplaceAll(cleanHeader, ";", "_") // Replace semicolons
					// Add more aggressive cleaning if needed (regex for invalid chars)
					if cleanHeader == "" {
						cleanHeader = fmt.Sprintf("column_%d", i)
					}

					// Construct Metadata String - MARKING AS OPTIONAL IS KEY
					if typ == "BYTE_ARRAY" {
						currentMeta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", cleanHeader)
					} else {
						currentMeta[i] = fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", cleanHeader, typ)
					}
				} // End schema inference loop

				// Create Parquet Writer
				parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", baseName, currentComp, currentVer)
				path := filepath.Join(outDir, parquetFile)
				var createErr error
				fw, createErr = local.NewLocalFileWriter(path)
				if createErr != nil {
					log.Printf("ERROR line %d: Failed create parquet file %s: %v. Skipping section %s v%s.", lineNumber, path, createErr, currentComp, currentVer)
					currentHeaders = nil // Stop processing this section
					continue
				}
				pw, createErr = writer.NewCSVWriter(currentMeta, fw, 4) // Use concurrency 4
				if createErr != nil {
					log.Printf("ERROR line %d: Failed init Parquet CSV writer for %s: %v. Skipping section %s v%s.", lineNumber, path, createErr, currentComp, currentVer)
					fw.Close()           // Close the file handle we opened
					currentHeaders = nil // Stop processing this section
					continue
				}
				pw.CompressionType = parquet.CompressionCodec_SNAPPY // Set compression
				log.Printf("   Created writer for section %s v%s -> %s", currentComp, currentVer, path)
				log.Printf("   Schema: [%s]", strings.Join(currentMeta, "], ["))

				isFirstDataRowInSection = false // Writer is now initialized
			} // End writer initialization block

			// --- Write Data Row using WriteString ---
			if pw == nil {
				// Should not happen if logic is correct, but safeguard
				// log.Printf("DEBUG line %d: Skipping write, pw is nil (section %s v%s).", lineNumber, currentComp, currentVer)
				continue
			}

			// Prepare []*string slice for WriteString
			// Handle empty strings explicitly? Let's see if WriteString manages them with OPTIONAL schema.
			// If WriteString errors on empty strings for non-BYTE_ARRAY types, add logic here:
			// For example: if values[j] == "" && !strings.Contains(currentMeta[j], "BYTE_ARRAY") { rec[j] = nil } else { rec[j] = &values[j] }
			rec := make([]*string, len(values))
			for j := 0; j < len(values); j++ {
				// Simplest approach: pass pointer to the string.
				// WriteString *should* handle conversion based on schema,
				// and OPTIONAL *should* allow nil for empty/unparseable strings.
				rec[j] = &values[j]

				// If empty strings cause issues later for numeric/bool types:
				// if values[j] == "" && !strings.Contains(currentMeta[j], "BYTE_ARRAY") {
				//     rec[j] = nil // Pass nil pointer explicitly for empty non-strings
				// } else {
				//     // Need to copy the loop variable's value
				//     temp := values[j]
				//     rec[j] = &temp
				// }
			}

			if err := pw.WriteString(rec); err != nil {
				// This error might indicate bad data that WriteString couldn't convert
				log.Printf("WARN line %d: WriteString error section %s v%s: %v. Data: %v", lineNumber, currentComp, currentVer, err, values)
				// Decide if processing should continue or stop for this file/section
			}

		// case "C": // Footer record - usually indicates end of file/transmission. Ignored for now.
		// 	log.Printf("DEBUG line %d: Found 'C' record (ignored).", lineNumber)

		default: // Ignore other record types
			// log.Printf("DEBUG line %d: Skipping unknown record type '%s'", lineNumber, recordType)
			continue
		} // End switch recordType
	} // End for scanner.Scan()

	// --- Finalize the Very Last Section ---
	if pw != nil {
		log.Printf("   Finalizing last section %s v%s...", currentComp, currentVer)
		if err := pw.WriteStop(); err != nil {
			// Log error but try to close file anyway
			log.Printf("ERROR: Failed to stop Parquet writer for last section %s v%s: %v", currentComp, currentVer, err)
		} else {
			log.Printf("   Successfully stopped writer for last section %s v%s", currentComp, currentVer)
		}
		if err := fw.Close(); err != nil {
			log.Printf("ERROR: Failed to close Parquet file for last section %s v%s: %v", currentComp, currentVer, err)
		}
	}

	// --- Check for Scanner Errors ---
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			log.Printf("ERROR: Scanner error reading CSV %s: line too long around line %d. Consider increasing scanner buffer size.", csvPath, lineNumber)
		} else {
			log.Printf("ERROR: Scanner error reading CSV %s near line %d: %v", csvPath, lineNumber, err)
		}
		return fmt.Errorf("scanner error reading CSV %s: %w", csvPath, err)
	}

	log.Printf("Finished processing %s successfully.", csvPath)
	return nil // Success for this file
}

// --- File Download and Extraction ---

// downloadAndExtractLatest fetches the latest .zip from each feed URL
// and extracts its .CSV files into dir. Returns an error if critical steps fail.
func downloadAndExtractLatest(dir string) error {
	// Use a client with a timeout
	client := &http.Client{
		Timeout: 60 * time.Second, // Example: 60 second timeout
	}
	var overallErr error // Track non-fatal errors
	filesExtractedCount := 0

	for _, baseURL := range feedURLs {
		log.Printf(" Checking feed: %s", baseURL)
		resp, err := client.Get(baseURL)
		if err != nil {
			log.Printf("  WARN: Failed GET %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("GET %s: %w", baseURL, err))
			continue
		}

		// Read body first, then close
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close() // Close immediately
		if readErr != nil {
			log.Printf("  WARN: Failed read body %s: %v. Skipping URL.", baseURL, readErr)
			overallErr = errors.Join(overallErr, fmt.Errorf("read body %s: %w", baseURL, readErr))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("  WARN: Bad status '%s' for %s. Skipping URL.", resp.Status, baseURL)
			overallErr = errors.Join(overallErr, fmt.Errorf("bad status %s for %s", resp.Status, baseURL))
			continue
		}

		// Parse HTML
		root, err := html.Parse(bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf("  WARN: Failed parse HTML %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse HTML %s: %w", baseURL, err))
			continue
		}

		// Parse Base URL
		base, err := url.Parse(baseURL)
		if err != nil {
			log.Printf("  ERROR: Failed parse base URL %s: %v. Skipping URL.", baseURL, err) // Should be fatal?
			overallErr = errors.Join(overallErr, fmt.Errorf("parse base URL %s: %w", baseURL, err))
			continue
		}

		// Find and Sort Links
		links := parseLinks(root, ".zip")
		if len(links) == 0 {
			log.Printf("  INFO: No *.zip files found linked at %s", baseURL)
			continue
		}
		sort.Strings(links) // Sorts alphabetically/lexicographically
		latestRelative := links[len(links)-1]

		// Resolve URL
		latestURL, err := base.Parse(latestRelative) // Handles relative/absolute links
		if err != nil {
			log.Printf("  WARN: Failed resolve ZIP URL '%s' relative to %s: %v. Skipping ZIP.", latestRelative, baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("resolve ZIP URL %s: %w", latestRelative, err))
			continue
		}
		zipURL := latestURL.String()
		log.Printf("  Found latest: %s", zipURL)

		// Download File
		log.Printf("   Downloading %s ...", zipURL)
		data, err := downloadFile(client, zipURL)
		if err != nil {
			log.Printf("   WARN: Failed download %s: %v. Skipping ZIP.", zipURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("download %s: %w", zipURL, err))
			continue
		}
		log.Printf("   Downloaded %d bytes.", len(data))

		// --- Extract CSV from ZIP ---
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			log.Printf("   WARN: Cannot open downloaded data from %s as ZIP: %v. Skipping ZIP.", zipURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("open zip %s: %w", zipURL, err))
			continue
		}

		extractedFromThisZip := 0
		for _, f := range zr.File {
			// Basic checks for validity
			if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
				continue
			}
			baseName := filepath.Base(f.Name) // Prevent path traversal
			if baseName == "" || baseName == "." || baseName == ".." || strings.HasPrefix(baseName, ".") {
				log.Printf("   WARN: Skipping potentially unsafe or hidden file in zip: %s", f.Name)
				continue
			}
			outPath := filepath.Join(dir, baseName)

			// Open file inside zip
			in, err := f.Open()
			if err != nil {
				log.Printf("   ERROR: Failed open '%s' within zip: %v", f.Name, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("open zip entry %s: %w", f.Name, err))
				continue // Skip this file
			}

			// Create output file
			out, err := os.Create(outPath)
			if err != nil {
				in.Close()
				log.Printf("   ERROR: Failed create output file %s: %v", outPath, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("create output %s: %w", outPath, err))
				continue // Skip this file
			}

			// Copy data
			copiedBytes, err := io.Copy(out, in)
			in.Close()  // Close reader
			out.Close() // Close writer *before* checking copy error

			if err != nil {
				log.Printf("   ERROR: Failed copy data for '%s' to %s: %v", f.Name, outPath, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("copy %s: %w", f.Name, err))
				os.Remove(outPath) // Attempt cleanup
				continue           // Skip this file
			}

			log.Printf("    Extracted '%s' (%d bytes) to %s", f.Name, copiedBytes, outPath)
			extractedFromThisZip++
			filesExtractedCount++
		} // End loop zip entries

		if extractedFromThisZip == 0 {
			log.Printf("  INFO: No *.csv files found or extracted within %s", zipURL)
		}
	} // End loop feedURLs

	log.Printf(" Finished download/extract phase. Extracted %d CSV files total.", filesExtractedCount)
	if filesExtractedCount == 0 {
		log.Println(" WARN: No CSV files were extracted from any source feeds.")
		// Could return specific error: return errors.New("no CSV files extracted")
	}

	return overallErr // Return collected non-fatal errors, or nil if none
}

// --- Utility Functions ---

// parseLinks finds <a> tag hrefs ending with suffix.
func parseLinks(n *html.Node, suffix string) []string {
	var out []string
	var walk func(*html.Node)
	walk = func(nd *html.Node) {
		if nd.Type == html.ElementNode && nd.Data == "a" {
			for _, a := range nd.Attr {
				if a.Key == "href" {
					// Check suffix case-insensitively
					if strings.HasSuffix(strings.ToLower(a.Val), strings.ToLower(suffix)) {
						out = append(out, a.Val)
					}
				}
			}
		}
		for c := nd.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}

// downloadFile GETs a URL and returns body bytes or error.
func downloadFile(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Optionally read some of the body for more context on error
		// limitReader := io.LimitReader(resp.Body, 1024)
		// bodyBytes, _ := io.ReadAll(limitReader)
		// return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, url, string(bodyBytes))
		return nil, fmt.Errorf("bad status '%s' fetching %s", resp.Status, url)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading body from %s: %w", url, err)
	}
	return bodyBytes, nil
}
