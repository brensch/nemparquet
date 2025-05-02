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
	// Use Glob for case-insensitivity if needed (though .CSV is standard)
	csvFiles, err := filepath.Glob(filepath.Join(*inputDir, "*.[cC][sS][vV]"))
	if err != nil {
		log.Fatalf("FATAL: Failed to search for CSV files in %s: %v", *inputDir, err)
	}
	if len(csvFiles) == 0 {
		log.Printf("INFO: No *.CSV files found in %s to process.", *inputDir)
		log.Println("NEM Parquet Converter finished.")
		return
	}

	log.Printf("Found %d CSV files to process.", len(csvFiles))

	processedCount := 0
	errorCount := 0
	for _, csvPath := range csvFiles {
		log.Printf("Processing %s...", csvPath)
		if err := processCSVSections(csvPath, *outputDir); err != nil {
			log.Printf("ERROR processing %s: %v", csvPath, err)
			errorCount++
		} else {
			processedCount++
			log.Printf("Finished processing %s successfully.", csvPath) // Moved success log here
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
// attempts to infer schema from the first data row *without blank fields*,
// and writes each section to a Parquet file using WriteString, handling blanks as NULLs.
func processCSVSections(csvPath, outDir string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open CSV %s: %w", csvPath, err)
	}
	defer file.Close()

	// Use PeekableScanner to allow looking ahead for the next record type
	scanner := NewPeekableScanner(file)
	// Consider increasing buffer size if necessary
	// const maxBuf = 4 * 1024 * 1024 // 4MB
	// buf := make([]byte, maxBuf)
	// scanner.Buffer(buf, maxBuf) // Apply to underlying scanner if using custom buffer

	var pw *writer.CSVWriter
	var fw source.ParquetFile
	var currentHeaders []string
	var currentComp, currentVer string
	var currentMeta []string        // Holds schema definition strings for the active section
	var schemaInferred bool = false // Flag to track if schema is set for the current section

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
				if err := pw.WriteStop(); err != nil {
					log.Printf("   WARN: Error stopping Parquet writer for section %s v%s: %v", currentComp, currentVer, err)
				} else {
					log.Printf("   Successfully stopped writer for %s v%s", currentComp, currentVer)
				}
				if err := fw.Close(); err != nil {
					log.Printf("   WARN: Error closing Parquet file for section %s v%s: %v", currentComp, currentVer, err)
				}
				pw = nil // Reset state for next section
				fw = nil
				currentMeta = nil
				schemaInferred = false // Reset schema flag
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
			schemaInferred = false // Mark that schema needs inference for this new section
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

			// --- Extract String Values (handle missing trailing commas) ---
			values := make([]string, len(currentHeaders)) // Initialize with empty strings
			numDataCols := len(rec) - 4
			hasBlanks := false // Flag for schema inference check
			for i := 0; i < len(currentHeaders); i++ {
				if i < numDataCols {
					values[i] = strings.TrimSpace(rec[i+4])
					if values[i] == "" {
						hasBlanks = true // Mark if any field is blank
					}
				} else {
					values[i] = ""   // Assign empty string if data column is missing
					hasBlanks = true // Missing columns count as blanks
				}
			}

			// --- Infer Schema and Initialize Writer (if not already done for this section) ---
			if !schemaInferred {
				// Check if the *next* record is also 'D'. If not, we must infer from the current row.
				peekedType, _ := scanner.PeekRecordType() // Ignore error on peek
				mustInferNow := hasBlanks && (peekedType != "D")

				if !hasBlanks || mustInferNow {
					// Infer schema from this row if it has no blanks OR if it's the last 'D' before 'I'/EOF
					if mustInferNow && hasBlanks {
						log.Printf("   WARN line %d: No non-blank data row found for schema inference before next section/EOF in %s v%s. Inferring from current row (line %d), blanks will default to STRING.", lineNumber, currentComp, currentVer, lineNumber)
					} else {
						log.Printf("   Inferring schema for %s v%s from data row at line %d", currentComp, currentVer, lineNumber)
					}

					currentMeta = make([]string, len(currentHeaders))
					for i := range currentHeaders {
						var typ string // Parquet type name
						val := strings.Trim(values[i], `"`)

						// Simple Type Inference (can be expanded)
						// Default to BYTE_ARRAY if blank during inference (especially if mustInferNow)
						if val == "" {
							typ = "BYTE_ARRAY" // Default blank fields to string during inference
						} else if val == "TRUE" || val == "FALSE" || strings.EqualFold(val, "true") || strings.EqualFold(val, "false") {
							typ = "BOOLEAN"
						} else if _, err := strconv.ParseInt(val, 10, 32); err == nil {
							typ = "INT32"
						} else if _, err := strconv.ParseInt(val, 10, 64); err == nil {
							typ = "INT64"
						} else if _, err := strconv.ParseFloat(val, 64); err == nil {
							// Could check for float32 range here if needed
							typ = "DOUBLE" // Use DOUBLE for safety
						} else {
							// Default to String for dates, or anything else
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
						currentHeaders = nil   // Stop processing this section
						schemaInferred = false // Reset schema flag (though section is skipped)
						continue               // Skip to next record ('I' or EOF)
					}
					pw, createErr = writer.NewCSVWriter(currentMeta, fw, 4) // Use concurrency 4
					if createErr != nil {
						log.Printf("ERROR line %d: Failed init Parquet CSV writer for %s: %v. Skipping section %s v%s.", lineNumber, path, createErr, currentComp, currentVer)
						fw.Close()           // Close the file handle we opened
						currentHeaders = nil // Stop processing this section
						schemaInferred = false
						continue // Skip to next record ('I' or EOF)
					}
					pw.CompressionType = parquet.CompressionCodec_SNAPPY // Set compression
					log.Printf("   Created writer for section %s v%s -> %s", currentComp, currentVer, path)
					log.Printf("   Schema: [%s]", strings.Join(currentMeta, "], ["))

					schemaInferred = true // Mark schema as inferred and writer initialized

				} else {
					// This row has blanks, and the next row might be 'D', so skip inference for now
					log.Printf("   Skipping schema inference on blank-containing row at line %d for %s v%s. Looking for cleaner row...", lineNumber, currentComp, currentVer)
					continue // Skip writing this row for now, move to the next line
				}
			} // End writer initialization block (!schemaInferred)

			// --- Write Data Row using WriteString (only if schema is inferred) ---
			if !schemaInferred || pw == nil {
				// Should not happen if logic above is correct, but safeguard
				// log.Printf("DEBUG line %d: Skipping write, schema not inferred or pw is nil (section %s v%s).", lineNumber, currentComp, currentVer)
				continue
			}

			// Prepare []*string slice for WriteString, handling blanks as nil for non-string types
			recPtrs := make([]*string, len(values))
			for j := 0; j < len(values); j++ { // <--- Loop uses 'j' correctly here
				isEmpty := values[j] == ""
				// Check the *inferred* schema type for this column
				isStringType := strings.Contains(currentMeta[j], "type=BYTE_ARRAY")

				if isEmpty && !isStringType {
					// If value is empty AND column type is NOT string (e.g., INT, DOUBLE, BOOLEAN),
					// pass a nil pointer to represent NULL for this OPTIONAL field.
					recPtrs[j] = nil
				} else {
					// Otherwise (value is not empty OR column is string type),
					// pass a pointer to the actual string value.
					// Use a temporary variable inside the loop scope to avoid capture issues.
					temp := values[j]
					recPtrs[j] = &temp
				}
			}

			if err := pw.WriteString(recPtrs); err != nil {
				// Log detailed error including the problematic data and inferred schema type
				problematicData := "N/A"
				schemaForProblem := "N/A"
				// Find the first column causing the issue (error message might not be specific)
				// This is a guess; the library might not pinpoint the exact column.
				for k := 0; k < len(values); k++ {
					isEmpty := values[k] == ""
					isStringType := strings.Contains(currentMeta[k], "type=BYTE_ARRAY")
					if isEmpty && !isStringType {
						continue
					} // Skip nils we intended

					// Try a minimal parse check based on schema (very basic)
					var parseErr error
					valStr := values[k]
					if strings.Contains(currentMeta[k], "type=BOOLEAN") {
						if !(strings.EqualFold(valStr, "true") || strings.EqualFold(valStr, "false") || valStr == "") {
							parseErr = errors.New("not bool")
						}
					} else if strings.Contains(currentMeta[k], "type=INT32") || strings.Contains(currentMeta[k], "type=INT64") {
						_, parseErr = strconv.ParseInt(valStr, 10, 64)
					} else if strings.Contains(currentMeta[k], "type=DOUBLE") || strings.Contains(currentMeta[k], "type=FLOAT") {
						_, parseErr = strconv.ParseFloat(valStr, 64)
					}
					// If a parse error likely occurred here, report this column
					if parseErr != nil {
						problematicData = fmt.Sprintf("Column %d ('%s'): '%s'", k, currentHeaders[k], values[k])
						schemaForProblem = currentMeta[k]
						break // Report first likely issue
					}
				}

				log.Printf("WARN line %d: WriteString error section %s v%s: %v. Schema: [%s]. Problematic Data Guess: [%s]. Raw Data: %v",
					lineNumber, currentComp, currentVer, err, schemaForProblem, problematicData, values)
				// Decide if processing should continue or stop for this file/section
			}

		// case "C": // Footer record - usually indicates end of file/transmission. Ignored for now.
		//  log.Printf("DEBUG line %d: Found 'C' record (ignored).", lineNumber)

		default: // Ignore other record types
			// log.Printf("DEBUG line %d: Skipping unknown record type '%s'", lineNumber, recordType)
			continue
		} // End switch recordType
	} // End for scanner.Scan()

	// --- Finalize the Very Last Section ---
	if pw != nil {
		log.Printf("   Finalizing last section %s v%s...", currentComp, currentVer)
		if err := pw.WriteStop(); err != nil {
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

	// Success log moved to main loop upon function return without error
	return nil // Success for this file
}

// --- File Download and Extraction ---

// downloadAndExtractLatest fetches the latest .zip from each feed URL
// and extracts its .CSV files into dir. Returns an error if critical steps fail.
func downloadAndExtractLatest(dir string) error {
	// Use a client with a timeout
	client := &http.Client{
		Timeout: 120 * time.Second, // Increased timeout for potentially large files
	}
	var overallErr error // Track non-fatal errors
	filesExtractedCount := 0

	for _, baseURL := range feedURLs {
		log.Printf(" Checking feed: %s", baseURL)
		resp, err := client.Get(baseURL)
		if err != nil {
			log.Printf("   WARN: Failed GET %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("GET %s: %w", baseURL, err))
			continue
		}

		// Read body first, then close
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close() // Close immediately
		if readErr != nil {
			log.Printf("   WARN: Failed read body %s: %v. Skipping URL.", baseURL, readErr)
			overallErr = errors.Join(overallErr, fmt.Errorf("read body %s: %w", baseURL, readErr))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("   WARN: Bad status '%s' for %s. Skipping URL.", resp.Status, baseURL)
			overallErr = errors.Join(overallErr, fmt.Errorf("bad status %s for %s", resp.Status, baseURL))
			continue
		}

		// Parse HTML
		root, err := html.Parse(bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf("   WARN: Failed parse HTML %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse HTML %s: %w", baseURL, err))
			continue
		}

		// Parse Base URL
		base, err := url.Parse(baseURL)
		if err != nil {
			// This is less likely but critical if it happens
			log.Printf("   ERROR: Failed parse base URL %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse base URL %s: %w", baseURL, err))
			continue
		}

		// Find and Sort Links
		links := parseLinks(root, ".zip")
		if len(links) == 0 {
			log.Printf("   INFO: No *.zip files found linked at %s", baseURL)
			continue
		}
		sort.Strings(links) // Sorts alphabetically/lexicographically (usually time-based names work)
		latestRelative := links[len(links)-1]

		// Resolve URL
		latestURL, err := base.Parse(latestRelative) // Handles relative/absolute links
		if err != nil {
			log.Printf("   WARN: Failed resolve ZIP URL '%s' relative to %s: %v. Skipping ZIP.", latestRelative, baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("resolve ZIP URL %s: %w", latestRelative, err))
			continue
		}
		zipURL := latestURL.String()
		log.Printf("   Found latest: %s", zipURL)

		// --- Download File ---
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
			// Use EqualFold for case-insensitive extension check
			if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
				continue
			}
			// Sanitize filename: Use Base, check for empty/dots, remove potentially harmful chars
			cleanBaseName := filepath.Base(f.Name)
			if cleanBaseName == "" || cleanBaseName == "." || cleanBaseName == ".." || strings.HasPrefix(cleanBaseName, ".") {
				log.Printf("   WARN: Skipping potentially unsafe or hidden file in zip: %s", f.Name)
				continue
			}
			// Further sanitize if needed (e.g., remove control characters)
			// cleanBaseName = sanitizeFilename(cleanBaseName)

			outPath := filepath.Join(dir, cleanBaseName)

			// Check if file already exists (optional: overwrite or skip?)
			// if _, err := os.Stat(outPath); err == nil {
			// 	log.Printf("   INFO: Skipping extraction, file already exists: %s", outPath)
			// 	continue
			// }

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

			log.Printf("     Extracted '%s' (%d bytes) to %s", f.Name, copiedBytes, outPath)
			extractedFromThisZip++
			filesExtractedCount++
		} // End loop zip entries

		if extractedFromThisZip == 0 {
			log.Printf("   INFO: No *.csv files found or extracted within %s", zipURL)
		}
	} // End loop feedURLs

	log.Printf(" Finished download/extract phase. Extracted %d CSV files total.", filesExtractedCount)
	if filesExtractedCount == 0 && overallErr == nil { // Only warn if no files AND no other errors occurred
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
		limitReader := io.LimitReader(resp.Body, 1024)
		bodyBytes, _ := io.ReadAll(limitReader)
		return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, url, string(bodyBytes))
		// return nil, fmt.Errorf("bad status '%s' fetching %s", resp.Status, url)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading body from %s: %w", url, err)
	}
	return bodyBytes, nil
}

// --- Peekable Scanner Wrapper ---

// PeekableScanner wraps bufio.Scanner to allow peeking at the next line's type.
type PeekableScanner struct {
	scanner    *bufio.Scanner
	peekedLine *string  // Stores the peeked line
	peekErr    error    // Stores error from peeking
	file       *os.File // Keep ref for potential underlying buffer changes
}

// NewPeekableScanner creates a new scanner wrapper.
func NewPeekableScanner(f *os.File) *PeekableScanner {
	return &PeekableScanner{
		scanner: bufio.NewScanner(f),
		file:    f,
	}
}

// Scan advances the scanner, consuming the peeked line if it exists.
func (ps *PeekableScanner) Scan() bool {
	if ps.peekedLine != nil {
		// Consume the peeked line, reset state
		ps.peekedLine = nil
		ps.peekErr = nil
		return true // We had a peeked line, so Scan succeeds
	}
	// No peeked line, perform a normal scan
	return ps.scanner.Scan()
}

// Text returns the current line (either the peeked one or the one from the last Scan).
func (ps *PeekableScanner) Text() string {
	if ps.peekedLine != nil {
		// This shouldn't normally be called if Scan() wasn't called after Peek,
		// but return the peeked line if it exists.
		return *ps.peekedLine
	}
	return ps.scanner.Text()
}

// Err returns the last error encountered by the scanner or during peeking.
func (ps *PeekableScanner) Err() error {
	if ps.peekErr != nil {
		return ps.peekErr
	}
	return ps.scanner.Err()
}

// PeekRecordType looks at the next line without advancing the main scanner position.
// It returns the record type (e.g., "D", "I", "C") or an empty string if EOF/error/blank.
func (ps *PeekableScanner) PeekRecordType() (string, error) {
	if ps.peekedLine != nil {
		// Already peeked, return the type from the stored line
		return ps.extractRecordType(*ps.peekedLine), ps.peekErr
	}

	// Need to peek ahead
	if ps.scanner.Scan() {
		line := ps.scanner.Text()
		ps.peekedLine = &line // Store the scanned line
		ps.peekErr = nil
		return ps.extractRecordType(line), nil
	} else {
		// Scan failed (EOF or error)
		ps.peekErr = ps.scanner.Err() // Store the error
		ps.peekedLine = nil           // Ensure no line is stored
		return "", ps.peekErr
	}
}

// extractRecordType gets the first comma-separated value.
func (ps *PeekableScanner) extractRecordType(line string) string {
	trimmedLine := strings.TrimSpace(line)
	if trimmedLine == "" {
		return ""
	}
	parts := strings.SplitN(trimmedLine, ",", 2) // Split only on the first comma
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return "" // Should not happen if line wasn't blank
}

// Buffer sets the buffer for the underlying scanner.
// NOTE: This should be called *before* any Scan or Peek operations.
func (ps *PeekableScanner) Buffer(buf []byte, max int) {
	ps.scanner.Buffer(buf, max)
}
