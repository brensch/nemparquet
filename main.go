package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime" // Added for multi-core
	"strconv"
	"strings" // Added for concurrency
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
)

// Directories hosting the CSV feeds on NEMWEB
var feedURLs = []string{
	"https://nemweb.com.au/Reports/Current/FPP/",
	"https://nemweb.com.au/Reports/Current/FPPDAILY/",
	"https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	"https://nemweb.com.au/Reports/Current/PD7Day/",
	"https://nemweb.com.au/Reports/Current/P5_Reports/",
	// Add archive URLs here if needed
}

// --- Time Zone and Date Handling ---

var (
	// NEM time is UTC+10, fixed offset.
	nemLocation *time.Location
	// Regex to identify the specific date format YYYY/MM/DD HH:MI:SS, optionally surrounded by quotes.
	nemDateTimeRegex *regexp.Regexp
)

func init() {
	// Define the NEM time zone location (UTC+10)
	nemLocation = time.FixedZone("NEM", 10*60*60) // 10 hours * 60 mins * 60 secs

	// Compile the regex for date format detection
	// Matches "YYYY/MM/DD HH:MI:SS" exactly, allowing for optional surrounding quotes
	nemDateTimeRegex = regexp.MustCompile(`^"?\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}"?$`)
}

// isNEMDateTime checks if a string matches the YYYY/MM/DD HH:MI:SS format (with optional quotes).
func isNEMDateTime(s string) bool {
	return nemDateTimeRegex.MatchString(s)
}

// nemDateTimeToEpochMS converts a "YYYY/MM/DD HH:MI:SS" string (in NEMtime),
// potentially surrounded by double quotes, to Unix epoch milliseconds (int64).
// Returns 0 and error if parsing fails.
func nemDateTimeToEpochMS(s string) (int64, error) {
	// Trim surrounding double quotes before parsing
	trimmedS := strings.Trim(s, `"`)

	// Define the layout matching the input string format (without quotes)
	layout := "2006/01/02 15:04:05" // Go's reference time format matching YYYY/MM/DD HH:MI:SS
	t, err := time.ParseInLocation(layout, trimmedS, nemLocation)
	if err != nil {
		// Return the original string 's' in the error message for better debugging
		return 0, fmt.Errorf("failed to parse NEM time (after trimming quotes) from '%s': %w", s, err)
	}
	// Return milliseconds since epoch
	return t.UnixMilli(), nil
}

// --- Constants ---
const (
	// Maximum number of 'D' rows to check for schema inference before giving up on finding a clean row.
	schemaInferenceRowLimit = 100
)

// --- Main Application Logic ---
func main() {
	inputDir := flag.String("input", "./input_csv", "Directory to download & extract CSVs into")
	outputDir := flag.String("output", "./output_parquet", "Directory to write Parquet files to")
	dbPath := flag.String("db", ":memory:", "DuckDB database file path (use ':memory:' for in-memory)")
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Number of concurrent workers for Parquet encoding") // Added worker flag
	flag.Parse()

	log.Printf("Starting NEM Parquet Converter...")
	log.Printf("Input directory: %s", *inputDir)
	log.Printf("Output directory: %s", *outputDir)
	log.Printf("DuckDB path: %s", *dbPath)
	log.Printf("Parquet encoding workers: %d", *numWorkers)               // Log worker count
	log.Printf("Schema inference row limit: %d", schemaInferenceRowLimit) // Log the limit

	// Ensure directories exist
	for _, d := range []string{*inputDir, *outputDir} {
		if d != "" && d != ":memory:" {
			if err := os.MkdirAll(d, 0o755); err != nil {
				log.Fatalf("FATAL: Failed to create directory %s: %v", d, err)
			}
		}
	}

	// // --- Download and Extract ---
	// log.Println("--- Downloading and Extracting CSVs ---")
	// if err := downloadAndExtractFeedFiles(*inputDir); err != nil {
	// 	log.Printf("WARN: Download/extraction process encountered errors: %v", err)
	// }

	// // --- Process CSVs (Parallelized) ---
	// log.Println("--- Processing CSV Files (Parallel) ---")
	// csvFiles, err := filepath.Glob(filepath.Join(*inputDir, "*.[cC][sS][vV]"))
	// if err != nil {
	// 	log.Fatalf("FATAL: Failed to search for CSV files in %s: %v", *inputDir, err)
	// }
	// if len(csvFiles) == 0 {
	// 	log.Printf("INFO: No *.CSV files found in %s to process.", *inputDir)
	// 	log.Println("NEM Parquet Converter finished (no CSVs found).")
	// 	return
	// }

	// log.Printf("Found %d CSV files to process.", len(csvFiles))

	// // Setup for parallel processing
	// filePaths := make(chan string, len(csvFiles)) // Channel to send file paths
	// var wg sync.WaitGroup
	// var mu sync.Mutex // Mutex to protect shared counters
	// processedCount := 0
	// skippedCount := 0 // Counter for skipped files
	// errorCount := 0

	// // Start worker goroutines
	// for i := 0; i < *numWorkers; i++ {
	// 	wg.Add(1)
	// 	go func(workerID int) {
	// 		defer wg.Done()
	// 		log.Printf("Worker %d started.", workerID)
	// 		for csvPath := range filePaths {
	// 			log.Printf("Worker %d processing %s...", workerID, csvPath)
	// 			// processCSVSections now returns a boolean indicating if it was skipped
	// 			skipped, err := processCSVSections(csvPath, *outputDir)

	// 			mu.Lock()
	// 			if err != nil {
	// 				log.Printf("Worker %d ERROR processing %s: %v", workerID, csvPath, err)
	// 				errorCount++
	// 			} else if skipped {
	// 				skippedCount++
	// 				log.Printf("Worker %d Skipped processing %s (output files exist).", workerID, csvPath)
	// 			} else {
	// 				processedCount++
	// 				log.Printf("Worker %d Finished processing %s successfully.", workerID, csvPath)
	// 			}
	// 			mu.Unlock()
	// 		}
	// 		log.Printf("Worker %d finished.", workerID)
	// 	}(i)
	// }

	// // Send file paths to the channel
	// for _, csvPath := range csvFiles {
	// 	filePaths <- csvPath
	// }
	// close(filePaths) // Close the channel to signal workers no more jobs

	// // Wait for all workers to finish
	// wg.Wait()

	// log.Println("--- Processing Summary ---")
	// mu.Lock() // Lock to safely read final counts
	// log.Printf("Successfully processed %d CSV files.", processedCount)
	// log.Printf("Skipped %d CSV files (output already exists).", skippedCount)
	// if errorCount > 0 {
	// 	log.Printf("Encountered errors processing %d CSV files.", errorCount)
	// }
	// mu.Unlock()

	// // --- Inspect Parquet Files ---
	// // This step runs after all processing is complete.
	// // Check if *any* file was processed or skipped (meaning output might exist)
	// mu.Lock()
	// shouldInspect := processedCount > 0 || skippedCount > 0
	// mu.Unlock()
	// if shouldInspect {
	// 	log.Println("--- Inspecting Parquet File Schemas & Counts ---")
	// 	absOutputDir, err := filepath.Abs(*outputDir)
	// 	if err != nil {
	// 		log.Printf("ERROR getting absolute path for output directory %s: %v", *outputDir, err)
	// 		log.Println("Skipping Parquet file inspection.")
	// 	} else {
	// 		if err := inspectParquetFiles(absOutputDir, *dbPath); err != nil {
	// 			log.Printf("ERROR inspecting Parquet files: %v", err)
	// 		} else {
	// 			log.Println("Parquet file inspection finished.")
	// 		}
	// 	}
	// } else {
	// 	log.Println("Skipping Parquet file inspection: No CSV files were processed or skipped.")
	// }

	// --- Run DuckDB FPP Analysis ---
	// This step runs after all processing is complete.
	// Only run if there were no *processing* errors and some files were processed or skipped.
	mu.Lock() // Lock to safely read errorCount and processedCount/skippedCount
	canRunAnalysis := errorCount == 0 && (processedCount > 0 || skippedCount > 0)
	mu.Unlock()

	if canRunAnalysis {
		log.Println("--- Running DuckDB FPP Analysis ---")
		absOutputDir, err := filepath.Abs(*outputDir)
		if err != nil {
			log.Printf("ERROR getting absolute path for output directory %s: %v", *outputDir, err)
			log.Println("Skipping DuckDB Analysis.")
		} else {
			analysisCtx := context.Background()
			if err := runDuckDBAnalysis(analysisCtx, absOutputDir, *dbPath); err != nil {
				log.Printf("ERROR running DuckDB analysis: %v", err)
			} else {
				log.Println("DuckDB FPP Analysis finished successfully.")
			}
		}
	} else if processedCount == 0 && skippedCount == 0 {
		log.Println("Skipping DuckDB Analysis: No relevant Parquet files found or generated.")
	} else { // errorCount > 0
		log.Println("Skipping DuckDB Analysis due to CSV processing errors.")
	}

	log.Println("NEM Parquet Converter finished.")
}

// checkExistingOutputFiles scans a CSV to find potential output filenames and checks if they exist.
// Returns true if *all* potential outputs exist, false otherwise.
// Returns an error if the preliminary scan fails significantly.
func checkExistingOutputFiles(csvPath, outDir string) (bool, error) {
	baseName := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))
	potentialOutputs := []string{}

	file, err := os.Open(csvPath)
	if err != nil {
		// If we can't even open the input, we can't check. Assume it needs processing.
		log.Printf(" [%s] WARN: Could not open input CSV for pre-check: %v. Assuming processing is needed.", baseName, err)
		return false, nil // Not an error preventing processing, just the check itself failed.
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Use a reasonable buffer for the pre-check scan
	scanner.Buffer(make([]byte, 64*1024), 1*1024*1024) // 64KB initial, 1MB max

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

		if recordType == "I" {
			if len(rec) < 5 {
				log.Printf("[%s] WARN line %d (pre-check): Malformed 'I' record: %s. Skipping.", baseName, lineNumber, line)
				continue
			}
			comp, ver := strings.TrimSpace(rec[2]), strings.TrimSpace(rec[3])
			if comp != "" && ver != "" {
				// Sanitize component and version slightly for filename robustness if needed
				// (Current logic is basic; more complex sanitization could be added)
				fileName := fmt.Sprintf("%s_%s_v%s.parquet", baseName, comp, ver)
				potentialOutputs = append(potentialOutputs, fileName)
			} else {
				log.Printf("[%s] WARN line %d (pre-check): 'I' record has empty component or version: %s. Skipping.", baseName, lineNumber, line)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		// If scanning failed, we didn't get a full list. Assume processing is needed.
		log.Printf("[%s] WARN: Error during pre-check scan near line %d: %v. Assuming processing is needed.", baseName, lineNumber, err)
		return false, nil // Not an error preventing processing
	}

	if len(potentialOutputs) == 0 {
		// No 'I' records found? Maybe an empty or invalid CSV. Assume processing is needed
		// (the main function will handle it properly).
		log.Printf(" [%s] INFO: No 'I' records found during pre-check. Proceeding with processing.", baseName)
		return false, nil
	}

	// Check if all potential output files exist
	log.Printf(" [%s] Pre-checking existence of %d potential output files...", baseName, len(potentialOutputs))
	allExist := true
	for _, fname := range potentialOutputs {
		fpath := filepath.Join(outDir, fname)
		if _, err := os.Stat(fpath); os.IsNotExist(err) {
			log.Printf(" [%s]   -> Output file %s does NOT exist. Processing required.", baseName, fname)
			allExist = false
			break // No need to check further
		} else if err != nil {
			// Some other error checking the file (e.g., permissions)
			log.Printf(" [%s] WARN: Error checking status of %s: %v. Assuming processing is needed.", baseName, fname, err)
			allExist = false
			break
		}
		// If Stat returns nil error, the file exists. Continue checking.
	}

	return allExist, nil // Return the check result and no error from the check itself
}

// --- Core CSV to Parquet Conversion ---
// Modified to return (bool, error) where bool indicates if processing was skipped.
func processCSVSections(csvPath, outDir string) (skipped bool, err error) {
	baseName := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))

	// --- Check if output files already exist ---
	allOutputsExist, checkErr := checkExistingOutputFiles(csvPath, outDir)
	if checkErr != nil {
		// Log the error from the check function, but proceed with processing anyway
		log.Printf("[%s] ERROR during output file pre-check: %v. Attempting to process regardless.", baseName, checkErr)
	}
	if allOutputsExist {
		// Skip processing this file entirely
		return true, nil // Signal that it was skipped, no error
	}
	// --- End Check ---

	// --- Proceed with normal processing if not skipped ---
	file, err := os.Open(csvPath)
	if err != nil {
		return false, fmt.Errorf("open CSV %s: %w", csvPath, err) // Return false (not skipped), and the error
	}
	defer file.Close()

	scanner := NewPeekableScanner(file)
	var pw *writer.CSVWriter
	var fw source.ParquetFile
	var currentHeaders []string
	var currentComp, currentVer string
	var currentMeta []string // Holds schema definition strings
	var isDateColumn []bool  // Tracks which columns were inferred as dates
	var schemaInferred bool = false
	var rowsCheckedForSchema int = 0 // Counter for schema inference attempt

	lineNumber := 0

	// Increase buffer size for potentially long lines
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024) // 64KB initial, max 10MB

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
			// Finish previous section if one was active
			if pw != nil {
				log.Printf("   [%s] Finalizing section %s v%s...", baseName, currentComp, currentVer)
				if err := pw.WriteStop(); err != nil {
					// Log error but continue with next section
					log.Printf("   [%s] WARN: Error stopping Parquet writer for section %s v%s: %v", baseName, currentComp, currentVer, err)
				} else {
					log.Printf("   [%s] Successfully stopped writer for %s v%s", baseName, currentComp, currentVer)
				}
				if err := fw.Close(); err != nil {
					log.Printf("   [%s] WARN: Error closing Parquet file for section %s v%s: %v", baseName, currentComp, currentVer, err)
				}
				pw = nil
				fw = nil
				currentMeta = nil
				isDateColumn = nil
				schemaInferred = false
			}
			if len(rec) < 5 {
				log.Printf("[%s] WARN line %d: Malformed 'I' record (less than 5 fields): %s. Skipping section.", baseName, lineNumber, line)
				currentHeaders = nil // Invalidate headers
				continue
			}
			currentComp, currentVer = strings.TrimSpace(rec[2]), strings.TrimSpace(rec[3])
			// Extract headers, handling potential trailing empty strings from Split
			currentHeaders = make([]string, 0, len(rec)-4)
			for _, h := range rec[4:] {
				currentHeaders = append(currentHeaders, strings.TrimSpace(h))
			}
			if len(currentHeaders) == 0 {
				log.Printf("[%s] WARN line %d: 'I' record has no header columns. Skipping section %s v%s.", baseName, lineNumber, currentComp, currentVer)
				continue // Skip this section
			}
			schemaInferred = false   // Reset for the new section
			rowsCheckedForSchema = 0 // Reset schema check counter for the new section
			log.Printf(" [%s] Found section header %s v%s at line %d", baseName, currentComp, currentVer, lineNumber)

		case "D":
			if len(currentHeaders) == 0 {
				// Skip data rows if no valid header was found for the current section
				continue
			}
			if len(rec) < 4 {
				log.Printf("[%s] WARN line %d: Malformed 'D' record (less than 4 fields): %s. Skipping row.", baseName, lineNumber, line)
				continue
			}
			// Extract values, corresponding to the expected number of headers
			values := make([]string, len(currentHeaders))
			numDataColsInRecord := len(rec) - 4 // Number of data fields actually present in this row
			hasBlanks := false
			for i := 0; i < len(currentHeaders); i++ {
				// Check bounds to avoid index out of range if row has fewer fields than headers
				if i < numDataColsInRecord {
					// Trim surrounding double quotes and space from individual data values
					values[i] = strings.TrimSpace(strings.Trim(rec[i+4], `"`))
					if values[i] == "" {
						hasBlanks = true
					}
				} else {
					// If the row has fewer columns than expected headers, treat missing columns as blank
					values[i] = ""
					hasBlanks = true
				}
			}

			// Infer Schema and Initialize Writer
			if !schemaInferred {
				rowsCheckedForSchema++ // Increment check counter for this section

				// Peek ahead to see if the next line is also a 'D' record
				peekedType, peekErr := scanner.PeekRecordType()

				// Determine if we MUST infer the schema now:
				// - Current row has no blanks, OR
				// - We can't peek ahead (error or EOF), OR
				// - The next line is NOT a data ('D') record, OR
				// - We've exceeded the row check limit for schema inference
				mustInferNow := !hasBlanks || (peekErr != nil || peekedType != "D") || rowsCheckedForSchema >= schemaInferenceRowLimit

				if mustInferNow {
					// Log warnings if inference is forced under non-ideal conditions
					if hasBlanks && rowsCheckedForSchema >= schemaInferenceRowLimit {
						log.Printf(" [%s] WARN line %d: Reached schema inference limit (%d rows) for section %s v%s. Inferring schema from current row which contains blanks. Blanks will default to STRING/nullable.", baseName, lineNumber, schemaInferenceRowLimit, currentComp, currentVer)
					} else if hasBlanks && (peekErr != nil || peekedType != "D") {
						log.Printf(" [%s] WARN line %d: No non-blank data row found for schema inference before next section/EOF. Inferring from current row, blanks will default to STRING/nullable. Section %s v%s.", baseName, lineNumber, currentComp, currentVer)
					} else {
						log.Printf(" [%s] Inferring schema for %s v%s from data row at line %d (attempt %d)", baseName, currentComp, currentVer, lineNumber, rowsCheckedForSchema)
					}

					// --- Start Schema Inference and Writer Initialization ---
					currentMeta = make([]string, len(currentHeaders))
					isDateColumn = make([]bool, len(currentHeaders)) // Initialize date tracking
					for i := range currentHeaders {
						var typ string
						val := values[i] // Use the value from the current row for inference

						// Infer type based on value content (Order matters)
						isDate := false
						if isNEMDateTime(val) {
							typ = "INT64" // Store dates as epoch milliseconds
							isDate = true
						} else if val == "" {
							typ = "BYTE_ARRAY" // Blank defaults to string (nullable)
						} else if val == "TRUE" || val == "FALSE" || strings.EqualFold(val, "true") || strings.EqualFold(val, "false") {
							typ = "BOOLEAN"
						} else if _, err := strconv.ParseInt(val, 10, 64); err == nil {
							typ = "INT64" // Use INT64 for flexibility
						} else if _, err := strconv.ParseFloat(val, 64); err == nil {
							typ = "DOUBLE"
						} else {
							typ = "BYTE_ARRAY" // Default to string
						}
						isDateColumn[i] = isDate

						// Clean header name for Parquet columns
						cleanHeader := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(currentHeaders[i], " ", "_"), ".", "_"), ";", "_")
						if cleanHeader == "" {
							cleanHeader = fmt.Sprintf("column_%d", i) // Fallback
						}

						// Build schema definition string (make all optional for robustness)
						if typ == "BYTE_ARRAY" {
							currentMeta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", cleanHeader)
						} else {
							currentMeta[i] = fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", cleanHeader, typ)
						}
					}
					log.Printf(" [%s]     Inferred Schema for %s v%s: [%s]", baseName, currentComp, currentVer, strings.Join(currentMeta, "], ["))

					// Initialize Parquet writer
					parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", baseName, currentComp, currentVer)
					path := filepath.Join(outDir, parquetFile)
					var createErr error
					fw, createErr = local.NewLocalFileWriter(path)
					if createErr != nil {
						log.Printf("[%s] ERROR line %d: Failed create parquet file %s: %v. Skipping section %s v%s.", baseName, lineNumber, path, createErr, currentComp, currentVer)
						currentHeaders = nil   // Invalidate headers
						schemaInferred = false // Allow retrying inference if another 'I' appears later
						continue               // Skip to the next line in the CSV
					}

					pw, createErr = writer.NewCSVWriter(currentMeta, fw, 4) // Use 4 goroutines
					if createErr != nil {
						log.Printf("[%s] ERROR line %d: Failed init Parquet CSV writer for %s: %v. Skipping section %s v%s.", baseName, lineNumber, path, createErr, currentComp, currentVer)
						fw.Close()           // Attempt to close file
						pw = nil             // Ensure pw is nil
						currentHeaders = nil // Invalidate headers
						schemaInferred = false
						continue // Skip to the next line
					}
					pw.CompressionType = parquet.CompressionCodec_SNAPPY
					log.Printf(" [%s]   Created writer for section %s v%s -> %s", baseName, currentComp, currentVer, path)
					schemaInferred = true // Mark schema as inferred
					// --- End Schema Inference and Writer Initialization ---

				} else {
					// Schema not inferred yet, current row has blanks, next is 'D', limit not reached.
					// Skip this blank row and wait for a cleaner one (or the limit).
					log.Printf(" [%s]   Skipping schema inference on blank-containing row at line %d for %s v%s. Looking for cleaner row (attempt %d/%d)...", baseName, lineNumber, currentComp, currentVer, rowsCheckedForSchema, schemaInferenceRowLimit)
					continue // Skip this row, go to the next scan
				}
			} // End of if !schemaInferred

			// Write Data Row (only if schema was successfully inferred and writer is ready)
			if !schemaInferred || pw == nil {
				continue // Should not happen if logic above is correct, but safeguards
			}

			// Prepare data pointers for writing
			recPtrs := make([]*string, len(values))
			for j := 0; j < len(values); j++ {
				isEmpty := values[j] == ""
				finalValue := values[j]

				// Convert date string to epoch milliseconds if it's a date column and not empty
				if isDateColumn[j] && !isEmpty {
					epochMS, convErr := nemDateTimeToEpochMS(values[j]) // Use convErr to avoid shadowing outer err
					if convErr != nil {
						originalVal := "N/A"
						if j+4 < len(rec) {
							originalVal = rec[j+4]
						}
						log.Printf(" [%s] WARN line %d: Failed convert date '%s' (original: '%s') col '%s': %v. Writing NULL.", baseName, lineNumber, values[j], originalVal, currentHeaders[j], convErr)
						recPtrs[j] = nil
						continue
					}
					finalValue = strconv.FormatInt(epochMS, 10)
				}

				isTargetStringType := false
				if j < len(currentMeta) {
					isTargetStringType = strings.Contains(currentMeta[j], "type=BYTE_ARRAY")
				}

				// Treat empty value as NULL *unless* the target type is string
				if isEmpty && !isTargetStringType {
					recPtrs[j] = nil
				} else {
					temp := finalValue
					recPtrs[j] = &temp
				}
			}

			// Write the row to the Parquet writer
			if writeErr := pw.WriteString(recPtrs); writeErr != nil { // Use writeErr
				// Attempt to provide context on write errors
				problematicData := "N/A"
				schemaForProblem := "N/A"
				for k := 0; k < len(recPtrs); k++ {
					if recPtrs[k] != nil {
						valStr := *recPtrs[k]
						var parseErr error = nil
						if k < len(currentMeta) {
							meta := currentMeta[k]
							// Simplified type checks based on schema string
							if isDateColumn[k] { // Expecting INT64 string here
								_, parseErr = strconv.ParseInt(valStr, 10, 64)
							} else if strings.Contains(meta, "type=BOOLEAN") {
								if !(strings.EqualFold(valStr, "true") || strings.EqualFold(valStr, "false") || valStr == "") {
									parseErr = errors.New("not a valid boolean string")
								}
							} else if strings.Contains(meta, "type=INT") {
								_, parseErr = strconv.ParseInt(valStr, 10, 64)
							} else if strings.Contains(meta, "type=DOUBLE") || strings.Contains(meta, "type=FLOAT") {
								_, parseErr = strconv.ParseFloat(valStr, 64)
							}
						}
						if parseErr != nil {
							problematicData = fmt.Sprintf("Column %d ('%s'): prepared value '%s'", k, currentHeaders[k], valStr)
							if k+4 < len(rec) {
								problematicData += fmt.Sprintf(" (original CSV: '%s')", rec[k+4])
							}
							if k < len(currentMeta) {
								schemaForProblem = currentMeta[k]
							} else {
								schemaForProblem = "Schema meta OOB"
							}
							break
						}
					}
				}
				log.Printf("[%s] WARN line %d: WriteString error section %s v%s: %v. Schema: [%s]. Cause: %s. Raw: %v",
					baseName, lineNumber, currentComp, currentVer, writeErr, schemaForProblem, problematicData, values)
				// Continue processing other rows
			}

		// case "T": // Trailer - handled by EOF
		default:
			// Ignore other record types ('C', 'H', etc.)
			continue
		}
	} // End scanner loop

	// Finalize the last section if one was active
	if pw != nil {
		log.Printf(" [%s] Finalizing last section %s v%s...", baseName, currentComp, currentVer)
		if err := pw.WriteStop(); err != nil {
			log.Printf("[%s] ERROR: Failed stop writer last section %s v%s: %v", baseName, currentComp, currentVer, err)
			// Accumulate error? For now, just log.
		} else {
			log.Printf(" [%s] Successfully stopped writer last section %s v%s", baseName, currentComp, currentVer)
		}
		if err := fw.Close(); err != nil {
			log.Printf("[%s] ERROR: Failed close file last section %s v%s: %v", baseName, currentComp, currentVer, err)
			// Accumulate error?
		}
		pw = nil
		fw = nil
	}

	// Check for scanner errors
	if scanErr := scanner.Err(); scanErr != nil { // Use scanErr
		if errors.Is(scanErr, bufio.ErrTooLong) {
			log.Printf("[%s] ERROR: Scanner error: line too long near line %d. Increase buffer.", baseName, lineNumber)
		} else {
			log.Printf("[%s] ERROR: Scanner error near line %d: %v", baseName, lineNumber, scanErr)
		}
		return false, fmt.Errorf("scanner error CSV %s: %w", csvPath, scanErr) // Return scanner error
	}

	return false, nil // Processed successfully (or with non-fatal write warnings), not skipped
}

// --- File Download and Extraction ---
// (No changes needed in downloadAndExtractFeedFiles)
func downloadAndExtractFeedFiles(dir string) error {
	client := &http.Client{Timeout: 120 * time.Second}
	var overallErr error          // Use errors.Join (Go 1.20+) or a slice for multiple errors
	totalFilesExtractedCount := 0 // Track total across all feeds

	for _, baseURL := range feedURLs {
		log.Printf(" Checking feed: %s", baseURL)
		resp, err := client.Get(baseURL)
		if err != nil {
			log.Printf(" 	WARN: Failed GET %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("GET %s: %w", baseURL, err))
			continue
		}
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Printf(" 	WARN: Failed read body %s: %v. Skipping URL.", baseURL, readErr)
			overallErr = errors.Join(overallErr, fmt.Errorf("read body %s: %w", baseURL, readErr))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf(" 	WARN: Bad status '%s' for %s. Skipping URL.", resp.Status, baseURL)
			overallErr = errors.Join(overallErr, fmt.Errorf("bad status %s for %s", resp.Status, baseURL))
			continue
		}
		root, err := html.Parse(bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf(" 	WARN: Failed parse HTML %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse HTML %s: %w", baseURL, err))
			continue
		}
		base, err := url.Parse(baseURL)
		if err != nil {
			log.Printf(" 	ERROR: Failed parse base URL %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse base URL %s: %w", baseURL, err))
			continue
		}

		// Find ALL Links
		links := parseLinks(root, ".zip")
		if len(links) == 0 {
			log.Printf(" 	INFO: No *.zip files found linked at %s", baseURL)
			continue
		}

		log.Printf(" 	Found %d zip files at %s. Processing all...", len(links), baseURL)

		// --- Loop through ALL found links ---
		for _, relativeLink := range links {
			// Resolve URL
			zipURLAbs, err := base.Parse(relativeLink) // Handles relative/absolute links
			if err != nil {
				log.Printf(" 	WARN: Failed resolve ZIP URL '%s' relative to %s: %v. Skipping ZIP.", relativeLink, baseURL, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("resolve ZIP URL %s: %w", relativeLink, err))
				continue
			}
			zipURL := zipURLAbs.String()
			log.Printf(" 	Processing: %s", zipURL) // Changed log from "Found latest"

			// --- Download File ---
			log.Printf(" 	 	Downloading %s ...", zipURL)
			data, err := downloadFile(client, zipURL)
			if err != nil {
				log.Printf(" 	 	WARN: Failed download %s: %v. Skipping ZIP.", zipURL, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("download %s: %w", zipURL, err))
				continue
			}
			log.Printf(" 	 	Downloaded %d bytes.", len(data))

			// --- Extract CSV from ZIP ---
			zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				log.Printf(" 	 	WARN: Cannot open downloaded data from %s as ZIP: %v. Skipping ZIP.", zipURL, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("open zip %s: %w", zipURL, err))
				continue
			}

			extractedFromThisZip := 0
			for _, f := range zr.File {
				if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
					continue
				}
				// Sanitize filename before joining with directory
				cleanBaseName := filepath.Base(f.Name)
				if cleanBaseName == "" || cleanBaseName == "." || cleanBaseName == ".." || strings.HasPrefix(cleanBaseName, ".") {
					log.Printf(" 	 	WARN: Skipping potentially unsafe or hidden file in zip: %s", f.Name)
					continue
				}
				outPath := filepath.Join(dir, cleanBaseName)

				// Check if file already exists (optional: overwrite or skip?)
				// Consider adding a flag to control overwrite behavior
				if _, err := os.Stat(outPath); err == nil {
					log.Printf(" 	 	INFO: Skipping extraction, file already exists: %s", outPath)
					continue // Skip if file exists
				}

				in, err := f.Open()
				if err != nil {
					log.Printf(" 	 	ERROR: Failed open '%s' within zip: %v", f.Name, err)
					overallErr = errors.Join(overallErr, fmt.Errorf("open zip entry %s: %w", f.Name, err))
					continue
				}
				out, err := os.Create(outPath)
				if err != nil {
					in.Close()
					log.Printf(" 	 	ERROR: Failed create output file %s: %v", outPath, err)
					overallErr = errors.Join(overallErr, fmt.Errorf("create output %s: %w", outPath, err))
					continue
				}
				copiedBytes, err := io.Copy(out, in)
				in.Close()  // Close input file handle
				out.Close() // Close output file handle
				if err != nil {
					log.Printf(" 	 	ERROR: Failed copy data for '%s' to %s: %v", f.Name, outPath, err)
					overallErr = errors.Join(overallErr, fmt.Errorf("copy %s: %w", f.Name, err))
					os.Remove(outPath) // Clean up incomplete file
					continue
				}

				log.Printf(" 	 	 	Extracted '%s' (%d bytes) to %s", f.Name, copiedBytes, outPath)
				extractedFromThisZip++
				totalFilesExtractedCount++ // Increment total counter
			} // End loop zip entries

			if extractedFromThisZip == 0 {
				log.Printf(" 	 	INFO: No *.csv files found or extracted within %s", zipURL)
			}

		} // --- End loop through all links ---

	} // End loop feedURLs

	log.Printf(" Finished download/extract phase. Extracted %d CSV files total from all feeds.", totalFilesExtractedCount)
	if totalFilesExtractedCount == 0 && overallErr == nil {
		log.Println(" WARN: No CSV files were extracted from any source feeds.")
	}

	return overallErr
}

// --- DuckDB Parquet File Inspection ---
// (No changes needed in inspectParquetFiles)
func inspectParquetFiles(parquetDir string, dbPath string) error {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", dbPath, err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()
	log.Printf("DuckDB (Inspect): Installing and loading Parquet extension...")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(context.Background(), setupSQL); err != nil {
		// Log warning but continue if possible
		log.Printf("WARN: Failed to install/load parquet extension for inspection: %v", err)
		// Note: Inspection will likely fail if parquet extension isn't loaded.
		// We could return the error here, or let the subsequent queries fail.
		// Let's return it as inspection requires the extension.
		return fmt.Errorf("failed to install/load parquet extension for inspection: %w", err)
	} else {
		log.Printf("DuckDB (Inspect): Parquet extension loaded.")
	}
	globPattern := filepath.Join(parquetDir, "*.parquet")
	parquetFiles, err := filepath.Glob(globPattern)
	if err != nil {
		return fmt.Errorf("failed to glob for parquet files in %s: %w", parquetDir, err)
	}
	if len(parquetFiles) == 0 {
		log.Printf("No *.parquet files found in %s to inspect.", parquetDir)
		return nil
	}
	log.Printf("Found %d parquet files to inspect in %s", len(parquetFiles), parquetDir)
	for _, filePath := range parquetFiles {
		baseName := filepath.Base(filePath)
		log.Printf("--- Inspecting: %s ---", baseName)
		// DuckDB recommends using '/' for path separators even on Windows
		duckdbFilePath := strings.ReplaceAll(filePath, `\`, `/`)
		escapedFilePath := strings.ReplaceAll(duckdbFilePath, "'", "''") // Escape single quotes

		// Use DESCRIBE to get schema
		describeSQL := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s');", escapedFilePath)
		schemaRows, err := conn.QueryContext(context.Background(), describeSQL)
		if err != nil {
			log.Printf(" 	ERROR getting schema for %s: %v (SQL: %s)", baseName, err, describeSQL)
			continue // Skip to next file
		}
		log.Println(" 	Schema:")
		log.Printf(" 	 	%-30s | %-20s | %-5s | %-5s | %-5s | %s\n", "Column Name", "Column Type", "Null", "Key", "Default", "Extra")
		log.Println(" 	 	" + strings.Repeat("-", 90))
		schemaColumnCount := 0
		for schemaRows.Next() {
			var colName, colType, nullVal, keyVal, defaultVal, extraVal sql.NullString
			if err := schemaRows.Scan(&colName, &colType, &nullVal, &keyVal, &defaultVal, &extraVal); err != nil {
				log.Printf(" 	ERROR scanning schema row for %s: %v", baseName, err)
				break // Stop scanning schema for this file
			}
			log.Printf(" 	 	%-30s | %-20s | %-5s | %-5s | %-5s | %s\n", colName.String, colType.String, nullVal.String, keyVal.String, defaultVal.String, extraVal.String)

			// Basic check for date column type - expecting BIGINT for epoch milliseconds
			if strings.Contains(strings.ToLower(colName.String), "datetime") && colType.String != "BIGINT" && colType.String != "NULL" {
				log.Printf(" 	 	>>> UNEXPECTED TYPE for date column '%s': Expected BIGINT (INT64) or NULL, got %s <<<", colName.String, colType.String)
			}
			schemaColumnCount++
		}
		if err = schemaRows.Err(); err != nil {
			log.Printf(" 	ERROR iterating schema rows for %s: %v", baseName, err)
		}
		schemaRows.Close() // Always close the rows after processing

		if schemaColumnCount == 0 && err == nil {
			log.Println(" 	WARN: DESCRIBE returned no columns. File might be empty, corrupted, or format unsupported by DuckDB.")
		}

		// Use COUNT(*) to get row count
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s');", escapedFilePath)
		var rowCount int64 = -1 // Initialize with a value indicating failure
		err = conn.QueryRowContext(context.Background(), countSQL).Scan(&rowCount)
		if err != nil {
			log.Printf(" 	ERROR getting row count for %s: %v (SQL: %s)", baseName, err, countSQL)
		} else {
			log.Printf(" 	Row Count: %d", rowCount)
		}
		log.Println(strings.Repeat("-", 40))
	}
	return nil
}

// --- DuckDB FPP Calculation ---
// (No changes needed in runDuckDBAnalysis)
func runDuckDBAnalysis(ctx context.Context, parquetDir string, dbPath string) error {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", dbPath, err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()
	log.Printf("DuckDB (Analysis): Installing and loading Parquet extension...")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
		return fmt.Errorf("failed to install/load parquet extension: %w", err)
	}
	log.Printf("DuckDB (Analysis): Parquet extension loaded.")

	// DuckDB recommends using '/' for path separators even on Windows
	duckdbParquetDir := strings.ReplaceAll(parquetDir, `\`, `/`)

	// Define Views
	// Ensure column names match cleaned headers used in processCSVSections
	// Use TRY_CAST for robustness against parsing issues in underlying data
	// Assuming cleaned headers "FPP_UNITID", "INTERVAL_DATETIME", "CONTRIBUTION_FACTOR", "BIDTYPE", "RCR", "REGIONID", "RAISEREGRRP", "LOWERREGRRP"
	// Note: INTERVAL_DATETIME is expected to be INT64 (epoch ms) due to our conversion.
	createCfViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW fpp_cf AS SELECT FPP_UNITID AS unit, INTERVAL_DATETIME, TRY_CAST(CONTRIBUTION_FACTOR AS DOUBLE) AS cf FROM read_parquet('%s/*CONTRIBUTION_FACTOR*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)
	createRcrViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW rcr AS SELECT INTERVAL_DATETIME, BIDTYPE AS SERVICE, TRY_CAST(RCR AS DOUBLE) AS rcr FROM read_parquet('%s/*FPP_RCR*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)
	createPriceViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW price AS SELECT INTERVAL_DATETIME, REGIONID, TRY_CAST(RAISEREGRRP AS DOUBLE) AS price_raisereg, TRY_CAST(LOWERREGRRP AS DOUBLE) AS price_lowerreg FROM read_parquet('%s/*PRICESOLUTION*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)

	// Execute View Creation
	log.Printf("DuckDB (Analysis): Creating view fpp_cf...")
	if _, err = conn.ExecContext(ctx, createCfViewSQL); err != nil {
		return fmt.Errorf("failed to create view fpp_cf: %w\nSQL:\n%s", err, createCfViewSQL)
	}
	log.Printf("DuckDB (Analysis): Creating view rcr...")
	if _, err = conn.ExecContext(ctx, createRcrViewSQL); err != nil {
		return fmt.Errorf("failed to create view rcr: %w\nSQL:\n%s", err, createRcrViewSQL)
	}
	log.Printf("DuckDB (Analysis): Creating view price...")
	if _, err = conn.ExecContext(ctx, createPriceViewSQL); err != nil {
		return fmt.Errorf("failed to create view price: %w\nSQL:\n%s", err, createPriceViewSQL)
	}
	log.Println("DuckDB (Analysis): Views created.")

	// Diagnostics: Check Distinct Service/BIDTYPE values
	log.Println("DuckDB (Analysis): Checking distinct SERVICE values in rcr view...")
	distinctServiceSQL := `SELECT DISTINCT SERVICE FROM rcr ORDER BY 1 NULLS LAST LIMIT 20;` // Limit output for readability
	serviceRows, err := conn.QueryContext(ctx, distinctServiceSQL)
	if err != nil {
		log.Printf("WARN: Failed to query distinct service values: %v", err)
	} else {
		log.Println(" 	Distinct SERVICE values found (limit 20):")
		distinctServices := []string{}
		for serviceRows.Next() {
			var serviceName sql.NullString
			if err := serviceRows.Scan(&serviceName); err != nil {
				log.Printf(" 	WARN: Error scanning service name: %v", err)
				break
			}
			if serviceName.Valid {
				distinctServices = append(distinctServices, serviceName.String)
			} else {
				distinctServices = append(distinctServices, "NULL")
			}
		}
		serviceRows.Close()
		log.Printf(" 	 	[%s]", strings.Join(distinctServices, ", "))
		contains := func(slice []string, val string) bool {
			for _, item := range slice {
				if item == val {
					return true
				}
			}
			return false
		}
		if len(distinctServices) == 0 {
			log.Println(" 	 	WARN: No SERVICE values found in rcr view. Joins may fail.")
		} else if !contains(distinctServices, "RAISEREG") || !contains(distinctServices, "LOWERREG") {
			log.Println(" 	 	WARN: Expected SERVICE values 'RAISEREG' and/or 'LOWERREG' not found in the sample.")
		}
	}

	// Helper to check if a string is in a slice

	// Diagnostics: Check Date Range (on INT64 epoch milliseconds)
	log.Println("DuckDB (Analysis): Checking epoch date ranges (INT64)...")
	epochCheckSQL := `
	WITH epochs AS (
		(SELECT 'fpp_cf' as tbl, INTERVAL_DATETIME FROM fpp_cf WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000) UNION ALL
		(SELECT 'rcr' as tbl, INTERVAL_DATETIME FROM rcr WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000) UNION ALL
		(SELECT 'price' as tbl, INTERVAL_DATETIME FROM price WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000)
	) SELECT tbl, COUNT(*) as sample_count, MIN(INTERVAL_DATETIME) as min_epoch_ms, MAX(INTERVAL_DATETIME) as max_epoch_ms
	FROM epochs GROUP BY tbl;` // Removed to_timestamp from this query as it expects seconds, not milliseconds
	epochRows, err := conn.QueryContext(ctx, epochCheckSQL)
	if err != nil {
		log.Printf("WARN: Failed to query epoch date ranges: %v", err)
	} else {
		log.Println(" 	Epoch Date Range Check Results (from sample):")
		log.Printf(" 	%-10s | %-12s | %-20s | %-20s | %-25s | %-25s\n", "Table", "Sample Count", "Min Epoch MS", "Max Epoch MS", "Min Approx TS (UTC)", "Max Approx TS (UTC)")
		log.Println(" 	" + strings.Repeat("-", 125))
		for epochRows.Next() {
			var tbl sql.NullString
			var sCnt sql.NullInt64
			var minE, maxE sql.NullInt64
			// DuckDB's `to_timestamp` works with seconds, so we need to convert ms to seconds for display
			// var minTs, maxTs time.Time // Use time.Time to handle conversion from Unix seconds

			if err := epochRows.Scan(&tbl, &sCnt, &minE, &maxE); err != nil { // Scan only epoch values
				log.Printf(" 	WARN: Error scanning epoch range row: %v", err)
				break
			}

			// Manually convert epoch milliseconds to time.Time for logging display
			minTsUTC := time.Unix(0, minE.Int64*int64(time.Millisecond)) // Unix(seconds, nanoseconds). ms * 1,000,000 = ns
			maxTsUTC := time.Unix(0, maxE.Int64*int64(time.Millisecond)) // Unix(seconds, nanoseconds). ms * 1,000,000 = ns

			log.Printf(" 	%-10s | %-12d | %-20d | %-20d | %-25s | %-25s\n", tbl.String, sCnt.Int64, minE.Int64, maxE.Int64, minTsUTC.UTC().Format(time.RFC3339), maxTsUTC.UTC().Format(time.RFC3339))
		}
		if err = epochRows.Err(); err != nil {
			log.Printf(" 	ERROR iterating epoch range rows: %v", err)
		}
		epochRows.Close()
	}

	// Calculate Combined FPP View
	// Joins on INTERVAL_DATETIME (INT64 epoch ms)
	// Uses COALESCE to treat NULL cf, rcr, price values as 0 in calculations
	calculateFppSQL := `
	CREATE OR REPLACE VIEW fpp_combined AS
	SELECT cf.unit, cf.INTERVAL_DATETIME, cf.cf, r.rcr, r.SERVICE,
		COALESCE(p.price_raisereg, 0.0) AS price_raisereg,
		COALESCE(p.price_lowerreg, 0.0) AS price_lowerreg,
		CASE UPPER(r.SERVICE) WHEN 'RAISEREG' THEN COALESCE(p.price_raisereg, 0.0) WHEN 'LOWERREG' THEN COALESCE(p.price_lowerreg, 0.0) ELSE 0.0 END AS service_price,
		COALESCE(cf.cf, 0.0) * COALESCE(r.rcr, 0.0) * CASE UPPER(r.SERVICE) WHEN 'RAISEREG' THEN COALESCE(p.price_raisereg, 0.0) WHEN 'LOWERREG' THEN COALESCE(p.price_lowerreg, 0.0) ELSE 0.0 END AS fpp_cost
	FROM fpp_cf cf
	JOIN rcr r ON cf.INTERVAL_DATETIME = r.INTERVAL_DATETIME
	JOIN price p ON cf.INTERVAL_DATETIME = p.INTERVAL_DATETIME
	WHERE UPPER(r.SERVICE) IN ('RAISEREG', 'LOWERREG');` // Only include relevant SERVICE types

	log.Printf("DuckDB (Analysis): Calculating combined FPP view (joining on epoch ms)...")
	if _, err = conn.ExecContext(ctx, calculateFppSQL); err != nil {
		return fmt.Errorf("failed to calculate combined FPP view: %w\nSQL:\n%s", err, calculateFppSQL)
	}
	log.Println("DuckDB (Analysis): Combined FPP view created.")

	// Diagnostics: Check Join Count
	checkJoinSQL := `SELECT COUNT(*) FROM fpp_combined;`
	var joinRowCount int64 = -1
	err = conn.QueryRowContext(ctx, checkJoinSQL).Scan(&joinRowCount)
	if err != nil {
		log.Printf("WARN: Failed to check row count of fpp_combined view: %v", err)
	} else {
		log.Printf("INFO: Row count of fpp_combined view (before date filter): %d", joinRowCount)
		if joinRowCount == 0 {
			log.Printf(">>> WARNING: Joins/SERVICE filter produced 0 rows BEFORE date filtering. Check data overlap in Parquet files and SERVICE values in FPP_RCR data. <<<")
		}
	}

	// Calculate Epoch Milliseconds for Date Range Filter
	// Filter is for April 2025 in NEM time (UTC+10)
	startFilterTime, err := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/01 00:00:00", nemLocation)
	if err != nil {
		log.Printf("WARN: Failed to parse start date for filter: %v. Analysis filter may be incorrect.", err)
		// Use zero time or a default range if parsing fails? Or return error?
		// For now, log warning and let subsequent query potentially fail or use default epoch start.
		// Let's set a default if parsing fails to avoid crashing.
		startFilterTime = time.Unix(0, 0) // Epoch 0
	}
	endFilterTime, err := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/30 23:59:59", nemLocation)
	if err != nil {
		log.Printf("WARN: Failed to parse end date for filter: %v. Analysis filter may be incorrect.", err)
		// Use a default end time if parsing fails.
		endFilterTime = time.Now().In(nemLocation) // Use current time as a fallback end
	}

	startEpochMS := startFilterTime.UnixMilli()
	endEpochMS := endFilterTime.UnixMilli()
	log.Printf("DuckDB (Analysis): Filtering between Epoch MS: %d (%s) and %d (%s) (NEM Time)", startEpochMS, startFilterTime.Format(time.RFC3339), endEpochMS, endFilterTime.Format(time.RFC3339))

	// Aggregate FPP over billing period (April 2025 in NEM time)
	// Use the calculated epoch millisecond range for filtering
	aggregateFppSQL := fmt.Sprintf(`
	SELECT unit,
		SUM(CASE WHEN UPPER(SERVICE) = 'RAISEREG' THEN fpp_cost ELSE 0 END) AS total_raise_fpp,
		SUM(CASE WHEN UPPER(SERVICE) = 'LOWERREG' THEN fpp_cost ELSE 0 END) AS total_lower_fpp,
		SUM(fpp_cost) AS total_fpp
	FROM fpp_combined
	WHERE INTERVAL_DATETIME >= %d AND INTERVAL_DATETIME <= %d
	GROUP BY unit
	ORDER BY unit;`, startEpochMS, endEpochMS)

	log.Printf("DuckDB (Analysis): Aggregating FPP results for April 2025 (using epoch ms)...")
	rows, err := conn.QueryContext(ctx, aggregateFppSQL)
	if err != nil {
		return fmt.Errorf("failed to execute final FPP aggregation query: %w\nSQL:\n%s", err, aggregateFppSQL)
	}
	defer rows.Close() // Ensure rows are closed

	log.Println("--- FPP Calculation Results (April 2025 NEM Time) ---")
	log.Printf("%-20s | %-20s | %-20s | %-20s\n", "Unit", "Total Raise FPP", "Total Lower FPP", "Total FPP")
	log.Println(strings.Repeat("-", 85))
	rowCount := 0
	for rows.Next() {
		var unit string
		var totalRaise, totalLower, totalFpp sql.NullFloat64
		if err := rows.Scan(&unit, &totalRaise, &totalLower, &totalFpp); err != nil {
			log.Printf("ERROR: Failed to scan result row: %v", err)
			continue
		}
		printFloat := func(f sql.NullFloat64) string {
			if f.Valid {
				return fmt.Sprintf("%.4f", f.Float64)
			}
			return "NULL" // Or "-" or "0.0000" depending on desired output for nulls
		}
		log.Printf("%-20s | %-20s | %-20s | %-20s\n", unit, printFloat(totalRaise), printFloat(totalLower), printFloat(totalFpp))
		rowCount++
	}
	// Check for errors that occurred during iteration
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating result rows: %w", err)
	}

	if rowCount == 0 {
		log.Println("No FPP results found for the specified date range (April 2025 NEM Time).")
		log.Println("ADVICE: Check diagnostic logs. Reasons could be:")
		log.Println(" 	1. No data overlap in the required Parquet files for the date range (check epoch ranges diagnostic).")
		log.Println(" 	2. Joins failed (check SERVICE values diagnostic).")
		log.Println(" 	3. All calculated fpp_cost values were zero or NULL within the date range.")
		log.Println(" 	4. Incorrect date range conversion to epoch milliseconds.")
	}
	log.Println(strings.Repeat("-", 85))

	return nil
}

// --- Utility Functions ---
// (parseLinks, downloadFile functions remain the same)
func parseLinks(n *html.Node, suffix string) []string {
	var out []string
	var walk func(*html.Node)
	walk = func(nd *html.Node) {
		if nd.Type == html.ElementNode && nd.Data == "a" {
			for _, a := range nd.Attr {
				if a.Key == "href" {
					// Check for both the suffix and that it's not just the base directory link
					if strings.HasSuffix(strings.ToLower(a.Val), strings.ToLower(suffix)) && a.Val != "/" {
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

func downloadFile(client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request for %s: %w", url, err)
	}
	// Add a User-Agent header as some servers might block requests without one
	req.Header.Set("User-Agent", "NEMParquetConverter/1.0 (Go-client)")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s failed: %w", url, err)
	}
	defer resp.Body.Close() // Ensure the response body is closed

	if resp.StatusCode != http.StatusOK {
		// Read a small portion of the body for error context
		limitReader := io.LimitReader(resp.Body, 512) // Read up to 512 bytes of the error body
		bodyBytes, _ := io.ReadAll(limitReader)
		return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, url, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading body from %s: %w", url, err)
	}
	return bodyBytes, nil
}

// --- Peekable Scanner Wrapper ---
// (PeekableScanner struct and methods remain the same)
// Adds context for cancellation if needed (not strictly used in this version, but good practice)
type PeekableScanner struct {
	scanner    *bufio.Scanner
	peekedLine *string
	peekErr    error
	//file 	 	*os.File // Keep if needed for seeking/re-reading, but not used by Scanner
	context context.Context
}

func NewPeekableScanner(r io.Reader) *PeekableScanner {
	scanner := bufio.NewScanner(r)
	// Default buffer - increased in processCSVSections
	// scanner.Buffer(make([]byte, 4096), 1024*1024) // Example default: 4KB initial, max 1MB
	return &PeekableScanner{scanner: scanner, context: context.Background()}
}

func (ps *PeekableScanner) Scan() bool {
	// Check context for cancellation before scanning (optional)
	select {
	case <-ps.context.Done():
		ps.peekErr = ps.context.Err() // Set context error
		return false                  // Stop scanning
	default:
		if ps.peekedLine != nil {
			ps.peekedLine = nil // Consume peeked line
			ps.peekErr = nil
			return true
		}
		return ps.scanner.Scan()
	}
}

func (ps *PeekableScanner) Text() string {
	if ps.peekedLine != nil {
		return *ps.peekedLine
	}
	return ps.scanner.Text()
}

func (ps *PeekableScanner) Err() error {
	// Priority to context error
	if ps.peekErr != nil {
		return ps.peekErr
	}
	return ps.scanner.Err()
}

// PeekRecordType reads the next line without consuming it to determine its type.
// It handles blank lines and comments, skipping them to find the first non-empty, non-comment line.
func (ps *PeekableScanner) PeekRecordType() (string, error) {
	if ps.peekedLine != nil {
		return ps.extractRecordType(*ps.peekedLine), ps.peekErr
	}

	// Loop to skip blank lines and comments (usually lines starting with # or similar, though NEM CSVs don't typically have them)
	for ps.scanner.Scan() {
		line := ps.scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		// Skip blank lines and potential comment lines
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "#") {
			continue
		}

		// Found a non-empty, non-comment line, store it and return its type
		ps.peekedLine = &line
		ps.peekErr = nil // Clear any previous peek error
		return ps.extractRecordType(line), nil
	}

	// Scanner finished without finding a non-empty line
	ps.peekErr = ps.scanner.Err() // Capture the final scanner error (e.g., EOF, ErrTooLong)
	ps.peekedLine = nil           // Ensure no peeked line is left
	return "", ps.peekErr
}

func (ps *PeekableScanner) extractRecordType(line string) string {
	trimmedLine := strings.TrimSpace(line)
	if trimmedLine == "" {
		return ""
	}
	// Split only on the first comma to isolate the record type field
	parts := strings.SplitN(trimmedLine, ",", 2)
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return "" // Should not happen for non-empty line
}

// Buffer sets the internal buffer for the scanner. Called after creating NewPeekableScanner.
func (ps *PeekableScanner) Buffer(buf []byte, max int) {
	ps.scanner.Buffer(buf, max)
}
