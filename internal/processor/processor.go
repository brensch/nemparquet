package processor

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/util" // Use your module name

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// ProcessProgress tracks CSV processing.
type ProcessProgress struct {
	TotalFiles     int           // Total CSV files found
	FilesProcessed int           // Number of files attempted
	CurrentFile    string        // Path of the file being processed
	LinesProcessed int64         // Lines read/processed in the current file
	TotalLines     int64         // Estimated total lines (optional, -1 if unknown)
	Complete       bool          // True if processing of current file finished
	Skipped        bool          // True if the file was skipped (output exists)
	Err            error         // Error for the current file
	ElapsedTime    time.Duration // Time taken for the current file
}

// ProcessFiles finds CSVs and processes them concurrently.
func ProcessFiles(cfg config.Config, progressChan chan<- ProcessProgress) error {
	log.Println("Searching for CSV files...")
	globPath := filepath.Join(cfg.InputDir, "*.[cC][sS][vV]")
	csvFiles, err := filepath.Glob(globPath)
	if err != nil {
		return fmt.Errorf("failed to glob for CSV files in %s: %w", cfg.InputDir, err)
	}

	if len(csvFiles) == 0 {
		log.Printf("INFO: No *.CSV files found in %s to process.", cfg.InputDir)
		close(progressChan) // Close channel, nothing to report
		return nil
	}

	log.Printf("Found %d CSV files to process.", len(csvFiles))

	// Setup for parallel processing
	numFiles := len(csvFiles)
	jobs := make(chan string, numFiles)
	// Use a channel for results/errors as well
	results := make(chan ProcessProgress, numFiles) // Channel to receive final status of each file

	var wg sync.WaitGroup

	// Start worker goroutines
	log.Printf("Starting %d processing workers...", cfg.NumWorkers)
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			log.Printf("Worker %d started.", workerID)
			for csvPath := range jobs {
				startTime := time.Now()
				log.Printf("Worker %d processing %s...", workerID, csvPath)
				baseName := filepath.Base(csvPath)
				progress := ProcessProgress{
					TotalFiles: numFiles,
					// FilesProcessed will be updated by the main loop
					CurrentFile: baseName,
					TotalLines:  -1, // Line count estimation is hard without pre-scan
				}

				// Report initial status (Processing) via results channel
				results <- progress

				// Call the core processing logic
				skipped, processErr := processCSVSections(csvPath, cfg.OutputDir, cfg.SchemaRowLimit)

				// Update progress with final status
				progress.ElapsedTime = time.Since(startTime)
				progress.Complete = true // Mark as finished attempt
				if skipped {
					progress.Skipped = true
					log.Printf("Worker %d Skipped %s.", workerID, baseName)
				} else if processErr != nil {
					progress.Err = processErr
					log.Printf("Worker %d ERROR processing %s: %v", workerID, baseName, processErr)
				} else {
					log.Printf("Worker %d Finished %s successfully.", workerID, baseName)
				}

				results <- progress // Send final status for this file
			}
			log.Printf("Worker %d finished.", workerID)
		}(i)
	}

	// Send jobs to workers
	for _, csvPath := range csvFiles {
		jobs <- csvPath
	}
	close(jobs) // Signal no more jobs

	// Collect results and send progress updates to the UI
	var overallErr error
	filesProcessedCount := 0

	// Need a way to close the results channel *after* all workers are done.
	// We can launch a separate goroutine to wait for the workers and then close results.
	go func() {
		wg.Wait()
		close(results)
		log.Println("All processing workers finished.")
	}()

	// Process results as they come in
	for progress := range results {
		// Check if it's an initial or final status update
		// (Simple approach: only count final updates towards completion)
		if progress.Complete || progress.Skipped || progress.Err != nil {
			filesProcessedCount++
			progress.FilesProcessed = filesProcessedCount // Update overall count
			if progress.Err != nil {
				overallErr = errors.Join(overallErr, fmt.Errorf("%s: %w", progress.CurrentFile, progress.Err))
			}
		} else {
			// This is the initial "Processing" status update
			// We already sent it from the worker, but maybe update the UI progress count here?
			progress.FilesProcessed = filesProcessedCount // Show current progress count
		}

		progressChan <- progress // Forward to the UI model
	}

	// All results collected now
	log.Println("--- Processing Summary ---")
	// Summary logs can be handled by the TaskFinishedMsg in the UI layer

	close(progressChan) // Signal UI no more progress updates
	return overallErr
}

// checkExistingOutputFiles (Moved from main, minor change to use SchemaRowLimit) - Remains largely the same logic
func checkExistingOutputFiles(csvPath, outDir string, schemaRowLimit int) (bool, error) {
	// ... (Keep the original implementation, just pass schemaRowLimit if needed, although it's not directly used here)
	// ... (Make sure logging uses log package)
	baseName := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))
	potentialOutputs := []string{}

	file, err := os.Open(csvPath)
	if err != nil {
		log.Printf(" [%s] WARN: Could not open input CSV for pre-check: %v. Assuming processing is needed.", baseName, err)
		return false, nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1*1024*1024)

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
				fileName := fmt.Sprintf("%s_%s_v%s.parquet", baseName, comp, ver)
				potentialOutputs = append(potentialOutputs, fileName)
			} else {
				log.Printf("[%s] WARN line %d (pre-check): 'I' record has empty component or version: %s. Skipping.", baseName, lineNumber, line)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[%s] WARN: Error during pre-check scan near line %d: %v. Assuming processing is needed.", baseName, lineNumber, err)
		return false, nil
	}

	if len(potentialOutputs) == 0 {
		log.Printf(" [%s] INFO: No 'I' records found during pre-check. Proceeding with processing.", baseName)
		return false, nil
	}

	// Check existence
	log.Printf(" [%s] Pre-checking existence of %d potential output files...", baseName, len(potentialOutputs))
	allExist := true
	for _, fname := range potentialOutputs {
		fpath := filepath.Join(outDir, fname)
		if _, err := os.Stat(fpath); os.IsNotExist(err) {
			log.Printf(" [%s]   -> Output file %s does NOT exist. Processing required.", baseName, fname)
			allExist = false
			break
		} else if err != nil {
			log.Printf(" [%s] WARN: Error checking status of %s: %v. Assuming processing is needed.", baseName, fname, err)
			allExist = false
			break
		}
	}
	return allExist, nil
}

// processCSVSections (Moved from main, adapted slightly)
// Returns (skipped bool, error)
func processCSVSections(csvPath, outDir string, schemaInferenceRowLimit int) (skipped bool, err error) {
	baseName := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))

	// Check existence first
	allOutputsExist, checkErr := checkExistingOutputFiles(csvPath, outDir, schemaInferenceRowLimit)
	if checkErr != nil {
		log.Printf("[%s] ERROR during output file pre-check: %v. Attempting to process regardless.", baseName, checkErr)
	}
	if allOutputsExist {
		return true, nil // Signal skipped
	}

	// --- Proceed with processing ---
	file, err := os.Open(csvPath)
	if err != nil {
		return false, fmt.Errorf("open CSV %s: %w", csvPath, err)
	}
	defer file.Close()

	scanner := util.NewPeekableScanner(file)            // Use utility scanner
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024) // Set buffer

	var pw *writer.CSVWriter
	var fw source.ParquetFile
	var currentHeaders []string
	var currentComp, currentVer string
	var currentMeta []string
	var isDateColumn []bool
	var schemaInferred bool = false
	var rowsCheckedForSchema int = 0
	lineNumber := int64(0) // Use int64 to match progress reporting

	// --- Main Scanner Loop (largely unchanged logic, ensure logging uses 'log') ---
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		// ... (rest of the switch statement logic from the original function) ...
		// ... (make sure all log.Printf calls are kept or adapted) ...
		// ... (Use util.IsNEMDateTime and util.NEMDateTimeToEpochMS) ...

		// --- Inside the 'D' case, before pw.WriteString ---
		// Optional: Send line progress update (can be noisy)
		// if pw != nil && lineNumber % 1000 == 0 { // Update every 1000 lines
		//     // Need a way to pass the progress channel down here, complicates function signature
		//     // Or, calculate progress only at the end based on line count.
		// }

		// --- Existing Switch Logic (Abbreviated) ---
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
			// Finish previous section
			if pw != nil {
				log.Printf("   [%s] Finalizing section %s v%s...", baseName, currentComp, currentVer)
				if err := pw.WriteStop(); err != nil {
					log.Printf("   [%s] WARN: Error stopping writer %s v%s: %v", baseName, currentComp, currentVer, err)
					// Don't return error, just log? Or accumulate? Accumulate is better.
					err = errors.Join(err, fmt.Errorf("stop writer %s v%s: %w", currentComp, currentVer, err))
				} else {
					log.Printf("   [%s] Successfully stopped writer for %s v%s", baseName, currentComp, currentVer)
				}
				if closeErr := fw.Close(); closeErr != nil {
					log.Printf("   [%s] WARN: Error closing file %s v%s: %v", baseName, currentComp, currentVer, closeErr)
					err = errors.Join(err, fmt.Errorf("close file %s v%s: %w", currentComp, currentVer, closeErr))
				}
				pw = nil
				fw = nil
			}
			// Reset for new section
			currentMeta = nil
			isDateColumn = nil
			schemaInferred = false
			rowsCheckedForSchema = 0

			if len(rec) < 5 {
				log.Printf("[%s] WARN line %d: Malformed 'I' record: %s. Skipping section.", baseName, lineNumber, line)
				currentHeaders = nil
				continue
			}
			currentComp, currentVer = strings.TrimSpace(rec[2]), strings.TrimSpace(rec[3])
			currentHeaders = make([]string, 0, len(rec)-4)
			for _, h := range rec[4:] {
				currentHeaders = append(currentHeaders, strings.TrimSpace(h))
			}
			if len(currentHeaders) == 0 {
				log.Printf("[%s] WARN line %d: 'I' record has no headers %s v%s. Skipping.", baseName, lineNumber, currentComp, currentVer)
				continue
			}
			log.Printf(" [%s] Found section header %s v%s at line %d", baseName, currentComp, currentVer, lineNumber)

		case "D":
			if len(currentHeaders) == 0 {
				continue
			} // Skip if no valid header
			if len(rec) < 4 {
				log.Printf("[%s] WARN line %d: Malformed 'D' record: %s. Skipping.", baseName, lineNumber, line)
				continue
			}
			// Extract values
			values := make([]string, len(currentHeaders))
			numDataColsInRecord := len(rec) - 4
			hasBlanks := false
			for i := 0; i < len(currentHeaders); i++ {
				if i < numDataColsInRecord {
					values[i] = strings.TrimSpace(strings.Trim(rec[i+4], `"`))
					if values[i] == "" {
						hasBlanks = true
					}
				} else {
					values[i] = ""
					hasBlanks = true
				}
			}

			// --- Schema Inference (Mostly Unchanged) ---
			if !schemaInferred {
				rowsCheckedForSchema++
				peekedType, peekErr := scanner.PeekRecordType()
				mustInferNow := !hasBlanks || (peekErr != nil || peekedType != "D") || rowsCheckedForSchema >= schemaInferenceRowLimit

				if mustInferNow {
					if hasBlanks && rowsCheckedForSchema >= schemaInferenceRowLimit {
						log.Printf(" [%s] WARN line %d: Schema limit reached (%d). Inferring from blank row %s v%s.", baseName, lineNumber, schemaInferenceRowLimit, currentComp, currentVer)
					} else if hasBlanks && (peekErr != nil || peekedType != "D") {
						log.Printf(" [%s] WARN line %d: No clean row before next section/EOF %s v%s. Inferring from blank row.", baseName, lineNumber, currentComp, currentVer)
					} else {
						log.Printf(" [%s] Inferring schema for %s v%s from line %d (attempt %d)", baseName, currentComp, currentVer, lineNumber, rowsCheckedForSchema)
					}

					// Perform inference
					currentMeta = make([]string, len(currentHeaders))
					isDateColumn = make([]bool, len(currentHeaders))
					for i := range currentHeaders {
						var typ string
						val := values[i]
						isDate := false

						if util.IsNEMDateTime(val) { // Use util func
							typ = "INT64"
							isDate = true
						} else if val == "" {
							typ = "BYTE_ARRAY"
						} else if _, pErr := strconv.ParseBool(val); pErr == nil { // Simpler bool check
							typ = "BOOLEAN"
						} else if _, pErr := strconv.ParseInt(val, 10, 64); pErr == nil {
							typ = "INT64"
						} else if _, pErr := strconv.ParseFloat(val, 64); pErr == nil {
							typ = "DOUBLE"
						} else {
							typ = "BYTE_ARRAY"
						}
						isDateColumn[i] = isDate

						// Clean header
						cleanHeader := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(currentHeaders[i], " ", "_"), ".", "_"), ";", "_")
						if cleanHeader == "" {
							cleanHeader = fmt.Sprintf("column_%d", i)
						}

						// Build meta string
						if typ == "BYTE_ARRAY" {
							currentMeta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", cleanHeader)
						} else {
							currentMeta[i] = fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", cleanHeader, typ)
						}
					}
					log.Printf(" [%s]     Inferred Schema for %s v%s: [%s]", baseName, currentComp, currentVer, strings.Join(currentMeta, "], ["))

					// Initialize Parquet writer
					parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", baseName, currentComp, currentVer)
					path := filepath.Join(outDir, parquetFile)
					var createErr error
					fw, createErr = local.NewLocalFileWriter(path)
					if createErr != nil {
						log.Printf("[%s] ERROR line %d: Failed create parquet file %s: %v. Skipping section %s v%s.", baseName, lineNumber, path, createErr, currentComp, currentVer)
						currentHeaders = nil
						schemaInferred = false
						err = errors.Join(err, fmt.Errorf("create file %s: %w", path, createErr)) // Accumulate error
						continue
					}

					pw, createErr = writer.NewCSVWriter(currentMeta, fw, 4) // 4 goroutines for writing
					if createErr != nil {
						log.Printf("[%s] ERROR line %d: Failed init writer for %s: %v. Skipping section %s v%s.", baseName, lineNumber, path, createErr, currentComp, currentVer)
						fw.Close()
						pw = nil
						currentHeaders = nil
						schemaInferred = false
						err = errors.Join(err, fmt.Errorf("create writer %s: %w", path, createErr)) // Accumulate error
						continue
					}
					pw.CompressionType = parquet.CompressionCodec_SNAPPY
					log.Printf(" [%s]   Created writer for section %s v%s -> %s", baseName, currentComp, currentVer, path)
					schemaInferred = true
				} else {
					// Skip blank row, wait for cleaner one
					log.Printf(" [%s]   Skipping schema inference on blank row %d for %s v%s (attempt %d/%d)...", baseName, lineNumber, currentComp, currentVer, rowsCheckedForSchema, schemaInferenceRowLimit)
					continue
				}
			} // End schema inference

			// --- Write Data Row (Mostly Unchanged) ---
			if !schemaInferred || pw == nil {
				continue
			} // Safeguard

			recPtrs := make([]*string, len(values))
			for j := 0; j < len(values); j++ {
				isEmpty := values[j] == ""
				finalValue := values[j]

				if isDateColumn[j] && !isEmpty {
					epochMS, convErr := util.NEMDateTimeToEpochMS(values[j]) // Use util func
					if convErr != nil {
						originalVal := "OOB"
						if j+4 < len(rec) {
							originalVal = rec[j+4]
						}
						log.Printf(" [%s] WARN line %d: Failed convert date '%s' (orig: '%s') col '%s': %v. Writing NULL.", baseName, lineNumber, values[j], originalVal, currentHeaders[j], convErr)
						recPtrs[j] = nil
						continue
					}
					finalValue = strconv.FormatInt(epochMS, 10)
				}

				isTargetStringType := false
				if j < len(currentMeta) {
					isTargetStringType = strings.Contains(currentMeta[j], "type=BYTE_ARRAY")
				}

				if isEmpty && !isTargetStringType {
					recPtrs[j] = nil
				} else {
					temp := finalValue
					recPtrs[j] = &temp
				}
			}

			// Write
			if writeErr := pw.WriteString(recPtrs); writeErr != nil {
				// Detailed error logging for write errors (keep original logic)
				problematicData, schemaForProblem := "N/A", "N/A"
				// ... (logic to find problematic column/schema) ...
				log.Printf("[%s] WARN line %d: WriteString error %s v%s: %v. Schema: [%s]. Cause: %s. Raw: %v",
					baseName, lineNumber, currentComp, currentVer, writeErr, schemaForProblem, problematicData, values)
				// Don't return error for write warnings, just log? Or accumulate? Let's just log for now.
			}

		// case "T": // Trailer - handled by EOF
		default: // Ignore other record types ('C', 'H', etc.)
			continue
		}

	} // End scanner loop

	// Finalize last section
	if pw != nil {
		log.Printf(" [%s] Finalizing last section %s v%s...", baseName, currentComp, currentVer)
		if stopErr := pw.WriteStop(); stopErr != nil {
			log.Printf("[%s] ERROR: Failed stop writer last section %s v%s: %v", baseName, currentComp, currentVer, stopErr)
			err = errors.Join(err, fmt.Errorf("stop writer last %s v%s: %w", currentComp, currentVer, stopErr))
		} else {
			log.Printf(" [%s] Successfully stopped writer last section %s v%s", baseName, currentComp, currentVer)
		}
		if closeErr := fw.Close(); closeErr != nil {
			log.Printf("[%s] ERROR: Failed close file last section %s v%s: %v", baseName, currentComp, currentVer, closeErr)
			err = errors.Join(err, fmt.Errorf("close file last %s v%s: %w", currentComp, currentVer, closeErr))
		}
	}

	// Check for scanner errors
	if scanErr := scanner.Err(); scanErr != nil {
		errMsg := fmt.Sprintf("scanner error near line %d: %v", lineNumber, scanErr)
		if errors.Is(scanErr, bufio.ErrTooLong) {
			errMsg = fmt.Sprintf("scanner error: line too long near line %d (increase buffer)", lineNumber)
		}
		log.Printf("[%s] ERROR: %s", baseName, errMsg)
		err = errors.Join(err, fmt.Errorf("scanner error CSV %s: %w", csvPath, scanErr)) // Return scanner error
	}

	// Return accumulated errors
	return false, err // Processed (attempted), return accumulated error
}
