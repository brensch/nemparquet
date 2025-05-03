package processor

import (
	"archive/zip" // Need zip package here now
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io" // Need io for reader
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"

	"github.com/xitongsys/parquet-go-source/local" // Keep for writer target
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source" // Keep for writer target
	"github.com/xitongsys/parquet-go/writer"
)

// StartProcessorWorkers launches goroutines to process zip paths from a channel.
// It waits for all workers to finish using the provided WaitGroup.
// Errors encountered by workers are stored in the provided sync.Map.
func StartProcessorWorkers(
	ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger,
	numWorkers int, pathsChan <-chan string, // Read from channel
	wg *sync.WaitGroup, errorsMap *sync.Map, // Use pointers for WG and Map
	forceProcess bool,
) {
	logger.Info("Starting processor workers", slog.Int("count", numWorkers))
	for i := 0; i < numWorkers; i++ {
		wg.Add(1) // Add to WG for each worker started
		go func(workerID int) {
			defer wg.Done() // Signal completion when goroutine exits
			l := logger.With(slog.Int("worker_id", workerID))
			l.Debug("Processing worker started")

			// Read zip file paths from the channel until it's closed
			for zipFilePath := range pathsChan {
				// Create a new logger instance for this specific job with its context
				jobLogger := l.With(slog.String("zip_file_path", zipFilePath))

				jobLogger.Info("Checking processing state.")
				// 1. Find original URL from DB using output path
				absZipFilePath, pathErr := filepath.Abs(zipFilePath)
				if pathErr != nil {
					jobLogger.Error("Failed get absolute path, skipping.", "error", pathErr)
					errorsMap.Store(zipFilePath, pathErr) // Store error against the path key
					continue                              // Skip to next job
				}

				originalZipURL, foundURL, dbErr := db.GetZipURLForOutputPath(ctx, dbConn, absZipFilePath)
				if dbErr != nil {
					jobLogger.Error("DB error finding original URL, skipping.", "error", dbErr)
					errorsMap.Store(zipFilePath, dbErr)
					continue
				}
				if !foundURL {
					jobLogger.Warn("Could not find original download URL in DB for this zip file, skipping.", slog.String("abs_path", absZipFilePath))
					// Log a skip event? Or just let it be skipped silently? Log for now.
					// Note: We don't have the original URL to log against here, which is the issue.
					// We could log against the path, but that might pollute the log.
					// errorsMap.Store(zipFilePath, fmt.Errorf("original URL not found in DB for path %s", absZipFilePath)) // Store skip reason as error?
					continue
				}
				jobLogger = jobLogger.With(slog.String("zip_url", originalZipURL)) // Add URL context

				// 2. Check if already processed successfully using the URL
				processCompleted, dbErr := db.HasEventOccurred(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventProcessEnd)
				if dbErr != nil {
					jobLogger.Warn("Failed check DB state for process completion, proceeding cautiously.", "error", dbErr)
					// Log DB error against the URL
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventError, "", "", fmt.Sprintf("db check process failed: %v", dbErr), "", nil)
				}

				if !forceProcess && processCompleted {
					jobLogger.Info("Skipping processing, already completed according to DB.")
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventSkipProcess, "", "", "Already processed", "", nil)
					continue // Skip to next job
				}

				jobLogger.Info("Starting processing.")
				startTime := time.Now()
				// Log process start for the *original URL*
				db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventProcessStart, "", "", "", "", nil)

				// 3. Process the Zip Archive
				processErr := processSingleZipArchive(ctx, cfg, jobLogger, zipFilePath, originalZipURL) // Pass job-specific logger
				duration := time.Since(startTime)

				// 4. Log event based on result for the *original URL*
				if processErr != nil {
					jobLogger.Error("Failed to process zip archive", "error", processErr, slog.Duration("duration", duration.Round(time.Millisecond)))
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventError, "", "", processErr.Error(), "", &duration)
					errorsMap.Store(zipFilePath, processErr) // Store error against path
				} else {
					jobLogger.Info("Processing successful.", slog.Duration("duration", duration.Round(time.Millisecond)))
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventProcessEnd, "", "", "", "", &duration)
				}

			} // End job loop (channel closed)
			l.Debug("Processing worker finished")
		}(i)
	}
}

// processSingleZipArchive opens a zip archive from disk and processes contained CSVs.
func processSingleZipArchive(ctx context.Context, cfg config.Config, logger *slog.Logger, zipFilePath string, originalZipURL string) error {
	l := logger // Use logger with existing context (worker, zip path, zip url)

	l.Debug("Opening zip archive from disk.")
	zr, err := zip.OpenReader(zipFilePath)
	if err != nil {
		l.Error("Failed to open zip archive", "error", err)
		return fmt.Errorf("zip.OpenReader %s: %w", zipFilePath, err)
	}
	defer zr.Close()

	var processingErrors error
	csvFoundCount := 0

	// Derive base name for parquet files from the zip file name
	zipBaseName := strings.TrimSuffix(filepath.Base(zipFilePath), filepath.Ext(zipFilePath))

	// Loop through files within the zip archive
	for _, f := range zr.File {
		// Check context cancellation before processing each internal file
		select {
		case <-ctx.Done():
			l.Warn("Processing cancelled.")
			return errors.Join(processingErrors, ctx.Err()) // Combine errors with context error
		default:
			// Continue processing file
		}

		if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
			continue
		}
		csvFoundCount++
		csvBaseName := filepath.Base(f.Name)
		csvLogger := l.With(slog.String("internal_csv", csvBaseName)) // Create logger specific to this CSV
		csvLogger.Debug("Found CSV inside archive, starting stream processing.")

		// Open the file *within* the zip archive
		rc, err := f.Open()
		if err != nil {
			csvLogger.Error("Failed to open internal CSV file.", "error", err)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("open internal %s: %w", csvBaseName, err))
			// Log CSV processing error event? Or just aggregate for zip error event? Aggregate for now.
			continue // Process next file in zip
		}

		// Call the stream processor
		streamErr := processCSVStream(ctx, cfg, csvLogger, rc, zipBaseName, cfg.OutputDir)
		closeErr := rc.Close() // Close immediately after processing attempt

		if streamErr != nil {
			csvLogger.Error("Failed to process internal CSV stream.", "error", streamErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("process stream %s: %w", csvBaseName, streamErr))
			// Log CSV error event?
			// db.LogFileEvent(ctx, getDB(), csvBaseName, db.FileTypeCsv, db.EventError, originalZipURL, "", streamErr.Error(), "", nil) // Requires passing dbConn down
		} else {
			csvLogger.Debug("Successfully processed CSV stream to Parquet sections.")
		}
		if closeErr != nil {
			// Log error closing the internal reader, but don't necessarily fail the whole zip for it
			csvLogger.Warn("Error closing internal CSV reader.", "error", closeErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("close reader %s: %w", csvBaseName, closeErr))
		}
	}

	if csvFoundCount == 0 {
		l.Info("No CSV files found within this zip archive.")
		// Is this an error? Or just an empty zip? Treat as success for now.
	} else if processingErrors != nil {
		l.Warn("Finished processing zip, but encountered errors with internal CSVs.", "error", processingErrors)
	} else {
		l.Debug("Finished processing all CSVs found in zip archive.")
	}

	return processingErrors // Return aggregated errors from processing internal CSVs
}

// processCSVStream reads CSV data from a reader (e.g., from within a zip)
// and writes Parquet sections.
func processCSVStream(ctx context.Context, cfg config.Config, logger *slog.Logger, csvReader io.Reader, zipBaseName string, outputDir string) error {
	// This function mirrors the logic of the old processSingleCSV/processCSVSections,
	// but operates on a Reader instead of opening a file path.

	scanner := util.NewPeekableScanner(csvReader) // Use scanner on the provided reader
	// Buffer settings might need adjustment depending on typical line lengths
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024) // Example buffer

	var pw *writer.CSVWriter
	var fw source.ParquetFile
	var currentHeaders []string
	var currentComp, currentVer string
	var currentMeta []string
	var isDateColumn []bool
	var schemaInferred bool = false
	var rowsCheckedForSchema int = 0
	lineNumber := int64(0)
	var accumulatedErrors error
	var currentParquetPath string // Track path for logging and closing

	// Ensure final writer/file is closed if loop exits unexpectedly
	defer func() {
		if pw != nil {
			logger.Warn("Closing Parquet writer due to unexpected exit in processCSVStream", slog.String("path", currentParquetPath))
			if err := pw.WriteStop(); err != nil {
				logger.Error("Error stopping Parquet writer on deferred close", "path", currentParquetPath, "error", err)
			}
		}
		if fw != nil {
			if err := fw.Close(); err != nil {
				logger.Error("Error closing Parquet file on deferred close", "path", currentParquetPath, "error", err)
			}
		}
	}()

	// --- Main Scanner Loop (adapted for slog, takes reader) ---
	for scanner.Scan() {
		// Check context cancellation at the start of each line processing
		select {
		case <-ctx.Done():
			logger.Warn("Stream processing cancelled.")
			return errors.Join(accumulatedErrors, ctx.Err()) // Combine errors with context error
		default:
			// Continue processing line
		}

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
		l := logger.With(slog.Int64("line", lineNumber), slog.String("record_type", recordType))

		switch recordType {
		case "I":
			// Finalize previous Parquet writer if active
			if pw != nil {
				l.Debug("Finalizing previous section", slog.String("comp", currentComp), slog.String("ver", currentVer), slog.String("path", currentParquetPath))
				if err := pw.WriteStop(); err != nil {
					l.Warn("Error stopping writer", "error", err)
					accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("stop writer %s: %w", currentParquetPath, err))
				}
				if err := fw.Close(); err != nil {
					l.Warn("Error closing file", "error", err)
					accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("close file %s: %w", currentParquetPath, err))
				}
				pw = nil
				fw = nil
				currentParquetPath = ""
			}
			currentMeta = nil
			isDateColumn = nil
			schemaInferred = false
			rowsCheckedForSchema = 0 // Reset for new section

			if len(rec) < 5 {
				l.Warn("Malformed 'I' record, skipping section", "record", line)
				currentHeaders = nil
				continue
			}
			currentComp, currentVer = strings.TrimSpace(rec[2]), strings.TrimSpace(rec[3])
			currentHeaders = make([]string, 0, len(rec)-4)
			for _, h := range rec[4:] {
				currentHeaders = append(currentHeaders, strings.TrimSpace(h))
			}
			if len(currentHeaders) == 0 {
				l.Warn("'I' record has no header columns, skipping section.", "comp", currentComp, "ver", currentVer)
				continue
			}
			l.Debug("Found section header.", slog.String("comp", currentComp), slog.String("ver", currentVer))

		case "D":
			if len(currentHeaders) == 0 {
				continue
			} // Skip data if no header active
			l = l.With(slog.String("comp", currentComp), slog.String("ver", currentVer)) // Add section context
			if len(rec) < 4 {
				l.Warn("Malformed 'D' record, skipping row.", "record", line)
				continue
			}
			values := make([]string, len(currentHeaders))
			numDataCols := len(rec) - 4
			hasBlanks := false
			for i := 0; i < len(currentHeaders); i++ {
				if i < numDataCols {
					values[i] = strings.TrimSpace(strings.Trim(rec[i+4], `"`))
					if values[i] == "" {
						hasBlanks = true
					}
				} else {
					values[i] = ""
					hasBlanks = true
				}
			}

			if !schemaInferred { // --- Schema Inference ---
				rowsCheckedForSchema++
				peekedType, peekErr := scanner.PeekRecordType()
				mustInferNow := !hasBlanks || (peekErr != nil || peekedType != "D") || rowsCheckedForSchema >= cfg.SchemaRowLimit
				if mustInferNow {
					if hasBlanks {
						l.Warn("Inferring schema from row with blanks", "attempt", rowsCheckedForSchema)
					} else {
						l.Debug("Inferring schema", "attempt", rowsCheckedForSchema)
					}
					currentMeta = make([]string, len(currentHeaders))
					isDateColumn = make([]bool, len(currentHeaders))
					for i := range currentHeaders { // ... inference logic ...
						var typ string
						val := values[i]
						isDate := false
						if util.IsNEMDateTime(val) {
							typ = "INT64"
							isDate = true
						} else if val == "" {
							typ = "BYTE_ARRAY"
						} else if _, pErr := strconv.ParseBool(val); pErr == nil {
							typ = "BOOLEAN"
						} else if _, pErr := strconv.ParseInt(val, 10, 64); pErr == nil {
							typ = "INT64"
						} else if _, pErr := strconv.ParseFloat(val, 64); pErr == nil {
							typ = "DOUBLE"
						} else {
							typ = "BYTE_ARRAY"
						}
						isDateColumn[i] = isDate
						cleanH := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(currentHeaders[i], " ", "_"), ".", "_"), ";", "_")
						if cleanH == "" {
							cleanH = fmt.Sprintf("column_%d", i)
						}
						if typ == "BYTE_ARRAY" {
							currentMeta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", cleanH)
						} else {
							currentMeta[i] = fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", cleanH, typ)
						}
					}
					l.Debug("Inferred schema", slog.String("schema_str", strings.Join(currentMeta, "; ")))

					// Initialize Parquet writer - use zipBaseName from argument
					parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", zipBaseName, currentComp, currentVer)
					path := filepath.Join(outputDir, parquetFile)
					currentParquetPath = path // Store path for potential deferred close

					// Check if output file already exists - skip section if it does? Or overwrite?
					// For simplicity, let's overwrite for now. Add flag later if needed.
					// if _, statErr := os.Stat(path); statErr == nil {
					//     l.Warn("Output parquet file already exists, skipping section.", "path", path)
					//     currentHeaders = nil // Invalidate headers to skip data rows for this section
					//     schemaInferred = false // Allow re-inferring if another 'I' appears
					//     continue
					// }

					var createErr error
					fw, createErr = local.NewLocalFileWriter(path) // Write to output dir
					if createErr != nil {
						l.Error("Failed create parquet file", "path", path, "error", createErr)
						currentHeaders = nil
						schemaInferred = false // Skip section
						accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("create file %s: %w", path, createErr))
						continue // Skip to next line in CSV, effectively skipping section data
					}
					pw, createErr = writer.NewCSVWriter(currentMeta, fw, 4) // Use 4 write threads
					if createErr != nil {
						l.Error("Failed init parquet writer", "path", path, "error", createErr)
						fw.Close()
						pw = nil
						currentHeaders = nil
						schemaInferred = false // Skip section
						accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("create writer %s: %w", path, createErr))
						continue
					}
					pw.CompressionType = parquet.CompressionCodec_SNAPPY
					l.Debug("Created Parquet writer.", slog.String("path", path))
					schemaInferred = true
				} else {
					l.Debug("Skipping schema inference on blank row, waiting for cleaner row.", slog.Int("attempt", rowsCheckedForSchema))
					continue // Skip this data row, wait for inference
				}
			} // --- End Schema Inference ---

			if !schemaInferred || pw == nil {
				continue
			} // --- Write Data Row ---
			recPtrs := make([]*string, len(values))
			for j := 0; j < len(values); j++ { // ... data prep logic ...
				isEmpty := values[j] == ""
				finalValue := values[j]
				if isDateColumn[j] && !isEmpty {
					epochMS, convErr := util.NEMDateTimeToEpochMS(values[j])
					if convErr != nil {
						l.Warn("Failed convert date, writing NULL.", "column", currentHeaders[j], "value", values[j], "error", convErr)
						recPtrs[j] = nil
						continue
					}
					finalValue = strconv.FormatInt(epochMS, 10)
				}
				isTargetString := j < len(currentMeta) && strings.Contains(currentMeta[j], "type=BYTE_ARRAY")
				if isEmpty && !isTargetString {
					recPtrs[j] = nil
				} else {
					temp := finalValue
					recPtrs[j] = &temp
				}
			}
			if writeErr := pw.WriteString(recPtrs); writeErr != nil {
				l.Warn("WriteString error, skipping row.", "error", writeErr) // Don't log data by default
				// accumulatedErrors = errors.Join(accumulatedErrors, writeErr) // Optionally accumulate write errors
			}

		default: // Ignore other record types
			continue
		}
	} // End scanner loop

	// Finalize last section normally (defer handles unexpected exits)
	if pw != nil {
		logger.Debug("Finalizing last section", slog.String("comp", currentComp), slog.String("ver", currentVer), slog.String("path", currentParquetPath))
		if err := pw.WriteStop(); err != nil {
			logger.Warn("Error stopping writer for last section", "error", err)
			accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("stop writer %s: %w", currentParquetPath, err))
		}
		if err := fw.Close(); err != nil {
			logger.Warn("Error closing file for last section", "error", err)
			accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("close file %s: %w", currentParquetPath, err))
		}
	}
	// Check for scanner errors at the end
	if scanErr := scanner.Err(); scanErr != nil {
		logger.Error("Scanner error processing stream", "error", scanErr)
		accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("scanner: %w", scanErr))
	}

	return accumulatedErrors
}
