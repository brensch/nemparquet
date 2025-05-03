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
					// Optionally store a specific error/skip reason
					// errorsMap.Store(zipFilePath, fmt.Errorf("original URL not found in DB for path %s", absZipFilePath))
					continue
				}
				jobLogger = jobLogger.With(slog.String("zip_url", originalZipURL)) // Add URL context

				// 2. Check if already processed successfully using the URL
				processCompleted, dbErr := db.HasEventOccurred(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventProcessEnd)
				if dbErr != nil {
					jobLogger.Warn("Failed check DB state for process completion, proceeding cautiously.", "error", dbErr)
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
			continue // Process next file in zip
		}

		// Call the stream processor
		streamErr := processCSVStream(ctx, cfg, csvLogger, rc, zipBaseName, cfg.OutputDir)
		closeErr := rc.Close() // Close immediately after processing attempt

		if streamErr != nil {
			csvLogger.Error("Failed to process internal CSV stream.", "error", streamErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("process stream %s: %w", csvBaseName, streamErr))
		} else {
			csvLogger.Debug("Successfully processed CSV stream to Parquet sections.")
		}
		if closeErr != nil {
			csvLogger.Warn("Error closing internal CSV reader.", "error", closeErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("close reader %s: %w", csvBaseName, closeErr))
		}
	}

	if csvFoundCount == 0 {
		l.Info("No CSV files found within this zip archive.")
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
	scanner := util.NewPeekableScanner(csvReader)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

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

	// Defer cleanup for unexpected exits (errors, panics, context cancellation)
	defer func() {
		if pw != nil {
			// Check if exiting normally (accumulatedErrors is nil AND context is not Done)
			// If exiting normally, the cleanup after the loop should handle it.
			// Only log/cleanup here if exiting abnormally.
			normalExit := accumulatedErrors == nil && ctx.Err() == nil
			if !normalExit {
				logger.Warn("Closing Parquet writer/file due to error or cancellation in processCSVStream",
					slog.String("path", currentParquetPath),
					slog.Any("accumulated_error", accumulatedErrors),
					slog.Any("context_error", ctx.Err()),
				)
				if stopErr := pw.WriteStop(); stopErr != nil {
					logger.Error("Error stopping Parquet writer on deferred close", "path", currentParquetPath, "error", stopErr)
				}
				if closeErr := fw.Close(); closeErr != nil {
					logger.Error("Error closing Parquet file on deferred close", "path", currentParquetPath, "error", closeErr)
				}
			}
		}
	}()

	// --- Main Scanner Loop ---
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			logger.Warn("Stream processing cancelled.")
			accumulatedErrors = errors.Join(accumulatedErrors, ctx.Err())
			return accumulatedErrors // Exit loop and function
		default:
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
				currentParquetPath = "" // Reset after successful close
			}
			currentMeta = nil
			isDateColumn = nil
			schemaInferred = false
			rowsCheckedForSchema = 0 // Reset

			if len(rec) < 5 {
				l.Warn("Malformed 'I' record", "record", line)
				currentHeaders = nil
				continue
			}
			currentComp, currentVer = strings.TrimSpace(rec[2]), strings.TrimSpace(rec[3])
			currentHeaders = make([]string, 0, len(rec)-4)
			for _, h := range rec[4:] {
				currentHeaders = append(currentHeaders, strings.TrimSpace(h))
			}
			if len(currentHeaders) == 0 {
				l.Warn("'I' record no headers", "comp", currentComp, "ver", currentVer)
				continue
			}
			l.Debug("Found section header.", "comp", currentComp, "ver", currentVer)

		case "D":
			if len(currentHeaders) == 0 {
				continue
			}
			l = l.With(slog.String("comp", currentComp), slog.String("ver", currentVer))
			if len(rec) < 4 {
				l.Warn("Malformed 'D' record", "record", line)
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
					// *** Log schema inference warning if needed ***
					if hasBlanks {
						l.Warn("Inferring schema from row with blanks", "attempt", rowsCheckedForSchema, slog.Int("limit", cfg.SchemaRowLimit))
					} else {
						l.Debug("Inferring schema", "attempt", rowsCheckedForSchema)
					}
					currentMeta = make([]string, len(currentHeaders))
					isDateColumn = make([]bool, len(currentHeaders))
					for i := range currentHeaders { /* ... inference logic ... */
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
					parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", zipBaseName, currentComp, currentVer)
					path := filepath.Join(outputDir, parquetFile)
					currentParquetPath = path
					var createErr error
					fw, createErr = local.NewLocalFileWriter(path)
					if createErr != nil {
						l.Error("Failed create parquet file", "path", path, "error", createErr)
						currentHeaders = nil
						schemaInferred = false
						accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("create file %s: %w", path, createErr))
						continue
					}
					pw, createErr = writer.NewCSVWriter(currentMeta, fw, 4)
					if createErr != nil {
						l.Error("Failed init parquet writer", "path", path, "error", createErr)
						fw.Close()
						pw = nil
						currentHeaders = nil
						schemaInferred = false
						accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("create writer %s: %w", path, createErr))
						continue
					}
					pw.CompressionType = parquet.CompressionCodec_SNAPPY
					l.Debug("Created Parquet writer.", slog.String("path", path))
					schemaInferred = true
				} else {
					l.Debug("Skipping schema inference on blank row", slog.Int("attempt", rowsCheckedForSchema))
					continue
				}
			} // --- End Schema Inference ---

			if !schemaInferred || pw == nil {
				continue
			} // --- Write Data Row ---
			recPtrs := make([]*string, len(values))
			for j := 0; j < len(values); j++ { /* ... data prep logic ... */
				isEmpty := values[j] == ""
				finalValue := values[j]
				if isDateColumn[j] && !isEmpty {
					epochMS, convErr := util.NEMDateTimeToEpochMS(values[j])
					if convErr != nil {
						l.Warn("Failed convert date", "col", currentHeaders[j], "val", values[j], "error", convErr)
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
				l.Warn("WriteString error", "error", writeErr)
			}

		default:
			continue
		}
	} // End scanner loop

	// --- Reinstated Cleanup Block for Normal Loop Exit ---
	// This runs only if the loop finishes without error or cancellation.
	scanErr := scanner.Err() // Check scanner error *before* final cleanup
	if scanErr == nil && ctx.Err() == nil {
		// Finalize the very last section if one was active
		if pw != nil {
			logger.Debug("Finalizing last section after loop", slog.String("comp", currentComp), slog.String("ver", currentVer), slog.String("path", currentParquetPath))
			if err := pw.WriteStop(); err != nil {
				logger.Warn("Error stopping writer for last section", "error", err)
				accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("stop writer %s: %w", currentParquetPath, err))
			}
			if err := fw.Close(); err != nil {
				logger.Warn("Error closing file for last section", "error", err)
				accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("close file %s: %w", currentParquetPath, err))
			}
			// Set to nil to prevent defer block from running cleanup again
			pw = nil
			fw = nil
			currentParquetPath = ""
		}
	}
	// --- End Reinstated Cleanup Block ---

	// Check for scanner errors again after potential cleanup attempts
	if scanErr != nil {
		logger.Error("Scanner error processing stream", "error", scanErr)
		accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("scanner: %w", scanErr))
	}

	return accumulatedErrors // Return accumulated errors (or nil if none)
}
