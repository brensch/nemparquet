package processor

import (
	"archive/zip"
	// "bufio" // No longer needed for main loop
	"context"
	"database/sql"
	"encoding/csv" // Import encoding/csv
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util" // Keep for datetime

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// StartProcessorWorkers launches goroutines to process zip paths from a channel.
// It respects the forceProcess flag when checking DB state.
func StartProcessorWorkers(
	ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger,
	numWorkers int, pathsChan <-chan string, // Read from channel
	wg *sync.WaitGroup, errorsMap *sync.Map, // Use pointers for WG and Map
	forceProcess bool, // Add forceProcess flag
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
				// 1. Find original identifier (URL or inner zip name) from DB using output path
				absZipFilePath, pathErr := filepath.Abs(zipFilePath)
				if pathErr != nil {
					jobLogger.Error("Failed get absolute path, skipping.", "error", pathErr)
					errorsMap.Store(zipFilePath, pathErr) // Store error against the path key
					continue                              // Skip to next job
				}

				// Use the renamed GetIdentifierForOutputPath function
				identifier, fileType, foundID, dbErr := db.GetIdentifierForOutputPath(ctx, dbConn, absZipFilePath)
				if dbErr != nil {
					jobLogger.Error("DB error finding identifier for path, skipping.", "error", dbErr)
					errorsMap.Store(zipFilePath, dbErr)
					continue
				}
				if !foundID {
					jobLogger.Warn("Could not find identifier (URL/inner name) in DB for this zip path, skipping.", slog.String("abs_path", absZipFilePath))
					// Optionally store a specific error/skip reason
					// errorsMap.Store(zipFilePath, fmt.Errorf("identifier not found in DB for path %s", absZipFilePath))
					continue
				}
				// Log the correct file type found
				jobLogger = jobLogger.With(slog.String("identifier", identifier), slog.String("file_type", fileType))

				// 2. Check if already processed successfully using the identifier and type
				processCompleted := false // Assume not completed initially
				if !forceProcess {        // Only check DB if not forcing
					var checkErr error
					// Use the found identifier and filetype for the check
					processCompleted, checkErr = db.HasEventOccurred(ctx, dbConn, identifier, fileType, db.EventProcessEnd)
					if checkErr != nil {
						jobLogger.Warn("Failed check DB state for process completion, proceeding cautiously.", "error", checkErr)
						db.LogFileEvent(ctx, dbConn, identifier, fileType, db.EventError, "", "", fmt.Sprintf("db check process failed: %v", checkErr), "", nil)
						processCompleted = false // Assume not completed if check fails
					}
				} else {
					jobLogger.Info("Force process enabled, skipping DB completion check.")
				}

				if processCompleted { // Skip only if not forcing AND already completed
					jobLogger.Info("Skipping processing, already completed according to DB.")
					db.LogFileEvent(ctx, dbConn, identifier, fileType, db.EventSkipProcess, "", "", "Already processed", "", nil)
					continue // Skip to next job
				}

				// --- Proceed with Processing ---
				jobLogger.Info("Starting processing.")
				startTime := time.Now()
				// Log process start for the identifier
				db.LogFileEvent(ctx, dbConn, identifier, fileType, db.EventProcessStart, "", "", "", "", nil)

				// 3. Process the Zip Archive
				// Pass identifier and fileType for potential logging inside
				processErr := processSingleZipArchive(ctx, cfg, jobLogger, zipFilePath, identifier, fileType)
				duration := time.Since(startTime)

				// 4. Log event based on result for the identifier
				if processErr != nil {
					jobLogger.Error("Failed to process zip archive", "error", processErr, slog.Duration("duration", duration.Round(time.Millisecond)))
					db.LogFileEvent(ctx, dbConn, identifier, fileType, db.EventError, "", "", processErr.Error(), "", &duration)
					errorsMap.Store(zipFilePath, processErr) // Store error against path
				} else {
					jobLogger.Info("Processing successful.", slog.Duration("duration", duration.Round(time.Millisecond)))
					db.LogFileEvent(ctx, dbConn, identifier, fileType, db.EventProcessEnd, "", "", "", "", &duration)
				}

			} // End job loop (channel closed)
			l.Debug("Processing worker finished")
		}(i)
	}
}

// processSingleZipArchive opens a zip archive from disk and processes contained CSVs.
// Now takes identifier and fileType for context.
func processSingleZipArchive(ctx context.Context, cfg config.Config, logger *slog.Logger, zipFilePath string, identifier string, fileType string) error {
	l := logger // Use logger with existing context

	l.Debug("Opening zip archive from disk.")
	zr, err := zip.OpenReader(zipFilePath)
	if err != nil {
		l.Error("Failed to open zip archive", "error", err)
		return fmt.Errorf("zip.OpenReader %s: %w", zipFilePath, err)
	}
	defer zr.Close()

	var processingErrors error
	csvFoundCount := 0
	zipBaseName := strings.TrimSuffix(filepath.Base(zipFilePath), filepath.Ext(zipFilePath))

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
		// Pass identifier (original zip URL/name) as source context if needed by stream processor?
		// Currently processCSVStream only uses zipBaseName (from the local file) for output naming.
		streamErr := processCSVStream(ctx, cfg, csvLogger, rc, zipBaseName, cfg.OutputDir)
		closeErr := rc.Close() // Close immediately after processing attempt

		if streamErr != nil {
			csvLogger.Error("Failed to process internal CSV stream.", "error", streamErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("process stream %s: %w", csvBaseName, streamErr))
			// Log CSV error event?
			// db.LogFileEvent(ctx, getDB(), csvBaseName, db.FileTypeCsv, db.EventError, identifier, "", streamErr.Error(), "", nil) // Requires passing dbConn down
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

// --- Section Processing Logic (Using encoding/csv) ---

// csvSection holds state for processing one I..D..D section
type csvSection struct {
	Cfg             config.Config
	Logger          *slog.Logger
	ZipBaseName     string // Base name derived from the zip file
	OutputDir       string
	Comp            string   // Component from 'I' record
	Ver             string   // Version from 'I' record
	Headers         []string // Headers from 'I' record
	Meta            []string // Parquet schema meta strings
	IsDateColumn    []bool   // Flags for date columns
	SchemaInferred  bool
	RowsChecked     int
	ParquetFilePath string
	FileWriter      source.ParquetFile // Interface for file writing
	ParquetWriter   *writer.CSVWriter  // Parquet library writer
}

// newCSVSection initializes a section processor.
func newCSVSection(ctx context.Context, cfg config.Config, logger *slog.Logger, zipBaseName, outputDir, comp, ver string, headers []string) (*csvSection, error) {
	s := &csvSection{
		Cfg:         cfg,
		Logger:      logger.With(slog.String("comp", comp), slog.String("ver", ver)),
		ZipBaseName: zipBaseName,
		OutputDir:   outputDir,
		Comp:        comp,
		Ver:         ver,
		Headers:     headers,
	}
	s.Logger.Debug("New CSV section initialized.")
	return s, nil
}

// inferSchemaAndInitWriter attempts to infer the schema from a data row and initializes the Parquet writer.
func (s *csvSection) inferSchemaAndInitWriter(values []string) error {
	s.Logger.Debug("Inferring schema and initializing writer.")
	s.Meta = make([]string, len(s.Headers))
	s.IsDateColumn = make([]bool, len(s.Headers))

	for i := range s.Headers {
		var typ string
		val := values[i] // Value from the first valid data row
		headerLower := strings.ToLower(s.Headers[i])
		isDate := false

		if util.IsNEMDateTime(val) {
			typ = "INT64" // Store dates as epoch milliseconds
			isDate = true
		} else if val == "" {
			// HEURISTIC FOR BLANK VALUES during inference
			if strings.Contains(headerLower, "datetime") || strings.Contains(headerLower, "_date") || strings.Contains(headerLower, "_time") {
				typ = "INT64"
				isDate = true
			} else if strings.Contains(headerLower, "cost") || strings.Contains(headerLower, "rate") || strings.Contains(headerLower, "price") || strings.Contains(headerLower, "value") || strings.Contains(headerLower, "factor") || strings.Contains(headerLower, "mw") || strings.Contains(headerLower, "usage") || strings.Contains(headerLower, "rcr") || strings.HasSuffix(headerLower, "fpp") {
				typ = "DOUBLE"
			} else if strings.Contains(headerLower, "versionno") || strings.Contains(headerLower, "runno") || strings.HasSuffix(headerLower, "id") {
				typ = "INT64" // Assume IDs are numeric even if sometimes blank
			} else {
				typ = "BYTE_ARRAY" // Default remaining blanks to string
			}
			s.Logger.Debug("Inferred type from blank value based on header heuristic", slog.String("header", s.Headers[i]), slog.String("inferred_type", typ))
		} else if _, pErr := strconv.ParseBool(val); pErr == nil {
			typ = "BOOLEAN"
		} else if _, pErr := strconv.ParseInt(val, 10, 64); pErr == nil {
			typ = "INT64"
		} else if _, pErr := strconv.ParseFloat(val, 64); pErr == nil {
			typ = "DOUBLE"
		} else {
			typ = "BYTE_ARRAY" // Default non-empty, non-parsable to string
		}
		s.IsDateColumn[i] = isDate // Store if it's a date type

		// Clean header name for Parquet columns
		cleanH := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(s.Headers[i], " ", "_"), ".", "_"), ";", "_")
		if cleanH == "" {
			cleanH = fmt.Sprintf("column_%d", i)
		} // Fallback

		// Define schema element as optional
		if typ == "BYTE_ARRAY" {
			s.Meta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", cleanH)
		} else {
			s.Meta[i] = fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", cleanH, typ)
		}
	}
	s.Logger.Debug("Inferred schema", slog.String("schema_str", strings.Join(s.Meta, "; ")))

	// Initialize Parquet writer
	parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", s.ZipBaseName, s.Comp, s.Ver)
	s.ParquetFilePath = filepath.Join(s.OutputDir, parquetFile)

	var createErr error
	s.FileWriter, createErr = local.NewLocalFileWriter(s.ParquetFilePath)
	if createErr != nil {
		s.Logger.Error("Failed create parquet file", "path", s.ParquetFilePath, "error", createErr)
		return fmt.Errorf("create file %s: %w", s.ParquetFilePath, createErr)
	}

	s.ParquetWriter, createErr = writer.NewCSVWriter(s.Meta, s.FileWriter, 4) // Use 4 write threads
	if createErr != nil {
		s.Logger.Error("Failed init parquet writer", "path", s.ParquetFilePath, "error", createErr)
		s.FileWriter.Close() // Attempt to close the file handle
		return fmt.Errorf("create writer %s: %w", s.ParquetFilePath, createErr)
	}
	s.ParquetWriter.CompressionType = parquet.CompressionCodec_SNAPPY
	s.Logger.Debug("Created Parquet writer.", slog.String("path", s.ParquetFilePath))
	s.SchemaInferred = true
	return nil
}

// writeRow prepares, validates, and writes a single data row.
// Returns a specific validation error if pre-write checks fail, or an error
// from WriteString itself. Returns nil on success.
func (s *csvSection) writeRow(record []string) error {
	if !s.SchemaInferred || s.ParquetWriter == nil {
		return errors.New("cannot write row: schema not inferred or writer not initialized")
	}

	numExpectedFields := len(s.Headers)
	// Pad/truncate record to match header count
	if len(record) < numExpectedFields {
		s.Logger.Warn("Data row has fewer columns than header, padding with blanks.", "data_cols", len(record), "header_cols", numExpectedFields)
		paddedRecord := make([]string, numExpectedFields)
		copy(paddedRecord, record)
		for i := len(record); i < numExpectedFields; i++ {
			paddedRecord[i] = ""
		}
		record = paddedRecord
	} else if len(record) > numExpectedFields {
		s.Logger.Warn("Data row has more columns than header, truncating data.", "data_cols", len(record), "header_cols", numExpectedFields)
		record = record[:numExpectedFields]
	}

	recPtrs := make([]*string, numExpectedFields)
	validationFailed := false // Flag to track if validation fails for this row

	var accumulatedErrors error
	for j := 0; j < numExpectedFields; j++ {
		value := record[j]
		isEmpty := value == ""
		finalValue := value // Start with the original value from the (padded/truncated) record

		// Determine target type from schema meta string
		isTargetStringType := false
		targetType := "UNKNOWN" // Default
		if j < len(s.Meta) {
			metaLower := strings.ToLower(s.Meta[j])
			if strings.Contains(metaLower, "type=byte_array") {
				isTargetStringType = true
				targetType = "STRING"
			} else if strings.Contains(metaLower, "type=int") {
				targetType = "INT64"
			} else if strings.Contains(metaLower, "type=double") || strings.Contains(metaLower, "type=float") {
				targetType = "DOUBLE"
			} else if strings.Contains(metaLower, "type=boolean") {
				targetType = "BOOLEAN"
			}
		}

		// Convert date strings if applicable and not empty
		if s.IsDateColumn[j] && !isEmpty {
			targetType = "DATETIME_AS_INT64" // Override type for validation check
			epochMS, convErr := util.NEMDateTimeToEpochMS(value)
			if convErr != nil {
				// Return a specific validation error for this column
				err := fmt.Errorf("validation failed: column '%s': %w", s.Headers[j], convErr)
				// s.Logger.Warn(err.Error(), "value", value) // Logged by caller
				return err // Stop processing this row and return validation error
			}
			finalValue = strconv.FormatInt(epochMS, 10) // Use converted value for validation/writing
		}

		// *** PRE-WRITE VALIDATION ***
		if isEmpty && !isTargetStringType {
			// Empty string for a non-string target type. Prepare nil pointer.
			recPtrs[j] = nil
		} else if !isEmpty {
			// Value is not empty, check if it's parseable for non-string types
			parseError := false
			var underlyingParseErr error
			switch targetType {
			case "INT64", "DATETIME_AS_INT64":
				_, underlyingParseErr = strconv.ParseInt(finalValue, 10, 64)
				if underlyingParseErr != nil {
					parseError = true
				}
			case "DOUBLE":
				_, underlyingParseErr = strconv.ParseFloat(finalValue, 64)
				if underlyingParseErr != nil {
					parseError = true
				}
			case "BOOLEAN":
				_, underlyingParseErr = strconv.ParseBool(finalValue)
				if underlyingParseErr != nil {
					parseError = true
				}
			}

			if parseError {
				// Return a specific validation error
				err := fmt.Errorf("validation failed: column '%s' (type %s): cannot parse value: %w", s.Headers[j], targetType, underlyingParseErr)
				// s.Logger.Warn(err.Error(), "value", fmt.Sprintf("%q", value)) // Logged by caller
				validationFailed = true                                 // Mark row as failed
				accumulatedErrors = errors.Join(accumulatedErrors, err) // Accumulate validation error
				recPtrs[j] = nil                                        // Set to nil to avoid WriteString error if possible, though row will be skipped
				continue                                                // Continue processing other columns for potential errors? Or break? Let's continue.
				// break // Stop processing this row if any column fails validation
			}

			// Validation passed or it's a string type, prepare pointer
			temp := finalValue
			recPtrs[j] = &temp

		} else { // Value is empty AND target is string
			temp := ""
			recPtrs[j] = &temp
		}
	} // End loop preparing recPtrs

	// If validation failed for any column, return the accumulated validation errors
	if validationFailed {
		return accumulatedErrors // Return the specific validation error(s)
	}

	// Validation passed, attempt to write the prepared row
	if writeErr := s.ParquetWriter.WriteString(recPtrs); writeErr != nil {
		s.Logger.Warn("WriteString error", "error", writeErr)
		// Return the error from WriteString itself
		return writeErr
	}

	return nil // Row written successfully
}

// close finalizes the current Parquet section.
func (s *csvSection) close() error {
	var closeErrors error
	l := s.Logger.With(slog.String("path", s.ParquetFilePath)) // Add path context
	if s.ParquetWriter != nil {
		l.Debug("Stopping Parquet writer.")
		if err := s.ParquetWriter.WriteStop(); err != nil {
			l.Warn("Error stopping writer", "error", err)
			closeErrors = errors.Join(closeErrors, fmt.Errorf("stop writer %s: %w", s.ParquetFilePath, err))
		}
	}
	if s.FileWriter != nil {
		l.Debug("Closing Parquet file.")
		if err := s.FileWriter.Close(); err != nil {
			l.Warn("Error closing file", "error", err)
			closeErrors = errors.Join(closeErrors, fmt.Errorf("close file %s: %w", s.ParquetFilePath, err))
		}
	}
	// Mark as closed to prevent double closing in defer
	s.ParquetWriter = nil
	s.FileWriter = nil
	return closeErrors
}

// --- Main Stream Processing Function (Using encoding/csv) ---

// processCSVStream reads CSV data using encoding/csv and writes Parquet sections.
func processCSVStream(ctx context.Context, cfg config.Config, logger *slog.Logger, csvReader io.Reader, zipBaseName string, outputDir string) error {
	reader := csv.NewReader(csvReader)
	reader.Comment = '#'
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	var currentSection *csvSection
	var accumulatedErrors error
	lineNumber := int64(0)

	// Defer cleanup
	defer func() {
		if currentSection != nil && currentSection.ParquetWriter != nil {
			normalExit := accumulatedErrors == nil && ctx.Err() == nil
			if !normalExit {
				logger.Warn("Closing Parquet writer/file due to error or cancellation", slog.String("path", currentSection.ParquetFilePath), slog.Any("error", accumulatedErrors), slog.Any("context_error", ctx.Err()))
				if err := currentSection.close(); err != nil {
					logger.Error("Error during deferred cleanup of section", "error", err)
				}
			}
		}
	}()

	// --- Main Record Reading Loop ---
	for {
		select {
		case <-ctx.Done():
			logger.Warn("Stream processing cancelled.")
			accumulatedErrors = errors.Join(accumulatedErrors, ctx.Err())
			return accumulatedErrors
		default:
		}
		lineNumber++
		record, err := reader.Read()
		if err == io.EOF {
			logger.Debug("Reached end of CSV stream.")
			break
		}
		if err != nil {
			if parseErr, ok := err.(*csv.ParseError); ok {
				logger.Warn("CSV parsing error, skipping row.", slog.Int64("line", lineNumber), slog.String("csv_error", parseErr.Error()))
				accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv parse line %d: %w", lineNumber, err))
				continue
			}
			logger.Error("Error reading CSV stream", slog.Int64("line", lineNumber), "error", err)
			accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv read line %d: %w", lineNumber, err))
			return accumulatedErrors
		}
		if len(record) == 0 {
			continue
		}
		recordType := record[0]
		l := logger.With(slog.Int64("line", lineNumber), slog.String("record_type", recordType))

		switch recordType {
		case "I":
			if currentSection != nil {
				if err := currentSection.close(); err != nil {
					accumulatedErrors = errors.Join(accumulatedErrors, err)
				}
				currentSection = nil
			}
			if len(record) < 4 {
				l.Warn("Malformed 'I' record", "record", record)
				continue
			}
			comp, ver := record[2], record[3]
			headers := make([]string, 0, len(record)-4)
			for _, h := range record[4:] {
				headers = append(headers, h)
			}
			if len(headers) == 0 {
				l.Warn("'I' record no headers", "comp", comp, "ver", ver)
				continue
			}
			var sectionErr error
			currentSection, sectionErr = newCSVSection(ctx, cfg, logger, zipBaseName, outputDir, comp, ver, headers)
			if sectionErr != nil {
				l.Error("Failed to initialize new CSV section", "error", sectionErr)
				accumulatedErrors = errors.Join(accumulatedErrors, sectionErr)
				return accumulatedErrors
			}

		case "D":
			if currentSection == nil {
				continue
			}
			l = l.With(slog.String("comp", currentSection.Comp), slog.String("ver", currentSection.Ver))
			if len(record) < 4 {
				l.Warn("Malformed 'D' record", "record", record)
				continue
			}
			dataValues := record[4:]
			if !currentSection.SchemaInferred {
				currentSection.RowsChecked++
				hasBlanks := false
				for i := 0; i < len(currentSection.Headers); i++ {
					if i >= len(dataValues) || dataValues[i] == "" {
						hasBlanks = true
						break
					}
				}
				mustInferNow := !hasBlanks || currentSection.RowsChecked >= cfg.SchemaRowLimit
				if mustInferNow {
					if hasBlanks {
						l.Warn("Inferring schema from row with blanks", "attempt", currentSection.RowsChecked, slog.Int("limit", cfg.SchemaRowLimit))
					} else {
						l.Debug("Inferring schema", "attempt", currentSection.RowsChecked)
					}
					inferenceValues := make([]string, len(currentSection.Headers))
					for i := range inferenceValues {
						if i < len(dataValues) {
							inferenceValues[i] = dataValues[i]
						} else {
							inferenceValues[i] = ""
						}
					}
					err := currentSection.inferSchemaAndInitWriter(inferenceValues)
					if err != nil {
						l.Error("Failed to infer schema/init writer, skipping section.", "error", err)
						accumulatedErrors = errors.Join(accumulatedErrors, err)
						currentSection = nil
						continue
					}
				} else {
					l.Debug("Skipping schema inference on blank row", slog.Int("attempt", currentSection.RowsChecked))
					continue
				}
			}

			if currentSection != nil && currentSection.SchemaInferred {
				// Call writeRow which now includes validation and returns specific error
				writeErr := currentSection.writeRow(dataValues)
				if writeErr != nil {
					// Log the specific validation or WriteString error and accumulate it.
					// Continue processing next row.
					accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("write line %d: %w", lineNumber, writeErr))
					l.Warn("Accumulated error processing row, continuing.", "error", writeErr) // Logged in writeRow too
				}
			}

		default:
			continue
		}
	} // End record reading loop

	// --- Cleanup Block for Normal Loop Exit ---
	if ctx.Err() == nil { // Only close normally if context wasn't cancelled
		if currentSection != nil {
			if err := currentSection.close(); err != nil {
				accumulatedErrors = errors.Join(accumulatedErrors, err)
			}
			currentSection = nil
		}
	}

	// Scanner errors handled in loop
	return accumulatedErrors
}
