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

// StartProcessorWorkers (Unchanged)
func StartProcessorWorkers(
	ctx context.Context, cfg config.Config, dbConn *sql.DB, logger *slog.Logger,
	numWorkers int, pathsChan <-chan string,
	wg *sync.WaitGroup, errorsMap *sync.Map,
	forceProcess bool,
) {
	// ... (Implementation unchanged) ...
	logger.Info("Starting processor workers", slog.Int("count", numWorkers))
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			l := logger.With(slog.Int("worker_id", workerID))
			l.Debug("Processing worker started")
			for zipFilePath := range pathsChan {
				jobLogger := l.With(slog.String("zip_file_path", zipFilePath))
				jobLogger.Info("Checking processing state.")
				absZipFilePath, pathErr := filepath.Abs(zipFilePath)
				if pathErr != nil {
					jobLogger.Error("Failed get absolute path, skipping.", "error", pathErr)
					errorsMap.Store(zipFilePath, pathErr)
					continue
				}
				originalZipURL, foundURL, dbErr := db.GetZipURLForOutputPath(ctx, dbConn, absZipFilePath)
				if dbErr != nil {
					jobLogger.Error("DB error finding original URL, skipping.", "error", dbErr)
					errorsMap.Store(zipFilePath, dbErr)
					continue
				}
				if !foundURL {
					jobLogger.Warn("Could not find original download URL in DB, skipping.", slog.String("abs_path", absZipFilePath))
					continue
				}
				jobLogger = jobLogger.With(slog.String("zip_url", originalZipURL))
				processCompleted, dbErr := db.HasEventOccurred(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventProcessEnd)
				if dbErr != nil {
					jobLogger.Warn("Failed check DB state for process completion, proceeding cautiously.", "error", dbErr)
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventError, "", "", fmt.Sprintf("db check process failed: %v", dbErr), "", nil)
				}
				if !forceProcess && processCompleted {
					jobLogger.Info("Skipping processing, already completed according to DB.")
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventSkipProcess, "", "", "Already processed", "", nil)
					continue
				}
				jobLogger.Info("Starting processing.")
				startTime := time.Now()
				db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventProcessStart, "", "", "", "", nil)
				processErr := processSingleZipArchive(ctx, cfg, jobLogger, zipFilePath, originalZipURL)
				duration := time.Since(startTime)
				if processErr != nil {
					jobLogger.Error("Failed to process zip archive", "error", processErr, slog.Duration("duration", duration.Round(time.Millisecond)))
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventError, "", "", processErr.Error(), "", &duration)
					errorsMap.Store(zipFilePath, processErr)
				} else {
					jobLogger.Info("Processing successful.", slog.Duration("duration", duration.Round(time.Millisecond)))
					db.LogFileEvent(ctx, dbConn, originalZipURL, db.FileTypeZip, db.EventProcessEnd, "", "", "", "", &duration)
				}
			}
			l.Debug("Processing worker finished")
		}(i)
	}
}

// processSingleZipArchive (Unchanged)
func processSingleZipArchive(ctx context.Context, cfg config.Config, logger *slog.Logger, zipFilePath string, originalZipURL string) error {
	// ... (Implementation unchanged) ...
	l := logger
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
		select {
		case <-ctx.Done():
			l.Warn("Processing cancelled.")
			return errors.Join(processingErrors, ctx.Err())
		default:
		}
		if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
			continue
		}
		csvFoundCount++
		csvBaseName := filepath.Base(f.Name)
		csvLogger := l.With(slog.String("internal_csv", csvBaseName))
		csvLogger.Debug("Found CSV inside archive, starting stream processing.")
		rc, err := f.Open()
		if err != nil {
			csvLogger.Error("Failed to open internal CSV file.", "error", err)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("open internal %s: %w", csvBaseName, err))
			continue
		}
		streamErr := processCSVStream(ctx, cfg, csvLogger, rc, zipBaseName, cfg.OutputDir)
		closeErr := rc.Close()
		if streamErr != nil {
			csvLogger.Error("Failed to process internal CSV stream.", "error", streamErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("process stream %s: %w", csvBaseName, streamErr))
		} else {
			csvLogger.Debug("Successfully processed CSV stream.")
		}
		if closeErr != nil {
			csvLogger.Warn("Error closing internal CSV reader.", "error", closeErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("close reader %s: %w", csvBaseName, closeErr))
		}
	}
	if csvFoundCount == 0 {
		l.Info("No CSV files found within this zip archive.")
	} else if processingErrors != nil {
		l.Warn("Finished processing zip with internal CSV errors.", "error", processingErrors)
	} else {
		l.Debug("Finished processing all CSVs found in zip archive.")
	}
	return processingErrors
}

// --- Section Processing Logic (Using encoding/csv) ---

// csvSection (Unchanged)
type csvSection struct {
	Cfg             config.Config
	Logger          *slog.Logger
	ZipBaseName     string
	OutputDir       string
	Comp            string
	Ver             string
	Headers         []string
	Meta            []string
	IsDateColumn    []bool
	SchemaInferred  bool
	RowsChecked     int
	ParquetFilePath string
	FileWriter      source.ParquetFile
	ParquetWriter   *writer.CSVWriter
}

// newCSVSection (Unchanged)
func newCSVSection(ctx context.Context, cfg config.Config, logger *slog.Logger, zipBaseName, outputDir, comp, ver string, headers []string) (*csvSection, error) {
	// ... (Implementation unchanged) ...
	s := &csvSection{Cfg: cfg, Logger: logger.With(slog.String("comp", comp), slog.String("ver", ver)), ZipBaseName: zipBaseName, OutputDir: outputDir, Comp: comp, Ver: ver, Headers: headers}
	s.Logger.Debug("New CSV section initialized.")
	return s, nil
}

// inferSchemaAndInitWriter (Unchanged - still uses first valid data row)
func (s *csvSection) inferSchemaAndInitWriter(values []string) error {
	// ... (Implementation unchanged - heuristic based on header name for blanks) ...
	s.Logger.Debug("Inferring schema and initializing writer.")
	s.Meta = make([]string, len(s.Headers))
	s.IsDateColumn = make([]bool, len(s.Headers))
	for i := range s.Headers {
		var typ string
		val := values[i]
		headerLower := strings.ToLower(s.Headers[i])
		isDate := false
		if util.IsNEMDateTime(val) {
			typ = "INT64"
			isDate = true
		} else if val == "" {
			if strings.Contains(headerLower, "datetime") || strings.Contains(headerLower, "_date") || strings.Contains(headerLower, "_time") {
				typ = "INT64"
				isDate = true
			} else if strings.Contains(headerLower, "cost") || strings.Contains(headerLower, "rate") || strings.Contains(headerLower, "price") || strings.Contains(headerLower, "value") || strings.Contains(headerLower, "factor") || strings.Contains(headerLower, "mw") || strings.Contains(headerLower, "usage") || strings.Contains(headerLower, "rcr") || strings.HasSuffix(headerLower, "fpp") {
				typ = "DOUBLE"
			} else if strings.Contains(headerLower, "versionno") || strings.Contains(headerLower, "runno") || strings.HasSuffix(headerLower, "id") {
				typ = "INT64"
			} else {
				typ = "BYTE_ARRAY"
			}
			s.Logger.Debug("Inferred type from blank value based on header heuristic", slog.String("header", s.Headers[i]), slog.String("inferred_type", typ))
		} else if _, pErr := strconv.ParseBool(val); pErr == nil {
			typ = "BOOLEAN"
		} else if _, pErr := strconv.ParseInt(val, 10, 64); pErr == nil {
			typ = "INT64"
		} else if _, pErr := strconv.ParseFloat(val, 64); pErr == nil {
			typ = "DOUBLE"
		} else {
			typ = "BYTE_ARRAY"
		}
		s.IsDateColumn[i] = isDate
		cleanH := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(s.Headers[i], " ", "_"), ".", "_"), ";", "_")
		if cleanH == "" {
			cleanH = fmt.Sprintf("column_%d", i)
		}
		if typ == "BYTE_ARRAY" {
			s.Meta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", cleanH)
		} else {
			s.Meta[i] = fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", cleanH, typ)
		}
	}
	s.Logger.Debug("Inferred schema", slog.String("schema_str", strings.Join(s.Meta, "; ")))
	parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", s.ZipBaseName, s.Comp, s.Ver)
	s.ParquetFilePath = filepath.Join(s.OutputDir, parquetFile)
	var createErr error
	s.FileWriter, createErr = local.NewLocalFileWriter(s.ParquetFilePath)
	if createErr != nil {
		s.Logger.Error("Failed create parquet file", "path", s.ParquetFilePath, "error", createErr)
		return fmt.Errorf("create file %s: %w", s.ParquetFilePath, createErr)
	}
	s.ParquetWriter, createErr = writer.NewCSVWriter(s.Meta, s.FileWriter, 4)
	if createErr != nil {
		s.Logger.Error("Failed init parquet writer", "path", s.ParquetFilePath, "error", createErr)
		s.FileWriter.Close()
		return fmt.Errorf("create writer %s: %w", s.ParquetFilePath, createErr)
	}
	s.ParquetWriter.CompressionType = parquet.CompressionCodec_SNAPPY
	s.Logger.Debug("Created Parquet writer.", slog.String("path", s.ParquetFilePath))
	s.SchemaInferred = true
	return nil
}

// writeRow prepares and writes a single data row (slice of strings) to the Parquet file.
func (s *csvSection) writeRow(record []string) error { // Accept the record directly
	if !s.SchemaInferred || s.ParquetWriter == nil {
		return errors.New("cannot write row: schema not inferred or writer not initialized")
	}

	// Ensure record has at least the standard prefix columns + headers length
	// If it's shorter, parquet-go might handle padding, but being explicit is safer.
	// However, the main issue is handling the *correct* number of fields based on headers.
	numExpectedFields := len(s.Headers)
	if len(record) < numExpectedFields {
		s.Logger.Warn("Data row has fewer columns than header, padding with blanks.", "data_cols", len(record), "header_cols", numExpectedFields)
		// Pad the record slice with empty strings up to the expected length
		paddedRecord := make([]string, numExpectedFields)
		copy(paddedRecord, record) // Copy existing data
		for i := len(record); i < numExpectedFields; i++ {
			paddedRecord[i] = "" // Fill remaining with empty strings
		}
		record = paddedRecord // Use the padded record
	} else if len(record) > numExpectedFields {
		s.Logger.Warn("Data row has more columns than header, truncating data.", "data_cols", len(record), "header_cols", numExpectedFields)
		record = record[:numExpectedFields] // Truncate to expected length
	}

	recPtrs := make([]*string, numExpectedFields)
	for j := 0; j < numExpectedFields; j++ {
		// Use the value directly from the (potentially padded/truncated) record slice
		value := record[j] // No need to trim, csv.Reader handles quotes/whitespace
		isEmpty := value == ""
		finalValue := value

		// Convert date strings if applicable and not empty
		if s.IsDateColumn[j] && !isEmpty {
			epochMS, convErr := util.NEMDateTimeToEpochMS(value) // Use value here
			if convErr != nil {
				s.Logger.Warn("Failed convert date, writing NULL.", "column", s.Headers[j], "value", value, "error", convErr)
				recPtrs[j] = nil
				continue
			}
			finalValue = strconv.FormatInt(epochMS, 10)
		}

		// Determine target type from schema meta string
		isTargetStringType := false
		if j < len(s.Meta) && strings.Contains(strings.ToLower(s.Meta[j]), "type=byte_array") {
			isTargetStringType = true
		}

		// Handle empty strings based on target type
		if isEmpty {
			if isTargetStringType {
				temp := ""
				recPtrs[j] = &temp
			} else {
				recPtrs[j] = nil // NULL for non-string types
			}
		} else {
			temp := finalValue
			recPtrs[j] = &temp
		}
	}

	// Write the prepared row
	if writeErr := s.ParquetWriter.WriteString(recPtrs); writeErr != nil {
		s.Logger.Warn("WriteString error", "error", writeErr)
		return writeErr // Return the error
	}
	return nil
}

// close (Unchanged)
func (s *csvSection) close() error {
	// ... (Implementation unchanged) ...
	var closeErrors error
	l := s.Logger.With(slog.String("path", s.ParquetFilePath))
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
	s.ParquetWriter = nil
	s.FileWriter = nil
	return closeErrors
}

// --- Main Stream Processing Function (Using encoding/csv) ---

// processCSVStream reads CSV data using encoding/csv and writes Parquet sections.
func processCSVStream(ctx context.Context, cfg config.Config, logger *slog.Logger, csvReader io.Reader, zipBaseName string, outputDir string) error {
	// Use encoding/csv Reader
	reader := csv.NewReader(csvReader)
	reader.Comment = '#'        // Treat lines starting with # as comments (adjust if needed)
	reader.FieldsPerRecord = -1 // Allow variable number of fields per record
	reader.TrimLeadingSpace = true

	var currentSection *csvSection
	var accumulatedErrors error
	lineNumber := int64(0) // Track line number for logging context

	// Defer cleanup for the *last active* section
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
		// Check context cancellation before reading next record
		select {
		case <-ctx.Done():
			logger.Warn("Stream processing cancelled.")
			accumulatedErrors = errors.Join(accumulatedErrors, ctx.Err())
			return accumulatedErrors
		default:
			// Continue reading
		}

		lineNumber++
		record, err := reader.Read() // Read one record (slice of strings)

		// Handle end of file or read errors
		if err == io.EOF {
			logger.Debug("Reached end of CSV stream.")
			break // Exit loop cleanly
		}
		if err != nil {
			// Check for specific CSV parsing errors like wrong number of fields if needed
			if parseErr, ok := err.(*csv.ParseError); ok {
				logger.Warn("CSV parsing error, skipping row.", slog.Int64("line", lineNumber), slog.String("csv_error", parseErr.Error()))
				accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv parse line %d: %w", lineNumber, err))
				continue // Skip this problematic row
			}
			// Handle other potential read errors
			logger.Error("Error reading CSV stream", slog.Int64("line", lineNumber), "error", err)
			accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv read line %d: %w", lineNumber, err))
			return accumulatedErrors // Treat other read errors as fatal for this stream
		}

		// Process the record
		if len(record) == 0 {
			continue
		} // Skip empty records

		recordType := record[0] // No need to trim, csv.Reader handles it
		l := logger.With(slog.Int64("line", lineNumber), slog.String("record_type", recordType))

		switch recordType {
		case "I":
			// Finalize previous section
			if currentSection != nil {
				if err := currentSection.close(); err != nil {
					accumulatedErrors = errors.Join(accumulatedErrors, err)
				}
				currentSection = nil
			}

			// Start new section (validate record length)
			if len(record) < 4 {
				l.Warn("Malformed 'I' record (too few fields)", "record", record)
				continue
			}
			comp, ver := record[2], record[3] // No need to trim
			headers := make([]string, 0, len(record)-4)
			for _, h := range record[4:] {
				headers = append(headers, h)
			} // No need to trim
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
			} // Skip data if no section active
			l = l.With(slog.String("comp", currentSection.Comp), slog.String("ver", currentSection.Ver))

			// Validate record length relative to expected prefix + headers
			if len(record) < 4 {
				l.Warn("Malformed 'D' record (too few fields)", "record", record)
				continue
			}

			// Extract data values corresponding to headers
			// The slice `record[4:]` contains all data fields read by csv.Reader
			dataValues := record[4:]

			// Infer schema if not done yet, using the first valid 'D' record
			if !currentSection.SchemaInferred {
				// Check for blanks *before* inference attempt
				hasBlanks := false
				for i := 0; i < len(currentSection.Headers); i++ {
					if i >= len(dataValues) || dataValues[i] == "" { // Check bounds and value
						hasBlanks = true
						break
					}
				}

				// Decide if we need to infer now (or skip row if waiting for non-blank)
				currentSection.RowsChecked++
				// PeekRecordType is gone, so check next record type differently if needed,
				// or just infer when limit is hit or non-blank found.
				// Let's simplify: infer on first D row or if limit hit.
				// mustInferNow := currentSection.RowsChecked == 1 || currentSection.RowsChecked >= cfg.SchemaRowLimit
				// OR: infer on first non-blank row or if limit hit
				mustInferNow := !hasBlanks || currentSection.RowsChecked >= cfg.SchemaRowLimit

				if mustInferNow {
					if hasBlanks {
						l.Warn("Inferring schema from row with blanks", "attempt", currentSection.RowsChecked, slog.Int("limit", cfg.SchemaRowLimit))
					} else {
						l.Debug("Inferring schema", "attempt", currentSection.RowsChecked)
					}

					// Pass the correct number of values based on headers for inference
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
					l.Debug("Skipping schema inference on blank row, waiting for cleaner row.", slog.Int("attempt", currentSection.RowsChecked))
					continue // Skip this data row
				}
			}

			// Write Data Row if schema is ready
			if currentSection != nil && currentSection.SchemaInferred {
				// Pass only the data values corresponding to the headers to writeRow
				err := currentSection.writeRow(dataValues) // writeRow now handles padding/truncating
				if err != nil {
					accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("write line %d: %w", lineNumber, err))
				}
			}

		default: // Ignore other record types like 'C'
			continue
		}
	} // End record reading loop

	// --- Cleanup Block for Normal Loop Exit ---
	// This runs only if the loop finished due to EOF without error or cancellation.
	if ctx.Err() == nil {
		// Finalize the very last section if one was active
		if currentSection != nil {
			if err := currentSection.close(); err != nil {
				accumulatedErrors = errors.Join(accumulatedErrors, err)
			}
			currentSection = nil // Mark as cleaned up
		}
	}
	// --- End Cleanup Block ---

	// Scanner errors are handled within the loop now with csv.Reader

	return accumulatedErrors
}
