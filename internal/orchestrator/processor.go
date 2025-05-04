package orchestrator

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync" // For schema cache mutex
	"time" // For result channel timeout

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"
)

// Global cache for schemas. Key: tableName (group__table__version), Value: *SchemaInfo
var schemaCacheMap = make(map[string]*SchemaInfo)
var schemaCacheMutex sync.Mutex // Mutex to protect schemaCacheMap

// processZip handles unzipping, parsing, schema inference/creation, and data appending for a single zip file.
// It uses a mutex to serialize schema inference and creation checks per table name.
// Schema inference now reads directly from the scanner row-by-row.
func processZip(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB, // Used only for logging final status
	logger *slog.Logger,
	zipFilePath string,
	schemaChan chan<- SchemaCreateRequest, // Channel to send schema creation requests
	writeChan chan<- WriteOperation, // Channel to send data rows for writing
) error {
	l := logger.With(slog.String("zip_file", filepath.Base(zipFilePath)))
	l.Info("Starting processing of zip file.")
	processingStart := time.Now()

	// 1. Unzip the file
	zipReader, err := zip.OpenReader(zipFilePath)
	if err != nil {
		db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventError, "", zipFilePath, fmt.Sprintf("Failed to open zip: %v", err), "", nil)
		return fmt.Errorf("failed to open zip file %s: %w", zipFilePath, err)
	}
	defer zipReader.Close()

	var processingErr error // Accumulate errors during processing
	var csvFile *zip.File
	for _, f := range zipReader.File {
		if !f.FileInfo().IsDir() && strings.EqualFold(filepath.Ext(f.Name), ".csv") {
			csvFile = f
			l.Debug("Found CSV file in zip.", slog.String("csv_name", f.Name))
			break
		}
	}

	if csvFile == nil {
		errMsg := "No CSV file found within the zip archive"
		l.Warn(errMsg)
		db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventError, "", zipFilePath, errMsg, "", nil)
		return errors.New(errMsg)
	}

	rc, err := csvFile.Open()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to open CSV stream %s: %v", csvFile.Name, err)
		l.Error(errMsg)
		db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventError, "", zipFilePath, errMsg, "", nil)
		return fmt.Errorf(errMsg)
	}
	defer rc.Close()

	scanner := util.NewPeekableScanner(rc)
	bufferSize := 1024 * 1024 // 1 MB buffer
	maxScanTokenSize := bufferSize
	scanner.Buffer(make([]byte, bufferSize), maxScanTokenSize)

	var currentSchema *SchemaInfo
	var currentTableName string
	var headerRow []string
	// sampleRowsForInference is no longer needed

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		r := csv.NewReader(strings.NewReader(line))
		r.LazyQuotes = true
		r.TrimLeadingSpace = true
		fields, err := r.Read()
		if err != nil {
			if err == io.EOF {
				continue
			}
			l.Warn("Failed to parse CSV line, skipping.", "line", lineNumber, "error", err, "content", line)
			processingErr = errors.Join(processingErr, fmt.Errorf("line %d csv parse: %w", lineNumber, err))
			continue
		}
		if len(fields) == 0 {
			continue
		}

		recordType := strings.TrimSpace(fields[0])

		if recordType == "C" {
			continue
		}

		if recordType == "I" {
			l.Debug("Processing 'I' record.", "line", lineNumber)
			// sampleRowsForInference = [][]string{} // No longer needed
			headerRow = fields // Store header for potential inference

			if len(fields) < 4 {
				l.Warn("Skipping 'I' record with insufficient columns.", "line", lineNumber, "columns", len(fields))
				processingErr = errors.Join(processingErr, fmt.Errorf("line %d 'I' record too short", lineNumber))
				currentSchema = nil
				currentTableName = ""
				continue
			}

			group := strings.TrimSpace(fields[1])
			table := strings.TrimSpace(fields[2])
			version := strings.TrimSpace(fields[3])
			if group == "" || table == "" || version == "" {
				l.Warn("Skipping 'I' record with empty group/table/version.", "line", lineNumber)
				processingErr = errors.Join(processingErr, fmt.Errorf("line %d 'I' record missing key fields", lineNumber))
				currentSchema = nil
				currentTableName = ""
				continue
			}
			tableName := fmt.Sprintf("%s__%s__%s", group, table, version)
			currentTableName = tableName
			currentSchema = nil // Reset schema, will check cache or infer below

			continue // Continue to next line ('D' or next 'I')
		}

		if recordType == "D" {
			if currentTableName == "" || headerRow == nil {
				l.Warn("Skipping 'D' record found before a valid 'I' record.", "line", lineNumber)
				processingErr = errors.Join(processingErr, fmt.Errorf("line %d 'D' record without preceding 'I'", lineNumber))
				continue
			}

			// --- Schema Check/Inference/Creation Logic (Mutex Protected) ---
			if currentSchema == nil {
				// Need to determine schema for this block. Lock, check cache, infer/create if needed.
				schemaCacheMutex.Lock()
				schema, found := schemaCacheMap[currentTableName]
				if found {
					// Schema exists in cache
					schemaCacheMutex.Unlock() // Unlock immediately after finding
					l.Debug("Schema found in cache.", "table", currentTableName)
					currentSchema = schema
				} else {
					// Schema not in cache. Keep lock, infer, create, store, unlock.
					l.Info("Schema not in cache, performing inference/creation.", "table", currentTableName)

					// Call the modified inferSchema, passing the scanner.
					// It will consume the necessary 'D' rows.
					inferredSchema, inferErr := inferSchema(scanner, headerRow, l)
					// Scanner position is now after the rows consumed by inferSchema.

					if inferErr != nil {
						schemaCacheMutex.Unlock() // Unlock on error
						l.Error("Schema inference failed.", "table", currentTableName, "error", inferErr)
						processingErr = errors.Join(processingErr, fmt.Errorf("schema inference %s: %w", currentTableName, inferErr))
						currentSchema = nil
						currentTableName = ""
						headerRow = nil // Invalidate block
						// Need to reprocess the current line if inference failed?
						// The current line was already consumed by inferSchema if it was a 'D' row.
						// If inferSchema failed *before* consuming this line (e.g., header error),
						// then we might process it incorrectly below.
						// Safest is to invalidate and continue to the *next* line from the scanner.
						continue
					}

					// Send request to create table and wait for result *while holding lock*
					resultChan := make(chan error, 1)
					createReq := SchemaCreateRequest{TableName: currentTableName, Schema: inferredSchema, ResultChan: resultChan}

					l.Debug("Sending schema creation request.", "table", currentTableName)
					var createErr error
					select {
					case schemaChan <- createReq:
						l.Debug("Waiting for schema creation result.", "table", currentTableName)
						select {
						case createErr = <-resultChan: // Blocks until resultChan is closed
							if createErr != nil {
								l.Error("Schema creation worker reported error.", "table", currentTableName, "error", createErr)
								processingErr = errors.Join(processingErr, fmt.Errorf("schema create %s: %w", currentTableName, createErr))
							} else {
								l.Info("Schema creation successful. Caching schema.", "table", currentTableName)
								schemaCacheMap[currentTableName] = inferredSchema // Store in map
								currentSchema = inferredSchema                    // Set for current processing
							}
						case <-ctx.Done():
							createErr = ctx.Err()
							l.Warn("Context cancelled waiting for schema creation.", "table", currentTableName, "error", ctx.Err())
							processingErr = errors.Join(processingErr, createErr)
						}
					case <-ctx.Done():
						createErr = ctx.Err()
						l.Warn("Context cancelled sending schema creation request.", "table", currentTableName, "error", ctx.Err())
						processingErr = errors.Join(processingErr, createErr)
					}

					schemaCacheMutex.Unlock() // Unlock *after* inference, creation attempt, and cache update

					// If creation failed or was cancelled, invalidate the block and continue
					if createErr != nil {
						currentSchema = nil
						currentTableName = ""
						headerRow = nil
						// Scanner is already past the inference rows, continue to next line
						continue
					}
				} // End of cache miss handling
			} // End of if currentSchema == nil

			// --- Data Row Processing ---
			// NOTE: The current 'D' row might have been consumed by inferSchema if it was within the inference window.
			// We need to process the *current* `fields` only if it wasn't consumed during inference.
			// However, the current logic calls inferSchema *within* the loop iteration for the first D row
			// that triggers the cache miss. inferSchema then consumes subsequent rows.
			// This means the D row that *triggered* the inference still needs processing after the schema is ready.

			if currentSchema != nil {
				// Process the 'fields' from the current scanner line
				parsedRow, parseErr := parseDataRow(fields, currentSchema, l)
				if parseErr != nil {
					l.Warn("Failed to parse data row, skipping.", "line", lineNumber, "error", parseErr)
					processingErr = errors.Join(processingErr, fmt.Errorf("line %d parse data: %w", lineNumber, parseErr))
					continue // Skip this row
				}

				writeOp := WriteOperation{TableName: currentTableName, Data: parsedRow}
				select {
				case writeChan <- writeOp:
					// Data sent
				case <-ctx.Done():
					l.Warn("Context cancelled while sending data row to writer.", "line", lineNumber, "error", ctx.Err())
					processingErr = errors.Join(processingErr, ctx.Err())
					db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventError, "", zipFilePath, fmt.Sprintf("Processing cancelled: %v", ctx.Err()), "", nil)
					return processingErr // Exit function on cancellation during write
				}
			} else {
				// Schema wasn't ready - implies an issue during lock/check/create phase
				l.Warn("Skipping 'D' record because schema is not ready (error during inference/create).", "line", lineNumber, "table", currentTableName)
				processingErr = errors.Join(processingErr, fmt.Errorf("line %d 'D' record schema not ready for table %s", lineNumber, currentTableName))
			}
		} // End 'D' record handling
	} // End scanner loop

	if err := scanner.Err(); err != nil {
		l.Error("Scanner error during processing.", "error", err)
		processingErr = errors.Join(processingErr, fmt.Errorf("scanner error: %w", err))
	}

	duration := time.Since(processingStart)
	if processingErr != nil {
		l.Error("Zip file processing finished with errors.", "duration", duration, "error", processingErr)
		db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventError, "", zipFilePath, fmt.Sprintf("Processing finished with errors: %v", processingErr), "", &duration)
		return processingErr
	}

	l.Info("Zip file processing finished successfully.", "duration", duration)
	db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventProcessEnd, "", zipFilePath, "Processing successful", "", &duration)
	return nil
}

// Removed placeholder peekRemainingData function
