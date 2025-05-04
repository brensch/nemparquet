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
	"sync" // For schema cache mutex and waiter struct
	"time" // For result channel timeout

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"
)

// Global cache for schemas. Key: tableName (group__table__version), Value: *SchemaInfo or *schemaWaiter
var schemaCache sync.Map

// schemaWaiter is used as a placeholder in the cache while schema inference/creation is in progress.
type schemaWaiter struct {
	once   sync.Once     // Ensures done channel is closed only once
	done   chan struct{} // Closed when schema is ready or failed
	schema *SchemaInfo   // Populated on success
	err    error         // Populated on failure
}

// newSchemaWaiter creates a new waiter struct.
func newSchemaWaiter() *schemaWaiter {
	return &schemaWaiter{done: make(chan struct{})}
}

// processZip handles unzipping, parsing, schema inference/creation, and data appending for a single zip file.
// It now includes logic to ensure only one worker infers/creates schema per table concurrently.
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
	sampleRowsForInference := [][]string{}

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
			sampleRowsForInference = [][]string{} // Reset samples for new block
			headerRow = fields

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
			currentSchema = nil // Reset schema, will check cache or infer

			// Check cache immediately (before collecting samples)
			schemaVal, loaded := schemaCache.Load(tableName)
			if loaded {
				switch v := schemaVal.(type) {
				case *SchemaInfo:
					l.Debug("Schema found in cache.", "table", tableName)
					currentSchema = v // Use cached schema
				case *schemaWaiter:
					l.Info("Schema inference in progress by another worker, waiting...", "table", tableName)
					// Wait for the other worker to finish
					select {
					case <-v.done: // Wait until channel is closed
						if v.err != nil {
							l.Error("Schema inference/creation failed by other worker.", "table", tableName, "error", v.err)
							processingErr = errors.Join(processingErr, fmt.Errorf("schema %s failed by other worker: %w", tableName, v.err))
							currentSchema = nil
							currentTableName = "" // Invalidate block
						} else if v.schema == nil {
							l.Error("Schema waiter finished but schema is nil and no error reported.", "table", tableName)
							processingErr = errors.Join(processingErr, fmt.Errorf("internal error: schema waiter %s finished unexpectedly", tableName))
							currentSchema = nil
							currentTableName = "" // Invalidate block
						} else {
							l.Info("Schema ready after waiting.", "table", tableName)
							currentSchema = v.schema // Use the schema prepared by the other worker
						}
					case <-ctx.Done():
						l.Warn("Context cancelled while waiting for schema.", "table", tableName, "error", ctx.Err())
						processingErr = errors.Join(processingErr, ctx.Err())
						currentSchema = nil
						currentTableName = "" // Invalidate block
					}
				default:
					l.Error("Unexpected type found in schema cache.", "table", tableName, "type", fmt.Sprintf("%T", schemaVal))
					processingErr = errors.Join(processingErr, fmt.Errorf("invalid type %T in schema cache for %s", schemaVal, tableName))
					currentSchema = nil
					currentTableName = "" // Invalidate block
				}
			}
			// If not loaded or waiter failed, currentSchema remains nil, inference will proceed below if needed.
			continue // Continue to next line ('D' or next 'I')
		}

		if recordType == "D" {
			if currentTableName == "" || headerRow == nil {
				l.Warn("Skipping 'D' record found before a valid 'I' record.", "line", lineNumber)
				processingErr = errors.Join(processingErr, fmt.Errorf("line %d 'D' record without preceding 'I'", lineNumber))
				continue
			}

			// --- Schema Inference/Creation Logic ---
			if currentSchema == nil {
				// Schema not yet available for this block. Collect sample or trigger inference.
				if len(sampleRowsForInference) < maxInferenceRows {
					rowCopy := make([]string, len(fields))
					copy(rowCopy, fields)
					sampleRowsForInference = append(sampleRowsForInference, rowCopy)
				}

				nextRecordType, peekErr := scanner.PeekRecordType()
				isLastDRow := (peekErr == io.EOF || (peekErr == nil && nextRecordType != "D"))

				// Trigger inference if buffer full OR it's the last D row in the block
				if len(sampleRowsForInference) > 0 && (len(sampleRowsForInference) >= maxInferenceRows || isLastDRow) {

					// --- Attempt to become the schema creator ---
					waiter := newSchemaWaiter()
					// Try to store our waiter. If LoadOrStore loads, another worker is already on it.
					actual, loaded := schemaCache.LoadOrStore(currentTableName, waiter)

					if loaded { // Another worker is handling inference/creation
						l.Debug("Schema inference already started by another worker, waiting...", "table", currentTableName)
						// Wait for the other worker
						waiterToUse, ok := actual.(*schemaWaiter)
						if !ok {
							l.Error("Expected schemaWaiter in cache but found different type after LoadOrStore.", "table", currentTableName, "type", fmt.Sprintf("%T", actual))
							processingErr = errors.Join(processingErr, fmt.Errorf("cache inconsistency for %s: expected *schemaWaiter, got %T", currentTableName, actual))
							currentSchema = nil
							currentTableName = ""
							headerRow = nil // Invalidate block
							continue
						}
						select {
						case <-waiterToUse.done:
							if waiterToUse.err != nil {
								l.Error("Schema inference/creation failed by other worker.", "table", currentTableName, "error", waiterToUse.err)
								processingErr = errors.Join(processingErr, fmt.Errorf("schema %s failed by other worker: %w", currentTableName, waiterToUse.err))
								currentSchema = nil
								currentTableName = ""
								headerRow = nil // Invalidate block
							} else if waiterToUse.schema == nil {
								l.Error("Schema waiter finished but schema is nil and no error reported.", "table", currentTableName)
								processingErr = errors.Join(processingErr, fmt.Errorf("internal error: schema waiter %s finished unexpectedly", currentTableName))
								currentSchema = nil
								currentTableName = ""
								headerRow = nil // Invalidate block
							} else {
								l.Info("Schema ready after waiting.", "table", currentTableName)
								currentSchema = waiterToUse.schema
							}
						case <-ctx.Done():
							l.Warn("Context cancelled while waiting for schema.", "table", currentTableName, "error", ctx.Err())
							processingErr = errors.Join(processingErr, ctx.Err())
							currentSchema = nil
							currentTableName = ""
							headerRow = nil // Invalidate block
						}
					} else { // This worker is responsible for inference
						l.Info("Performing schema inference and creation.", "table", currentTableName, "sample_rows", len(sampleRowsForInference))
						var inferredSchema *SchemaInfo
						var inferErr error
						var createErr error

						// Ensure waiter.done is closed eventually, even on panic
						defer waiter.once.Do(func() { close(waiter.done) })

						inferredSchema, inferErr = inferSchema(headerRow, sampleRowsForInference, l)
						if inferErr != nil {
							l.Error("Schema inference failed.", "table", currentTableName, "error", inferErr)
							processingErr = errors.Join(processingErr, fmt.Errorf("schema inference %s: %w", currentTableName, inferErr))
							waiter.err = inferErr
							schemaCache.Delete(currentTableName) // Remove placeholder
							currentSchema = nil
							currentTableName = ""
							headerRow = nil // Invalidate block
							continue        // Skip to next line
						}

						// Send request to create table
						resultChan := make(chan error, 1)
						createReq := SchemaCreateRequest{TableName: currentTableName, Schema: inferredSchema, ResultChan: resultChan}

						l.Debug("Sending schema creation request.", "table", currentTableName)
						select {
						case schemaChan <- createReq:
							l.Debug("Waiting for schema creation result.", "table", currentTableName)
							select {
							case createErr = <-resultChan: // Blocks until resultChan is closed by creator
								if createErr != nil {
									l.Error("Schema creation worker reported error.", "table", currentTableName, "error", createErr)
									processingErr = errors.Join(processingErr, fmt.Errorf("schema create %s: %w", currentTableName, createErr))
									waiter.err = createErr
									schemaCache.Delete(currentTableName)
								} else {
									l.Info("Schema creation successful. Caching schema.", "table", currentTableName)
									waiter.schema = inferredSchema
									schemaCache.Store(currentTableName, inferredSchema) // Replace placeholder
									currentSchema = inferredSchema                      // Set for processing
								}
							case <-time.After(30 * time.Second):
								createErr = fmt.Errorf("schema create timeout for %s", currentTableName)
								l.Error("Timeout waiting for schema creation result.", "table", currentTableName)
								processingErr = errors.Join(processingErr, createErr)
								waiter.err = createErr
								schemaCache.Delete(currentTableName)
							case <-ctx.Done():
								createErr = ctx.Err()
								l.Warn("Context cancelled waiting for schema creation.", "table", currentTableName, "error", ctx.Err())
								processingErr = errors.Join(processingErr, createErr)
								waiter.err = createErr
								schemaCache.Delete(currentTableName)
							}
						case <-ctx.Done():
							createErr = ctx.Err()
							l.Warn("Context cancelled sending schema creation request.", "table", currentTableName, "error", ctx.Err())
							processingErr = errors.Join(processingErr, createErr)
							waiter.err = createErr
							schemaCache.Delete(currentTableName) // Ensure placeholder is removed
						}

						// If creation failed or was cancelled, invalidate the block
						if createErr != nil {
							currentSchema = nil
							currentTableName = ""
							headerRow = nil
							continue // Skip to next line
						}
					} // End of responsibility block
				} // End of inference trigger block
			} // End of if currentSchema == nil

			// --- Data Row Processing ---
			if currentSchema != nil {
				parsedRow, parseErr := parseDataRow(fields, currentSchema, l)
				if parseErr != nil {
					l.Warn("Failed to parse data row, skipping.", "line", lineNumber, "error", parseErr)
					processingErr = errors.Join(processingErr, fmt.Errorf("line %d parse data: %w", lineNumber, parseErr))
					continue
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
				// This should ideally not be reached if logic above is correct,
				// but indicates schema wasn't ready for this D row.
				l.Warn("Skipping 'D' record because schema is not ready.", "line", lineNumber, "table", currentTableName)
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
