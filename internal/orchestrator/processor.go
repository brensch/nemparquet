package orchestrator

import (
	"archive/zip"
	"context"
	"crypto/sha1" // For hashing column names
	"database/sql"
	"encoding/csv"
	"encoding/hex" // For encoding hash
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"

	// "reflect" // No longer needed
	"strings"
	"sync" // For schema cache mutex
	"time" // For result channel timeout

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"
)

// Global cache for schemas. Key: tableName (group__table__version__colhash), Value: *SchemaInfo
var schemaCacheMap = make(map[string]*SchemaInfo)
var schemaCacheMutex sync.Mutex // Mutex to protect schemaCacheMap

// generateTableName creates a unique table name including a hash of the column names.
func generateTableName(group, table, version string, columnNames []string) string {
	colString := strings.Join(columnNames, "|-|")
	hasher := sha1.New()
	hasher.Write([]byte(colString))
	colHash := hex.EncodeToString(hasher.Sum(nil))[:10]
	cleanGroup := strings.ReplaceAll(group, "-", "_")
	cleanTable := strings.ReplaceAll(table, "-", "_")
	cleanVersion := strings.ReplaceAll(version, "-", "_")
	return fmt.Sprintf("%s__%s__%s__%s", cleanGroup, cleanTable, cleanVersion, colHash)
}

// processZip handles unzipping, parsing, schema inference/creation, and data appending for a single zip file.
// Table names now include a hash of column names. Inference considers the first D row.
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

	var currentSchema *SchemaInfo // The schema to use for the current block
	var currentTableName string   // The specific table name for the current block (includes col hash)
	var headerRowFields []string  // Fields from the most recent 'I' record

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
			headerRowFields = fields // Store full header row

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
			fileColumns := headerRowFields[4:] // Extract column names

			tableName := generateTableName(group, table, version, fileColumns)
			currentTableName = tableName
			currentSchema = nil // Reset schema, check cache below

			schemaCacheMutex.Lock()
			cachedSchema, found := schemaCacheMap[currentTableName]
			schemaCacheMutex.Unlock()

			if found {
				l.Debug("Schema found in cache for table.", "table", currentTableName)
				currentSchema = cachedSchema
			} else {
				l.Debug("Schema not in cache, inference required.", "table", currentTableName)
				currentSchema = nil
			}
			continue
		}

		if recordType == "D" {
			if currentTableName == "" || headerRowFields == nil {
				l.Warn("Skipping 'D' record found before a valid 'I' record or after schema error.", "line", lineNumber)
				continue
			}

			// --- Schema Inference/Creation Logic (if needed) ---
			if currentSchema == nil {
				schemaCacheMutex.Lock()
				schema, found := schemaCacheMap[currentTableName]
				if found {
					schemaCacheMutex.Unlock()
					l.Debug("Schema found in cache after acquiring lock.", "table", currentTableName)
					currentSchema = schema
				} else {
					l.Info("Schema not in cache, performing inference/creation.", "table", currentTableName)

					// Call inferSchema, passing the *current* 'D' row fields ('fields')
					// and the scanner (positioned *after* the current row).
					inferredSchema, inferErr := inferSchema(scanner, headerRowFields, fields, l)
					// Scanner position is now after any additional rows consumed by inferSchema.

					if inferErr != nil {
						schemaCacheMutex.Unlock()
						l.Error("Schema inference failed.", "table", currentTableName, "error", inferErr)
						processingErr = errors.Join(processingErr, fmt.Errorf("schema inference %s: %w", currentTableName, inferErr))
						currentSchema = nil
						currentTableName = ""
						headerRowFields = nil
						continue // Continue to the next line from the scanner
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
						case createErr = <-resultChan:
							if createErr != nil {
								l.Error("Schema creation worker reported error.", "table", currentTableName, "error", createErr)
								processingErr = errors.Join(processingErr, fmt.Errorf("schema create %s: %w", currentTableName, createErr))
							} else {
								l.Info("Schema creation successful. Caching schema.", "table", currentTableName)
								schemaCacheMap[currentTableName] = inferredSchema
								currentSchema = inferredSchema
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

					schemaCacheMutex.Unlock()

					if createErr != nil {
						currentSchema = nil
						currentTableName = ""
						headerRowFields = nil
						continue // Continue to the next line from the scanner
					}
				} // End of cache miss handling
			} // End of if currentSchema == nil

			// --- Data Row Processing ---
			if currentSchema != nil {
				// Process the 'fields' from the current scanner line.
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
					return processingErr
				}
			} else {
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
