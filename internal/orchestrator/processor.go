package orchestrator

import (
	"archive/zip"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/util"
)

// ErrSchemaValidationFailed indicates a non-recoverable schema mismatch or missing definition.
var ErrSchemaValidationFailed = errors.New("schema validation failed")

// Global cache for *validated* schemas (based on predefined + file match).
// Key: Full table name (group__table__version__colhash), Value: *SchemaInfo
var validatedSchemaCache sync.Map

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

// processZip handles unzipping, parsing, schema validation/creation, and data appending.
// It uses predefined schemas loaded from the config. Assumes tables exist.
func processZip(
	ctx context.Context,
	cfg config.Config,
	dbConnPool *sql.DB,
	logger *slog.Logger,
	zipFilePath string,
	// Removed schemaChan chan<- SchemaCreateRequest,
	writeChan chan<- WriteOperation,
) error { // Return type is just error
	l := logger.With(slog.String("zip_file", filepath.Base(zipFilePath)))
	l.Info("Starting processing of zip file (using predefined schemas).")
	processingStart := time.Now()

	// 1. Unzip
	zipReader, err := zip.OpenReader(zipFilePath)
	if err != nil {
		db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventError, "", zipFilePath, fmt.Sprintf("Failed to open zip: %v", err), "", nil)
		return fmt.Errorf("failed to open zip file %s: %w", zipFilePath, err) // Return regular error
	}
	defer zipReader.Close()

	var processingErr error // Accumulate non-fatal errors during processing
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
		return errors.New(errMsg) // Return regular error
	}
	rc, err := csvFile.Open()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to open CSV stream %s: %v", csvFile.Name, err)
		l.Error(errMsg)
		db.LogFileEvent(ctx, dbConnPool, zipFilePath, db.FileTypeZip, db.EventError, "", zipFilePath, errMsg, "", nil)
		return fmt.Errorf(errMsg) // Return regular error
	}
	defer rc.Close()

	scanner := util.NewPeekableScanner(rc)
	bufferSize := 1024 * 1024
	maxScanTokenSize := bufferSize
	scanner.Buffer(make([]byte, bufferSize), maxScanTokenSize)

	var currentSchema *SchemaInfo // The schema *validated* for the current block
	var currentTableName string   // The specific table name (includes col hash)
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
			l.Warn("Failed to parse CSV line, skipping.", "line", lineNumber, "error", err)
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
			headerRowFields = fields
			currentSchema = nil // Reset schema for the new block
			currentTableName = ""

			if len(fields) < 4 {
				l.Warn("Skipping 'I' record: insufficient columns.", "line", lineNumber)
				processingErr = errors.Join(processingErr, fmt.Errorf("line %d 'I' record too short", lineNumber))
				continue
			}
			group := strings.TrimSpace(fields[1])
			table := strings.TrimSpace(fields[2])
			version := strings.TrimSpace(fields[3])
			if group == "" || table == "" || version == "" {
				l.Warn("Skipping 'I' record: empty group/table/version.", "line", lineNumber)
				processingErr = errors.Join(processingErr, fmt.Errorf("line %d 'I' record missing key fields", lineNumber))
				continue
			}

			baseIdentifier := fmt.Sprintf("%s__%s__%s", group, table, version)
			fileColumns := headerRowFields[4:]
			tableNameWithHash := generateTableName(group, table, version, fileColumns)
			currentTableName = tableNameWithHash

			// Check cache for the *specific* hashed table name first
			schemaVal, foundInCache := validatedSchemaCache.Load(currentTableName)
			if foundInCache {
				l.Debug("Validated schema found in cache.", "table", currentTableName)
				currentSchema = schemaVal.(*SchemaInfo)
				continue // Schema is known and validated, proceed to 'D' rows
			}

			// Not in cache - Look up the base identifier in the predefined config
			predefinedSchema, foundPredefined := cfg.PredefinedSchemas[baseIdentifier]
			if !foundPredefined {
				l.Error("Fatal: No predefined schema found for base identifier. Cannot process.", "base_id", baseIdentifier, "table", currentTableName)
				// Return the specific fatal error
				return fmt.Errorf("%w: no predefined schema for base identifier '%s' (table: %s)", ErrSchemaValidationFailed, baseIdentifier, currentTableName)
			}

			// Predefined schema found, compare columns with the file's header
			predefinedCols := make([]string, len(predefinedSchema.Columns))
			predefinedTypes := make([]string, len(predefinedSchema.Columns))
			nemTimeCols := make(map[int]bool)
			for i, colDef := range predefinedSchema.Columns {
				predefinedCols[i] = colDef.Name
				predefinedTypes[i] = colDef.Type
				if strings.Contains(strings.ToUpper(colDef.Type), "TIMESTAMP") {
					if strings.Contains(colDef.Name, "DATETIME") {
						nemTimeCols[i] = true
					}
				}
			}

			// *** Compare columns and provide detailed error ***
			if !reflect.DeepEqual(fileColumns, predefinedCols) {
				// Construct detailed error message
				errMsg := fmt.Sprintf("file columns for '%s' (base: %s) do not match predefined schema.\n"+
					"  File Columns (%d): %v\n"+
					"  Predefined Columns (%d): %v",
					currentTableName, baseIdentifier,
					len(fileColumns), fileColumns,
					len(predefinedCols), predefinedCols)

				l.Error("Fatal: Schema mismatch.", "details", errMsg) // Log the detailed message

				// Return the specific fatal error with the detailed message
				return fmt.Errorf("%w: %s", ErrSchemaValidationFailed, errMsg)
			}

			// Columns match! Prepare SchemaInfo based on predefined types
			l.Info("File columns match predefined schema. Preparing schema info.", "table", currentTableName)
			schemaToUse := &SchemaInfo{
				ColumnNames:   predefinedCols, // Use names from predefined (should match fileColumns)
				DuckdbTypes:   predefinedTypes,
				NemTimeColIdx: nemTimeCols,
			}

			// Schema is validated, cache it. Table creation happened at startup.
			validatedSchemaCache.Store(currentTableName, schemaToUse) // Cache it
			currentSchema = schemaToUse                               // Set for processing

			continue // Move to next line
		} // End 'I' record handling

		if recordType == "D" {
			if currentTableName == "" || headerRowFields == nil {
				l.Warn("Skipping 'D' record found before a valid 'I' record or after schema error.", "line", lineNumber)
				continue
			}
			if currentSchema == nil {
				// This implies schema validation failed in 'I' block. Error already returned or logged.
				continue
			}

			// --- Data Row Processing ---
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
				return ctx.Err() // Exit function
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

// parseDataRow function (ensure it's accessible)
// ... (parseDataRow implementation remains the same as provided previously) ...
func parseDataRow(rowData []string, schema *SchemaInfo, logger *slog.Logger) ([]any, error) {
	if len(rowData) != len(schema.ColumnNames)+4 {
		return nil, fmt.Errorf("data row length (%d) does not match schema length (%d + 4)", len(rowData), len(schema.ColumnNames))
	}
	parsedData := make([]any, len(schema.ColumnNames))
	dataPortion := rowData[4:]
	for i, val := range dataPortion {
		trimmedVal := strings.Trim(val, `" `)
		if trimmedVal == "" {
			parsedData[i] = nil
			continue
		}
		colType := schema.DuckdbTypes[i]
		var parsedVal any
		var specificParseErr error
		switch colType {
		case "TIMESTAMP_MS":
			_, isNemCol := schema.NemTimeColIdx[i]
			if isNemCol || util.IsNEMDateTime(trimmedVal) {
				epochMs, parseErr := util.NEMDateTimeToEpochMS(trimmedVal)
				if parseErr != nil {
					logger.Warn("Failed to parse NEMDateTime, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", parseErr)
					parsedVal = nil
					specificParseErr = parseErr
				} else {
					parsedVal = time.UnixMilli(epochMs).UTC()
				}
			} else {
				logger.Warn("Column type is TIMESTAMP_MS but value is not NEM format, treating as VARCHAR.", "value", val, "column", schema.ColumnNames[i])
				parsedVal = trimmedVal
			}
		case "BIGINT":
			parsedVal, specificParseErr = strconv.ParseInt(trimmedVal, 10, 64)
			if specificParseErr != nil {
				logger.Warn("Failed to parse BIGINT, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", specificParseErr)
				parsedVal = nil
			}
		case "DOUBLE":
			parsedVal, specificParseErr = strconv.ParseFloat(trimmedVal, 64)
			if specificParseErr != nil {
				logger.Warn("Failed to parse DOUBLE, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", specificParseErr)
				parsedVal = nil
			}
		case "VARCHAR":
			fallthrough
		default:
			if strings.ContainsRune(trimmedVal, 0) {
				logger.Warn("Replacing null byte in VARCHAR data", "column", schema.ColumnNames[i])
				parsedVal = strings.ReplaceAll(trimmedVal, "\x00", "")
			} else {
				parsedVal = trimmedVal
			}
		}
		if colType == "VARCHAR" {
			switch v := parsedVal.(type) {
			case int64:
				parsedVal = strconv.FormatInt(v, 10)
				logger.Debug("Converted int64 to string for VARCHAR column.", "column", schema.ColumnNames[i], "value", parsedVal)
			case float64:
				parsedVal = strconv.FormatFloat(v, 'f', -1, 64)
				logger.Debug("Converted float64 to string for VARCHAR column.", "column", schema.ColumnNames[i], "value", parsedVal)
			case time.Time:
				parsedVal = v.Format(time.RFC3339Nano)
				logger.Debug("Converted time.Time to string for VARCHAR column.", "column", schema.ColumnNames[i], "value", parsedVal)
			}
		}
		if specificParseErr != nil && parsedVal != nil {
			logger.Error("Internal inconsistency: parse error occurred but parsed value is not nil.", "column", schema.ColumnNames[i], "value_type", fmt.Sprintf("%T", parsedVal), "error", specificParseErr)
			return nil, fmt.Errorf("parsing value '%s' for column '%s' as %s resulted in error '%w' but non-nil value", val, schema.ColumnNames[i], colType, specificParseErr)
		}
		parsedData[i] = parsedVal
	}
	return parsedData, nil
}
