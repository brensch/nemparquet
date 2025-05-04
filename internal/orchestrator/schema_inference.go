package orchestrator

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/brensch/nemparquet/internal/util" // For NEMDateTime helpers and PeekableScanner
)

// SchemaInfo holds the column names and inferred DuckDB types.
type SchemaInfo struct {
	ColumnNames   []string
	DuckdbTypes   []string
	NemTimeColIdx map[int]bool // Indices of columns detected as NEMDateTime
}

const maxInferenceRows = 10 // Total 'D' rows to check (including the first one)

// inferSchema analyzes the first triggering 'D' row and subsequent 'D' rows from the scanner
// to determine column names and DuckDB types.
// It reads up to maxInferenceRows-1 additional rows or until a non-'D' record is encountered.
// It consumes the additional rows it reads from the scanner.
func inferSchema(
	scanner *util.PeekableScanner, // Scanner positioned *after* the first triggering D row
	headerRow []string, // The 'I' record fields
	firstDataRow []string, // The first 'D' record fields that triggered inference
	logger *slog.Logger,
) (*SchemaInfo, error) {

	if len(headerRow) < 4 {
		return nil, fmt.Errorf("header row has insufficient columns (%d) for schema inference", len(headerRow))
	}
	colNames := headerRow[4:]
	numCols := len(colNames)
	if numCols == 0 {
		return nil, fmt.Errorf("header row contains no data column names after mandatory fields")
	}
	if len(firstDataRow) != numCols+4 {
		return nil, fmt.Errorf("first data row length (%d) does not match header length (%d + 4)", len(firstDataRow), numCols)
	}

	duckdbTypes := make([]string, numCols) // Store the best inferred type
	nemTimeCols := make(map[int]bool)
	seenNonEmpty := make([]bool, numCols) // Track if we've seen *any* non-empty value for a column
	rowsProcessed := 0

	// --- Process the first triggering data row ---
	logger.Debug("Inferring schema starting with the first data row.")
	processRowForInference(firstDataRow[4:], duckdbTypes, nemTimeCols, seenNonEmpty)
	rowsProcessed++

	// --- Process subsequent rows using the scanner ---
	logger.Debug("Scanning subsequent rows for schema inference.", "max_additional_rows", maxInferenceRows-1)
	for rowsProcessed < maxInferenceRows {
		// Peek first to see if the next record is 'D'
		nextRecordType, peekErr := scanner.PeekRecordType()
		if peekErr != nil {
			if errors.Is(peekErr, io.EOF) {
				logger.Debug("EOF reached during schema inference scan.")
				break // End of file
			}
			logger.Error("Scanner error during peek in schema inference.", "error", peekErr)
			// Don't return error yet, try to finalize based on what we have
			break
		}

		if nextRecordType != "D" {
			logger.Debug("Non-'D' record encountered during inference scan, stopping.", "record_type", nextRecordType)
			break // Stop inference
		}

		// Peek successful and it's a 'D' row, now actually Scan it
		if !scanner.Scan() {
			scanErr := scanner.Err()
			logger.Error("Scanner error during scan in schema inference.", "error", scanErr)
			// Don't return error yet, try to finalize
			break
		}
		rowsProcessed++ // Increment total rows processed (including the first one)
		line := scanner.Text()

		// Parse the 'D' row
		r := csv.NewReader(strings.NewReader(line))
		r.LazyQuotes = true
		r.TrimLeadingSpace = true
		fields, err := r.Read()
		if err != nil {
			logger.Warn("Failed to parse CSV line during inference, skipping row.", "row_num_in_scan", rowsProcessed, "error", err)
			continue // Skip this row for inference
		}

		if len(fields) != numCols+4 {
			logger.Warn("Schema inference row length mismatch, skipping row.", "row_num_in_scan", rowsProcessed, "expected_cols", numCols+4, "actual_cols", len(fields))
			continue
		}

		// Process this subsequent row
		processRowForInference(fields[4:], duckdbTypes, nemTimeCols, seenNonEmpty)

	} // End loop scanning subsequent rows

	logger.Info("Schema inference scan finished.", "rows_processed_for_inference", rowsProcessed)

	// Finalize types: Default any column where no non-empty value was ever seen to VARCHAR.
	for j := 0; j < numCols; j++ {
		if !seenNonEmpty[j] {
			logger.Debug("Column never saw non-empty data during inference, defaulting to VARCHAR.", "column_index", j, "column_name", colNames[j])
			duckdbTypes[j] = "VARCHAR"
		} else if duckdbTypes[j] == "" {
			// This case might happen if a column only ever had empty strings in the scanned rows
			logger.Warn("Column saw data but inferred type is empty, defaulting to VARCHAR.", "column_index", j, "column_name", colNames[j])
			duckdbTypes[j] = "VARCHAR"
		}
	}

	// Final check for NEMTime columns - ensure they are TIMESTAMP_MS
	for idx := range nemTimeCols {
		if duckdbTypes[idx] != "TIMESTAMP_MS" {
			logger.Debug("Overriding inferred type to TIMESTAMP_MS for detected NEM time column.", "column_index", idx, "column_name", colNames[idx], "original_type", duckdbTypes[idx])
			duckdbTypes[idx] = "TIMESTAMP_MS"
		}
	}

	logger.Info("Schema inferred.", "columns", colNames, "types", duckdbTypes)
	return &SchemaInfo{
		ColumnNames:   colNames,
		DuckdbTypes:   duckdbTypes,
		NemTimeColIdx: nemTimeCols,
	}, nil
}

// processRowForInference updates the inferred types based on a single data row portion.
func processRowForInference(dataPortion []string, duckdbTypes []string, nemTimeCols map[int]bool, seenNonEmpty []bool) {
	numCols := len(duckdbTypes)
	if len(dataPortion) != numCols {
		// Log mismatch if necessary, but function should be called with correct slice length
		return
	}

	for j, val := range dataPortion {
		trimmedVal := strings.Trim(val, `" `)

		if trimmedVal == "" {
			continue // Skip empty fields for type inference update
		}

		seenNonEmpty[j] = true // Mark that we've seen data for this column

		// Infer type for this specific value
		currentValType := "VARCHAR" // Default for this value
		isNemTime := false
		if util.IsNEMDateTime(trimmedVal) {
			currentValType = "TIMESTAMP_MS"
			isNemTime = true
			nemTimeCols[j] = true // Mark as NEM time
		} else if _, err := strconv.ParseInt(trimmedVal, 10, 64); err == nil {
			currentValType = "BIGINT"
		} else if _, err := strconv.ParseFloat(trimmedVal, 64); err == nil {
			currentValType = "DOUBLE"
		}

		// Update the overall best type for the column based on hierarchy
		existingType := duckdbTypes[j]
		if existingType == "" || existingType == "VARCHAR" {
			duckdbTypes[j] = currentValType
		} else if existingType == "BIGINT" && (currentValType == "DOUBLE" || currentValType == "TIMESTAMP_MS") {
			duckdbTypes[j] = currentValType // Upgrade BIGINT
		} else if existingType == "DOUBLE" && currentValType == "TIMESTAMP_MS" {
			duckdbTypes[j] = currentValType // Upgrade DOUBLE
		}
		// Ensure NEM time columns end up as TIMESTAMP_MS
		if isNemTime {
			duckdbTypes[j] = "TIMESTAMP_MS"
		}
	}
}

// parseDataRow remains the same as before
func parseDataRow(rowData []string, schema *SchemaInfo, logger *slog.Logger) ([]any, error) {
	if len(rowData) != len(schema.ColumnNames)+4 {
		return nil, fmt.Errorf("data row length (%d) does not match schema length (%d + 4)", len(rowData), len(schema.ColumnNames))
	}

	parsedData := make([]any, len(schema.ColumnNames))
	dataPortion := rowData[4:]

	for i, val := range dataPortion {
		trimmedVal := strings.Trim(val, `" `) // Trim quotes and spaces

		if trimmedVal == "" {
			parsedData[i] = nil // Represent empty strings as NULL
			continue
		}

		colType := schema.DuckdbTypes[i]
		var parsedVal any
		var err error

		switch colType {
		case "TIMESTAMP_MS":
			_, isNemCol := schema.NemTimeColIdx[i]
			if isNemCol || util.IsNEMDateTime(trimmedVal) {
				epochMs, parseErr := util.NEMDateTimeToEpochMS(trimmedVal)
				if parseErr != nil {
					logger.Warn("Failed to parse NEMDateTime, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", parseErr)
					parsedVal = nil
				} else {
					parsedVal = time.UnixMilli(epochMs).UTC()
				}
			} else {
				logger.Warn("Column type is TIMESTAMP_MS but value is not NEM format, treating as VARCHAR.", "value", val, "column", schema.ColumnNames[i])
				parsedVal = trimmedVal
			}
		case "BIGINT":
			parsedVal, err = strconv.ParseInt(trimmedVal, 10, 64)
			if err != nil {
				logger.Warn("Failed to parse BIGINT, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", err)
				parsedVal = nil
			}
		case "DOUBLE":
			parsedVal, err = strconv.ParseFloat(trimmedVal, 64)
			if err != nil {
				logger.Warn("Failed to parse DOUBLE, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", err)
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

		if err != nil && parsedVal != nil {
			return nil, fmt.Errorf("parsing value '%s' for column '%s' as %s: %w", val, schema.ColumnNames[i], colType, err)
		}
		parsedData[i] = parsedVal
	}

	return parsedData, nil
}
