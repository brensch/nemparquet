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

const maxInferenceRows = 10 // How many 'D' rows to check for schema inference

// inferSchema analyzes subsequent 'D' rows from the scanner to determine column names and DuckDB types.
// It reads up to maxInferenceRows or until a fully populated row is found, or a non-'D' record is encountered.
// It consumes the rows it reads from the scanner.
func inferSchema(scanner *util.PeekableScanner, headerRow []string, logger *slog.Logger) (*SchemaInfo, error) {
	if len(headerRow) < 4 { // Need at least I, group, table, version + one column
		return nil, fmt.Errorf("header row has insufficient columns (%d) for schema inference", len(headerRow))
	}

	// Column names are from the header row, starting after I, group, table, version
	colNames := headerRow[4:]
	numCols := len(colNames)
	if numCols == 0 {
		return nil, fmt.Errorf("header row contains no data column names after mandatory fields")
	}

	duckdbTypes := make([]string, numCols) // Store the best inferred type
	nemTimeCols := make(map[int]bool)
	seenNonEmpty := make([]bool, numCols) // Track if we've seen *any* non-empty value for a column
	rowsScanned := 0

	logger.Debug("Starting row-by-row schema inference.", "max_rows", maxInferenceRows)

	for rowsScanned < maxInferenceRows {
		// Peek first to see if the next record is 'D'
		nextRecordType, peekErr := scanner.PeekRecordType()
		if peekErr != nil {
			if errors.Is(peekErr, io.EOF) {
				logger.Debug("EOF reached during schema inference scan.")
				break // End of file, infer based on what we have
			}
			// Some other scanner error occurred during peek
			logger.Error("Scanner error during peek in schema inference.", "error", peekErr)
			return nil, fmt.Errorf("scanner peek error during inference: %w", peekErr)
		}

		if nextRecordType != "D" {
			logger.Debug("Non-'D' record encountered during inference scan, stopping.", "record_type", nextRecordType)
			break // Stop inference if we hit the next 'I' or 'C' or something else
		}

		// Peek successful and it's a 'D' row, now actually Scan it
		if !scanner.Scan() {
			// Error occurred during Scan (or EOF, though peek should have caught EOF)
			scanErr := scanner.Err()
			logger.Error("Scanner error during scan in schema inference.", "error", scanErr)
			if scanErr == nil { // Should not happen if Scan returns false, but safety check
				scanErr = io.EOF
			}
			// Don't return error yet, try to infer based on rows scanned so far
			break
		}
		rowsScanned++
		line := scanner.Text()

		// Parse the 'D' row
		r := csv.NewReader(strings.NewReader(line))
		r.LazyQuotes = true
		r.TrimLeadingSpace = true
		fields, err := r.Read()
		if err != nil {
			logger.Warn("Failed to parse CSV line during inference, skipping row.", "row_num_in_scan", rowsScanned, "error", err)
			continue // Skip this row for inference
		}

		if len(fields) != numCols+4 {
			logger.Warn("Schema inference row length mismatch, skipping row.", "row_num_in_scan", rowsScanned, "expected_cols", numCols+4, "actual_cols", len(fields))
			continue
		}

		dataPortion := fields[4:]
		isCompleteRow := true // Assume complete until proven otherwise

		// Update types based on this row
		for j, val := range dataPortion {
			trimmedVal := strings.Trim(val, `" `)

			if trimmedVal == "" {
				isCompleteRow = false // Found an empty field
				// Don't update type based on empty, but continue checking other columns
				continue
			}

			seenNonEmpty[j] = true // Mark that we've seen data for this column

			// Infer type for this value
			currentValType := "VARCHAR" // Default for this value
			if util.IsNEMDateTime(trimmedVal) {
				currentValType = "TIMESTAMP_MS"
				nemTimeCols[j] = true // Mark as NEM time regardless of current inferred type
			} else if _, err := strconv.ParseInt(trimmedVal, 10, 64); err == nil {
				currentValType = "BIGINT"
			} else if _, err := strconv.ParseFloat(trimmedVal, 64); err == nil {
				currentValType = "DOUBLE"
			}

			// Update the overall best type for the column
			existingType := duckdbTypes[j]
			if existingType == "" || existingType == "VARCHAR" {
				duckdbTypes[j] = currentValType // Upgrade from nothing or VARCHAR
			} else if existingType == "BIGINT" && currentValType == "DOUBLE" {
				duckdbTypes[j] = "DOUBLE" // Upgrade BIGINT to DOUBLE
			} else if existingType == "BIGINT" && currentValType == "TIMESTAMP_MS" {
				duckdbTypes[j] = "TIMESTAMP_MS" // Upgrade BIGINT to TIMESTAMP_MS
			} else if existingType == "DOUBLE" && currentValType == "TIMESTAMP_MS" {
				duckdbTypes[j] = "TIMESTAMP_MS" // Upgrade DOUBLE to TIMESTAMP_MS
			}
			// Note: We don't downgrade (e.g., from DOUBLE to BIGINT)
			// If a column is marked NEMTime, ensure final type is TIMESTAMP_MS
			if _, isNemCol := nemTimeCols[j]; isNemCol {
				duckdbTypes[j] = "TIMESTAMP_MS"
			}
		} // End loop through columns for this row

		// Check if this was a complete row
		if isCompleteRow {
			logger.Info("Found complete row during inference, finalizing schema.", "row_num_in_scan", rowsScanned)
			// Finalize any remaining unset types (shouldn't happen with complete row, but safety)
			for j := 0; j < numCols; j++ {
				if duckdbTypes[j] == "" {
					logger.Warn("Column type still empty after finding complete row, defaulting to VARCHAR.", "column_index", j)
					duckdbTypes[j] = "VARCHAR"
				}
			}
			return &SchemaInfo{
				ColumnNames:   colNames,
				DuckdbTypes:   duckdbTypes,
				NemTimeColIdx: nemTimeCols,
			}, nil // Return immediately with schema based on complete row
		}

	} // End loop scanning rows

	logger.Info("Schema inference scan finished.", "rows_scanned", rowsScanned)

	// If loop finished (max rows reached or non-'D' found) without finding a complete row,
	// finalize types based on what was seen. Default unseen columns to VARCHAR.
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

	logger.Info("Schema inferred (no complete row found).", "columns", colNames, "types", duckdbTypes)
	return &SchemaInfo{
		ColumnNames:   colNames,
		DuckdbTypes:   duckdbTypes,
		NemTimeColIdx: nemTimeCols,
	}, nil
}

// parseDataRow parses a 'D' record based on the inferred schema.
// It handles type conversions, including NEMDateTime to epoch milliseconds.
// (No changes needed from previous version based on the new inference logic)
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
			// Check if it's explicitly marked or just inferred
			if _, isMarked := schema.NemTimeColIdx[i]; isMarked || util.IsNEMDateTime(trimmedVal) {
				epochMs, parseErr := util.NEMDateTimeToEpochMS(trimmedVal)
				if parseErr != nil {
					logger.Warn("Failed to parse NEMDateTime, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", parseErr)
					parsedVal = nil // Insert NULL on parse failure
				} else {
					// Convert epoch milliseconds to time.Time for DuckDB TIMESTAMP_MS
					// Ensure it's stored as UTC in the database if timezone matters downstream
					parsedVal = time.UnixMilli(epochMs).UTC()
				}
			} else {
				// If type is TIMESTAMP_MS but not NEM format, treat as VARCHAR? Or error?
				logger.Warn("Column type is TIMESTAMP_MS but value is not NEM format, treating as VARCHAR.", "value", val, "column", schema.ColumnNames[i])
				parsedVal = trimmedVal // Store original string
			}
		case "BIGINT":
			parsedVal, err = strconv.ParseInt(trimmedVal, 10, 64)
			if err != nil {
				logger.Warn("Failed to parse BIGINT, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", err)
				parsedVal = nil // Insert NULL on parse failure
			}
		case "DOUBLE":
			parsedVal, err = strconv.ParseFloat(trimmedVal, 64)
			if err != nil {
				logger.Warn("Failed to parse DOUBLE, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", err)
				parsedVal = nil // Insert NULL on parse failure
			}
		case "VARCHAR":
			fallthrough // Treat as VARCHAR by default
		default:
			// Ensure the string doesn't contain null bytes, which DuckDB might reject
			if strings.ContainsRune(trimmedVal, 0) {
				logger.Warn("Replacing null byte in VARCHAR data", "column", schema.ColumnNames[i])
				parsedVal = strings.ReplaceAll(trimmedVal, "\x00", "") // Replace null bytes
			} else {
				parsedVal = trimmedVal // Store as string
			}
		}

		// If an error occurred during specific parsing (and we didn't set NULL), return it
		if err != nil && parsedVal != nil {
			return nil, fmt.Errorf("parsing value '%s' for column '%s' as %s: %w", val, schema.ColumnNames[i], colType, err)
		}
		parsedData[i] = parsedVal
	}

	return parsedData, nil
}
