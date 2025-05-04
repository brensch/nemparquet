package orchestrator

import (
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/brensch/nemparquet/internal/util" // For NEMDateTime helpers
)

// SchemaInfo holds the column names and inferred DuckDB types.
type SchemaInfo struct {
	ColumnNames   []string
	DuckdbTypes   []string
	NemTimeColIdx map[int]bool // Indices of columns detected as NEMDateTime
}

const maxInferenceRows = 10 // How many 'D' rows to check for schema inference

// inferSchema analyzes sample data rows to determine column names and DuckDB types.
// It takes the header row ('I' record) and a slice of potential data rows ('D' records).
func inferSchema(headerRow []string, sampleDataRows [][]string, logger *slog.Logger) (*SchemaInfo, error) {
	if len(headerRow) < 4 { // Need at least I, group, table, version + one column
		return nil, fmt.Errorf("header row has insufficient columns (%d) for schema inference", len(headerRow))
	}

	// Column names are from the header row, starting after I, group, table, version
	colNames := headerRow[4:]
	numCols := len(colNames)
	if numCols == 0 {
		return nil, fmt.Errorf("header row contains no data column names after mandatory fields")
	}

	duckdbTypes := make([]string, numCols)
	nemTimeCols := make(map[int]bool)
	bestSampleRow := -1 // Index of the row used for primary type inference

	// Find the first sample row with no empty fields (up to maxInferenceRows)
	for i, row := range sampleDataRows {
		if len(row) != numCols+4 { // Ensure row length matches header (+4 for I,G,T,V)
			logger.Warn("Schema inference sample row length mismatch, skipping row.", "expected_cols", numCols+4, "actual_cols", len(row), "row_index", i)
			continue
		}
		dataPortion := row[4:]
		hasEmpty := false
		for _, val := range dataPortion {
			if strings.TrimSpace(val) == "" {
				hasEmpty = true
				break
			}
		}
		if !hasEmpty {
			bestSampleRow = i
			break
		}
	}

	// Infer types based on the best sample row found, or default to VARCHAR
	if bestSampleRow != -1 {
		logger.Debug("Using sample row for type inference.", "row_index", bestSampleRow)
		refRow := sampleDataRows[bestSampleRow][4:]
		for j, val := range refRow {
			trimmedVal := strings.Trim(val, `" `) // Trim quotes and spaces
			if util.IsNEMDateTime(trimmedVal) {
				duckdbTypes[j] = "TIMESTAMP_MS" // Store as timestamp with millisecond precision
				nemTimeCols[j] = true
			} else if _, err := strconv.ParseInt(trimmedVal, 10, 64); err == nil {
				duckdbTypes[j] = "BIGINT"
			} else if _, err := strconv.ParseFloat(trimmedVal, 64); err == nil {
				duckdbTypes[j] = "DOUBLE"
			} else {
				duckdbTypes[j] = "VARCHAR" // Default for anything else
			}
		}
	} else {
		logger.Warn("No sample row found without empty fields. Defaulting all columns to VARCHAR.")
		for j := 0; j < numCols; j++ {
			duckdbTypes[j] = "VARCHAR"
		}
	}

	// Refine types: If a column was initially VARCHAR but looks like NEMDateTime in *any* sample row, upgrade it.
	// Also, default any remaining empty columns (if bestSampleRow wasn't found or had defaults) to VARCHAR.
	for j := 0; j < numCols; j++ {
		if duckdbTypes[j] == "" || (bestSampleRow == -1 && duckdbTypes[j] == "VARCHAR") { // Check unassigned or default VARCHARs
			foundType := "VARCHAR" // Assume VARCHAR unless proven otherwise
			for _, row := range sampleDataRows {
				if len(row) == numCols+4 {
					val := strings.Trim(row[j+4], `" `)
					if val != "" { // Found a non-empty value
						if util.IsNEMDateTime(val) {
							foundType = "TIMESTAMP_MS"
							nemTimeCols[j] = true
							break // Found NEMDateTime, no need to check further for this column
						} else if _, err := strconv.ParseInt(val, 10, 64); err == nil {
							if foundType == "VARCHAR" { // Only upgrade from VARCHAR
								foundType = "BIGINT"
							}
						} else if _, err := strconv.ParseFloat(val, 64); err == nil {
							if foundType == "VARCHAR" || foundType == "BIGINT" { // Upgrade from VARCHAR or BIGINT
								foundType = "DOUBLE"
							}
						} // else keep VARCHAR
					}
				}
			}
			duckdbTypes[j] = foundType // Assign the best type found across samples
		}
	}

	logger.Info("Schema inferred.", "columns", colNames, "types", duckdbTypes)
	return &SchemaInfo{
		ColumnNames:   colNames,
		DuckdbTypes:   duckdbTypes,
		NemTimeColIdx: nemTimeCols,
	}, nil
}

// parseDataRow parses a 'D' record based on the inferred schema.
// It handles type conversions, including NEMDateTime to epoch milliseconds.
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
					// Log error but potentially insert NULL or original string? Let's error for now.
					logger.Warn("Failed to parse NEMDateTime, setting NULL.", "value", val, "column", schema.ColumnNames[i], "error", parseErr)
					// err = fmt.Errorf("parsing NEMDateTime '%s' for column '%s': %w", val, schema.ColumnNames[i], parseErr)
					parsedVal = nil // Insert NULL on parse failure
				} else {
					// Convert epoch milliseconds to time.Time for DuckDB TIMESTAMP_MS
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
			parsedVal = trimmedVal // Store as string
		}

		// If an error occurred during specific parsing (and we didn't set NULL), return it
		if err != nil && parsedVal != nil {
			return nil, fmt.Errorf("parsing value '%s' for column '%s' as %s: %w", val, schema.ColumnNames[i], colType, err)
		}
		parsedData[i] = parsedVal
	}

	return parsedData, nil
}

// readCSV extracts records from an io.Reader assuming CSV format.
// Used internally by processZip to get records from the unzipped file.
func readCSV(r io.Reader) ([][]string, error) {
	// Configure the CSV reader
	// Be careful with LazyQuotes, it can hide issues. Set other options as needed.
	reader := csv.NewReader(r)
	reader.Comment = '#'           // Default comment character
	reader.LazyQuotes = true       // Allow unescaped quotes
	reader.TrimLeadingSpace = true // Trim leading space

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read all CSV records: %w", err)
	}
	return records, nil
}
