package inspector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp" // Import regexp for filename parsing
	"sort"
	"strings"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"

	_ "github.com/marcboeker/go-duckdb"
)

// Define a structure to hold summary info for each file type
type fileTypeSummary struct {
	fileType        string // e.g., "EST_PERF_COST_RATE_v1"
	fileCount       int    // How many files of this type
	totalRowCount   int64
	minTimestamp    sql.NullInt64 // Store as epoch ms (BIGINT)
	maxTimestamp    sql.NullInt64 // Store as epoch ms (BIGINT)
	schema          string        // Representative schema string
	columnNames     []string      // List of column names from schema
	schemaErr       error         // Error getting schema
	statsErr        error         // Error getting stats
	hasTimestampCol bool          // Flag indicating if INTERVAL_DATETIME exists
	firstFilePath   string        // Path to one file of this type (for getting schema)
}

// Regex pattern for the expected NEM filename format
// Captures the type identifier after the timestamp and long number.
// Allows for flexibility in the prefix before the date.
// Ensures _v followed by digits at the end of the captured group.
var (
	nemPatternRegex = regexp.MustCompile(`^.*_\d+_(.+_v\d+)\.parquet$`)
	// If the timestamp/ID lengths vary slightly, adjust digits: e.g., \d{8,12}_\d{10,18}
)

// ExtractFileType extracts the type identifier part (e.g., "FORECAST_DEFAULT_CF_v1")
// from a Parquet filename conforming to the expected NEM pattern.
// Returns an error if the filename does not match the pattern.
func extractFileType(filename string) (string, error) {
	matches := nemPatternRegex.FindStringSubmatch(filename)
	if len(matches) > 1 {
		// Check if the captured group itself looks like a valid type_vX
		// (This adds robustness if the regex accidentally captures too much)
		// For now, assume the regex capture is correct if it matches.
		return matches[1], nil // Return the captured group
	}

	// Filename did not match the expected NEM pattern
	return "", fmt.Errorf("filename '%s' does not match expected NEM pattern (..._YYYYMMDDHHMM_LONGNUMBER_TYPE_vX.parquet)", filename)
}

// InspectParquet summarizes Parquet files by type.
func InspectParquet(cfg config.Config, logger *slog.Logger) error {
	logger.Info("--- Starting Parquet File Summary Inspection ---")

	db, err := sql.Open("duckdb", cfg.DbPath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb (%s): %w", cfg.DbPath, err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	logger.Debug("Installing and loading Parquet extension.")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
		logger.Warn("Failed install/load parquet extension.", "error", err)
	} else {
		logger.Debug("Parquet extension loaded.")
	}

	// 1. Glob all files
	globPattern := filepath.Join(cfg.OutputDir, "*.parquet")
	parquetFiles, err := filepath.Glob(globPattern)
	if err != nil {
		return fmt.Errorf("failed glob parquet files in %s: %w", cfg.OutputDir, err)
	}
	if len(parquetFiles) == 0 {
		logger.Info("No *.parquet files found.", "dir", cfg.OutputDir)
		return nil
	}
	logger.Info("Found parquet files to summarize.", slog.Int("count", len(parquetFiles)), slog.String("dir", cfg.OutputDir))

	// 2. Categorize files by type, handling extraction errors
	filesByType := make(map[string][]string)
	var categorizationErrors error
	for _, fp := range parquetFiles {
		baseFilename := filepath.Base(fp)
		fileType, err := extractFileType(baseFilename) // Use updated function
		if err != nil {
			logger.Warn("Skipping file due to unexpected name format.", slog.String("file", baseFilename), slog.String("error", err.Error()))
			categorizationErrors = errors.Join(categorizationErrors, err) // Collect errors
			continue                                                      // Skip this file
		}
		filesByType[fileType] = append(filesByType[fileType], fp)
	}
	// Log if any files were skipped during categorization
	if categorizationErrors != nil {
		logger.Warn("Some files were skipped during categorization due to unexpected filenames.", "error", categorizationErrors)
	}
	if len(filesByType) == 0 {
		logger.Info("No files matched the expected naming pattern for categorization.")
		return categorizationErrors // Return errors if any occurred
	}

	// 3. Process each type
	summaries := make(map[string]*fileTypeSummary)
	var orderedTypes []string
	for fileType, files := range filesByType {
		if len(files) == 0 {
			continue
		} // Should not happen after filtering
		orderedTypes = append(orderedTypes, fileType)
		l := logger.With(slog.String("file_type", fileType))
		l.Info("Processing file type", slog.Int("file_count", len(files)))

		summary := &fileTypeSummary{fileType: fileType, fileCount: len(files), firstFilePath: files[0]}
		summaries[fileType] = summary

		// --- Get Schema and Column Names from first file ---
		l.Debug("Getting representative schema.", slog.String("from_file", summary.firstFilePath))
		schemaStr, columnNames, schemaErr := getSchemaAndColumns(ctx, conn, summary.firstFilePath)
		summary.schema = schemaStr
		summary.columnNames = columnNames
		summary.schemaErr = schemaErr
		if schemaErr != nil {
			l.Error("Failed getting schema for type", "error", schemaErr)
		}

		// --- Check if the relevant timestamp column exists ---
		timestampColumnName := "INTERVAL_DATETIME"
		summary.hasTimestampCol = false
		if summary.schemaErr == nil {
			for _, colName := range summary.columnNames {
				if strings.EqualFold(colName, timestampColumnName) {
					summary.hasTimestampCol = true
					break
				}
			}
		}
		l.Debug("Timestamp column check", slog.Bool("found_"+timestampColumnName, summary.hasTimestampCol))

		// --- Get Aggregated Stats ---
		l.Debug("Getting aggregated statistics.")
		var escapedFilePaths []string
		for _, p := range files {
			dp := strings.ReplaceAll(p, `\`, `/`)
			ep := strings.ReplaceAll(dp, "'", "''")
			escapedFilePaths = append(escapedFilePaths, fmt.Sprintf("'%s'", ep))
		}
		fileListLiteral := fmt.Sprintf("[%s]", strings.Join(escapedFilePaths, ", "))

		var statsSQL string
		if summary.hasTimestampCol {
			statsSQL = fmt.Sprintf(`SELECT COUNT(*) as total_rows, MIN(%s) as min_ts, MAX(%s) as max_ts FROM read_parquet(%s);`, timestampColumnName, timestampColumnName, fileListLiteral)
		} else {
			statsSQL = fmt.Sprintf(`SELECT COUNT(*) as total_rows, NULL::BIGINT as min_ts, NULL::BIGINT as max_ts FROM read_parquet(%s);`, fileListLiteral)
		}

		l.Debug("Executing stats query", slog.String("sql", statsSQL))
		var totalRows sql.NullInt64
		var minTs, maxTs sql.NullInt64
		err = conn.QueryRowContext(ctx, statsSQL).Scan(&totalRows, &minTs, &maxTs)
		if err != nil {
			summary.statsErr = err
			l.Error("Failed getting statistics for type", "error", err)
		} else {
			summary.totalRowCount = totalRows.Int64
			summary.minTimestamp = minTs
			summary.maxTimestamp = maxTs
			l.Info("Statistics gathered.", slog.Int64("total_rows", summary.totalRowCount), slog.Int64("min_epoch_ms", summary.minTimestamp.Int64), slog.Int64("max_epoch_ms", summary.maxTimestamp.Int64), slog.Bool("min_valid", summary.minTimestamp.Valid), slog.Bool("max_valid", summary.maxTimestamp.Valid))
		}
	} // End loop file types

	// 4. Format and Print Output (Unchanged)
	logger.Info("--- Parquet File Summary ---")
	fmt.Println("\n--- Parquet File Summary ---")
	sort.Strings(orderedTypes)
	for _, fileType := range orderedTypes {
		summary := summaries[fileType]
		fmt.Printf("\n=== File Type: %s ===\n", summary.fileType)
		fmt.Printf("    (Found %d files)\n", summary.fileCount)
		fmt.Println("\n  Representative Schema:")
		if summary.schemaErr != nil {
			fmt.Printf("    ERROR retrieving schema: %v\n", summary.schemaErr)
		} else if summary.schema == "" {
			fmt.Println("    (Schema not found or file empty)")
		} else {
			schemaLines := strings.Split(summary.schema, "\n")
			for _, line := range schemaLines {
				fmt.Printf("    %s\n", line)
			}
		}
	}
	fmt.Println("\n--- Aggregated Statistics ---")
	fmt.Printf("%-40s | %-10s | %-15s | %-25s | %-25s | %s\n", "File Type", "File Count", "Total Rows", "Min Timestamp (UTC)", "Max Timestamp (UTC)", "Errors")
	fmt.Println(strings.Repeat("-", 130))
	for _, fileType := range orderedTypes {
		summary := summaries[fileType]
		var minTsStr, maxTsStr string = "N/A", "N/A"
		if summary.minTimestamp.Valid {
			minTsUTC := time.UnixMilli(summary.minTimestamp.Int64).UTC()
			minTsStr = minTsUTC.Format(time.RFC3339)
		}
		if summary.maxTimestamp.Valid {
			maxTsUTC := time.UnixMilli(summary.maxTimestamp.Int64).UTC()
			maxTsStr = maxTsUTC.Format(time.RFC3339)
		}
		errorStr := ""
		if summary.schemaErr != nil && summary.statsErr != nil {
			errorStr = "Schema & Stats Error"
		} else if summary.schemaErr != nil {
			errorStr = "Schema Error"
		} else if summary.statsErr != nil {
			errorStr = "Stats Error"
		}
		fmt.Printf("%-40s | %-10d | %-15d | %-25s | %-25s | %s\n", summary.fileType, summary.fileCount, summary.totalRowCount, minTsStr, maxTsStr, errorStr)
	}
	fmt.Println(strings.Repeat("-", 130))
	logger.Info("--- Parquet File Summary Inspection Finished ---")
	// Combine categorization errors with other errors
	var finalErr error = categorizationErrors
	for _, summary := range summaries {
		finalErr = errors.Join(finalErr, summary.schemaErr, summary.statsErr)
	}
	if finalErr != nil {
		logger.Warn("Inspection completed with errors.", "error", finalErr)
	}
	return finalErr
}

// getSchemaAndColumns (Unchanged)
func getSchemaAndColumns(ctx context.Context, conn *sql.Conn, filePath string) (schemaString string, columnNames []string, err error) {
	// ... (Implementation unchanged) ...
	duckdbFilePath := strings.ReplaceAll(filePath, `\`, `/`)
	escapedFilePath := strings.ReplaceAll(duckdbFilePath, "'", "''")
	describeSQL := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s');", escapedFilePath)
	schemaRows, err := conn.QueryContext(ctx, describeSQL)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "No files found") {
			return "(File not found or empty)", nil, nil
		}
		return "", nil, fmt.Errorf("query schema for %s: %w", filePath, err)
	}
	defer schemaRows.Close()
	var schemaBuilder strings.Builder
	columnNames = []string{}
	schemaBuilder.WriteString(fmt.Sprintf("  %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", "Column Name", "Column Type", "Null", "Key", "Default", "Extra"))
	schemaBuilder.WriteString("  " + strings.Repeat("-", 90) + "\n")
	columnCount := 0
	for schemaRows.Next() {
		var colName, colType, nullVal, keyVal, defaultVal, extraVal sql.NullString
		if scanErr := schemaRows.Scan(&colName, &colType, &nullVal, &keyVal, &defaultVal, &extraVal); scanErr != nil {
			return "", nil, fmt.Errorf("scan schema row for %s: %w", filePath, scanErr)
		}
		schemaBuilder.WriteString(fmt.Sprintf("  %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", colName.String, colType.String, nullVal.String, keyVal.String, defaultVal.String, extraVal.String))
		if colName.Valid {
			columnNames = append(columnNames, colName.String)
		}
		columnCount++
	}
	if err = schemaRows.Err(); err != nil {
		return "", nil, fmt.Errorf("iterate schema rows for %s: %w", filePath, err)
	}
	if columnCount == 0 {
		return "(No columns found)", nil, nil
	}
	return strings.TrimRight(schemaBuilder.String(), "\n"), columnNames, nil
}
