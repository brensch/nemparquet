package inspector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog" // Import os for Stat
	"path/filepath"
	"regexp" // Import regexp for filename parsing
	"sort"
	"strings"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	// For GetNEMLocation if needed for display
	_ "github.com/marcboeker/go-duckdb"
)

// Define a structure to hold summary info for each file type
type fileTypeSummary struct {
	fileType      string // e.g., "EST_PERF_COST_RATE_v1"
	fileCount     int    // How many files of this type
	totalRowCount int64
	minTimestamp  sql.NullInt64 // Store as epoch ms (BIGINT)
	maxTimestamp  sql.NullInt64 // Store as epoch ms (BIGINT)
	schema        string        // Representative schema string
	schemaErr     error         // Error getting schema
	statsErr      error         // Error getting stats
	firstFilePath string        // Path to one file of this type (for getting schema)
}

// Regex to extract the type (Comp_vVer) from the filename pattern: zipBaseName_Comp_vVer.parquet
// Assumes zipBaseName doesn't contain underscores. Adjust if needed.
// It captures the part between the first underscore and the ".parquet" extension.
// More robust: Capture specifically Comp_vVer part if possible.
// Example: "test_zip_file_EST_PERF_COST_RATE_v1.parquet" -> captures "EST_PERF_COST_RATE_v1"
// Revised Regex: Capture everything after the *first* underscore up to the last underscore before .parquet
// This assumes zipBaseName might have underscores, but Comp_vVer won't have internal underscores matching the version pattern.
// Let's try a simpler approach first: Split by "_" and rejoin middle parts.
// var fileNameRegex = regexp.MustCompile(`^.*?_(.*)\.parquet$`)
// Simpler: Assume format is {anything}_{Comp}_{vVer}.parquet
var fileNameRegex = regexp.MustCompile(`^.*_(.+_v\d+)\.parquet$`)

// ExtractFileType extracts the "Comp_vVer" part from a Parquet filename.
func extractFileType(filename string) string {
	matches := fileNameRegex.FindStringSubmatch(filename)
	if len(matches) > 1 {
		return matches[1] // Return the captured group
	}
	// Fallback if regex doesn't match (e.g., different filename format)
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext) // Return base name without extension as fallback
}

// InspectParquet summarizes Parquet files by type.
func InspectParquet(cfg config.Config, logger *slog.Logger) error {
	logger.Info("--- Starting Parquet File Summary Inspection ---")

	db, err := sql.Open("duckdb", cfg.DbPath) // Use :memory: for isolation? ":memory:"
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", cfg.DbPath, err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Longer timeout for potentially more complex queries
	defer cancel()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()

	logger.Debug("Installing and loading Parquet extension.")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
		logger.Warn("Failed to install/load parquet extension. Inspection might fail.", "error", err)
	} else {
		logger.Debug("Parquet extension loaded.")
	}

	// 1. Glob all files
	globPattern := filepath.Join(cfg.OutputDir, "*.parquet")
	parquetFiles, err := filepath.Glob(globPattern)
	if err != nil {
		return fmt.Errorf("failed to glob for parquet files in %s: %w", cfg.OutputDir, err)
	}
	if len(parquetFiles) == 0 {
		logger.Info("No *.parquet files found to inspect.", slog.String("dir", cfg.OutputDir))
		return nil
	}
	logger.Info("Found parquet files to summarize.", slog.Int("count", len(parquetFiles)), slog.String("dir", cfg.OutputDir))

	// 2. Categorize files by type
	filesByType := make(map[string][]string)
	for _, fp := range parquetFiles {
		fileType := extractFileType(filepath.Base(fp))
		filesByType[fileType] = append(filesByType[fileType], fp)
	}

	// 3. Process each type
	summaries := make(map[string]*fileTypeSummary)
	var orderedTypes []string // Keep order for printing
	for fileType, files := range filesByType {
		if len(files) == 0 {
			continue
		} // Should not happen
		orderedTypes = append(orderedTypes, fileType)

		l := logger.With(slog.String("file_type", fileType))
		l.Info("Processing file type", slog.Int("file_count", len(files)))

		summary := &fileTypeSummary{
			fileType:      fileType,
			fileCount:     len(files),
			firstFilePath: files[0], // Use first file to get schema
		}
		summaries[fileType] = summary

		// --- Get Schema from first file ---
		l.Debug("Getting representative schema.", slog.String("from_file", summary.firstFilePath))
		schemaStr, schemaErr := getSchema(ctx, conn, summary.firstFilePath)
		summary.schema = schemaStr
		summary.schemaErr = schemaErr
		if schemaErr != nil {
			l.Error("Failed getting schema for type", "error", schemaErr)
			// Continue to next type, summary will show schema error
		}

		// --- Get Aggregated Stats using glob pattern ---
		l.Debug("Getting aggregated statistics.")
		// Create glob pattern for this type (ensure path separators are correct for DuckDB)
		// duckdbOutputDir := strings.ReplaceAll(cfg.OutputDir, `\`, `/`)
		// Need to escape special characters in fileType if using complex globs,
		// but for simple types, this should be okay.
		// Construct pattern like /path/to/output/*_FILETYPE.parquet
		// We need the part *before* the type in the filename. This is tricky if zipBaseName varies.
		// SAFER APPROACH: Use read_parquet with a LIST of files.
		// duckdbGlobPattern := filepath.Join(duckdbOutputDir, "*"+fileType+".parquet") // Might be too broad if zipBaseName varies
		// escapedGlobPattern := strings.ReplaceAll(duckdbGlobPattern, "'", "''")

		// Convert file list to DuckDB list literal if possible, or use temp table.
		// Let's try list literal syntax 'path1', 'path2', ...
		var escapedFilePaths []string
		for _, p := range files {
			dp := strings.ReplaceAll(p, `\`, `/`)
			ep := strings.ReplaceAll(dp, "'", "''") // Escape single quotes
			escapedFilePaths = append(escapedFilePaths, fmt.Sprintf("'%s'", ep))
		}
		fileListLiteral := fmt.Sprintf("[%s]", strings.Join(escapedFilePaths, ", "))

		// Query assumes INTERVAL_DATETIME column exists and is BIGINT (epoch ms)
		// Use TRY_CAST for robustness if type might vary? No, schema should be consistent per type.
		statsSQL := fmt.Sprintf(`
            SELECT
                COUNT(*) as total_rows,
                MIN(INTERVAL_DATETIME) as min_ts,
                MAX(INTERVAL_DATETIME) as max_ts
            FROM read_parquet(%s);
        `, fileListLiteral) // Pass list literal directly

		l.Debug("Executing stats query", slog.String("sql", statsSQL)) // Log the query for debugging
		var totalRows sql.NullInt64                                    // Use NullInt64 in case COUNT returns NULL for empty input
		var minTs, maxTs sql.NullInt64
		err = conn.QueryRowContext(ctx, statsSQL).Scan(&totalRows, &minTs, &maxTs)
		if err != nil {
			summary.statsErr = err
			l.Error("Failed getting statistics for type", "error", err)
			// Continue to next type
		} else {
			summary.totalRowCount = totalRows.Int64 // Assign 0 if NULL
			summary.minTimestamp = minTs
			summary.maxTimestamp = maxTs
			l.Info("Statistics gathered.",
				slog.Int64("total_rows", summary.totalRowCount),
				slog.Int64("min_epoch_ms", summary.minTimestamp.Int64), // Will be 0 if NULL
				slog.Int64("max_epoch_ms", summary.maxTimestamp.Int64), // Will be 0 if NULL
			)
		}
	} // End loop file types

	// 4. Format and Print Output
	logger.Info("--- Parquet File Summary ---")
	fmt.Println("\n--- Parquet File Summary ---")

	// Sort types for consistent output order
	sort.Strings(orderedTypes)

	for _, fileType := range orderedTypes {
		summary := summaries[fileType]
		fmt.Printf("\n=== File Type: %s ===\n", summary.fileType)
		fmt.Printf("    (Found %d files)\n", summary.fileCount)

		// Print Schema
		fmt.Println("\n  Representative Schema:")
		if summary.schemaErr != nil {
			fmt.Printf("    ERROR retrieving schema: %v\n", summary.schemaErr)
		} else if summary.schema == "" {
			fmt.Println("    (Schema not found or file empty)")
		} else {
			// Print schema string with indentation
			schemaLines := strings.Split(summary.schema, "\n")
			for _, line := range schemaLines {
				fmt.Printf("    %s\n", line)
			}
		}
	}

	// Print Summary Table
	fmt.Println("\n--- Aggregated Statistics ---")
	fmt.Printf("%-40s | %-10s | %-15s | %-25s | %-25s | %s\n",
		"File Type", "File Count", "Total Rows", "Min Timestamp (UTC)", "Max Timestamp (UTC)", "Errors")
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

		fmt.Printf("%-40s | %-10d | %-15d | %-25s | %-25s | %s\n",
			summary.fileType,
			summary.fileCount,
			summary.totalRowCount,
			minTsStr,
			maxTsStr,
			errorStr,
		)
	}
	fmt.Println(strings.Repeat("-", 130))

	logger.Info("--- Parquet File Summary Inspection Finished ---")
	// Consolidate errors from summary map for final return value
	var finalErr error
	for _, summary := range summaries {
		finalErr = errors.Join(finalErr, summary.schemaErr, summary.statsErr)
	}
	if finalErr != nil {
		logger.Warn("Inspection completed with errors.", "error", finalErr)
	}

	return finalErr
}

// getSchema retrieves the schema string for a single Parquet file.
func getSchema(ctx context.Context, conn *sql.Conn, filePath string) (string, error) {
	// Ensure correct path separators and escape quotes
	duckdbFilePath := strings.ReplaceAll(filePath, `\`, `/`)
	escapedFilePath := strings.ReplaceAll(duckdbFilePath, "'", "''")

	describeSQL := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s');", escapedFilePath)
	schemaRows, err := conn.QueryContext(ctx, describeSQL)
	if err != nil {
		// Check if file not found or empty
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "No files found") {
			return "(File not found or empty)", nil // Return specific message, not error
		}
		return "", fmt.Errorf("query schema for %s: %w", filePath, err)
	}
	defer schemaRows.Close()

	var schemaBuilder strings.Builder
	schemaBuilder.WriteString(fmt.Sprintf("  %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", "Column Name", "Column Type", "Null", "Key", "Default", "Extra"))
	schemaBuilder.WriteString("  " + strings.Repeat("-", 90) + "\n")
	columnCount := 0
	for schemaRows.Next() {
		var colName, colType, nullVal, keyVal, defaultVal, extraVal sql.NullString
		if scanErr := schemaRows.Scan(&colName, &colType, &nullVal, &keyVal, &defaultVal, &extraVal); scanErr != nil {
			return "", fmt.Errorf("scan schema row for %s: %w", filePath, scanErr)
		}
		schemaBuilder.WriteString(fmt.Sprintf("  %-30s | %-20s | %-5s | %-5s | %-5s | %s\n",
			colName.String, colType.String, nullVal.String, keyVal.String, defaultVal.String, extraVal.String))
		columnCount++
	}
	if err = schemaRows.Err(); err != nil {
		return "", fmt.Errorf("iterate schema rows for %s: %w", filePath, err)
	}

	if columnCount == 0 {
		return "(No columns found)", nil // Indicate empty schema without erroring
	}

	return strings.TrimRight(schemaBuilder.String(), "\n"), nil // Trim trailing newline
}
