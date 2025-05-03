package inspector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time" // Import time for context timeout

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"

	_ "github.com/marcboeker/go-duckdb"
)

// InspectParquet connects to DuckDB and inspects parquet files, using slog.
func InspectParquet(cfg config.Config, logger *slog.Logger) error {
	logger.Info("--- Starting Parquet File Inspection ---")

	// Use the configured DB path for potential temporary operations if needed,
	// or connect to :memory: if inspection should be isolated.
	// Using cfg.DbPath allows inspection even if main DB is :memory:
	// but might interfere if state DB is persistent and inspection does writes (it shouldn't).
	// Let's use cfg.DbPath for consistency.
	db, err := sql.Open("duckdb", cfg.DbPath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", cfg.DbPath, err)
	}
	defer db.Close()

	// Use a background context with timeout for DB operations
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // Example timeout
	defer cancel()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()

	logger.Debug("Installing and loading Parquet extension for inspection.")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
		// Log warning but continue cautiously, extension might be preloaded/installed
		logger.Warn("Failed to install/load parquet extension. Inspection might fail if not already available.", "error", err)
	} else {
		logger.Debug("Parquet extension loaded (or already available).")
	}

	globPattern := filepath.Join(cfg.OutputDir, "*.parquet")
	parquetFiles, err := filepath.Glob(globPattern)
	if err != nil {
		return fmt.Errorf("failed to glob for parquet files in %s: %w", cfg.OutputDir, err)
	}

	if len(parquetFiles) == 0 {
		logger.Info("No *.parquet files found to inspect.", slog.String("dir", cfg.OutputDir))
		return nil
	}

	logger.Info("Inspecting parquet files", slog.Int("count", len(parquetFiles)), slog.String("dir", cfg.OutputDir))
	var inspectErrors error

	for _, filePath := range parquetFiles {
		// Check context cancellation before processing each file
		select {
		case <-ctx.Done():
			logger.Warn("Inspection cancelled by context.")
			return errors.Join(inspectErrors, ctx.Err())
		default:
			// Continue processing file
		}

		baseName := filepath.Base(filePath)
		l := logger.With(slog.String("file", baseName)) // Logger with file context
		l.Info("Inspecting file")

		// Ensure correct path separators for DuckDB and escape single quotes
		duckdbFilePath := strings.ReplaceAll(filePath, `\`, `/`)
		escapedFilePath := strings.ReplaceAll(duckdbFilePath, "'", "''")

		// Describe Schema
		describeSQL := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s');", escapedFilePath)
		schemaRows, err := conn.QueryContext(ctx, describeSQL)
		if err != nil {
			l.Error("Failed getting schema", "error", err)
			inspectErrors = errors.Join(inspectErrors, fmt.Errorf("schema %s: %w", baseName, err))
			continue // Skip to next file
		}

		l.Debug("Schema:") // Use Debug level for detailed schema output
		schemaColumnCount := 0
		fmt.Printf("--- Schema: %s ---\n", baseName) // Print header to stdout/stderr
		fmt.Printf("  %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", "Column Name", "Column Type", "Null", "Key", "Default", "Extra")
		fmt.Println("  " + strings.Repeat("-", 90))
		for schemaRows.Next() {
			var colName, colType, nullVal, keyVal, defaultVal, extraVal sql.NullString
			if scanErr := schemaRows.Scan(&colName, &colType, &nullVal, &keyVal, &defaultVal, &extraVal); scanErr != nil {
				l.Error("Failed scanning schema row", "error", scanErr)
				inspectErrors = errors.Join(inspectErrors, fmt.Errorf("scan schema %s: %w", baseName, scanErr))
				break // Stop scanning schema for this file
			}
			// Log detailed schema info
			l.Debug("Column",
				slog.String("name", colName.String),
				slog.String("type", colType.String),
				slog.String("null", nullVal.String),
				slog.String("key", keyVal.String),
				slog.String("default", defaultVal.String),
				slog.String("extra", extraVal.String),
			)
			// Print human-readable schema
			fmt.Printf("  %-30s | %-20s | %-5s | %-5s | %-5s | %s\n",
				colName.String, colType.String, nullVal.String, keyVal.String, defaultVal.String, extraVal.String)

			// Basic check for date column type
			if strings.Contains(strings.ToLower(colName.String), "datetime") && colType.String != "BIGINT" && colType.String != "NULL" {
				warnMsg := fmt.Sprintf(">>> UNEXPECTED TYPE for date col '%s': Expected BIGINT, got %s <<<", colName.String, colType.String)
				l.Warn(warnMsg)
				fmt.Printf("  %s\n", warnMsg) // Print warning too
			}
			schemaColumnCount++
		}
		schemaErr := schemaRows.Err() // Check errors after iterating
		if schemaErr != nil {
			l.Error("Error iterating schema rows", "error", schemaErr)
			inspectErrors = errors.Join(inspectErrors, fmt.Errorf("iterate schema %s: %w", baseName, schemaErr))
		}
		schemaRows.Close() // Close rows

		if schemaColumnCount == 0 && schemaErr == nil {
			l.Warn("DESCRIBE returned no columns. File might be empty or invalid.")
			fmt.Println("  WARN: DESCRIBE returned no columns. File might be empty or invalid.")
		}

		// Get Row Count
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s');", escapedFilePath)
		var rowCount int64 = -1
		// Use a separate context timeout for potentially long count query? Or rely on overall ctx.
		err = conn.QueryRowContext(ctx, countSQL).Scan(&rowCount)
		if err != nil {
			l.Error("Failed getting row count", "error", err)
			inspectErrors = errors.Join(inspectErrors, fmt.Errorf("count %s: %w", baseName, err))
			fmt.Println("  ERROR getting row count:", err)
		} else {
			l.Info("File inspection summary", slog.Int64("row_count", rowCount))
			fmt.Printf("  Row Count: %d\n", rowCount)
		}
		fmt.Println(strings.Repeat("-", 40)) // Separator for next file
	} // End loop parquetFiles

	logger.Info("--- Parquet File Inspection Finished ---")
	if inspectErrors != nil {
		logger.Warn("Inspection completed with errors.", "error", inspectErrors)
	}
	return inspectErrors
}
