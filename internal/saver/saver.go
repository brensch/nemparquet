package saver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os" // For MkdirAll
	"path/filepath"
	"strings"
	"sync"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"

	_ "github.com/marcboeker/go-duckdb" // Register DuckDB driver
)

// SaveTablesToParquet connects to the DuckDB database and saves each user table
// to a separate Parquet file in the specified output directory.
func SaveTablesToParquet(cfg config.Config, logger *slog.Logger) error {
	logger.Info("--- Starting DuckDB Table to Parquet Save Process ---")

	// 1. Ensure Output Directory Exists
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory '%s': %w", cfg.OutputDir, err)
	}
	logger.Info("Output directory ensured.", slog.String("dir", cfg.OutputDir))

	// 2. Connect to DuckDB
	db, err := sql.Open("duckdb", cfg.DbPath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb (%s): %w", cfg.DbPath, err)
	}
	defer db.Close()

	// Set a reasonable timeout for the overall save operation
	// Adjust as needed based on expected database size
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping duckdb (%s): %w", cfg.DbPath, err)
	}
	logger.Info("Connected to DuckDB.", slog.String("path", cfg.DbPath))

	// 3. Get list of tables
	tableQuery := `PRAGMA show_tables;`
	rows, err := db.QueryContext(ctx, tableQuery)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		// Optional: Add filters here if needed
		// e.g., if strings.HasPrefix(tableName, "internal_") { continue }
		tableNames = append(tableNames, tableName)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating tables: %w", err)
	}
	rows.Close()

	if len(tableNames) == 0 {
		logger.Info("No user tables found in the database to save.")
		return nil
	}
	logger.Info("Found tables to save.", slog.Int("count", len(tableNames)))

	// 4. Save each table to Parquet (concurrently)
	var wg sync.WaitGroup
	var saveErrorsMu sync.Mutex
	var saveErrors []error
	// Limit concurrency? Saving many files might benefit from some limit.
	// saveSem := semaphore.NewWeighted(int64(runtime.NumCPU())) // Example limit

	for _, tableName := range tableNames {
		// Check context before starting next iteration/goroutine
		select {
		case <-ctx.Done():
			logger.Warn("Context cancelled before saving all tables.", "error", ctx.Err())
			goto waitForWorkers // Skip remaining tables and wait for started ones
		default:
		}

		wg.Add(1)
		go func(tn string) {
			defer wg.Done()
			l := logger.With(slog.String("table", tn))
			l.Info("Saving table to Parquet...")

			// Construct output path
			// Replace potentially problematic characters in table name for filename
			safeFilename := strings.ReplaceAll(tn, `"`, "")           // Remove quotes
			safeFilename = strings.ReplaceAll(safeFilename, "/", "_") // Replace slashes
			// Add more replacements if needed
			outputFilePath := filepath.Join(cfg.OutputDir, safeFilename+".parquet")
			duckdbFilePath := strings.ReplaceAll(outputFilePath, `\`, `/`) // DuckDB needs forward slashes

			// Construct COPY TO command
			// Ensure table name is properly quoted for the SQL query
			quotedTableName := fmt.Sprintf(`"%s"`, strings.ReplaceAll(tn, `"`, `""`))
			copySQL := fmt.Sprintf(`COPY %s TO '%s' (FORMAT PARQUET);`,
				quotedTableName,
				strings.ReplaceAll(duckdbFilePath, "'", "''"), // Escape single quotes in path
			)

			l.Debug("Executing COPY TO command.", slog.String("sql_preview", fmt.Sprintf("COPY %s TO ...", quotedTableName)), slog.String("output_path", outputFilePath))

			// Execute using the pool (database/sql handles connection management)
			_, execErr := db.ExecContext(ctx, copySQL)
			if execErr != nil {
				l.Error("Failed to save table to Parquet.", "error", execErr)
				saveErrorsMu.Lock()
				saveErrors = append(saveErrors, fmt.Errorf("save %s: %w", tn, execErr))
				saveErrorsMu.Unlock()
			} else {
				l.Info("Successfully saved table to Parquet.", slog.String("output_path", outputFilePath))
			}
		}(tableName)
	}

waitForWorkers:
	logger.Info("Waiting for save operations to complete...")
	wg.Wait()
	logger.Info("All save operations finished.")

	finalErr := errors.Join(saveErrors...)
	if finalErr != nil {
		logger.Error("Save process completed with errors.", "error", finalErr)
		return finalErr
	}

	logger.Info("--- DuckDB Table to Parquet Save Process Finished Successfully ---")
	return nil
}
