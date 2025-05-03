package inspector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/brensch/nemparquet/internal/config" // Use your module name

	_ "github.com/marcboeker/go-duckdb"
)

// InspectParquet connects to DuckDB and inspects parquet files.
func InspectParquet(cfg config.Config) error {
	log.Println("--- Starting Parquet File Inspection ---")
	db, err := sql.Open("duckdb", cfg.DbPath) // Use path from config
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", cfg.DbPath, err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background()) // Use background context for inspection
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()

	log.Println("DuckDB (Inspect): Installing and loading Parquet extension...")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(context.Background(), setupSQL); err != nil {
		// Log warning but potentially continue if extension loaded previously
		log.Printf("WARN: Failed to install/load parquet extension: %v. Inspection might fail.", err)
		// Return error as inspection requires it
		return fmt.Errorf("install/load parquet extension: %w", err)

	} else {
		log.Println("DuckDB (Inspect): Parquet extension loaded.")
	}

	// Use output dir from config
	globPattern := filepath.Join(cfg.OutputDir, "*.parquet")
	parquetFiles, err := filepath.Glob(globPattern)
	if err != nil {
		return fmt.Errorf("failed to glob for parquet files in %s: %w", cfg.OutputDir, err)
	}

	if len(parquetFiles) == 0 {
		log.Printf("No *.parquet files found in %s to inspect.", cfg.OutputDir)
		return nil
	}

	log.Printf("Found %d parquet files to inspect in %s", len(parquetFiles), cfg.OutputDir)
	var inspectErrors error

	for _, filePath := range parquetFiles {
		baseName := filepath.Base(filePath)
		log.Printf("--- Inspecting: %s ---", baseName)
		// DuckDB path formatting
		duckdbFilePath := strings.ReplaceAll(filePath, `\`, `/`)
		escapedFilePath := strings.ReplaceAll(duckdbFilePath, "'", "''")

		// Describe Schema
		describeSQL := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s');", escapedFilePath)
		schemaRows, err := conn.QueryContext(context.Background(), describeSQL)
		if err != nil {
			log.Printf("  ERROR getting schema for %s: %v (SQL: %s)", baseName, err, describeSQL)
			inspectErrors = errors.Join(inspectErrors, fmt.Errorf("schema %s: %w", baseName, err))
			continue
		}

		log.Println("  Schema:")
		log.Printf("    %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", "Column Name", "Column Type", "Null", "Key", "Default", "Extra")
		log.Println("    " + strings.Repeat("-", 90))
		schemaColumnCount := 0
		for schemaRows.Next() {
			var colName, colType, nullVal, keyVal, defaultVal, extraVal sql.NullString
			if scanErr := schemaRows.Scan(&colName, &colType, &nullVal, &keyVal, &defaultVal, &extraVal); scanErr != nil {
				log.Printf("  ERROR scanning schema row for %s: %v", baseName, scanErr)
				inspectErrors = errors.Join(inspectErrors, fmt.Errorf("scan schema row %s: %w", baseName, scanErr))
				break
			}
			log.Printf("    %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", colName.String, colType.String, nullVal.String, keyVal.String, defaultVal.String, extraVal.String)

			// Basic check for date column type
			if strings.Contains(strings.ToLower(colName.String), "datetime") && colType.String != "BIGINT" && colType.String != "NULL" {
				log.Printf("    >>> UNEXPECTED TYPE for date col '%s': Expected BIGINT, got %s <<<", colName.String, colType.String)
			}
			schemaColumnCount++
		}
		if err = schemaRows.Err(); err != nil {
			log.Printf("  ERROR iterating schema rows for %s: %v", baseName, err)
			inspectErrors = errors.Join(inspectErrors, fmt.Errorf("iterate schema rows %s: %w", baseName, err))
		}
		schemaRows.Close()

		if schemaColumnCount == 0 && err == nil {
			log.Println("  WARN: DESCRIBE returned no columns. File might be empty or invalid.")
		}

		// Get Row Count
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s');", escapedFilePath)
		var rowCount int64 = -1
		err = conn.QueryRowContext(context.Background(), countSQL).Scan(&rowCount)
		if err != nil {
			log.Printf("  ERROR getting row count for %s: %v (SQL: %s)", baseName, err, countSQL)
			inspectErrors = errors.Join(inspectErrors, fmt.Errorf("count %s: %w", baseName, err))
		} else {
			log.Printf("  Row Count: %d", rowCount)
		}
		log.Println(strings.Repeat("-", 40))
	}

	log.Println("--- Parquet File Inspection Finished ---")
	return inspectErrors
}
