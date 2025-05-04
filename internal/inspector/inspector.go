package inspector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"

	_ "github.com/marcboeker/go-duckdb" // Register DuckDB driver
)

// tableSummary holds summary info for each table in the database.
type tableSummary struct {
	tableName       string
	totalRowCount   sql.NullInt64 // Use NullInt64 for count
	minTimestamp    sql.NullInt64 // Store as epoch ms (BIGINT)
	maxTimestamp    sql.NullInt64 // Store as epoch ms (BIGINT)
	schema          string        // Formatted schema string
	columnNames     []string      // List of column names from schema
	schemaErr       error         // Error getting schema
	statsErr        error         // Error getting stats
	hasTimestampCol bool          // Flag indicating if a likely timestamp column exists
	timestampCol    string        // Name of the identified timestamp column
}

// InspectDuckDB summarizes tables within the specified DuckDB database.
func InspectDuckDB(cfg config.Config, logger *slog.Logger) error {
	logger.Info("--- Starting DuckDB Table Summary Inspection ---")

	// Use read-only mode if possible, though not strictly necessary for inspection queries
	// dsn := fmt.Sprintf("%s?access_mode=read_only", cfg.DbPath)
	dsn := cfg.DbPath // Use the configured path directly

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return fmt.Errorf("failed to open duckdb (%s): %w", dsn, err)
	}
	defer db.Close()

	// Set a reasonable timeout for inspection queries
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Get a connection for executing multiple queries
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// 1. Get list of tables
	// Using PRAGMA show_tables is simpler than information_schema for DuckDB
	tableQuery := `PRAGMA show_tables;`
	rows, err := conn.QueryContext(ctx, tableQuery)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			rows.Close() // Close rows before returning error
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		// Optional: Filter out internal DuckDB tables or specific prefixes if needed
		// if strings.HasPrefix(tableName, "duckdb_") { continue }
		tableNames = append(tableNames, tableName)
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating tables: %w", err)
	}
	rows.Close() // Close explicitly after loop

	if len(tableNames) == 0 {
		logger.Info("No user tables found in the database.", "path", cfg.DbPath)
		return nil
	}
	logger.Info("Found tables to summarize.", slog.Int("count", len(tableNames)), slog.String("db", cfg.DbPath))

	// 2. Process each table
	summaries := make(map[string]*tableSummary)
	sort.Strings(tableNames) // Process tables alphabetically

	for _, tableName := range tableNames {
		l := logger.With(slog.String("table_name", tableName))
		l.Info("Processing table")

		summary := &tableSummary{tableName: tableName}
		summaries[tableName] = summary

		// --- Get Schema and Column Names ---
		l.Debug("Getting table schema.")
		// Using PRAGMA table_info is generally reliable in DuckDB
		schemaStr, columnNames, timestampColName, schemaErr := getTableSchemaAndTimestampCol(ctx, conn, tableName)
		summary.schema = schemaStr
		summary.columnNames = columnNames
		summary.schemaErr = schemaErr
		summary.hasTimestampCol = (timestampColName != "")
		summary.timestampCol = timestampColName
		if schemaErr != nil {
			l.Error("Failed getting schema for table", "error", schemaErr)
			// Continue to next table if schema fails? Or try stats anyway? Let's continue.
			continue
		}
		l.Debug("Timestamp column check", slog.Bool("found", summary.hasTimestampCol), slog.String("col_name", summary.timestampCol))

		// --- Get Aggregated Stats (Count, Min/Max Timestamp) ---
		l.Debug("Getting table statistics.")
		var countSQL, minMaxSQL string

		// Use prepared statements potentially? For now, direct query. Quote table name.
		safeTableName := fmt.Sprintf(`"%s"`, strings.ReplaceAll(tableName, `"`, `""`)) // Basic quoting

		countSQL = fmt.Sprintf(`SELECT COUNT(*) FROM %s;`, safeTableName)

		if summary.hasTimestampCol {
			safeTimestampCol := fmt.Sprintf(`"%s"`, strings.ReplaceAll(summary.timestampCol, `"`, `""`))
			// Assuming timestamp column is stored as TIMESTAMP_MS (epoch milliseconds)
			minMaxSQL = fmt.Sprintf(`SELECT MIN(epoch_ms(%s)), MAX(epoch_ms(%s)) FROM %s;`, safeTimestampCol, safeTimestampCol, safeTableName)
		}

		// Query count
		l.Debug("Executing count query")
		err = conn.QueryRowContext(ctx, countSQL).Scan(&summary.totalRowCount)
		if err != nil {
			summary.statsErr = errors.Join(summary.statsErr, fmt.Errorf("count query failed: %w", err))
			l.Error("Failed getting row count", "error", err)
			// Continue processing other stats if possible
		} else {
			l.Debug("Row count gathered.", slog.Int64("count", summary.totalRowCount.Int64))
		}

		// Query Min/Max Timestamp if applicable
		if summary.hasTimestampCol && minMaxSQL != "" {
			l.Debug("Executing min/max timestamp query")
			err = conn.QueryRowContext(ctx, minMaxSQL).Scan(&summary.minTimestamp, &summary.maxTimestamp)
			if err != nil {
				summary.statsErr = errors.Join(summary.statsErr, fmt.Errorf("min/max timestamp query failed: %w", err))
				l.Error("Failed getting min/max timestamp", "error", err)
			} else {
				l.Debug("Timestamps gathered.",
					slog.Int64("min_epoch_ms", summary.minTimestamp.Int64),
					slog.Int64("max_epoch_ms", summary.maxTimestamp.Int64),
					slog.Bool("min_valid", summary.minTimestamp.Valid),
					slog.Bool("max_valid", summary.maxTimestamp.Valid))
			}
		}

		if summary.statsErr != nil {
			l.Warn("Statistics gathering encountered errors.", "error", summary.statsErr)
		} else if summary.totalRowCount.Valid { // Only log full success if count is valid
			l.Info("Statistics gathered successfully.", slog.Int64("total_rows", summary.totalRowCount.Int64))
		}

	} // End loop tables

	// 3. Format and Print Output
	logger.Info("--- DuckDB Table Summary ---")
	fmt.Println("\n--- DuckDB Table Summary ---")
	fmt.Println("\n--- Table Schemas ---")

	for _, tableName := range tableNames {
		summary := summaries[tableName]
		fmt.Printf("\n=== Table: %s ===\n", summary.tableName)
		fmt.Println("\n  Schema:")
		if summary.schemaErr != nil {
			fmt.Printf("    ERROR retrieving schema: %v\n", summary.schemaErr)
		} else if summary.schema == "" {
			fmt.Println("    (Schema not found or table empty?)")
		} else {
			schemaLines := strings.Split(summary.schema, "\n")
			for _, line := range schemaLines {
				// Indent schema lines for readability
				fmt.Printf("    %s\n", line)
			}
		}
	}

	fmt.Println("\n--- Aggregated Statistics ---")
	fmt.Printf("%-60s | %-15s | %-25s | %-25s | %s\n", "Table Name", "Total Rows", "Min Timestamp (UTC)", "Max Timestamp (UTC)", "Errors")
	fmt.Println(strings.Repeat("-", 140)) // Adjust separator length

	for _, tableName := range tableNames {
		summary := summaries[tableName]
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

		rowCountStr := "N/A"
		if summary.totalRowCount.Valid {
			rowCountStr = fmt.Sprintf("%d", summary.totalRowCount.Int64)
		}

		// Truncate long table names if necessary for display
		displayTableName := summary.tableName
		if len(displayTableName) > 58 {
			displayTableName = displayTableName[:55] + "..."
		}

		fmt.Printf("%-60s | %-15s | %-25s | %-25s | %s\n",
			displayTableName,
			rowCountStr,
			minTsStr,
			maxTsStr,
			errorStr)
	}
	fmt.Println(strings.Repeat("-", 140)) // Adjust separator length
	logger.Info("--- DuckDB Table Summary Inspection Finished ---")

	// Collect final errors
	var finalErr error
	for _, summary := range summaries {
		finalErr = errors.Join(finalErr, summary.schemaErr, summary.statsErr)
	}
	if finalErr != nil {
		logger.Warn("Inspection completed with errors.", "error", finalErr)
	}
	return finalErr
}

// getTableSchemaAndTimestampCol retrieves the schema description and identifies a likely timestamp column.
func getTableSchemaAndTimestampCol(ctx context.Context, conn *sql.Conn, tableName string) (schemaString string, columnNames []string, timestampCol string, err error) {
	// Use PRAGMA table_info for schema details
	safeTableName := strings.ReplaceAll(tableName, "'", "''") // Escape single quotes for SQL literal
	describeSQL := fmt.Sprintf("PRAGMA table_info('%s');", safeTableName)

	schemaRows, err := conn.QueryContext(ctx, describeSQL)
	if err != nil {
		// Handle specific error if table doesn't exist cleanly
		if strings.Contains(err.Error(), "Table with name") && strings.Contains(err.Error(), "does not exist") {
			return "(Table not found)", nil, "", nil // Not an error for inspection, just doesn't exist
		}
		return "", nil, "", fmt.Errorf("query schema for %s: %w", tableName, err)
	}
	defer schemaRows.Close()

	var schemaBuilder strings.Builder
	columnNames = []string{}
	timestampCol = "" // Reset timestamp column name

	// Header for the schema output
	schemaBuilder.WriteString(fmt.Sprintf("%-4s | %-30s | %-20s | %-6s | %-15s | %s\n", "CID", "Name", "Type", "NotNull", "Default", "PK"))
	schemaBuilder.WriteString(strings.Repeat("-", 90) + "\n")

	columnCount := 0
	for schemaRows.Next() {
		var cid sql.NullInt64 // Column ID
		var name sql.NullString
		var colType sql.NullString
		var notnull sql.NullBool
		var dfltValue sql.NullString // Default value
		var pk sql.NullBool          // Primary key flag

		if scanErr := schemaRows.Scan(&cid, &name, &colType, &notnull, &dfltValue, &pk); scanErr != nil {
			return "", nil, "", fmt.Errorf("scan schema row for %s: %w", tableName, scanErr)
		}

		// Format the line for the schema string
		schemaBuilder.WriteString(fmt.Sprintf("%-4d | %-30s | %-20s | %-6t | %-15s | %t\n",
			cid.Int64,
			name.String,
			colType.String,
			notnull.Bool,
			dfltValue.String, // Handle potential NULL default
			pk.Bool))

		// Store column name
		if name.Valid {
			columnNames = append(columnNames, name.String)
			// Identify potential timestamp column (case-insensitive check for common names)
			// Prioritize INTERVAL_DATETIME if found
			lowerColName := strings.ToLower(name.String)
			if timestampCol == "" || lowerColName == "interval_datetime" { // Found first potential or the preferred one
				if lowerColName == "interval_datetime" || strings.Contains(lowerColName, "timestamp") || strings.Contains(lowerColName, "_datetime") {
					// Check if the type is compatible (TIMESTAMP related or BIGINT for epoch)
					colTypeLower := strings.ToLower(colType.String)
					if strings.Contains(colTypeLower, "timestamp") || colTypeLower == "bigint" {
						timestampCol = name.String // Assign if it's a likely candidate
					}
				}
			}
		}
		columnCount++
	}
	if err = schemaRows.Err(); err != nil {
		return "", nil, "", fmt.Errorf("iterate schema rows for %s: %w", tableName, err)
	}
	if columnCount == 0 {
		return "(No columns found or table empty)", nil, "", nil
	}
	return strings.TrimRight(schemaBuilder.String(), "\n"), columnNames, timestampCol, nil
}
