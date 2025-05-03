package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog" // Keep slog for potential logging inside DB funcs if needed later
	"os"       // Needed for os.Stat in GetWorkItems
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb" // Driver
	// Removed: "github.com/lib/pq" // No longer needed
)

// Constants for event types
const (
	EventDiscovered    = "discovered"
	EventDownloadStart = "download_start"
	EventDownloadEnd   = "download_end"
	EventExtractStart  = "extract_start" // Might not be used now
	EventExtractEnd    = "extract_end"   // Might not be used now
	EventProcessStart  = "process_start"
	EventProcessEnd    = "process_end"
	EventError         = "error"
	EventSkipDownload  = "skip_download"
	EventSkipExtract   = "skip_extract" // Might not be used now
	EventSkipProcess   = "skip_process"
)

// Constants for file types
const (
	FileTypeZip = "zip"
	FileTypeCsv = "csv"
)

// Schema SQL
const schemaSequenceSQL = `CREATE SEQUENCE IF NOT EXISTS event_log_id_seq;`
const schemaTableSQL = `
CREATE TABLE IF NOT EXISTS nem_event_log (
    log_id          BIGINT PRIMARY KEY DEFAULT nextval('event_log_id_seq'),
    filename        VARCHAR NOT NULL,      -- zip URL or CSV base name
    filetype        VARCHAR NOT NULL,      -- 'zip', 'csv'
    event           VARCHAR NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    source_url      VARCHAR,
    output_path     VARCHAR,               -- Path where zip was saved or parquet generated
    message         VARCHAR,
    sha256_hash     VARCHAR,
    duration_ms     BIGINT
);
CREATE INDEX IF NOT EXISTS idx_nem_event_log_file ON nem_event_log (filename, filetype);
CREATE INDEX IF NOT EXISTS idx_nem_event_log_event_time ON nem_event_log (event, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_nem_event_log_output_path ON nem_event_log (output_path); -- Index for new query
`

// InitializeSchema creates the sequence and tables in the correct order.
func InitializeSchema(db *sql.DB) error {
	// 1. Create Sequence First
	_, err := db.Exec(schemaSequenceSQL)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return fmt.Errorf("failed to execute sequence setup: %w", err)
	}
	// 2. Create Table and Indices
	_, err = db.Exec(schemaTableSQL)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return fmt.Errorf("failed to execute table/index setup: %w", err)
	}
	return nil
}

// LogFileEvent inserts a new event record into the log.
func LogFileEvent(ctx context.Context, db *sql.DB, filename, filetype, event, sourceURL, outputPath, message, sha256 string, duration *time.Duration) error {
	query := `
        INSERT INTO nem_event_log (filename, filetype, event, event_timestamp, source_url, output_path, message, sha256_hash, duration_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    `
	var durationMs sql.NullInt64
	if duration != nil {
		durationMs = sql.NullInt64{Int64: duration.Milliseconds(), Valid: true}
	}

	_, err := db.ExecContext(ctx, query,
		filename,
		filetype,
		event,
		time.Now().UTC(),
		sql.NullString{String: sourceURL, Valid: sourceURL != ""},
		sql.NullString{String: outputPath, Valid: outputPath != ""},
		sql.NullString{String: message, Valid: message != ""},
		sql.NullString{String: sha256, Valid: sha256 != ""},
		durationMs,
	)
	if err != nil {
		return fmt.Errorf("failed to log event '%s' for '%s': %w", event, filename, err)
	}
	return nil
}

// GetLatestFileEvent retrieves the most recent event record for a specific file.
func GetLatestFileEvent(ctx context.Context, db *sql.DB, filename, filetype string) (event string, timestamp time.Time, message string, found bool, err error) {
	query := `
        SELECT event, event_timestamp, message
        FROM nem_event_log
        WHERE filename = ? AND filetype = ?
        ORDER BY event_timestamp DESC, log_id DESC
        LIMIT 1;
    `
	var msg sql.NullString
	row := db.QueryRowContext(ctx, query, filename, filetype)
	err = row.Scan(&event, &timestamp, &msg)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", time.Time{}, "", false, nil // Not found, no error
		}
		return "", time.Time{}, "", false, fmt.Errorf("failed query latest event for '%s' (%s): %w", filename, filetype, err)
	}
	return event, timestamp, msg.String, true, nil
}

// HasEventOccurred checks if a specific event has ever happened for a file.
func HasEventOccurred(ctx context.Context, db *sql.DB, filename, filetype, event string) (bool, error) {
	query := `SELECT 1 FROM nem_event_log WHERE filename = ? AND filetype = ? AND event = ? LIMIT 1;`
	var exists int
	row := db.QueryRowContext(ctx, query, filename, filetype, event)
	err := row.Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil // Event has not occurred
		}
		return false, fmt.Errorf("failed check event '%s' for '%s' (%s): %w", event, filename, filetype, err)
	}
	return true, nil // Event has occurred
}

// GetCompletionStatusBatch checks a list of files for a specific completion event
// using a temporary table approach compatible with DuckDB.
// Returns a map where the key is the filename and the value is true if the completion event exists.
func GetCompletionStatusBatch(ctx context.Context, db *sql.DB, filenames []string, filetype string, completionEvent string) (map[string]bool, error) {
	completedFiles := make(map[string]bool)
	if len(filenames) == 0 {
		return completedFiles, nil
	}

	// Use a transaction for the multi-step temp table process
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for batch check: %w", err)
	}
	defer tx.Rollback() // Rollback is safe even after commit

	// 1. Create a temporary table
	tempTableName := fmt.Sprintf("temp_files_to_check_%d", time.Now().UnixNano())
	createTempTableSQL := fmt.Sprintf(`CREATE TEMP TABLE %s (filename TEXT PRIMARY KEY);`, tempTableName)
	_, err = tx.ExecContext(ctx, createTempTableSQL)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil, fmt.Errorf("failed to create temp table %s: %w", tempTableName, err)
		}
	}

	// 2. Insert filenames into the temporary table
	insertSQL := fmt.Sprintf(`INSERT INTO %s (filename) VALUES (?)`, tempTableName)
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare insert statement for temp table %s: %w", tempTableName, err)
	}

	for _, fn := range filenames {
		select {
		case <-ctx.Done():
			stmt.Close()
			return nil, ctx.Err()
		default:
			if _, err := stmt.ExecContext(ctx, fn); err != nil {
				stmt.Close()
				return nil, fmt.Errorf("failed to insert filename '%s' into temp table %s: %w", fn, tempTableName, err)
			}
		}
	}
	if err = stmt.Close(); err != nil {
		return nil, fmt.Errorf("failed to close insert statement for %s: %w", tempTableName, err)
	}

	// 3. Query the main log table joining with the temporary table
	query := fmt.Sprintf(`
        SELECT DISTINCT el.filename
        FROM nem_event_log el
        JOIN %s tfc ON el.filename = tfc.filename
        WHERE el.filetype = ?
          AND el.event = ?;
    `, tempTableName)
	rows, err := tx.QueryContext(ctx, query, filetype, completionEvent)
	if err != nil {
		return nil, fmt.Errorf("failed batch query status joining temp table %s (event=%s, type=%s): %w", tempTableName, completionEvent, filetype, err)
	}

	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed scanning batch status row: %w", err)
		}
		completedFiles[filename] = true
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("error iterating batch status results: %w", err)
	}
	rows.Close()

	// 4. Commit the transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction for batch check: %w", err)
	}

	return completedFiles, nil
}

// GetZipURLForOutputPath finds the original zip URL associated with a saved zip file path.
func GetZipURLForOutputPath(ctx context.Context, db *sql.DB, outputPath string) (zipURL string, found bool, err error) {
	query := `
		SELECT filename
		FROM nem_event_log
		WHERE output_path = ? AND event = ? AND filetype = ?
		ORDER BY event_timestamp DESC, log_id DESC
		LIMIT 1;
	`
	row := db.QueryRowContext(ctx, query, outputPath, EventDownloadEnd, FileTypeZip)
	err = row.Scan(&zipURL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil // Not found
		}
		return "", false, fmt.Errorf("failed query zip URL for path '%s': %w", outputPath, err)
	}
	return zipURL, true, nil
}

// GetWorkItems identifies files needing download or processing.
// Takes the list of all *discovered* zip URLs as input.
// Returns:
// - urlsToDownload: List of zip URLs that haven't had a successful download_end event.
// - pathsToProcess: List of local zip file paths that have been downloaded but not yet processed.
func GetWorkItems(ctx context.Context, db *sql.DB, allDiscoveredURLs []string) (urlsToDownload []string, pathsToProcess []string, err error) {
	urlsToDownload = []string{}
	pathsToProcess = []string{}
	if len(allDiscoveredURLs) == 0 {
		return urlsToDownload, pathsToProcess, nil
	}

	// 1. Find which discovered URLs have *ever* had a download_end event
	downloadedMap, err := GetCompletionStatusBatch(ctx, db, allDiscoveredURLs, FileTypeZip, EventDownloadEnd)
	if err != nil {
		return nil, nil, fmt.Errorf("failed getting download status batch: %w", err)
	}

	// 2. Find which discovered URLs have *ever* had a process_end event
	processedMap, err := GetCompletionStatusBatch(ctx, db, allDiscoveredURLs, FileTypeZip, EventProcessEnd)
	if err != nil {
		return nil, nil, fmt.Errorf("failed getting process status batch: %w", err)
	}

	// 3. Query to get the latest output_path for successfully downloaded files
	urlsDownloadedNotProcessed := []string{}
	for url := range downloadedMap {
		if _, processed := processedMap[url]; !processed {
			urlsDownloadedNotProcessed = append(urlsDownloadedNotProcessed, url)
		}
	}

	outputPathMap := make(map[string]string) // Map URL -> Output Path
	if len(urlsDownloadedNotProcessed) > 0 {
		// Use temp table approach for potentially large list
		tx, txErr := db.BeginTx(ctx, nil)
		if txErr != nil {
			return nil, nil, fmt.Errorf("begin tx for output paths: %w", txErr)
		}
		defer tx.Rollback() // Ensure rollback if commit doesn't happen

		tempTableName := fmt.Sprintf("temp_urls_dnp_%d", time.Now().UnixNano())
		createSQL := fmt.Sprintf(`CREATE TEMP TABLE %s (url TEXT PRIMARY KEY);`, tempTableName)
		if _, err := tx.ExecContext(ctx, createSQL); err != nil {
			return nil, nil, fmt.Errorf("create temp url table %s: %w", tempTableName, err)
		}

		insertSQL := fmt.Sprintf(`INSERT INTO %s (url) VALUES (?)`, tempTableName)
		stmt, err := tx.PrepareContext(ctx, insertSQL)
		if err != nil {
			return nil, nil, fmt.Errorf("prepare insert url %s: %w", tempTableName, err)
		}
		for _, url := range urlsDownloadedNotProcessed {
			if _, err := stmt.ExecContext(ctx, url); err != nil {
				stmt.Close()
				return nil, nil, fmt.Errorf("insert url %s: %w", url, err)
			}
		}
		stmt.Close()

		query := fmt.Sprintf(`
            WITH LatestDownload AS (
                SELECT
                    el.filename, el.output_path,
                    ROW_NUMBER() OVER(PARTITION BY el.filename ORDER BY el.event_timestamp DESC, el.log_id DESC) as rn
                FROM nem_event_log el
                JOIN %s temp ON el.filename = temp.url
                WHERE el.filetype = ? AND el.event = ? AND el.output_path IS NOT NULL AND el.output_path != ''
            )
            SELECT filename, output_path FROM LatestDownload WHERE rn = 1;
        `, tempTableName)

		rows, err := tx.QueryContext(ctx, query, FileTypeZip, EventDownloadEnd)
		if err != nil {
			return nil, nil, fmt.Errorf("query output paths for downloaded files: %w", err)
		}

		for rows.Next() {
			var url string
			var nullablePath sql.NullString
			if err := rows.Scan(&url, &nullablePath); err != nil {
				rows.Close()
				return nil, nil, fmt.Errorf("scan output path row: %w", err)
			}
			if nullablePath.Valid {
				outputPathMap[url] = nullablePath.String
			}
		}
		rowsCloseErr := rows.Close()
		if err = rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("iterate output path results: %w", err)
		}
		if rowsCloseErr != nil {
			return nil, nil, fmt.Errorf("close output path rows: %w", rowsCloseErr)
		}

		if err = tx.Commit(); err != nil {
			return nil, nil, fmt.Errorf("commit output path tx: %w", err)
		}
	}

	// 4. Categorize all discovered URLs
	for _, url := range allDiscoveredURLs {
		_, downloaded := downloadedMap[url]
		_, processed := processedMap[url]

		if !downloaded {
			urlsToDownload = append(urlsToDownload, url)
		} else if !processed {
			if path, ok := outputPathMap[url]; ok && path != "" {
				if _, statErr := os.Stat(path); statErr == nil {
					pathsToProcess = append(pathsToProcess, path)
				} else {
					slog.Warn("Downloaded file path found in DB but file missing on disk, queueing for re-download.", "url", url, "path", path, "error", statErr)
					urlsToDownload = append(urlsToDownload, url)
				}
			} else {
				slog.Warn("Downloaded file found but output path missing/empty in DB, queueing for re-download.", "url", url)
				urlsToDownload = append(urlsToDownload, url)
			}
		}
	}

	return urlsToDownload, pathsToProcess, nil
}

// DisplayFileHistory queries and prints the event log for files.
func DisplayFileHistory(ctx context.Context, db *sql.DB, filetypeFilter, eventFilter string, limit int) error {
	query := `
        SELECT filename, filetype, event, event_timestamp, message, duration_ms, source_url, output_path
        FROM nem_event_log
    `
	conditions := []string{}
	args := []any{}
	argCounter := 1 // Start with $1 for positional args

	if filetypeFilter != "" {
		conditions = append(conditions, fmt.Sprintf("filetype = $%d", argCounter))
		args = append(args, filetypeFilter)
		argCounter++
	}
	if eventFilter != "" {
		conditions = append(conditions, fmt.Sprintf("event = $%d", argCounter))
		args = append(args, eventFilter)
		argCounter++
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += fmt.Sprintf(" ORDER BY event_timestamp DESC, log_id DESC LIMIT $%d", argCounter)
	args = append(args, limit)

	fmt.Printf("--- Event Log History (Limit %d) ---\n", limit)
	fmt.Printf("%-50s | %-8s | %-15s | %-25s | %-10s | %s\n", "Filename/URL", "Type", "Event", "Timestamp (UTC)", "DurationMS", "Message/Details")
	fmt.Println(strings.Repeat("-", 150))

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query event log: %w \n Query: %s \n Args: %v", err, query, args)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var filename, filetype, event string
		var timestamp time.Time
		var message, sourceURL, outputPath sql.NullString
		var durationMs sql.NullInt64
		if err := rows.Scan(&filename, &filetype, &event, &timestamp, &message, &durationMs, &sourceURL, &outputPath); err != nil {
			return fmt.Errorf("failed to scan event log row: %w", err)
		}

		durationStr := ""
		if durationMs.Valid {
			durationStr = fmt.Sprintf("%d", durationMs.Int64)
		}

		details := message.String
		if sourceURL.Valid && sourceURL.String != "" {
			details += fmt.Sprintf(" (Source: %s)", filepath.Base(sourceURL.String))
		}
		if outputPath.Valid && outputPath.String != "" {
			details += fmt.Sprintf(" (Output: %s)", filepath.Base(outputPath.String))
		}

		fmt.Printf("%-50s | %-8s | %-15s | %-25s | %-10s | %s\n",
			filename, filetype, event, timestamp.Format(time.RFC3339), durationStr, details)
		count++
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating event log rows: %w", err)
	}
	fmt.Printf("Displayed %d records.\n", count)
	return nil
}
