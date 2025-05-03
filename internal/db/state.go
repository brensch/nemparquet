package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os" // Needed for os.Stat in GetPathsToProcess
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb" // Driver
	// "github.com/lib/pq" // No longer needed
)

// Constants for event types
const (
	EventDiscovered    = "discovered"
	EventDownloadStart = "download_start"
	EventDownloadEnd   = "download_end"
	EventExtractStart  = "extract_start" // Kept for potential future use, but not used in current workflow
	EventExtractEnd    = "extract_end"   // Kept for potential future use, but not used in current workflow
	EventProcessStart  = "process_start"
	EventProcessEnd    = "process_end"
	EventError         = "error"
	EventSkipDownload  = "skip_download"
	EventSkipExtract   = "skip_extract" // Kept for potential future use
	EventSkipProcess   = "skip_process"
)

// Constants for file types
const (
	FileTypeZip          = "zip"           // Represents an individual data zip file (inner or current)
	FileTypeCsv          = "csv"           // Represents a specific CSV section (less used for state now)
	FileTypeOuterArchive = "outer_archive" // Represents the main archive zip containing other zips
)

// Schema SQL
const schemaSequenceSQL = `CREATE SEQUENCE IF NOT EXISTS event_log_id_seq;`
const schemaTableSQL = `
CREATE TABLE IF NOT EXISTS nem_event_log (
    log_id          BIGINT PRIMARY KEY DEFAULT nextval('event_log_id_seq'),
    filename        VARCHAR NOT NULL,      -- Identifier: zip URL, inner zip name, or outer archive URL
    filetype        VARCHAR NOT NULL,      -- 'zip', 'csv', 'outer_archive'
    event           VARCHAR NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    source_url      VARCHAR,               -- e.g., outer archive URL for an inner zip event
    output_path     VARCHAR,               -- Path where inner zip was saved or parquet generated
    message         VARCHAR,
    sha256_hash     VARCHAR,
    duration_ms     BIGINT
);
-- Indices
CREATE INDEX IF NOT EXISTS idx_nem_event_log_file ON nem_event_log (filename, filetype);
CREATE INDEX IF NOT EXISTS idx_nem_event_log_event_time ON nem_event_log (event, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_nem_event_log_output_path ON nem_event_log (output_path);
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
		// Add filetype context to error message
		return fmt.Errorf("failed to log event '%s' for '%s' (type: %s): %w", event, filename, filetype, err)
	}
	return nil
}

// GetLatestFileEvent retrieves the most recent event record for a specific file identifier and type.
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

// HasEventOccurred checks if a specific event has ever happened for a file identifier and type.
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

// GetCompletionStatusBatch checks a list of file identifiers for a specific completion event and file type
// using a temporary table approach compatible with DuckDB.
// Returns a map where the key is the filename identifier and the value is true if the completion event exists.
func GetCompletionStatusBatch(ctx context.Context, db *sql.DB, filenames []string, filetype string, completionEvent string) (map[string]bool, error) {
	completedFiles := make(map[string]bool)
	if len(filenames) == 0 {
		return completedFiles, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed begin transaction for batch check: %w", err)
	}
	defer tx.Rollback()

	tempTableName := fmt.Sprintf("temp_files_to_check_%d", time.Now().UnixNano())
	createTempTableSQL := fmt.Sprintf(`CREATE TEMP TABLE %s (filename TEXT PRIMARY KEY);`, tempTableName)
	_, err = tx.ExecContext(ctx, createTempTableSQL)
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return nil, fmt.Errorf("failed create temp table %s: %w", tempTableName, err)
		}
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s (filename) VALUES (?)`, tempTableName)
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return nil, fmt.Errorf("failed prepare insert for temp table %s: %w", tempTableName, err)
	}

	for _, fn := range filenames {
		select {
		case <-ctx.Done():
			stmt.Close()
			return nil, ctx.Err()
		default:
			if _, err := stmt.ExecContext(ctx, fn); err != nil {
				stmt.Close()
				return nil, fmt.Errorf("failed insert filename '%s' into temp table %s: %w", fn, tempTableName, err)
			}
		}
	}
	if err = stmt.Close(); err != nil {
		return nil, fmt.Errorf("failed close insert statement for %s: %w", tempTableName, err)
	}

	query := fmt.Sprintf(`
        SELECT DISTINCT el.filename
        FROM nem_event_log el
        JOIN %s tfc ON el.filename = tfc.filename
        WHERE el.filetype = ? AND el.event = ?;
    `, tempTableName)
	rows, err := tx.QueryContext(ctx, query, filetype, completionEvent)
	if err != nil {
		return nil, fmt.Errorf("failed batch query joining temp table %s (event=%s, type=%s): %w", tempTableName, completionEvent, filetype, err)
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

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed commit transaction for batch check: %w", err)
	}

	return completedFiles, nil
}

// GetIdentifierForOutputPath finds the original identifier (URL or inner zip name) and its filetype
// associated with a saved zip file path by looking for the latest download_end event matching the path.
func GetIdentifierForOutputPath(ctx context.Context, db *sql.DB, outputPath string) (identifier string, filetype string, found bool, err error) {
	// We need both filename (identifier) and filetype associated with the output_path
	query := `
		SELECT filename, filetype
		FROM nem_event_log
		WHERE output_path = ? AND event = ?
		ORDER BY event_timestamp DESC, log_id DESC
		LIMIT 1;
	`
	// Event download_end should have been logged with the output_path for both outer archives (if saved)
	// and inner/regular zips.
	row := db.QueryRowContext(ctx, query, outputPath, EventDownloadEnd)
	err = row.Scan(&identifier, &filetype)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", false, nil // Not found
		}
		return "", "", false, fmt.Errorf("failed query identifier for path '%s': %w", outputPath, err)
	}
	return identifier, filetype, true, nil
}

// GetPathsToProcess identifies local zip file paths (filetype='zip') that have been downloaded
// but not yet successfully processed.
func GetPathsToProcess(ctx context.Context, db *sql.DB) ([]string, error) {
	paths := []string{}

	// 1. Get all distinct, non-null output paths associated with a 'zip' type download_end event
	queryPaths := `
		SELECT DISTINCT output_path
		FROM nem_event_log
		WHERE filetype = ? AND event = ? AND output_path IS NOT NULL AND output_path != '';
	`
	rowsPaths, err := db.QueryContext(ctx, queryPaths, FileTypeZip, EventDownloadEnd) // Only look for 'zip' type downloads
	if err != nil {
		return nil, fmt.Errorf("failed query downloaded zip paths: %w", err)
	}
	defer rowsPaths.Close()

	potentialPaths := []string{}
	for rowsPaths.Next() {
		var path sql.NullString
		if err := rowsPaths.Scan(&path); err != nil {
			return nil, fmt.Errorf("failed scanning downloaded zip path: %w", err)
		}
		if path.Valid && path.String != "" {
			potentialPaths = append(potentialPaths, path.String)
		}
	}
	if err = rowsPaths.Err(); err != nil {
		return nil, fmt.Errorf("error iterating downloaded zip paths: %w", err)
	}
	rowsPaths.Close() // Close early

	if len(potentialPaths) == 0 {
		return paths, nil
	} // No downloaded zip paths found

	// 2. For each potential path, find its identifier and check if it has been processed
	var checkErrors error
	// Create map of processed identifiers (URLs or inner zip names) for efficiency
	processedIdentifiers := make(map[string]bool)
	queryProcessed := `SELECT DISTINCT filename FROM nem_event_log WHERE filetype = ? AND event = ?;`
	rowsProcessed, err := db.QueryContext(ctx, queryProcessed, FileTypeZip, EventProcessEnd) // Check for 'zip' process end
	if err != nil {
		return nil, fmt.Errorf("failed query processed zip identifiers: %w", err)
	}
	for rowsProcessed.Next() {
		var id string
		if err := rowsProcessed.Scan(&id); err != nil {
			rowsProcessed.Close()
			return nil, fmt.Errorf("failed scanning processed zip identifier: %w", err)
		}
		processedIdentifiers[id] = true
	}
	rowsProcessed.Close()
	if err = rowsProcessed.Err(); err != nil {
		return nil, fmt.Errorf("error iterating processed zip identifiers: %w", err)
	}

	// Now check each path
	for _, p := range potentialPaths {
		identifier, fType, found, err := GetIdentifierForOutputPath(ctx, db, p)
		if err != nil {
			slog.Warn("Error finding identifier for path, skipping.", "path", p, "error", err)
			checkErrors = errors.Join(checkErrors, err)
			continue
		}
		if !found {
			slog.Warn("Could not find identifier for path in DB, skipping.", "path", p)
			continue
		}
		// Ensure we only process things logged as 'zip' type downloads
		if fType != FileTypeZip {
			slog.Debug("Identifier found for path, but filetype is not 'zip', skipping processing.", "path", p, "identifier", identifier, "filetype", fType)
			continue
		}

		if _, isProcessed := processedIdentifiers[identifier]; !isProcessed {
			// Check if file actually exists before adding
			if _, statErr := os.Stat(p); statErr == nil {
				paths = append(paths, p)
			} else {
				slog.Warn("Downloaded path needs processing but file missing.", "path", p, "identifier", identifier, "error", statErr)
			}
		}
	}

	// Return paths and any non-fatal errors encountered during checks
	return paths, checkErrors
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
	fmt.Printf("%-50s | %-15s | %-15s | %-25s | %-10s | %s\n", "Identifier", "Type", "Event", "Timestamp (UTC)", "DurationMS", "Message/Details") // Adjusted header
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

		// Truncate filename if too long for display
		displayFilename := filename
		if len(displayFilename) > 50 {
			displayFilename = "..." + displayFilename[len(displayFilename)-47:]
		}

		fmt.Printf("%-50s | %-15s | %-15s | %-25s | %-10s | %s\n",
			displayFilename, filetype, event, timestamp.Format(time.RFC3339), durationStr, details)
		count++
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating event log rows: %w", err)
	}
	fmt.Printf("Displayed %d records.\n", count)
	return nil
}
