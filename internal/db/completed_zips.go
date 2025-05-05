package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	// "time" // Not needed for this specific query
)

// GetCompletedZipIdentifiers queries the database for zip identifiers (URLs or filenames)
// that have successfully completed processing.
// It looks for EventProcessEnd associated with FileTypeZip in the nem_event_log table.
// Returns a map where keys are completed identifiers (for fast lookups) and a potential error.
func GetCompletedZipIdentifiers(ctx context.Context, dbConnPool *sql.DB, logger *slog.Logger) (map[string]bool, error) {
	logger.Debug("Querying database for completed zip identifiers (type='zip', event='process_end')...")
	completedIdentifiers := make(map[string]bool)

	// Query for 'zip' type files that have a 'process_end' event.
	// The 'filename' column holds the identifier (URL or inner zip name).
	query := `
		SELECT DISTINCT filename
		FROM nem_event_log
		WHERE filetype = ? AND event = ?;
	`
	rows, err := dbConnPool.QueryContext(ctx, query, FileTypeZip, EventProcessEnd)
	if err != nil {
		logger.Error("Failed to query for completed zip identifiers", "error", err, "query", query, "filetype", FileTypeZip, "event", EventProcessEnd)
		return nil, fmt.Errorf("query completed zip identifiers: %w", err)
	}
	defer rows.Close()

	count := 0
	var scanErrors error // Accumulate scan errors
	for rows.Next() {
		var identifier string
		if err := rows.Scan(&identifier); err != nil {
			logger.Error("Failed to scan completed zip identifier", "error", err)
			scanErrors = errors.Join(scanErrors, fmt.Errorf("scan completed zip identifier: %w", err))
			continue
		}
		if identifier != "" {
			completedIdentifiers[identifier] = true
			count++
		}
	}

	// Check for errors during row iteration
	if err := rows.Err(); err != nil {
		logger.Error("Error iterating over completed zip identifier query results", "error", err)
		scanErrors = errors.Join(scanErrors, fmt.Errorf("iterate completed zip identifiers: %w", err))
		return completedIdentifiers, scanErrors // Return potentially partial map and combined errors
	}

	logger.Info("Found completed zip identifiers in DB.", slog.Int("count", count))
	// Return map and any accumulated scan/iteration errors (which might be nil)
	return completedIdentifiers, scanErrors
}
