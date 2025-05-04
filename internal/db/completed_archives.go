package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	// Needed for temp table name generation
)

// GetCompletedArchiveURLs queries the database for archive URLs that have successfully completed processing.
// It looks for EventProcessEnd associated with FileTypeOuterArchive in the nem_event_log table.
// Returns a map where keys are completed URLs (for fast lookups) and a potential error.
func GetCompletedArchiveURLs(ctx context.Context, dbConnPool *sql.DB, logger *slog.Logger) (map[string]bool, error) {
	logger.Debug("Querying database for completed archive URLs...")
	completedURLs := make(map[string]bool)

	// Corrected table name from file_log to nem_event_log
	query := `
		SELECT DISTINCT filename
		FROM nem_event_log
		WHERE filetype = ? AND event = ?;
	`
	rows, err := dbConnPool.QueryContext(ctx, query, FileTypeOuterArchive, EventProcessEnd)
	if err != nil {
		// Log the specific error for easier debugging
		logger.Error("Failed to query for completed archives", "error", err, "query", query, "filetype", FileTypeOuterArchive, "event", EventProcessEnd)
		// Check if the error is specifically "table does not exist" - might indicate schema not initialized properly
		// For now, return the raw error.
		return nil, fmt.Errorf("query completed archives: %w", err)
	}
	defer rows.Close()

	count := 0
	var scanErrors error // Accumulate scan errors
	for rows.Next() {
		var identifier string
		if err := rows.Scan(&identifier); err != nil {
			logger.Error("Failed to scan completed archive identifier", "error", err)
			// Collect error but continue scanning other rows
			scanErrors = errors.Join(scanErrors, fmt.Errorf("scan completed archive identifier: %w", err))
			continue
		}
		if identifier != "" {
			completedURLs[identifier] = true
			count++
		}
	}

	// Check for errors during row iteration
	if err := rows.Err(); err != nil {
		logger.Error("Error iterating over completed archive query results", "error", err)
		// Combine iteration error with any previous scan errors
		scanErrors = errors.Join(scanErrors, fmt.Errorf("iterate completed archives: %w", err))
		return completedURLs, scanErrors // Return potentially partial map and combined errors
	}

	logger.Info("Found completed archive URLs in DB.", slog.Int("count", count))
	// Return map and any accumulated scan/iteration errors (which might be nil)
	return completedURLs, scanErrors
}

// --- Note: ---
// The GetCompletionStatusBatch function also exists in the original db package provided.
// It uses a temporary table approach which might be more efficient for very large lists of filenames.
// However, GetCompletedArchiveURLs directly queries the main table, which is simpler for getting *all* completed archives initially.
// Consider which approach is best based on expected usage patterns.
// If GetCompletionStatusBatch is preferred, ensure its table name is also nem_event_log.
