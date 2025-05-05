package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync" // Import sync

	"github.com/brensch/nemparquet/internal/config" // Need config for SchemaConfig type
)

// createAllPredefinedSchemas iterates through the schemas defined in the config
// and executes a "CREATE TABLE IF NOT EXISTS" for each one.
// This should be called once during application startup.
func CreateAllPredefinedSchemas(ctx context.Context, dbPool *sql.DB, schemas config.SchemaConfig, logger *slog.Logger) error {
	if len(schemas) == 0 {
		logger.Info("No predefined schemas loaded, skipping upfront schema creation.")
		return nil
	}
	logger.Info("Starting upfront creation of predefined schemas...", slog.Int("schema_count", len(schemas)))

	var wg sync.WaitGroup
	errChan := make(chan error, len(schemas)) // Buffered channel for errors

	// Limit concurrency for schema creation? Usually not necessary unless hundreds/thousands of tables.
	// sem := semaphore.NewWeighted(10) // Example limit

	for baseIdentifier, schemaDef := range schemas {
		// Check context before launching goroutine
		select {
		case <-ctx.Done():
			logger.Warn("Context cancelled before creating all schemas.", "error", ctx.Err())
			// Drain potential errors already sent before returning
			close(errChan)
			var combinedErr error = ctx.Err()
			for err := range errChan {
				combinedErr = errors.Join(combinedErr, err)
			}
			return combinedErr
		default:
		}

		wg.Add(1)
		go func(id string, sDef config.TableSchema) {
			defer wg.Done()
			l := logger.With(slog.String("base_identifier", id))

			// Generate table names based on *all* columns defined in the schema file
			// (This assumes the JSON represents the desired final state)
			columnNames := make([]string, len(sDef.Columns))
			colDefs := make([]string, len(sDef.Columns))
			for i, col := range sDef.Columns {
				columnNames[i] = col.Name
				safeColName := strings.ReplaceAll(col.Name, `"`, `""`)
				colDefs[i] = fmt.Sprintf(`"%s" %s`, safeColName, col.Type)
			}

			// Split baseIdentifier back into parts (assuming GROUP__TABLE__VERSION format)
			parts := strings.SplitN(id, "__", 3)
			if len(parts) != 3 {
				l.Error("Skipping schema creation: Invalid base identifier format.", "identifier", id)
				errChan <- fmt.Errorf("invalid base identifier format for schema creation: %s", id)
				return
			}
			group, table, version := parts[0], parts[1], parts[2]

			// Generate the specific table name including the hash of *predefined* columns
			tableNameWithHash := generateTableName(group, table, version, columnNames)
			safeTableName := strings.ReplaceAll(tableNameWithHash, `"`, `""`)

			createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s);`, safeTableName, strings.Join(colDefs, ", "))

			l.Debug("Ensuring table exists.", "table", tableNameWithHash) // Use hashed name in log
			_, execErr := dbPool.ExecContext(ctx, createTableSQL)
			if execErr != nil {
				l.Error("Failed to execute CREATE TABLE IF NOT EXISTS", "table", tableNameWithHash, "error", execErr)
				// Send error to channel
				select {
				case errChan <- fmt.Errorf("create table %s: %w", tableNameWithHash, execErr):
				case <-ctx.Done(): // Don't block if context is cancelled
				}
			} else {
				l.Debug("Ensured table exists.", "table", tableNameWithHash)
			}
		}(baseIdentifier, schemaDef)
	}

	wg.Wait()
	close(errChan) // Close channel after all goroutines are done

	// Collect errors
	var finalErr error
	for err := range errChan {
		finalErr = errors.Join(finalErr, err)
	}

	if finalErr != nil {
		logger.Error("Errors occurred during upfront schema creation.", "error", finalErr)
		return finalErr
	}

	logger.Info("Upfront schema creation process completed.")
	return nil
}
