package orchestrator

import (
	"context"
	"database/sql"
	"database/sql/driver" // Import driver package for driver.Value
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync" // Needed for appender map mutex
	"time"

	"github.com/marcboeker/go-duckdb" // Import the DuckDB driver package
)

// WriteOperation defines the data sent to the writer goroutine.
type WriteOperation struct {
	TableName string
	Data      []any // Parsed data row matching the table schema
	// Schema info is no longer needed here, managed by schema creator/cache
}

// SchemaCreateRequest defines the data sent to the schema creator goroutine.
type SchemaCreateRequest struct {
	TableName  string
	Schema     *SchemaInfo  // Contains column names and types
	ResultChan chan<- error // Channel to send back error or nil on completion
}

// duckDBWriterState holds the state for the writer goroutine.
type duckDBWriterState struct {
	conn      driver.Conn                 // Single connection for writing
	appenders map[string]*duckdb.Appender // Map table name to appender
	mu        sync.Mutex                  // Mutex to protect the appenders map
	logger    *slog.Logger
}

// runDuckDBWriter is the single goroutine responsible for all DuckDB appends.
// It listens on the writeChan for data rows to append.
func runDuckDBWriter(ctx context.Context, dbPath string, logger *slog.Logger, writeChan <-chan WriteOperation, writerWg *sync.WaitGroup) {
	defer writerWg.Done()
	logger.Info("DuckDB writer goroutine started.")

	state := &duckDBWriterState{
		appenders: make(map[string]*duckdb.Appender),
		logger:    logger,
	}

	// Establish the single writer connection
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		logger.Error("Writer: Failed to create DuckDB connector, exiting.", "error", err)
		return // Cannot proceed without a connection
	}
	state.conn, err = connector.Connect(context.Background()) // Use background context for connection lifecycle
	if err != nil {
		logger.Error("Writer: Failed to connect via DuckDB connector, exiting.", "error", err)
		return // Cannot proceed without a connection
	}
	defer func() {
		logger.Info("Writer: Closing writer DB connection.")
		state.flushAndCloseAllAppenders() // Ensure cleanup before closing connection
		if cerr := state.conn.Close(); cerr != nil {
			logger.Error("Writer: Error closing writer DB connection", "error", cerr)
		}
	}()
	logger.Info("Writer: DB connection acquired.")

	// Process write operations from the channel
	for {
		select {
		case op, ok := <-writeChan:
			if !ok {
				// Channel closed, means processing is done
				logger.Info("Writer: Write channel closed. Flushing remaining data.")
				state.flushAndCloseAllAppenders()
				logger.Info("Writer: Goroutine finished.")
				return
			}
			// Handle the write operation
			state.handleWriteOperation(ctx, op) // Pass context for potential logging

		case <-ctx.Done():
			// Context cancelled, initiate shutdown
			logger.Warn("Writer: Context cancelled. Flushing and exiting.")
			state.flushAndCloseAllAppenders() // Attempt cleanup
			logger.Warn("Writer: Goroutine finished due to cancellation.")
			return
		}
	}
}

// handleWriteOperation processes a single write request.
func (s *duckDBWriterState) handleWriteOperation(ctx context.Context, op WriteOperation) {
	s.mu.Lock() // Lock while accessing/modifying appender map
	appender, exists := s.appenders[op.TableName]
	if !exists {
		// Appender doesn't exist, create it
		var appenderErr error
		// Schema name is empty for default schema
		appender, appenderErr = duckdb.NewAppenderFromConn(s.conn, "", op.TableName)
		if appenderErr != nil {
			s.mu.Unlock() // Unlock before logging/returning
			s.logger.Error("Writer: Failed to create appender", "table", op.TableName, "error", appenderErr)
			// This usually means the table doesn't exist yet or schema mismatch.
			// The schema creator should handle existence. If it persists, it might be a timing issue or schema mismatch.
			return
		}
		s.appenders[op.TableName] = appender
		s.logger.Debug("Writer: Created new appender.", "table", op.TableName)
	}
	s.mu.Unlock() // Unlock after accessing/modifying map

	// Append the row data
	if appender != nil {
		// --- Convert []any to []driver.Value ---
		driverValues := make([]driver.Value, len(op.Data))
		for i, v := range op.Data {
			// Directly assign known compatible types. Handle potential nil.
			// The sql driver handles basic type conversions (int64, float64, string, bool, time.Time, []byte, nil).
			// If `v` is already a `driver.Value`, this assignment works directly.
			// If `v` is one of the base types, the driver handles it.
			driverValues[i] = v // Assign directly, relying on driver compatibility
		}
		// --- End Conversion ---

		// Pass the converted slice
		appendErr := appender.AppendRow(driverValues...)
		if appendErr != nil {
			s.logger.Error("Writer: Failed to append row", "table", op.TableName, "error", appendErr)
			// Invalidate appender on error? Closing it might be necessary.
			s.closeAndRemoveAppender(op.TableName, appender) // Attempt to close and remove the faulty appender
		}
	} else {
		// This case should ideally not happen if creation logic is sound
		s.logger.Warn("Writer: Skipping append because appender is nil (creation likely failed)", "table", op.TableName)
	}
}

// closeAndRemoveAppender closes a specific appender and removes it from the map.
// Assumes the mutex is NOT held by the caller.
func (s *duckDBWriterState) closeAndRemoveAppender(tableName string, appender *duckdb.Appender) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check if the appender in the map is the same one we intend to close
	if currentAppender, ok := s.appenders[tableName]; ok && currentAppender == appender {
		s.logger.Warn("Writer: Closing and removing appender due to error.", "table", tableName)
		// Flush is important here before closing on error
		flushErr := appender.Flush()
		if flushErr != nil {
			s.logger.Error("Writer: Error flushing appender before closing on error", "table", tableName, "error", flushErr)
		}
		closeErr := appender.Close()
		if closeErr != nil {
			s.logger.Error("Writer: Error closing appender after error", "table", tableName, "error", closeErr)
		}
		delete(s.appenders, tableName)
	} else {
		s.logger.Debug("Writer: Appender already removed or replaced before error handling.", "table", tableName)
	}
}

// flushAndCloseAllAppenders closes all active appenders. Called during shutdown.
// Assumes the mutex is NOT held by the caller.
func (s *duckDBWriterState) flushAndCloseAllAppenders() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Debug("Writer: Flushing and closing all appenders.", "count", len(s.appenders))
	for tableName, appender := range s.appenders {
		if appender != nil {
			l := s.logger.With(slog.String("table", tableName))
			// Flush is often implicitly called by Close, but explicit flush can be safer.
			if err := appender.Flush(); err != nil {
				l.Error("Writer: Error flushing appender on shutdown", "error", err)
			}
			if err := appender.Close(); err != nil {
				l.Error("Writer: Error closing appender on shutdown", "error", err)
			}
		}
	}
	// Clear the map after closing all
	s.appenders = make(map[string]*duckdb.Appender)
}

// runSchemaCreator is the single goroutine responsible for executing CREATE TABLE IF NOT EXISTS.
// It ensures that schema creation attempts are serialized.
func runSchemaCreator(ctx context.Context, dbPath string, logger *slog.Logger, schemaChan <-chan SchemaCreateRequest, creatorWg *sync.WaitGroup) {
	defer creatorWg.Done()
	logger.Info("Schema creator goroutine started.")

	// Use a separate connection for schema modifications
	// It's generally safer to separate DDL (schema changes) from DML (data changes/appends) connections.
	dbConn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		logger.Error("SchemaCreator: Failed to open DuckDB connection, exiting.", "error", err)
		// Need a way to signal failure back to the main process if this happens early.
		// For now, just log and exit the goroutine.
		// Consider sending error on a dedicated startup error channel if needed.
		return
	}
	defer dbConn.Close()

	// Ping to ensure connection is valid
	if err := dbConn.PingContext(ctx); err != nil {
		logger.Error("SchemaCreator: Failed to ping DuckDB connection, exiting.", "error", err)
		return
	}
	logger.Info("SchemaCreator: DB connection acquired.")

	for {
		select {
		case req, ok := <-schemaChan:
			if !ok {
				logger.Info("SchemaCreator: Schema channel closed. Goroutine finished.")
				return // Channel closed
			}

			l := logger.With(slog.String("table", req.TableName))
			l.Info("SchemaCreator: Received request to ensure table exists.")

			// Validate SchemaInfo before proceeding
			if req.Schema == nil || len(req.Schema.ColumnNames) == 0 || len(req.Schema.ColumnNames) != len(req.Schema.DuckdbTypes) {
				l.Error("SchemaCreator: Received invalid schema information.", "schema", req.Schema)
				select {
				case req.ResultChan <- errors.New("invalid schema info received"):
				default:
					l.Warn("SchemaCreator: Failed to send invalid schema error back.")
				}
				close(req.ResultChan)
				continue // Skip processing this invalid request
			}

			// Construct CREATE TABLE statement
			colDefs := make([]string, len(req.Schema.ColumnNames))
			for i, colName := range req.Schema.ColumnNames {
				// Ensure column names are quoted if they might contain special characters or keywords
				// Basic quoting, might need more robust quoting logic for complex names
				safeColName := strings.ReplaceAll(colName, `"`, `""`) // Escape double quotes
				colDefs[i] = fmt.Sprintf(`"%s" %s`, safeColName, req.Schema.DuckdbTypes[i])
			}
			// Ensure table name is also quoted
			safeTableName := strings.ReplaceAll(req.TableName, `"`, `""`)
			createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s);`, safeTableName, strings.Join(colDefs, ", "))

			l.Debug("SchemaCreator: Executing CREATE TABLE IF NOT EXISTS.") // Don't log full SQL by default for security/verbosity
			_, execErr := dbConn.ExecContext(ctx, createTableSQL)

			if execErr != nil {
				l.Error("SchemaCreator: Failed to execute CREATE TABLE IF NOT EXISTS", "error", execErr, "sql_preview", fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (...);", safeTableName))
			} else {
				l.Info("SchemaCreator: Ensured table exists (or already existed).")
			}

			// Send result back to requester
			// Use non-blocking send in case receiver is gone, although it shouldn't be.
			select {
			case req.ResultChan <- execErr: // Send error (or nil) back
			case <-ctx.Done():
				l.Warn("SchemaCreator: Context cancelled while trying to send result back.")
			case <-time.After(5 * time.Second): // Timeout for sending result back
				l.Warn("SchemaCreator: Timeout sending result back, receiver channel likely blocked or closed.")
			}
			// Close the result channel regardless of whether send succeeded.
			// The receiver should handle reading from a potentially closed channel.
			close(req.ResultChan)

		case <-ctx.Done():
			logger.Warn("SchemaCreator: Context cancelled. Goroutine finished.")
			return // Context cancelled
		}
	}
}
