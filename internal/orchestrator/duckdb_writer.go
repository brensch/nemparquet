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
	"time" // Import time

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
	dbPool    *sql.DB                     // Shared connection pool
	conn      driver.Conn                 // Acquired connection for appenders
	appenders map[string]*duckdb.Appender // Map table name to appender
	mu        sync.Mutex                  // Mutex to protect the appenders map
	logger    *slog.Logger
}

// runDuckDBWriter is the single goroutine responsible for all DuckDB appends.
// It acquires a single connection from the shared pool for appender operations.
func runDuckDBWriter(ctx context.Context, dbPool *sql.DB, logger *slog.Logger, writeChan <-chan WriteOperation, writerWg *sync.WaitGroup) {
	defer writerWg.Done()
	logger.Info("DuckDB writer goroutine started.")

	state := &duckDBWriterState{
		dbPool:    dbPool, // Store the pool
		appenders: make(map[string]*duckdb.Appender),
		logger:    logger,
	}

	// Acquire a single connection from the pool for the lifetime of this writer
	// Use background context for acquiring connection unless cancellation needed here
	rawConn, err := dbPool.Conn(context.Background())
	if err != nil {
		logger.Error("Writer: Failed to acquire connection from pool, exiting.", "error", err)
		return
	}
	// Get the underlying driver.Conn
	// This uses the Execer interface check, which might need adjustment based on the driver implementation details.
	// A more direct (but potentially less portable) way might exist if the driver exposes it.
	err = rawConn.Raw(func(driverConn interface{}) error {
		var ok bool
		state.conn, ok = driverConn.(driver.Conn)
		if !ok {
			return errors.New("connection does not implement driver.Conn")
		}
		// Check if it also implements driver.ConnBeginTx for potential future use
		// _, ok = driverConn.(driver.ConnBeginTx)
		// if !ok {
		// 	logger.Warn("Writer: Acquired connection does not implement driver.ConnBeginTx")
		// }
		return nil
	})
	if err != nil {
		rawConn.Close() // Close the *sql.Conn wrapper
		logger.Error("Writer: Failed to get underlying driver.Conn, exiting.", "error", err)
		return
	}
	// Defer closing the sql.Conn wrapper AND the underlying driver.Conn
	defer func() {
		logger.Info("Writer: Closing acquired DB connection.")
		state.flushAndCloseAllAppenders() // Ensure cleanup before closing connection
		// Closing state.conn might be handled by rawConn.Close(), depending on driver.
		// Closing rawConn is the standard way.
		if cerr := rawConn.Close(); cerr != nil {
			logger.Error("Writer: Error closing acquired *sql.Conn", "error", cerr)
		}
		// If driver requires explicit close of driver.Conn too:
		// if state.conn != nil {
		// 	if cerr := state.conn.Close(); cerr != nil {
		// 		logger.Error("Writer: Error closing underlying driver.Conn", "error", cerr)
		// 	}
		// }
	}()
	logger.Info("Writer: Acquired dedicated connection from pool.")

	// Process write operations from the channel
	for {
		select {
		case op, ok := <-writeChan:
			if !ok {
				logger.Info("Writer: Write channel closed. Flushing remaining data.")
				state.flushAndCloseAllAppenders()
				logger.Info("Writer: Goroutine finished.")
				return
			}
			state.handleWriteOperation(ctx, op)

		case <-ctx.Done():
			logger.Warn("Writer: Context cancelled. Flushing and exiting.")
			state.flushAndCloseAllAppenders()
			logger.Warn("Writer: Goroutine finished due to cancellation.")
			return
		}
	}
}

// handleWriteOperation processes a single write request.
func (s *duckDBWriterState) handleWriteOperation(ctx context.Context, op WriteOperation) {
	// Check if the dedicated connection is still valid
	if s.conn == nil {
		s.logger.Error("Writer: Skipping write, dedicated connection is nil.", "table", op.TableName)
		return
	}

	s.mu.Lock() // Lock while accessing/modifying appender map
	appender, exists := s.appenders[op.TableName]
	if !exists {
		var appenderErr error
		// Use the dedicated driver.Conn acquired earlier
		appender, appenderErr = duckdb.NewAppenderFromConn(s.conn, "", op.TableName)
		if appenderErr != nil {
			s.mu.Unlock()
			errMsgLower := strings.ToLower(appenderErr.Error())
			if strings.Contains(errMsgLower, "could not be found") || strings.Contains(errMsgLower, "does not exist") {
				s.logger.Error("Writer: Failed to create appender because table does not exist (schema creation likely failed or connection visibility issue).", "table", op.TableName, "original_error", appenderErr)
			} else {
				s.logger.Error("Writer: Failed to create appender for unknown reason.", "table", op.TableName, "error", appenderErr)
			}
			return
		}
		s.appenders[op.TableName] = appender
		s.logger.Debug("Writer: Created new appender.", "table", op.TableName)
	}
	s.mu.Unlock()

	if appender != nil {
		driverValues := make([]driver.Value, len(op.Data))
		for i, v := range op.Data {
			driverValues[i] = v
		}

		appendErr := appender.AppendRow(driverValues...)
		if appendErr != nil {
			s.logger.Error("Writer: Failed to append row", "table", op.TableName, "error", appendErr)
			s.closeAndRemoveAppender(op.TableName, appender)
		}
	} else {
		s.logger.Warn("Writer: Skipping append because appender is nil (creation likely failed)", "table", op.TableName)
	}
}

// closeAndRemoveAppender (no changes needed from previous version)
func (s *duckDBWriterState) closeAndRemoveAppender(tableName string, appender *duckdb.Appender) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if currentAppender, ok := s.appenders[tableName]; ok && currentAppender == appender {
		s.logger.Warn("Writer: Closing and removing appender due to error.", "table", tableName)
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

// flushAndCloseAllAppenders (no changes needed from previous version)
func (s *duckDBWriterState) flushAndCloseAllAppenders() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Debug("Writer: Flushing and closing all appenders.", "count", len(s.appenders))
	for tableName, appender := range s.appenders {
		if appender != nil {
			l := s.logger.With(slog.String("table", tableName))
			if err := appender.Flush(); err != nil {
				l.Error("Writer: Error flushing appender on shutdown", "error", err)
			}
			if err := appender.Close(); err != nil {
				l.Error("Writer: Error closing appender on shutdown", "error", err)
			}
		}
	}
	s.appenders = make(map[string]*duckdb.Appender)
}

// runSchemaCreator (Modified): Accepts *sql.DB pool instead of dbPath.
func runSchemaCreator(ctx context.Context, dbPool *sql.DB, logger *slog.Logger, schemaChan <-chan SchemaCreateRequest, creatorWg *sync.WaitGroup) {
	defer creatorWg.Done()
	logger.Info("Schema creator goroutine started (using shared pool).")

	// No need to open/ping/close connection here, database/sql manages the pool.

	for {
		select {
		case req, ok := <-schemaChan:
			if !ok {
				logger.Info("SchemaCreator: Schema channel closed. Goroutine finished.")
				return // Channel closed
			}

			l := logger.With(slog.String("table", req.TableName))
			l.Info("SchemaCreator: Received request to ensure table exists.")

			if req.Schema == nil || len(req.Schema.ColumnNames) == 0 || len(req.Schema.ColumnNames) != len(req.Schema.DuckdbTypes) {
				l.Error("SchemaCreator: Received invalid schema information.", "schema", req.Schema)
				select {
				case req.ResultChan <- errors.New("invalid schema info received"):
				default:
					l.Warn("SchemaCreator: Failed to send invalid schema error back.")
				}
				close(req.ResultChan)
				continue
			}

			colDefs := make([]string, len(req.Schema.ColumnNames))
			for i, colName := range req.Schema.ColumnNames {
				safeColName := strings.ReplaceAll(colName, `"`, `""`)
				colDefs[i] = fmt.Sprintf(`"%s" %s`, safeColName, req.Schema.DuckdbTypes[i])
			}
			safeTableName := strings.ReplaceAll(req.TableName, `"`, `""`)
			createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s);`, safeTableName, strings.Join(colDefs, ", "))

			l.Debug("SchemaCreator: Executing CREATE TABLE IF NOT EXISTS.")
			// Use the passed-in pool directly. database/sql handles connection acquire/release.
			_, execErr := dbPool.ExecContext(ctx, createTableSQL)

			if execErr != nil {
				l.Error("SchemaCreator: Failed to execute CREATE TABLE IF NOT EXISTS", "error", execErr, "sql_preview", fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (...);", safeTableName))
			} else {
				l.Info("SchemaCreator: Ensured table exists (or already existed).")
			}

			select {
			case req.ResultChan <- execErr:
			case <-ctx.Done():
				l.Warn("SchemaCreator: Context cancelled while trying to send result back.")
			case <-time.After(5 * time.Second):
				l.Warn("SchemaCreator: Timeout sending result back, receiver channel likely blocked or closed.")
			}
			close(req.ResultChan)

		case <-ctx.Done():
			logger.Warn("SchemaCreator: Context cancelled. Goroutine finished.")
			return // Context cancelled
		}
	}
}
