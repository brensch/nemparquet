package processor

import (
	"archive/zip"
	"context"
	"database/sql"        // Keep for checking results, passing pool
	"database/sql/driver" // Required for driver.Conn and driver.Value
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect" // For debug logging type
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db" // Keep for logging events
	"github.com/brensch/nemparquet/internal/util"

	"github.com/marcboeker/go-duckdb" // Import DuckDB driver connector etc.
)

// Regex for sanitizing SQL identifiers
var sanitizeRegex = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// sanitizeSQLIdentifier creates a safer SQL identifier.
func sanitizeSQLIdentifier(name string) string {
	sanitized := sanitizeRegex.ReplaceAllString(name, "_")
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "_" + sanitized
	}
	if sanitized == "" {
		return "_unnamed"
	}
	return strings.ToLower(sanitized)
}

// StartProcessorWorkers launches goroutines that process zip files.
// Each worker uses the main pool for checks but creates short-lived
// connections/appenders per CSV section.
func StartProcessorWorkers(
	ctx context.Context, cfg config.Config, dbConnPool *sql.DB, logger *slog.Logger, // Pass Pool
	numWorkers int, pathsChan <-chan string,
	wg *sync.WaitGroup, errorsMap *sync.Map,
	forceProcess bool,
) {
	logger.Info("Starting processor workers (DuckDB backend, connection per section)", slog.Int("count", numWorkers))
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			l := logger.With(slog.Int("worker_id", workerID))
			l.Debug("Processing worker started")

			// No persistent worker connection needed here.

			for zipFilePath := range pathsChan {
				jobCtx, cancelJob := context.WithCancel(ctx) // Context per job
				jobLogger := l.With(slog.String("zip_file_path", zipFilePath))

				// --- Perform DB checks using the main pool ---
				absZipFilePath, pathErr := filepath.Abs(zipFilePath)
				if pathErr != nil {
					jobLogger.Error("Failed get absolute path, skipping.", "error", pathErr)
					errorsMap.Store(zipFilePath, pathErr)
					cancelJob()
					continue
				}
				identifier, fileType, foundID, dbErr := db.GetIdentifierForOutputPath(jobCtx, dbConnPool, absZipFilePath)
				if dbErr != nil {
					jobLogger.Error("DB error finding identifier, skipping.", "error", dbErr)
					errorsMap.Store(zipFilePath, dbErr)
					cancelJob()
					continue
				}
				if !foundID {
					jobLogger.Warn("Could not find identifier in DB, skipping.", slog.String("abs_path", absZipFilePath))
					cancelJob()
					continue
				}
				jobLogger = jobLogger.With(slog.String("identifier", identifier), slog.String("file_type", fileType))

				// --- Check if already processed ---
				processCompleted := false
				if !forceProcess {
					var checkErr error
					processCompleted, checkErr = db.HasEventOccurred(jobCtx, dbConnPool, identifier, fileType, db.EventProcessEnd)
					if checkErr != nil {
						jobLogger.Warn("Failed check DB state, proceeding.", "error", checkErr)
						db.LogFileEvent(ctx, dbConnPool, identifier, fileType, db.EventError, "", "", fmt.Sprintf("db check process failed: %v", checkErr), "", nil)
						processCompleted = false
					}
				} else {
					jobLogger.Info("Force process enabled, skipping DB completion check.")
				}
				if processCompleted {
					jobLogger.Info("Skipping processing, already completed.", slog.String("identifier", identifier))
					db.LogFileEvent(ctx, dbConnPool, identifier, fileType, db.EventSkipProcess, "", "", "Already processed", "", nil)
					cancelJob()
					continue
				}

				// --- Proceed with Processing ---
				jobLogger.Info("Starting processing (writing to DuckDB per section).")
				startTime := time.Now()
				db.LogFileEvent(ctx, dbConnPool, identifier, fileType, db.EventProcessStart, "", "", "", "", nil) // Log start using pool

				// Pass the main DB pool (for logging) and config to the processor function
				// The processor will handle connections internally per section.
				processErr := processSingleZipArchive(jobCtx, cfg, jobLogger, dbConnPool, zipFilePath, identifier, fileType)
				duration := time.Since(startTime)

				if processErr != nil {
					if errors.Is(processErr, context.Canceled) || errors.Is(processErr, context.DeadlineExceeded) {
						jobLogger.Warn("Processing cancelled.", "cause", processErr)
					} else {
						jobLogger.Error("Failed to process zip archive", "error", processErr, slog.Duration("duration", duration.Round(time.Millisecond)))
						db.LogFileEvent(ctx, dbConnPool, identifier, fileType, db.EventError, "", "", processErr.Error(), "", &duration)
						errorsMap.Store(zipFilePath, processErr)
					}
				} else {
					jobLogger.Info("Processing successful (data ingested to DuckDB).", slog.Duration("duration", duration.Round(time.Millisecond)))
					db.LogFileEvent(ctx, dbConnPool, identifier, fileType, db.EventProcessEnd, "", "", "Data ingested to DuckDB", "", &duration)
				}
				cancelJob() // Cancel job context
			} // End job loop
			l.Debug("Processing worker finished")
		}(i)
	}
}

// processSingleZipArchive processes one zip file. It passes the pool down.
func processSingleZipArchive(
	ctx context.Context, cfg config.Config, logger *slog.Logger,
	dbPool *sql.DB, // <<< Pass pool
	zipFilePath string, identifier string, fileType string,
) error {
	l := logger
	l.Debug("Opening zip archive from disk.")
	zr, err := zip.OpenReader(zipFilePath)
	if err != nil {
		l.Error("Failed to open zip archive", "error", err)
		return fmt.Errorf("zip.OpenReader %s: %w", zipFilePath, err)
	}
	defer zr.Close()

	var processingErrors error
	csvFoundCount := 0
	zipBaseName := strings.TrimSuffix(filepath.Base(zipFilePath), filepath.Ext(zipFilePath))

	for _, f := range zr.File {
		select {
		case <-ctx.Done():
			l.Warn("Processing cancelled within zip.", "cause", ctx.Err())
			return errors.Join(processingErrors, ctx.Err())
		default:
		}
		if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
			continue
		}
		csvFoundCount++
		csvBaseName := filepath.Base(f.Name)
		csvLogger := l.With(slog.String("internal_csv", csvBaseName))
		csvLogger.Debug("Found CSV inside archive, starting stream processing.")

		rc, err := f.Open()
		if err != nil {
			csvLogger.Error("Failed to open internal CSV file.", "error", err)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("open internal %s: %w", csvBaseName, err))
			continue
		}
		defer rc.Close()

		streamCtx, cancelStream := context.WithCancel(ctx)
		// Pass the pool down to the stream processor
		streamErr := processCSVStream(streamCtx, cfg, csvLogger, dbPool, rc, zipBaseName) // <<< Pass pool
		cancelStream()

		if streamErr != nil {
			if errors.Is(streamErr, context.Canceled) || errors.Is(streamErr, context.DeadlineExceeded) {
				if errors.Is(streamErr, ctx.Err()) {
					csvLogger.Warn("CSV stream processing cancelled by parent job.", "cause", streamErr)
				} else {
					csvLogger.Warn("CSV stream processing cancelled.", "cause", streamErr)
				}
				processingErrors = errors.Join(processingErrors, streamErr)
				return processingErrors // Stop processing zip
			}
			csvLogger.Error("Failed to process internal CSV stream.", "error", streamErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("process stream %s: %w", csvBaseName, streamErr))
		} else {
			csvLogger.Debug("Successfully processed CSV stream.")
		}
	} // End loop files in zip

	if csvFoundCount == 0 {
		l.Info("No CSV files found within this zip archive.")
	} else if processingErrors != nil {
		if errors.Is(processingErrors, context.Canceled) || errors.Is(processingErrors, context.DeadlineExceeded) {
			l.Warn("Finished processing zip with cancellation.", "error", processingErrors)
		} else {
			l.Warn("Finished processing zip, but encountered errors.", "error", processingErrors)
		}
	} else {
		l.Debug("Finished processing all CSVs found in zip archive.")
	}
	return processingErrors
}

// --- Section Processing Logic ---

// csvSection holds state for processing one I..D..D section into DuckDB
type csvSection struct {
	Cfg            config.Config
	Logger         *slog.Logger
	DBPool         *sql.DB // <<< Keep pool for logging/checks if needed later
	TableName      string
	Headers        []string
	DuckdbTypes    []string
	ColumnNames    []string
	IsDateColumn   []bool
	SchemaInferred bool
	RowsChecked    int
	// Section-specific connection and appender:
	sectionConn     driver.Conn      // <<< Connection for this section
	appender        *duckdb.Appender // <<< Appender for this section
	AppenderCreated bool
	headerMap       map[string]int
	columnIndexMap  map[int]string
}

// newCSVSection initializes a section processor. Accepts the main pool.
func newCSVSection(
	ctx context.Context, cfg config.Config, logger *slog.Logger,
	dbPool *sql.DB, // <<< Accept Pool
	comp, ver string, headers []string,
) (*csvSection, error) {
	tableName := sanitizeSQLIdentifier(fmt.Sprintf("%s_v%s", comp, ver))
	s := &csvSection{
		Cfg: cfg, Logger: logger.With(slog.String("table", tableName)), DBPool: dbPool, // Store Pool
		TableName: tableName, Headers: headers, DuckdbTypes: make([]string, len(headers)),
		ColumnNames: make([]string, len(headers)), IsDateColumn: make([]bool, len(headers)),
		headerMap: make(map[string]int, len(headers)), columnIndexMap: make(map[int]string, len(headers)),
		// sectionConn and appender initialized later in inferSchemaAndEnsureTable
	}
	for i, h := range headers { // Sanitize column names
		colName := sanitizeSQLIdentifier(h)
		baseName := colName
		suffix := 1
		_, exists := s.headerMap[colName]
		for exists {
			colName = fmt.Sprintf("%s_%d", baseName, suffix)
			_, exists = s.headerMap[colName]
			suffix++
		}
		s.ColumnNames[i] = colName
		s.headerMap[colName] = i
	}
	s.Logger.Debug("New CSV section initialized.")
	return s, nil
}

// inferSchemaAndEnsureTable infers schema, creates connection, creates table, and initializes appender for the section.
func (s *csvSection) inferSchemaAndEnsureTable(ctx context.Context, values []string) error {
	s.Logger.Debug("Inferring schema, creating section connection, ensuring table, creating appender.")

	// --- Schema Inference (Data Only) ---
	colDefs := make([]string, len(s.Headers))
	for i := range s.Headers {
		val := values[i]
		sanitizedColName := s.ColumnNames[i]
		duckdbType := "VARCHAR"
		isDate := false
		if val != "" {
			if util.IsNEMDateTime(val) {
				duckdbType = "BIGINT"
				isDate = true
			} else if _, pErr := strconv.ParseFloat(val, 64); pErr == nil {
				if _, intErr := strconv.ParseInt(val, 10, 64); intErr == nil {
					duckdbType = "BIGINT"
				} else {
					duckdbType = "DOUBLE"
				}
			} else if _, pErr := strconv.ParseInt(val, 10, 64); pErr == nil {
				duckdbType = "BIGINT"
			} else {
				lcValue := strings.ToLower(val)
				if lcValue == "true" || lcValue == "false" {
					duckdbType = "BOOLEAN"
				}
			}
		}
		s.DuckdbTypes[i] = duckdbType
		s.IsDateColumn[i] = isDate
		s.columnIndexMap[i] = duckdbType
		colDefs[i] = fmt.Sprintf(`"%s" %s`, sanitizedColName, duckdbType)
	}
	s.Logger.Debug("Inferred DuckDB schema types (Data Only)", slog.Any("types", s.DuckdbTypes))
	// --- End Schema Inference ---

	// --- Create Connection for this Section ---
	connector, err := duckdb.NewConnector(s.Cfg.DbPath, nil)
	if err != nil {
		s.Logger.Error("Failed to create DuckDB connector for section", "error", err)
		return fmt.Errorf("create connector section %s: %w", s.TableName, err)
	}
	s.sectionConn, err = connector.Connect(ctx) // Use passed context
	if err != nil {
		s.Logger.Error("Failed to connect via DuckDB connector for section", "error", err)
		return fmt.Errorf("connect section %s: %w", s.TableName, err)
	}
	s.Logger.Debug("Created dedicated connection for section.", slog.String("table", s.TableName))
	// --- End Create Connection ---

	// --- Create Table using the section's connection ---
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s);`, s.TableName, strings.Join(colDefs, ", "))
	s.Logger.Debug("Executing CREATE TABLE IF NOT EXISTS using section connection.", slog.String("sql", createTableSQL))
	stmt, prepErr := s.sectionConn.Prepare(createTableSQL)
	if prepErr != nil {
		s.Logger.Error("Failed to prepare CREATE TABLE statement", "error", prepErr)
		s.sectionConn.Close()
		return fmt.Errorf("prepare CREATE TABLE %s: %w", s.TableName, prepErr)
	} // Close conn on error
	_, execErr := stmt.Exec(nil)
	closeErr := stmt.Close()
	if execErr != nil {
		s.Logger.Error("Failed to execute CREATE TABLE IF NOT EXISTS", "error", execErr)
		s.sectionConn.Close()
		return fmt.Errorf("exec CREATE TABLE %s: %w", s.TableName, execErr)
	} // Close conn on error
	if closeErr != nil {
		s.Logger.Warn("Failed to close CREATE TABLE statement", "error", closeErr)
	}
	s.Logger.Info("Table ensured.", slog.String("table", s.TableName))
	// --- End Create Table ---

	// --- Initialize DuckDB Appender using the section's connection ---
	var appenderErr error
	s.appender, appenderErr = duckdb.NewAppenderFromConn(s.sectionConn, "", s.TableName)
	if appenderErr != nil {
		s.Logger.Error("Failed to create DuckDB appender from section connection", "error", appenderErr)
		s.sectionConn.Close()
		return fmt.Errorf("create appender section %s: %w", s.TableName, appenderErr)
	} // Close conn on error
	s.AppenderCreated = true
	s.Logger.Debug("DuckDB appender created using section connection.")
	s.SchemaInferred = true
	return nil
}

// appendRow prepares data and appends it using the section's DuckDB Appender.
func (s *csvSection) appendRow(record []string, lineNumber int64) error {
	if !s.SchemaInferred || !s.AppenderCreated || s.appender == nil {
		return errors.New("cannot append row: section schema/appender not initialized")
	}
	numExpectedFields := len(s.Headers)
	// Pad/truncate record
	if len(record) < numExpectedFields {
		paddedRecord := make([]string, numExpectedFields)
		copy(paddedRecord, record)
		for i := len(record); i < numExpectedFields; i++ {
			paddedRecord[i] = ""
		}
		record = paddedRecord
	} else if len(record) > numExpectedFields {
		record = record[:numExpectedFields]
	}

	rowArgs := make([]driver.Value, numExpectedFields)
	var conversionErrors error
	for j := 0; j < numExpectedFields; j++ {
		valueStr := record[j]
		targetType := s.DuckdbTypes[j]
		isDate := s.IsDateColumn[j]
		var finalValue driver.Value
		var convErr error
		if valueStr == "" {
			finalValue = nil
		} else if isDate {
			epochMS, dtErr := util.NEMDateTimeToEpochMS(valueStr)
			if dtErr != nil {
				convErr = fmt.Errorf("col '%s': %w", s.Headers[j], dtErr)
				finalValue = nil
			} else {
				finalValue = epochMS
			}
		} else {
			switch targetType {
			case "BIGINT":
				valInt, pErr := strconv.ParseInt(valueStr, 10, 64)
				if pErr != nil {
					convErr = pErr
					finalValue = nil
				} else {
					finalValue = valInt
				}
			case "DOUBLE":
				valFloat, pErr := strconv.ParseFloat(valueStr, 64)
				if pErr != nil {
					convErr = pErr
					finalValue = nil
				} else {
					finalValue = valFloat
				}
			case "BOOLEAN":
				lcValue := strings.ToLower(valueStr)
				if lcValue == "true" || lcValue == "t" || lcValue == "1" || lcValue == "y" || lcValue == "yes" {
					finalValue = true
					convErr = nil
				} else if lcValue == "false" || lcValue == "f" || lcValue == "0" || lcValue == "n" || lcValue == "no" {
					finalValue = false
					convErr = nil
				} else {
					convErr = fmt.Errorf("invalid boolean string: %q", valueStr)
					finalValue = nil
				}
			case "VARCHAR":
				finalValue = valueStr
				convErr = nil
			default:
				convErr = fmt.Errorf("col '%s': unhandled target type '%s'", s.Headers[j], targetType)
				finalValue = nil
			}
		}
		if convErr != nil {
			err := fmt.Errorf("conversion failed: col '%s' (target %s): val %q: %w", s.Headers[j], targetType, valueStr, convErr)
			conversionErrors = errors.Join(conversionErrors, err)
		}
		rowArgs[j] = finalValue
	} // End loop columns
	if conversionErrors != nil {
		s.Logger.Warn("Row contains conversion errors, skipping append.", "line", lineNumber, "errors", conversionErrors)
		return conversionErrors
	}

	// +++ DEBUG LOGGING +++
	if s.Logger.Enabled(context.Background(), slog.LevelDebug) {
		logAttrs := make([]slog.Attr, 0, numExpectedFields*3+1)
		logAttrs = append(logAttrs, slog.Int64("line", lineNumber))
		for k := 0; k < numExpectedFields; k++ {
			colLogName := fmt.Sprintf("col_%d_%s", k, s.ColumnNames[k])
			logAttrs = append(logAttrs, slog.Any(colLogName+"_val", rowArgs[k]))
			var goType string
			if rowArgs[k] == nil {
				goType = "<nil>"
			} else {
				goType = reflect.TypeOf(rowArgs[k]).String()
			}
			logAttrs = append(logAttrs, slog.String(colLogName+"_goType", goType))
			logAttrs = append(logAttrs, slog.String(colLogName+"_duckdbType", s.DuckdbTypes[k]))
		}
		s.Logger.LogAttrs(context.Background(), slog.LevelDebug, "Executing AppendRow with data", logAttrs...)
	}
	// +++ END DEBUG LOGGING +++

	appendErr := s.appender.AppendRow(rowArgs...)
	if appendErr != nil {
		s.Logger.Error("DuckDB Appender.AppendRow failed", "line", lineNumber, "error", appendErr)
		return fmt.Errorf("duckdb append row: %w", appendErr)
	}
	return nil
}

// closeSection flushes and closes the section's appender AND connection.
func (s *csvSection) closeSection() error {
	var closeErrors error
	l := s.Logger.With(slog.String("table", s.TableName))
	// Close Appender first
	if s.appender != nil {
		l.Debug("Flushing and closing section appender.")
		if err := s.appender.Flush(); err != nil {
			l.Error("Failed to flush section appender", "error", err)
			closeErrors = errors.Join(closeErrors, fmt.Errorf("flush appender %s: %w", s.TableName, err))
		}
		if err := s.appender.Close(); err != nil {
			l.Error("Failed to close section appender", "error", err)
			closeErrors = errors.Join(closeErrors, fmt.Errorf("close appender %s: %w", s.TableName, err))
		}
		s.appender = nil
		s.AppenderCreated = false
	}
	// Close the section's connection
	if s.sectionConn != nil {
		l.Debug("Closing section DB connection.")
		if err := s.sectionConn.Close(); err != nil {
			l.Error("Failed to close section DB connection", "error", err)
			closeErrors = errors.Join(closeErrors, fmt.Errorf("close section conn %s: %w", s.TableName, err))
		}
		s.sectionConn = nil
	}
	return closeErrors
}

// processCSVStream reads CSV data and writes sections using section-specific connections/appenders.
func processCSVStream(
	ctx context.Context, cfg config.Config, logger *slog.Logger,
	dbPool *sql.DB, // <<< Pass pool for logging/checks
	csvReader io.Reader, zipBaseName string,
) error {
	reader := csv.NewReader(csvReader)
	reader.Comment = '#'
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true
	var currentSection *csvSection
	var accumulatedErrors error
	lineNumber := int64(0)

	// Defer closing the last section's appender AND connection
	defer func() {
		if currentSection != nil {
			l := logger
			if currentSection.Logger != nil {
				l = currentSection.Logger
			}
			l.Warn("Closing last section via defer", slog.String("table", currentSection.TableName), slog.Any("error", accumulatedErrors), slog.Any("context_error", ctx.Err()))
			if err := currentSection.closeSection(); err != nil {
				l.Error("Error during deferred cleanup of last section", "error", err)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Warn("Stream processing cancelled.", "cause", ctx.Err())
			accumulatedErrors = errors.Join(accumulatedErrors, ctx.Err())
			return accumulatedErrors
		default:
		}
		lineNumber++
		record, err := reader.Read()
		if err == io.EOF {
			logger.Debug("Reached end of CSV stream.")
			break
		}
		if err != nil {
			if parseErr, ok := err.(*csv.ParseError); ok {
				logger.Warn("CSV parsing error, skipping row.", "line", lineNumber, "csv_error", parseErr.Error())
				accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv parse line %d: %w", lineNumber, err))
				continue
			}
			logger.Error("Fatal error reading CSV stream", "line", lineNumber, "error", err)
			accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv read line %d: %w", lineNumber, err))
			return accumulatedErrors
		}
		if len(record) == 0 {
			continue
		}
		recordType := record[0]
		l := logger.With(slog.Int64("line", lineNumber), slog.String("record_type", recordType))

		switch recordType {
		case "I":
			// Finalize previous section (closes its appender and connection)
			if currentSection != nil {
				l.Debug("Closing previous section", slog.String("table", currentSection.TableName))
				if err := currentSection.closeSection(); err != nil {
					l.Error("Failed to close previous section cleanly", "error", err)
					accumulatedErrors = errors.Join(accumulatedErrors, err)
				}
				currentSection = nil
			}
			if len(record) < 4 {
				l.Warn("Malformed 'I' record, skipping section init.", "record", record)
				continue
			}
			comp, ver := record[2], record[3]
			headers := record[4:]
			if len(headers) == 0 {
				l.Warn("'I' record has no headers, skipping section init.", "comp", comp, "ver", ver)
				continue
			}
			var sectionErr error
			currentSection, sectionErr = newCSVSection(ctx, cfg, logger, dbPool, comp, ver, headers) // Pass pool
			if sectionErr != nil {
				l.Error("Failed to initialize new CSV section", "error", sectionErr)
				accumulatedErrors = errors.Join(accumulatedErrors, sectionErr)
				return accumulatedErrors
			}

		case "D":
			if currentSection == nil {
				continue
			}
			l = l.With(slog.String("table", currentSection.TableName))
			if len(record) < 4 {
				l.Warn("Malformed 'D' record, skipping row.", "record", record)
				continue
			}
			dataValues := record[4:]

			if !currentSection.SchemaInferred {
				currentSection.RowsChecked++
				inferenceValues := make([]string, len(currentSection.Headers))
				hasBlanks := false
				for i := range currentSection.Headers {
					if i >= len(dataValues) || dataValues[i] == "" {
						hasBlanks = true
						inferenceValues[i] = ""
					} else {
						inferenceValues[i] = dataValues[i]
					}
				}
				if hasBlanks {
					l.Info("Inferring schema using first 'D' row which contains blanks.")
				} else {
					l.Debug("Inferring schema from first 'D' row.")
				}

				// This now creates the sectionConn and appender inside
				err := currentSection.inferSchemaAndEnsureTable(ctx, inferenceValues)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						l.Warn("Context cancelled during schema inference/table creation.", "error", err)
						accumulatedErrors = errors.Join(accumulatedErrors, err)
						return accumulatedErrors
					}
					l.Error("Failed to infer schema/ensure table/init appender, skipping section.", "error", err)
					accumulatedErrors = errors.Join(accumulatedErrors, err)
					currentSection = nil
					continue // Skip section
				}
			}

			if currentSection != nil && currentSection.SchemaInferred {
				appendErr := currentSection.appendRow(dataValues, lineNumber) // Pass lineNumber
				if appendErr != nil {
					l.Warn("Failed to append row to DuckDB, skipping row.", "error", appendErr)
					accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("append line %d: %w", lineNumber, appendErr))
					// If append fails, maybe we should close the section and start fresh if another 'I' comes?
					// For now, just accumulate error and continue stream.
				}
			}

		default:
			continue
		}
	} // End record reading loop

	// Final cleanup handled by defer

	return accumulatedErrors
}
