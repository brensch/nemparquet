package processor

import (
	"archive/zip"
	"context"

	// "database/sql" // No longer needed directly in this file
	"database/sql/driver" // Still needed for driver.Value
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
	"sync" // <<< Added for Mutex in global registry

	// Keep for potential future use or logging
	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	// "github.com/brensch/nemparquet/internal/db" // No direct DB interaction here
	"github.com/brensch/nemparquet/internal/util"
	// "github.com/marcboeker/go-duckdb" // No direct DuckDB interaction here
)

// WriteOperation defines the data sent from parser to writer goroutine
type WriteOperation struct {
	TableName    string         // Target table name (sanitized)
	Data         []driver.Value // Row data ready for appending
	LineNumber   int64          // Original line number for logging
	IsFirstChunk bool           // Flag if this is the first chunk for a new table/section (for schema inference)
	Headers      []string       // Original headers (sent only with first chunk)
	ColumnNames  []string       // Sanitized column names (sent only with first chunk)
	DuckdbTypes  []string       // Inferred DuckDB types (sent only with first chunk)
	IsDateColumn []bool         // Date column flags (sent only with first chunk)
}

// --- Global Schema Info Registry ---
// Tracks tables for which the schema has been inferred and sent *in this run*.
// Stores the inferred types to avoid re-inference.
type SchemaInfo struct {
	DuckdbTypes  []string
	IsDateColumn []bool
	ColumnNames  []string // Store sanitized names too
	Headers      []string // Store original headers
	Sent         bool     // Has the first chunk been sent?
}

var (
	schemaRegistry      = make(map[string]*SchemaInfo)
	schemaRegistryMutex sync.Mutex // <<< CORRECTLY DECLARED MUTEX HERE
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

// ParseZipArchive processes one zip file, parsing CSVs and sending data to the writer channel.
// It no longer interacts directly with the database.
func ParseZipArchive(
	ctx context.Context, // Job-specific context
	cfg config.Config,
	logger *slog.Logger,
	writerChan chan<- WriteOperation, // <<< Channel to send data to writer
	zipFilePath string,
	identifier string, // Keep for potential logging context ?
	fileType string, // Keep for potential logging context ?
) error {
	l := logger
	l.Debug("Opening zip archive for parsing.", slog.String("path", zipFilePath))
	zr, err := zip.OpenReader(zipFilePath)
	if err != nil {
		l.Error("Parser failed to open zip archive", "error", err)
		return fmt.Errorf("zip.OpenReader %s: %w", zipFilePath, err)
	}
	defer zr.Close()

	var processingErrors error
	csvFoundCount := 0
	zipBaseName := strings.TrimSuffix(filepath.Base(zipFilePath), filepath.Ext(zipFilePath)) // Keep for logging

	for _, f := range zr.File {
		// Check for cancellation before processing each file within the zip
		select {
		case <-ctx.Done():
			l.Warn("Parsing cancelled within zip.", "cause", ctx.Err())
			return errors.Join(processingErrors, ctx.Err()) // Return context error

		default: // Continue processing this file
		}

		// Filter for CSV files only, ignore directories
		if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
			continue
		}

		csvFoundCount++
		csvBaseName := filepath.Base(f.Name)
		csvLogger := l.With(slog.String("internal_csv", csvBaseName))
		csvLogger.Debug("Found CSV inside archive, starting stream parsing (sending to writer).")

		// Open the CSV file stream within the zip
		rc, err := f.Open()
		if err != nil {
			csvLogger.Error("Parser failed to open internal CSV file.", "error", err)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("open internal %s: %w", csvBaseName, err))
			continue
		}
		defer rc.Close() // Ensure reader closed

		streamCtx, cancelStream := context.WithCancel(ctx)
		streamErr := processCSVStream(streamCtx, cfg, csvLogger, writerChan, rc, zipBaseName)
		cancelStream()

		if streamErr != nil {
			if errors.Is(streamErr, context.Canceled) || errors.Is(streamErr, context.DeadlineExceeded) {
				if errors.Is(streamErr, ctx.Err()) {
					csvLogger.Warn("CSV stream parsing cancelled by parent job.", "cause", streamErr)
				} else {
					csvLogger.Warn("CSV stream parsing cancelled.", "cause", streamErr)
				}
				processingErrors = errors.Join(processingErrors, streamErr)
				return processingErrors // Stop processing this zip entirely if cancelled
			}
			csvLogger.Error("Parser failed to process internal CSV stream.", "error", streamErr)
			processingErrors = errors.Join(processingErrors, fmt.Errorf("process stream %s: %w", csvBaseName, streamErr))
		} else {
			csvLogger.Debug("Parser successfully finished processing CSV stream.")
		}
	} // End loop files in zip

	if csvFoundCount == 0 {
		l.Info("Parser found no CSV files within this zip archive.")
	} else if processingErrors != nil {
		if errors.Is(processingErrors, context.Canceled) || errors.Is(processingErrors, context.DeadlineExceeded) {
			l.Warn("Parser finished processing zip with cancellation.", "error", processingErrors)
		} else {
			l.Warn("Parser finished processing zip, but encountered errors during parsing/sending.", "error", processingErrors)
		}
	} else {
		l.Debug("Parser finished processing all CSVs found in zip archive.")
	}
	return processingErrors
}

// --- Section Processing Logic ---

// csvSection holds state for PARSING one I..D..D section
type csvSection struct {
	Cfg            config.Config
	Logger         *slog.Logger
	WriterChan     chan<- WriteOperation // Channel to send data
	TableName      string                // Sanitized table name
	Headers        []string              // Original headers
	ColumnNames    []string              // Sanitized column names
	DuckdbTypes    []string              // Inferred DuckDB types (set once globally, copied locally)
	IsDateColumn   []bool                // Flags for date columns (set once globally, copied locally)
	SchemaKnown    bool                  // Flag: schema known *globally*?
	headerMap      map[string]int        // Map header name to index
	columnIndexMap map[int]string        // Map column index to DuckDB type name
	// firstChunkSent is now handled globally by the registry
}

// newCSVSection initializes a section processor for parsing.
func newCSVSection(
	ctx context.Context, cfg config.Config, logger *slog.Logger,
	writerChan chan<- WriteOperation, // <<< Pass writer channel
	comp, ver string, headers []string,
) (*csvSection, error) {
	tableName := sanitizeSQLIdentifier(fmt.Sprintf("%s_v%s", comp, ver))
	s := &csvSection{
		Cfg: cfg, Logger: logger.With(slog.String("table", tableName)), WriterChan: writerChan,
		TableName: tableName, Headers: headers, DuckdbTypes: make([]string, len(headers)), // Allocate slices
		ColumnNames: make([]string, len(headers)), IsDateColumn: make([]bool, len(headers)),
		headerMap: make(map[string]int, len(headers)), columnIndexMap: make(map[int]string, len(headers)),
		SchemaKnown: false, // Assume schema not known globally yet
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
	s.Logger.Debug("New CSV section parser initialized.")
	return s, nil
}

// inferAndRegisterSchema infers schema based on a single data row,
// stores it in the global registry if not already present, and sets local types.
func (s *csvSection) inferAndRegisterSchema(values []string) error {
	s.Logger.Debug("Attempting to infer and register schema globally.")

	// --- Final Conservative Schema Inference (Data Only) ---
	inferredTypes := make([]string, len(s.Headers))
	inferredDateFlags := make([]bool, len(s.Headers))
	for i := range s.Headers {
		val := values[i]
		duckdbType := "VARCHAR"
		isDate := false
		if val != "" {
			if util.IsNEMDateTime(val) {
				duckdbType = "BIGINT"
				isDate = true
			} else {
				lcValue := strings.ToLower(val)
				if lcValue == "true" || lcValue == "false" {
					duckdbType = "BOOLEAN"
				} else {
					_, floatErr := strconv.ParseFloat(val, 64)
					if floatErr == nil {
						_, intErr := strconv.ParseInt(val, 10, 64)
						if intErr == nil {
							duckdbType = "VARCHAR"
						} else {
							duckdbType = "DOUBLE"
						}
					} else {
						_, intErr := strconv.ParseInt(val, 10, 64)
						if intErr == nil {
							duckdbType = "BIGINT"
						}
					}
				}
			}
		}
		inferredTypes[i] = duckdbType
		inferredDateFlags[i] = isDate
	}
	s.Logger.Debug("Locally inferred DuckDB schema types", slog.Any("types", inferredTypes))
	// --- End Final Conservative Schema Inference ---

	// --- Register Schema Globally (Mutex Guarded) ---
	schemaRegistryMutex.Lock() // <<< Lock before checking/creating
	existingInfo, exists := schemaRegistry[s.TableName]
	if !exists {
		// Schema not registered yet, store this inference result globally
		s.Logger.Info("Registering schema globally.", slog.String("table", s.TableName))
		schemaRegistry[s.TableName] = &SchemaInfo{
			DuckdbTypes:  inferredTypes,
			IsDateColumn: inferredDateFlags,
			ColumnNames:  s.ColumnNames, // Store sanitized names globally too
			Headers:      s.Headers,     // Store original headers globally
			Sent:         false,         // Mark that schema is known, but first chunk not yet sent
		}
		// Use the newly inferred types for this section instance
		s.DuckdbTypes = inferredTypes
		s.IsDateColumn = inferredDateFlags
		for i, dt := range s.DuckdbTypes {
			s.columnIndexMap[i] = dt
		} // Update local map
		s.SchemaKnown = true
	} else {
		// Schema already registered globally, use the globally stored types
		s.Logger.Debug("Schema already registered globally, using stored types.", slog.String("table", s.TableName))
		s.DuckdbTypes = existingInfo.DuckdbTypes
		s.IsDateColumn = existingInfo.IsDateColumn
		// Ensure local column names match global ones (should always be same for same table)
		if !reflect.DeepEqual(s.ColumnNames, existingInfo.ColumnNames) {
			s.Logger.Warn("Mismatch between local and global column names for table! Using global.", slog.Any("local", s.ColumnNames), slog.Any("global", existingInfo.ColumnNames))
			s.ColumnNames = existingInfo.ColumnNames // Prioritize global
		}
		for i, dt := range s.DuckdbTypes {
			s.columnIndexMap[i] = dt
		} // Update local map
		s.SchemaKnown = true
	}
	schemaRegistryMutex.Unlock() // <<< Unlock AFTER successful creation/retrieval and registration
	// --- End Register Schema Globally ---

	return nil
}

// prepareAndSendData prepares data row and sends it via the channel.
// It now checks the global registry to decide if schema info needs to be sent.
func (s *csvSection) prepareAndSendData(ctx context.Context, record []string, lineNumber int64) error {
	// Schema must be known (either inferred now or previously globally)
	if !s.SchemaKnown {
		return errors.New("cannot prepare row: schema not known")
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
		// Use the locally set (potentially globally derived) types for conversion
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
		s.Logger.Warn("Row contains conversion errors, skipping send.", "line", lineNumber, "errors", conversionErrors)
		return conversionErrors
	}

	// --- Prepare WriteOperation ---
	op := WriteOperation{
		TableName:  s.TableName,
		Data:       rowArgs,
		LineNumber: lineNumber,
	}

	// --- Check Global Registry (Mutex Guarded) ---
	// Decide if this specific send operation should include the schema details
	schemaRegistryMutex.Lock() // <<< Lock before accessing registry
	globalInfo, exists := schemaRegistry[s.TableName]
	// Should always exist if s.SchemaKnown is true, but check defensively
	if exists && !globalInfo.Sent {
		op.IsFirstChunk = true
		// Use the globally stored schema info for consistency
		op.Headers = globalInfo.Headers
		op.ColumnNames = globalInfo.ColumnNames
		op.DuckdbTypes = globalInfo.DuckdbTypes
		op.IsDateColumn = globalInfo.IsDateColumn
		globalInfo.Sent = true // Mark as sent globally
		s.Logger.Debug("Sending first chunk with schema info (Globally)", slog.Int64("line", lineNumber))
	} else {
		op.IsFirstChunk = false // Schema already sent by some worker
		// s.Logger.Debug("Skipping schema info, already sent globally", slog.Int64("line", lineNumber))
	}
	schemaRegistryMutex.Unlock() // <<< Unlock after accessing registry
	// --- End Global Registry Check ---

	// +++ DEBUG LOGGING (Optional) +++
	// if s.Logger.Enabled(context.Background(), slog.LevelDebug) {
	// ... (logging code as before) ...
	// }
	// +++ END DEBUG LOGGING +++

	// Send data to writer channel, respecting context cancellation
	select {
	case <-ctx.Done():
		s.Logger.Warn("Context cancelled before sending data to writer channel", "line", lineNumber, "error", ctx.Err())
		return ctx.Err() // Propagate cancellation error
	case s.WriterChan <- op:
		// Successfully sent
		return nil
	}
}

// processCSVStream reads CSV data, parses, looks ahead for schema inference (once per table globally),
// and sends WriteOperation to writer channel.
func processCSVStream(
	ctx context.Context, cfg config.Config, logger *slog.Logger,
	writerChan chan<- WriteOperation, // <<< Pass writer channel
	csvReader io.Reader, zipBaseName string,
) error {
	reader := csv.NewReader(csvReader)
	reader.Comment = '#'
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true
	var currentSection *csvSection
	var accumulatedErrors error
	lineNumber := int64(0)
	var bufferedRows [][]string // Buffer for lookahead
	var bufferedLineNumbers []int64
	// isFirstDRecordInSection removed, logic moved to prepareAndSendData with global check

	for {
		select {
		case <-ctx.Done():
			logger.Warn("Stream processing cancelled.", "cause", ctx.Err())
			accumulatedErrors = errors.Join(accumulatedErrors, ctx.Err())
			return accumulatedErrors
		default:
		}

		var record []string
		var readErr error
		var currentLineNumber int64

		if len(bufferedRows) > 0 {
			record = bufferedRows[0]
			currentLineNumber = bufferedLineNumbers[0]
			bufferedRows = bufferedRows[1:]
			bufferedLineNumbers = bufferedLineNumbers[1:]
		} else {
			lineNumber++
			currentLineNumber = lineNumber
			record, readErr = reader.Read()
			if readErr == io.EOF {
				logger.Debug("Reached end of CSV stream.")
				break
			}
			if readErr != nil {
				if parseErr, ok := readErr.(*csv.ParseError); ok {
					logger.Warn("CSV parsing error, skipping row.", "line", currentLineNumber, "csv_error", parseErr.Error())
					accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv parse line %d: %w", currentLineNumber, readErr))
					continue
				}
				logger.Error("Fatal error reading CSV stream", "line", currentLineNumber, "error", readErr)
				accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("csv read line %d: %w", currentLineNumber, readErr))
				return accumulatedErrors
			}
		}

		if len(record) == 0 {
			continue
		}
		recordType := record[0]
		l := logger.With(slog.Int64("line", currentLineNumber), slog.String("record_type", recordType))

		switch recordType {
		case "I":
			if currentSection != nil {
				l.Debug("Ending previous section", slog.String("table", currentSection.TableName))
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
			currentSection, sectionErr = newCSVSection(ctx, cfg, logger, writerChan, comp, ver, headers)
			if sectionErr != nil {
				l.Error("Failed to initialize new CSV section parser", "error", sectionErr)
				accumulatedErrors = errors.Join(accumulatedErrors, sectionErr)
				return accumulatedErrors
			}
			bufferedRows = nil
			bufferedLineNumbers = nil // Reset buffer

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

			// --- Schema Inference Logic (Runs only if schema not known GLOBALLY) ---
			schemaRegistryMutex.Lock() // Lock before checking global registry
			_, schemaKnownGlobally := schemaRegistry[currentSection.TableName]
			schemaRegistryMutex.Unlock() // Unlock immediately after check

			if !schemaKnownGlobally {
				// Schema not known globally - This must be the first 'D' encountered for this table name globally.
				// Perform lookahead and inference. This block will only run ONCE per table name.
				if !currentSection.SchemaKnown { // Double check: Should only infer if not known locally either
					schemaLookaheadLimit := cfg.SchemaRowLimit
					if schemaLookaheadLimit <= 0 {
						schemaLookaheadLimit = 10
					}
					var inferenceRow []string
					var inferenceLine int64 = -1
					foundNonBlank := false

					// Start buffer with the current row
					bufferedRows = append(bufferedRows, dataValues)
					bufferedLineNumbers = append(bufferedLineNumbers, currentLineNumber)

					hasBlanks := false
					for _, val := range dataValues {
						if val == "" {
							hasBlanks = true
							break
						}
					}

					if !hasBlanks {
						l.Debug("Using first 'D' row for schema inference (no blanks).", slog.Int64("line", currentLineNumber))
						inferenceRow = dataValues
						inferenceLine = currentLineNumber
						foundNonBlank = true
					} else {
						l.Debug("First 'D' row has blanks, looking ahead for schema inference.", slog.Int("limit", schemaLookaheadLimit))
						for i := 1; i < schemaLookaheadLimit; i++ {
							select {
							case <-ctx.Done():
								return errors.Join(accumulatedErrors, ctx.Err())
							default:
							}
							lookaheadLineNumber := lineNumber + 1
							nextRecord, readErr := reader.Read()
							if readErr == io.EOF {
								l.Debug("EOF reached during schema lookahead.")
								break
							}
							if readErr != nil {
								if pErr, ok := readErr.(*csv.ParseError); ok {
									l.Warn("CSV parsing error during lookahead, skipping.", "line", lookaheadLineNumber, "csv_error", pErr.Error())
									accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("lookahead parse line %d: %w", lookaheadLineNumber, readErr))
									continue
								}
								l.Error("Fatal error reading CSV during lookahead", "line", lookaheadLineNumber, "error", readErr)
								accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("lookahead read line %d: %w", lookaheadLineNumber, readErr))
								return accumulatedErrors
							}
							lineNumber = lookaheadLineNumber // Update main line number

							if len(nextRecord) == 0 {
								continue
							}
							if nextRecord[0] != "D" {
								l.Debug("Non-'D' record encountered during lookahead, stopping.", "line", lineNumber, "next_type", nextRecord[0])
								bufferedRows = append([][]string{nextRecord}, bufferedRows...) // Prepend non-D row
								bufferedLineNumbers = append([]int64{lineNumber}, bufferedLineNumbers...)
								break
							}
							if len(nextRecord) < 4 {
								l.Warn("Malformed 'D' record during lookahead, skipping.", "line", lookaheadLineNumber)
								continue
							}

							nextDataValues := nextRecord[4:]
							bufferedRows = append(bufferedRows, nextDataValues)
							bufferedLineNumbers = append(bufferedLineNumbers, lineNumber)

							rowHasBlanks := false
							for _, val := range nextDataValues {
								if val == "" {
									rowHasBlanks = true
									break
								}
							}
							if !rowHasBlanks {
								l.Debug("Found non-blank row during lookahead for schema inference.", slog.Int64("line", lineNumber))
								inferenceRow = nextDataValues
								inferenceLine = lineNumber
								foundNonBlank = true
								break
							}
						} // End lookahead loop
					}

					// Select inference row
					if !foundNonBlank {
						if len(bufferedRows) > 0 {
							lastDIndex := -1
							for idx := len(bufferedRows) - 1; idx >= 0; idx-- {
								recordToCheck := bufferedRows[idx]
								if len(recordToCheck) > 0 && recordToCheck[0] == "D" {
									lastDIndex = idx
									break
								}
							}
							if lastDIndex != -1 && len(bufferedRows[lastDIndex]) > 4 {
								inferenceRow = bufferedRows[lastDIndex][4:]
								inferenceLine = bufferedLineNumbers[lastDIndex]
								l.Warn("No non-blank row found within lookahead limit, inferring schema from last 'D' row read.", "line", inferenceLine, slog.Int("limit", schemaLookaheadLimit))
							} else {
								l.Error("Buffer contains no suitable 'D' rows after lookahead to infer schema.", slog.String("table", currentSection.TableName))
								accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("no suitable D rows in buffer for schema inference in table %s", currentSection.TableName))
								currentSection = nil
								continue
							}
						} else {
							l.Error("No suitable 'D' rows found to infer schema for section.", slog.String("table", currentSection.TableName))
							accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("no D rows for schema inference in table %s", currentSection.TableName))
							currentSection = nil
							continue
						}
					}

					// Perform schema inference and register globally
					err := currentSection.inferAndRegisterSchema(inferenceRow) // Infers and updates registry
					if err != nil {
						l.Error("Failed to infer and register schema, skipping section.", "line", inferenceLine, "error", err)
						accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("schema inference/register line %d: %w", inferenceLine, err))
						currentSection = nil
						continue
					}
					// Schema is now known locally (SchemaKnown=true) and registered globally
				} // End if !currentSection.SchemaKnown (local check)
			} else {
				// Schema known globally, ensure it's loaded locally if this section just started
				if !currentSection.SchemaKnown {
					l.Debug("Schema already known globally, adopting types locally.", slog.Int64("line", currentLineNumber))
					schemaRegistryMutex.Lock() // Lock to safely read global info
					globalInfo, exists := schemaRegistry[currentSection.TableName]
					schemaRegistryMutex.Unlock() // Unlock after reading
					if exists {
						currentSection.DuckdbTypes = globalInfo.DuckdbTypes
						currentSection.IsDateColumn = globalInfo.IsDateColumn
						currentSection.ColumnNames = globalInfo.ColumnNames
						currentSection.Headers = globalInfo.Headers
						for i, dt := range currentSection.DuckdbTypes {
							currentSection.columnIndexMap[i] = dt
						}
						currentSection.SchemaKnown = true
					} else {
						// Should not happen if global check was correct, but handle defensively
						l.Error("Global schema info missing unexpectedly!", slog.String("table", currentSection.TableName))
						accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("global schema missing for table %s", currentSection.TableName))
						currentSection = nil
						continue
					}
				}
			} // End schema inference/adoption block

			// --- Process Data Row (Schema MUST be known by now if section is valid) ---
			if currentSection != nil && currentSection.SchemaKnown {
				// Determine if this specific call should mark the chunk as the first globally
				// This is now handled correctly within prepareAndSendData
				sendErr := currentSection.prepareAndSendData(ctx, dataValues, currentLineNumber) // Pass currentLineNumber
				if sendErr != nil {
					if errors.Is(sendErr, context.Canceled) || errors.Is(sendErr, context.DeadlineExceeded) {
						l.Warn("Context cancelled during send, stopping stream.", "error", sendErr)
						accumulatedErrors = errors.Join(accumulatedErrors, sendErr)
						return accumulatedErrors
					}
					l.Warn("Failed to prepare/send row, skipping.", "error", sendErr) // Conversion errors already logged
					accumulatedErrors = errors.Join(accumulatedErrors, fmt.Errorf("prepare/send line %d: %w", currentLineNumber, sendErr))
				}
			}

		default:
			continue // Ignore other record types
		}
	} // End record reading loop

	if currentSection != nil {
		logger.Debug("Finished processing last section", slog.String("table", currentSection.TableName))
	}
	return accumulatedErrors
}
