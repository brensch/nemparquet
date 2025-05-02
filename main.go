package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp" // Added for date regex
	"sort"
	"strconv"
	"strings"
	"time" // Added for time parsing and epoch conversion

	_ "github.com/marcboeker/go-duckdb"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/net/html"
)

// Directories hosting the CSV feeds on NEMWEB
var feedURLs = []string{
	"https://nemweb.com.au/Reports/Current/FPP/",
	"https://nemweb.com.au/Reports/Current/FPPDAILY/",
	"https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	"https://nemweb.com.au/Reports/Current/PD7Day/",
	"https://nemweb.com.au/Reports/Current/P5_Reports/",
}

// --- Time Zone and Date Handling ---

var (
	// NEM time is UTC+10, fixed offset.
	nemLocation *time.Location
	// Regex to identify the specific date format YYYY/MM/DD HH:MI:SS, optionally surrounded by quotes.
	nemDateTimeRegex *regexp.Regexp
)

func init() {
	// Define the NEM time zone location (UTC+10)
	nemLocation = time.FixedZone("NEM", 10*60*60) // 10 hours * 60 mins * 60 secs

	// Compile the regex for date format detection
	// Matches "YYYY/MM/DD HH:MI:SS" exactly, allowing for optional surrounding quotes
	nemDateTimeRegex = regexp.MustCompile(`^"?\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}"?$`)
}

// isNEMDateTime checks if a string matches the YYYY/MM/DD HH:MI:SS format (with optional quotes).
func isNEMDateTime(s string) bool {
	return nemDateTimeRegex.MatchString(s)
}

// nemDateTimeToEpochMS converts a "YYYY/MM/DD HH:MI:SS" string (in NEMtime),
// potentially surrounded by double quotes, to Unix epoch milliseconds (int64).
// Returns 0 and error if parsing fails.
func nemDateTimeToEpochMS(s string) (int64, error) {
	// Trim surrounding double quotes before parsing
	trimmedS := strings.Trim(s, `"`)

	// Define the layout matching the input string format (without quotes)
	layout := "2006/01/02 15:04:05" // Go's reference time format matching YYYY/MM/DD HH:MI:SS
	t, err := time.ParseInLocation(layout, trimmedS, nemLocation)
	if err != nil {
		// Return the original string 's' in the error message for better debugging
		return 0, fmt.Errorf("failed to parse NEM time (after trimming quotes) from '%s': %w", s, err)
	}
	// Return milliseconds since epoch
	return t.UnixMilli(), nil
}

// --- Main Application Logic ---
func main() {
	inputDir := flag.String("input", "./input_csv", "Directory to download & extract latest CSVs into")
	outputDir := flag.String("output", "./output_parquet", "Directory to write Parquet files to")
	dbPath := flag.String("db", ":memory:", "DuckDB database file path (use ':memory:' for in-memory)")
	flag.Parse()

	log.Printf("Starting NEM Parquet Converter...")
	log.Printf("Input directory: %s", *inputDir)
	log.Printf("Output directory: %s", *outputDir)
	log.Printf("DuckDB path: %s", *dbPath)

	// Ensure directories exist
	for _, d := range []string{*inputDir, *outputDir} {
		if d != "" && d != ":memory:" {
			if err := os.MkdirAll(d, 0o755); err != nil {
				log.Fatalf("FATAL: Failed to create directory %s: %v", d, err)
			}
		}
	}

	// --- Download and Extract ---
	log.Println("--- Downloading and Extracting CSVs ---")
	if err := downloadAndExtractLatest(*inputDir); err != nil {
		log.Printf("WARN: Download/extraction process encountered errors: %v", err)
	}

	// --- Process CSVs ---
	log.Println("--- Processing CSV Files ---")
	csvFiles, err := filepath.Glob(filepath.Join(*inputDir, "*.[cC][sS][vV]"))
	if err != nil {
		log.Fatalf("FATAL: Failed to search for CSV files in %s: %v", *inputDir, err)
	}
	if len(csvFiles) == 0 {
		log.Printf("INFO: No *.CSV files found in %s to process.", *inputDir)
		log.Println("NEM Parquet Converter finished (no CSVs found).")
		return
	}

	log.Printf("Found %d CSV files to process.", len(csvFiles))
	processedCount := 0
	errorCount := 0
	for _, csvPath := range csvFiles {
		log.Printf("Processing %s...", csvPath)
		if err := processCSVSections(csvPath, *outputDir); err != nil {
			log.Printf("ERROR processing %s: %v", csvPath, err)
			errorCount++
		} else {
			processedCount++
			log.Printf("Finished processing %s successfully.", csvPath)
		}
	}

	log.Println("--- Processing Summary ---")
	log.Printf("Successfully processed %d CSV files.", processedCount)
	if errorCount > 0 {
		log.Printf("Encountered errors processing %d CSV files.", errorCount)
	}

	// --- Inspect Parquet Files ---
	if processedCount > 0 {
		log.Println("--- Inspecting Parquet File Schemas & Counts ---")
		absOutputDir, err := filepath.Abs(*outputDir)
		if err != nil {
			log.Printf("ERROR getting absolute path for output directory %s: %v", *outputDir, err)
			log.Println("Skipping Parquet file inspection.")
		} else {
			if err := inspectParquetFiles(absOutputDir, *dbPath); err != nil {
				log.Printf("ERROR inspecting Parquet files: %v", err)
			} else {
				log.Println("Parquet file inspection finished.")
			}
		}
	} else {
		log.Println("Skipping Parquet file inspection: No CSV files were processed.")
	}

	// --- Run DuckDB FPP Analysis ---
	if errorCount == 0 && processedCount > 0 {
		log.Println("--- Running DuckDB FPP Analysis ---")
		absOutputDir, err := filepath.Abs(*outputDir)
		if err != nil {
			log.Printf("ERROR getting absolute path for output directory %s: %v", *outputDir, err)
			log.Println("Skipping DuckDB Analysis.")
		} else {
			analysisCtx := context.Background()
			if err := runDuckDBAnalysis(analysisCtx, absOutputDir, *dbPath); err != nil {
				log.Printf("ERROR running DuckDB analysis: %v", err)
			} else {
				log.Println("DuckDB FPP Analysis finished successfully.")
			}
		}
	} else if processedCount == 0 {
		log.Println("Skipping DuckDB Analysis: No CSV files were processed.")
	} else {
		log.Println("Skipping DuckDB Analysis due to processing errors.")
	}

	log.Println("NEM Parquet Converter finished.")
}

// --- Core CSV to Parquet Conversion (Modified for Epoch Dates) ---
func processCSVSections(csvPath, outDir string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open CSV %s: %w", csvPath, err)
	}
	defer file.Close()
	scanner := NewPeekableScanner(file)
	var pw *writer.CSVWriter
	var fw source.ParquetFile
	var currentHeaders []string
	var currentComp, currentVer string
	var currentMeta []string // Holds schema definition strings
	var isDateColumn []bool  // Tracks which columns were inferred as dates
	var schemaInferred bool = false
	baseName := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			continue
		}
		rec := strings.Split(trimmedLine, ",")
		if len(rec) == 0 {
			continue
		}
		recordType := strings.TrimSpace(rec[0])

		switch recordType {
		case "I":
			if pw != nil {
				log.Printf("   Finalizing section %s v%s...", currentComp, currentVer)
				if err := pw.WriteStop(); err != nil {
					log.Printf("   WARN: Error stopping Parquet writer for section %s v%s: %v", currentComp, currentVer, err)
				} else {
					log.Printf("   Successfully stopped writer for %s v%s", currentComp, currentVer)
				}
				if err := fw.Close(); err != nil {
					log.Printf("   WARN: Error closing Parquet file for section %s v%s: %v", currentComp, currentVer, err)
				}
				pw = nil
				fw = nil
				currentMeta = nil
				isDateColumn = nil
				schemaInferred = false
			}
			if len(rec) < 5 {
				log.Printf("WARN line %d: Malformed 'I' record in %s (less than 5 fields): %s. Skipping section.", lineNumber, csvPath, line)
				currentHeaders = nil
				continue
			}
			currentComp, currentVer = strings.TrimSpace(rec[2]), strings.TrimSpace(rec[3])
			currentHeaders = make([]string, len(rec)-4)
			for i, h := range rec[4:] {
				currentHeaders[i] = strings.TrimSpace(h)
			}
			if len(currentHeaders) == 0 {
				log.Printf("WARN line %d: 'I' record in %s has no header columns. Skipping section %s v%s.", lineNumber, csvPath, currentComp, currentVer)
				continue
			}
			schemaInferred = false
			log.Printf(" Found section header %s v%s at line %d", currentComp, currentVer, lineNumber)

		case "D":
			if len(currentHeaders) == 0 {
				continue
			}
			if len(rec) < 4 {
				log.Printf("WARN line %d: Malformed 'D' record in %s (less than 4 fields): %s. Skipping row.", lineNumber, csvPath, line)
				continue
			}
			values := make([]string, len(currentHeaders))
			numDataCols := len(rec) - 4
			hasBlanks := false
			for i := 0; i < len(currentHeaders); i++ {
				// Trim quotes from individual data values
				if i < numDataCols {
					values[i] = strings.TrimSpace(strings.Trim(rec[i+4], `"`)) // Trim space and quotes
					if values[i] == "" {
						hasBlanks = true
					}
				} else {
					values[i] = ""
					hasBlanks = true
				}
			}

			// --- Infer Schema and Initialize Writer (Modified for Date Detection) ---
			if !schemaInferred {
				peekedType, _ := scanner.PeekRecordType()
				mustInferNow := hasBlanks && (peekedType != "D")
				if !hasBlanks || mustInferNow {
					if mustInferNow && hasBlanks {
						log.Printf("   WARN line %d: No non-blank data row found for schema inference before next section/EOF in %s v%s. Inferring from current row (line %d), blanks will default to STRING.", lineNumber, currentComp, currentVer, lineNumber)
					} else {
						log.Printf("   Inferring schema for %s v%s from data row at line %d", currentComp, currentVer, lineNumber)
					}

					currentMeta = make([]string, len(currentHeaders))
					isDateColumn = make([]bool, len(currentHeaders)) // Initialize date tracking
					for i := range currentHeaders {
						var typ string   // Parquet type name
						val := values[i] // Use the already trimmed value
						isDate := false  // Flag for this column

						// Date detection logic
						if isNEMDateTime(val) { // Check the trimmed value
							typ = "INT64" // Store epoch milliseconds as INT64
							isDate = true
						} else if val == "" {
							typ = "BYTE_ARRAY"
						} else if val == "TRUE" || val == "FALSE" || strings.EqualFold(val, "true") || strings.EqualFold(val, "false") {
							typ = "BOOLEAN"
						} else if _, err := strconv.ParseInt(val, 10, 32); err == nil {
							typ = "INT32"
						} else if _, err := strconv.ParseInt(val, 10, 64); err == nil {
							typ = "INT64"
						} else if _, err := strconv.ParseFloat(val, 64); err == nil {
							typ = "DOUBLE"
						} else {
							typ = "BYTE_ARRAY"
						}

						isDateColumn[i] = isDate // Store if this column is a date

						cleanHeader := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(currentHeaders[i], " ", "_"), ".", "_"), ";", "_")
						if cleanHeader == "" {
							cleanHeader = fmt.Sprintf("column_%d", i)
						}

						if typ == "BYTE_ARRAY" {
							currentMeta[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", cleanHeader)
						} else {
							currentMeta[i] = fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", cleanHeader, typ)
						}
					} // End schema inference loop

					log.Printf("     Inferred Schema for %s v%s: [%s]", currentComp, currentVer, strings.Join(currentMeta, "], ["))

					// Create Parquet Writer
					parquetFile := fmt.Sprintf("%s_%s_v%s.parquet", baseName, currentComp, currentVer)
					path := filepath.Join(outDir, parquetFile)
					var createErr error
					fw, createErr = local.NewLocalFileWriter(path)
					if createErr != nil {
						log.Printf("ERROR line %d: Failed create parquet file %s: %v. Skipping section %s v%s.", lineNumber, path, createErr, currentComp, currentVer)
						currentHeaders = nil
						schemaInferred = false
						continue
					}
					pw, createErr = writer.NewCSVWriter(currentMeta, fw, 4)
					if createErr != nil {
						log.Printf("ERROR line %d: Failed init Parquet CSV writer for %s: %v. Skipping section %s v%s.", lineNumber, path, createErr, currentComp, currentVer)
						fw.Close()
						currentHeaders = nil
						schemaInferred = false
						continue
					}
					pw.CompressionType = parquet.CompressionCodec_SNAPPY
					log.Printf("   Created writer for section %s v%s -> %s", currentComp, currentVer, path)
					schemaInferred = true
				} else {
					log.Printf("   Skipping schema inference on blank-containing row at line %d for %s v%s. Looking for cleaner row...", lineNumber, currentComp, currentVer)
					continue
				}
			} // End writer initialization block (!schemaInferred)

			// --- Write Data Row using WriteString (Modified for Date Conversion) ---
			if !schemaInferred || pw == nil {
				continue
			}

			recPtrs := make([]*string, len(values))
			for j := 0; j < len(values); j++ {
				isEmpty := values[j] == ""
				finalValue := values[j] // Use the value already trimmed earlier

				if isDateColumn[j] && !isEmpty {
					epochMS, err := nemDateTimeToEpochMS(values[j]) // Pass the already trimmed value
					if err != nil {
						originalVal := "N/A"
						if j < numDataCols {
							originalVal = rec[j+4]
						}
						log.Printf("WARN line %d: Failed to convert date string '%s' (original: '%s') to epoch ms for column '%s': %v. Writing NULL.", lineNumber, values[j], originalVal, currentHeaders[j], err)
						recPtrs[j] = nil
						continue
					}
					finalValue = strconv.FormatInt(epochMS, 10)
				}

				isTargetStringType := strings.Contains(currentMeta[j], "type=BYTE_ARRAY")
				if isEmpty && !isTargetStringType {
					recPtrs[j] = nil
				} else {
					temp := finalValue
					recPtrs[j] = &temp
				}
			}

			if err := pw.WriteString(recPtrs); err != nil {
				problematicData := "N/A"
				schemaForProblem := "N/A"
				for k := 0; k < len(values); k++ {
					if recPtrs[k] == nil {
						continue
					}
					valStr := *recPtrs[k]
					var parseErr error
					if !isDateColumn[k] {
						if strings.Contains(currentMeta[k], "type=BOOLEAN") {
							if !(strings.EqualFold(valStr, "true") || strings.EqualFold(valStr, "false") || valStr == "") {
								parseErr = errors.New("not bool")
							}
						} else if strings.Contains(currentMeta[k], "type=INT32") || strings.Contains(currentMeta[k], "type=INT64") {
							_, parseErr = strconv.ParseInt(valStr, 10, 64)
						} else if strings.Contains(currentMeta[k], "type=DOUBLE") || strings.Contains(currentMeta[k], "type=FLOAT") {
							_, parseErr = strconv.ParseFloat(valStr, 64)
						}
					}
					if parseErr != nil {
						problematicData = fmt.Sprintf("Column %d ('%s'): '%s'", k, currentHeaders[k], values[k])
						schemaForProblem = currentMeta[k]
						break
					}
				}
				log.Printf("WARN line %d: WriteString error section %s v%s: %v. Schema: [%s]. Problematic Data Guess: [%s]. Raw Data (Trimmed): %v", lineNumber, currentComp, currentVer, err, schemaForProblem, problematicData, values)
			}

		default:
			continue
		}
	}

	if pw != nil {
		log.Printf("   Finalizing last section %s v%s...", currentComp, currentVer)
		if err := pw.WriteStop(); err != nil {
			log.Printf("ERROR: Failed to stop Parquet writer for last section %s v%s: %v", currentComp, currentVer, err)
		} else {
			log.Printf("   Successfully stopped writer for last section %s v%s", currentComp, currentVer)
		}
		if err := fw.Close(); err != nil {
			log.Printf("ERROR: Failed to close Parquet file for last section %s v%s: %v", currentComp, currentVer, err)
		}
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			log.Printf("ERROR: Scanner error reading CSV %s: line too long around line %d.", csvPath, lineNumber)
		} else {
			log.Printf("ERROR: Scanner error reading CSV %s near line %d: %v", csvPath, lineNumber, err)
		}
		return fmt.Errorf("scanner error reading CSV %s: %w", csvPath, err)
	}
	return nil
}

// --- DuckDB Parquet File Inspection ---
// (inspectParquetFiles function remains the same)
func inspectParquetFiles(parquetDir string, dbPath string) error {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", dbPath, err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()
	log.Printf("DuckDB (Inspect): Installing and loading Parquet extension...")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(context.Background(), setupSQL); err != nil {
		log.Printf("WARN: Failed to install/load parquet extension for inspection: %v", err)
	} else {
		log.Printf("DuckDB (Inspect): Parquet extension loaded.")
	}
	globPattern := filepath.Join(parquetDir, "*.parquet")
	parquetFiles, err := filepath.Glob(globPattern)
	if err != nil {
		return fmt.Errorf("failed to glob for parquet files in %s: %w", parquetDir, err)
	}
	if len(parquetFiles) == 0 {
		log.Printf("No *.parquet files found in %s to inspect.", parquetDir)
		return nil
	}
	log.Printf("Found %d parquet files to inspect in %s", len(parquetFiles), parquetDir)
	for _, filePath := range parquetFiles {
		baseName := filepath.Base(filePath)
		log.Printf("--- Inspecting: %s ---", baseName)
		escapedFilePath := strings.ReplaceAll(filePath, "'", "''")
		describeSQL := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s');", escapedFilePath)
		schemaRows, err := conn.QueryContext(context.Background(), describeSQL)
		if err != nil {
			log.Printf("  ERROR getting schema for %s: %v (SQL: %s)", baseName, err, describeSQL)
			continue
		}
		log.Println("  Schema:")
		log.Printf("    %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", "Column Name", "Column Type", "Null", "Key", "Default", "Extra")
		log.Println("    " + strings.Repeat("-", 90))
		schemaColumnCount := 0
		for schemaRows.Next() {
			var colName, colType, nullVal, keyVal, defaultVal, extraVal sql.NullString
			if err := schemaRows.Scan(&colName, &colType, &nullVal, &keyVal, &defaultVal, &extraVal); err != nil {
				log.Printf("  ERROR scanning schema row for %s: %v", baseName, err)
				break
			}
			log.Printf("    %-30s | %-20s | %-5s | %-5s | %-5s | %s\n", colName.String, colType.String, nullVal.String, keyVal.String, defaultVal.String, extraVal.String)
			if strings.Contains(strings.ToLower(colName.String), "datetime") && colType.String != "BIGINT" {
				log.Printf("    >>> UNEXPECTED TYPE for date column '%s': Expected BIGINT (INT64), got %s <<<", colName.String, colType.String)
			}
			schemaColumnCount++
		}
		if err = schemaRows.Err(); err != nil {
			log.Printf("  ERROR iterating schema rows for %s: %v", baseName, err)
		}
		schemaRows.Close()
		if schemaColumnCount == 0 && err == nil {
			log.Println("  WARN: DESCRIBE returned no columns. File might be empty or corrupted.")
		}
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s');", escapedFilePath)
		var rowCount int64 = -1
		err = conn.QueryRowContext(context.Background(), countSQL).Scan(&rowCount)
		if err != nil {
			log.Printf("  ERROR getting row count for %s: %v (SQL: %s)", baseName, err, countSQL)
		} else {
			log.Printf("  Row Count: %d", rowCount)
		}
		log.Println(strings.Repeat("-", 40))
	}
	return nil
}

// --- DuckDB FPP Calculation (Modified for Epoch Dates) ---
func runDuckDBAnalysis(ctx context.Context, parquetDir string, dbPath string) error {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", dbPath, err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()
	log.Printf("DuckDB (Analysis): Installing and loading Parquet extension...")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
		return fmt.Errorf("failed to install/load parquet extension: %w", err)
	}
	log.Printf("DuckDB (Analysis): Parquet extension loaded.")

	// --- Define Views (Reading INT64 for dates now) ---
	createCfViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW fpp_cf AS SELECT FPP_UNITID AS unit, INTERVAL_DATETIME, TRY_CAST(CONTRIBUTION_FACTOR AS DOUBLE) AS cf FROM read_parquet('%s/*CONTRIBUTION_FACTOR*.parquet', HIVE_PARTITIONING=0);`, parquetDir)
	createRcrViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW rcr AS SELECT INTERVAL_DATETIME, BIDTYPE AS SERVICE, TRY_CAST(RCR AS DOUBLE) AS rcr FROM read_parquet('%s/*FPP_RCR*.parquet', HIVE_PARTITIONING=0);`, parquetDir)
	createPriceViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW price AS SELECT INTERVAL_DATETIME, REGIONID, TRY_CAST(RAISEREGRRP AS DOUBLE) AS price_raisereg, TRY_CAST(LOWERREGRRP AS DOUBLE) AS price_lowerreg FROM read_parquet('%s/*PRICESOLUTION*.parquet', HIVE_PARTITIONING=0);`, parquetDir)

	// --- Execute View Creation ---
	log.Printf("DuckDB (Analysis): Creating view fpp_cf...")
	if _, err = conn.ExecContext(ctx, createCfViewSQL); err != nil {
		return fmt.Errorf("failed to create view fpp_cf: %w", err)
	}
	log.Printf("DuckDB (Analysis): Creating view rcr...")
	if _, err = conn.ExecContext(ctx, createRcrViewSQL); err != nil {
		return fmt.Errorf("failed to create view rcr: %w", err)
	}
	log.Printf("DuckDB (Analysis): Creating view price...")
	if _, err = conn.ExecContext(ctx, createPriceViewSQL); err != nil {
		return fmt.Errorf("failed to create view price: %w", err)
	}
	log.Println("DuckDB (Analysis): Views created.")

	// --- *** DIAGNOSTICS START *** ---
	// 1. Check Distinct Service/BIDTYPE values
	log.Println("DuckDB (Analysis): Checking distinct SERVICE values in rcr view...")
	distinctServiceSQL := `SELECT DISTINCT SERVICE FROM rcr ORDER BY 1 NULLS LAST LIMIT 20;`
	serviceRows, err := conn.QueryContext(ctx, distinctServiceSQL)
	if err != nil {
		log.Printf("WARN: Failed to query distinct service values: %v", err)
	} else {
		log.Println("  Distinct SERVICE values found (limit 20):")
		for serviceRows.Next() {
			var serviceName sql.NullString
			if err := serviceRows.Scan(&serviceName); err != nil {
				log.Printf("  WARN: Error scanning service name: %v", err)
				break
			}
			log.Printf("    - '%s'", serviceName.String)
		}
		serviceRows.Close()
	}

	// 2. Check Date Range - Using corrected to_timestamp function
	log.Println("DuckDB (Analysis): Checking epoch date ranges (INT64)...")
	// *** FIX: Use to_timestamp and divide by 1000 ***
	epochCheckSQL := `
	WITH epochs AS (
		(SELECT 'fpp_cf' as tbl, INTERVAL_DATETIME FROM fpp_cf WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000)
		UNION ALL
		(SELECT 'rcr' as tbl, INTERVAL_DATETIME FROM rcr WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000)
		UNION ALL
		(SELECT 'price' as tbl, INTERVAL_DATETIME FROM price WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000)
	)
	SELECT
		tbl,
		COUNT(*) as sample_count,
		MIN(INTERVAL_DATETIME) as min_epoch_ms,
		MAX(INTERVAL_DATETIME) as max_epoch_ms,
		-- Convert min/max epoch back to timestamp string for readability (using DuckDB function)
		-- Divide by 1000 to convert ms to seconds for to_timestamp
		to_timestamp(MIN(INTERVAL_DATETIME) / 1000) as min_epoch_ts,
		to_timestamp(MAX(INTERVAL_DATETIME) / 1000) as max_epoch_ts
	FROM epochs
	GROUP BY tbl;
	`
	epochRows, err := conn.QueryContext(ctx, epochCheckSQL)
	if err != nil {
		log.Printf("WARN: Failed to query epoch date ranges: %v", err)
	} else {
		log.Println("  Epoch Date Range Check Results (from sample):")
		log.Printf("  %-10s | %-12s | %-20s | %-20s | %-25s | %-25s\n", "Table", "Sample Count", "Min Epoch MS", "Max Epoch MS", "Min Approx TS (UTC)", "Max Approx TS (UTC)")
		log.Println("  " + strings.Repeat("-", 125))
		for epochRows.Next() {
			var tbl sql.NullString
			var sCnt sql.NullInt64
			var minE, maxE sql.NullInt64
			var minTs, maxTs sql.NullString // Timestamps as strings
			if err := epochRows.Scan(&tbl, &sCnt, &minE, &maxE, &minTs, &maxTs); err != nil {
				log.Printf("  WARN: Error scanning epoch range row: %v", err)
				break
			}
			log.Printf("  %-10s | %-12d | %-20d | %-20d | %-25s | %-25s\n", tbl.String, sCnt.Int64, minE.Int64, maxE.Int64, minTs.String, maxTs.String)
		}
		epochRows.Close()
	}
	// --- *** DIAGNOSTICS END *** ---

	// --- Calculate Combined FPP View (Joins on INT64 now) ---
	calculateFppSQL := `
	CREATE OR REPLACE VIEW fpp_combined AS
	SELECT cf.unit, cf.INTERVAL_DATETIME, cf.cf, r.rcr, r.SERVICE,
		CASE UPPER(r.SERVICE) WHEN 'RAISEREG' THEN p.price_raisereg WHEN 'LOWERREG' THEN p.price_lowerreg ELSE 0.0 END AS price,
		COALESCE(cf.cf, 0) * COALESCE(r.rcr, 0) * CASE UPPER(r.SERVICE) WHEN 'RAISEREG' THEN COALESCE(p.price_raisereg, 0) WHEN 'LOWERREG' THEN COALESCE(p.price_lowerreg, 0) ELSE 0.0 END AS fpp_cost
	FROM fpp_cf cf JOIN rcr r ON cf.INTERVAL_DATETIME = r.INTERVAL_DATETIME JOIN price p ON cf.INTERVAL_DATETIME = p.INTERVAL_DATETIME
	WHERE UPPER(r.SERVICE) IN ('RAISEREG', 'LOWERREG');`
	log.Printf("DuckDB (Analysis): Calculating combined FPP view (joining on epoch ms)...")
	if _, err = conn.ExecContext(ctx, calculateFppSQL); err != nil {
		return fmt.Errorf("failed to calculate combined FPP view: %w\nSQL:\n%s", err, calculateFppSQL)
	}
	log.Println("DuckDB (Analysis): Combined FPP view created.")

	// --- *** DIAGNOSTICS CONTINUED *** ---
	checkJoinSQL := `SELECT COUNT(*) FROM fpp_combined;`
	var joinRowCount int64 = -1
	err = conn.QueryRowContext(ctx, checkJoinSQL).Scan(&joinRowCount)
	if err != nil {
		log.Printf("WARN: Failed to check row count of fpp_combined view: %v", err)
	} else {
		log.Printf("INFO: Row count of fpp_combined view (before date filter): %d", joinRowCount)
		if joinRowCount == 0 {
			log.Printf(">>> WARNING: Joins/SERVICE filter produced 0 rows BEFORE date filtering. Check data overlap/SERVICE values. <<<")
		}
	}
	// --- *** DIAGNOSTICS END *** ---

	// --- Calculate Epoch Milliseconds for Date Range Filter ---
	startFilterTime, _ := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/01 00:00:00", nemLocation)
	endFilterTime, _ := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/30 23:59:59", nemLocation)
	startEpochMS := startFilterTime.UnixMilli()
	endEpochMS := endFilterTime.UnixMilli()
	log.Printf("DuckDB (Analysis): Filtering between Epoch MS: %d (%s) and %d (%s)", startEpochMS, startFilterTime.Format(time.RFC3339), endEpochMS, endFilterTime.Format(time.RFC3339))

	// --- Aggregate FPP over billing period (Using Epoch MS Filter) ---
	aggregateFppSQL := fmt.Sprintf(`
	SELECT unit, SUM(CASE WHEN UPPER(SERVICE) = 'RAISEREG' THEN fpp_cost ELSE 0 END) AS total_raise_fpp, SUM(CASE WHEN UPPER(SERVICE) = 'LOWERREG' THEN fpp_cost ELSE 0 END) AS total_lower_fpp, SUM(fpp_cost) AS total_fpp
	FROM fpp_combined WHERE INTERVAL_DATETIME BETWEEN %d AND %d GROUP BY unit ORDER BY unit;`, startEpochMS, endEpochMS)

	log.Printf("DuckDB (Analysis): Aggregating FPP results for April 2025 (using epoch ms)...")
	rows, err := conn.QueryContext(ctx, aggregateFppSQL)
	if err != nil {
		return fmt.Errorf("failed to execute final FPP aggregation query: %w\nSQL:\n%s", err, aggregateFppSQL)
	}
	defer rows.Close()
	log.Println("--- FPP Calculation Results ---")
	log.Printf("%-20s | %-20s | %-20s | %-20s\n", "Unit", "Total Raise FPP", "Total Lower FPP", "Total FPP")
	log.Println(strings.Repeat("-", 85))
	rowCount := 0
	for rows.Next() {
		var unit string
		var totalRaise, totalLower, totalFpp sql.NullFloat64
		if err := rows.Scan(&unit, &totalRaise, &totalLower, &totalFpp); err != nil {
			log.Printf("ERROR: Failed to scan result row: %v", err)
			continue
		}
		printFloat := func(f sql.NullFloat64) string {
			if f.Valid {
				return fmt.Sprintf("%.4f", f.Float64)
			}
			return "NULL"
		}
		log.Printf("%-20s | %-20s | %-20s | %-20s\n", unit, printFloat(totalRaise), printFloat(totalLower), printFloat(totalFpp))
		rowCount++
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating result rows: %w", err)
	}
	if rowCount == 0 {
		log.Println("No FPP results found for the specified date range.")
		log.Println("ADVICE: Check diagnostic logs. Reasons could be:")
		log.Println("  1. No data overlap in the required Parquet files for April 2025 (check epoch ranges).")
		log.Println("  2. Joins failed or SERVICE values ('RAISEREG'/'LOWERREG') not found.")
		log.Println("  3. All calculated fpp_cost values were zero or NULL within the date range.")
	}
	log.Println(strings.Repeat("-", 85))
	return nil
}

// --- File Download and Extraction ---
// (downloadAndExtractLatest function remains the same)
func downloadAndExtractLatest(dir string) error {
	client := &http.Client{Timeout: 120 * time.Second}
	var overallErr error
	filesExtractedCount := 0
	for _, baseURL := range feedURLs {
		log.Printf(" Checking feed: %s", baseURL)
		resp, err := client.Get(baseURL)
		if err != nil {
			log.Printf("   WARN: Failed GET %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("GET %s: %w", baseURL, err))
			continue
		}
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Printf("   WARN: Failed read body %s: %v. Skipping URL.", baseURL, readErr)
			overallErr = errors.Join(overallErr, fmt.Errorf("read body %s: %w", baseURL, readErr))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("   WARN: Bad status '%s' for %s. Skipping URL.", resp.Status, baseURL)
			overallErr = errors.Join(overallErr, fmt.Errorf("bad status %s for %s", resp.Status, baseURL))
			continue
		}
		root, err := html.Parse(bytes.NewReader(bodyBytes))
		if err != nil {
			log.Printf("   WARN: Failed parse HTML %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse HTML %s: %w", baseURL, err))
			continue
		}
		base, err := url.Parse(baseURL)
		if err != nil {
			log.Printf("   ERROR: Failed parse base URL %s: %v. Skipping URL.", baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("parse base URL %s: %w", baseURL, err))
			continue
		}
		links := parseLinks(root, ".zip")
		if len(links) == 0 {
			log.Printf("   INFO: No *.zip files found linked at %s", baseURL)
			continue
		}
		sort.Strings(links)
		latestRelative := links[len(links)-1]
		latestURL, err := base.Parse(latestRelative)
		if err != nil {
			log.Printf("   WARN: Failed resolve ZIP URL '%s' relative to %s: %v. Skipping ZIP.", latestRelative, baseURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("resolve ZIP URL %s: %w", latestRelative, err))
			continue
		}
		zipURL := latestURL.String()
		log.Printf("   Found latest: %s", zipURL)
		log.Printf("   Downloading %s ...", zipURL)
		data, err := downloadFile(client, zipURL)
		if err != nil {
			log.Printf("   WARN: Failed download %s: %v. Skipping ZIP.", zipURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("download %s: %w", zipURL, err))
			continue
		}
		log.Printf("   Downloaded %d bytes.", len(data))
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			log.Printf("   WARN: Cannot open downloaded data from %s as ZIP: %v. Skipping ZIP.", zipURL, err)
			overallErr = errors.Join(overallErr, fmt.Errorf("open zip %s: %w", zipURL, err))
			continue
		}
		extractedFromThisZip := 0
		for _, f := range zr.File {
			if f.FileInfo().IsDir() || !strings.EqualFold(filepath.Ext(f.Name), ".csv") {
				continue
			}
			cleanBaseName := filepath.Base(f.Name)
			if cleanBaseName == "" || cleanBaseName == "." || cleanBaseName == ".." || strings.HasPrefix(cleanBaseName, ".") {
				log.Printf("   WARN: Skipping potentially unsafe or hidden file in zip: %s", f.Name)
				continue
			}
			outPath := filepath.Join(dir, cleanBaseName)
			in, err := f.Open()
			if err != nil {
				log.Printf("   ERROR: Failed open '%s' within zip: %v", f.Name, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("open zip entry %s: %w", f.Name, err))
				continue
			}
			out, err := os.Create(outPath)
			if err != nil {
				in.Close()
				log.Printf("   ERROR: Failed create output file %s: %v", outPath, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("create output %s: %w", outPath, err))
				continue
			}
			copiedBytes, err := io.Copy(out, in)
			in.Close()
			out.Close()
			if err != nil {
				log.Printf("   ERROR: Failed copy data for '%s' to %s: %v", f.Name, outPath, err)
				overallErr = errors.Join(overallErr, fmt.Errorf("copy %s: %w", f.Name, err))
				os.Remove(outPath)
				continue
			}
			log.Printf("     Extracted '%s' (%d bytes) to %s", f.Name, copiedBytes, outPath)
			extractedFromThisZip++
			filesExtractedCount++
		}
		if extractedFromThisZip == 0 {
			log.Printf("   INFO: No *.csv files found or extracted within %s", zipURL)
		}
	}
	log.Printf(" Finished download/extract phase. Extracted %d CSV files total.", filesExtractedCount)
	if filesExtractedCount == 0 && overallErr == nil {
		log.Println(" WARN: No CSV files were extracted from any source feeds.")
	}
	return overallErr
}

// --- Utility Functions ---
// (parseLinks, downloadFile functions remain the same)
func parseLinks(n *html.Node, suffix string) []string {
	var out []string
	var walk func(*html.Node)
	walk = func(nd *html.Node) {
		if nd.Type == html.ElementNode && nd.Data == "a" {
			for _, a := range nd.Attr {
				if a.Key == "href" {
					if strings.HasSuffix(strings.ToLower(a.Val), strings.ToLower(suffix)) {
						out = append(out, a.Val)
					}
				}
			}
		}
		for c := nd.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return out
}
func downloadFile(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET %s failed: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		limitReader := io.LimitReader(resp.Body, 1024)
		bodyBytes, _ := io.ReadAll(limitReader)
		return nil, fmt.Errorf("bad status '%s' fetching %s: %s", resp.Status, url, string(bodyBytes))
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading body from %s: %w", url, err)
	}
	return bodyBytes, nil
}

// --- Peekable Scanner Wrapper ---
// (PeekableScanner struct and methods remain the same)
type PeekableScanner struct {
	scanner    *bufio.Scanner
	peekedLine *string
	peekErr    error
	file       *os.File
	context    context.Context
}

func NewPeekableScanner(f *os.File) *PeekableScanner {
	return &PeekableScanner{scanner: bufio.NewScanner(f), file: f, context: context.Background()}
}
func (ps *PeekableScanner) Scan() bool {
	if ps.peekedLine != nil {
		ps.peekedLine = nil
		ps.peekErr = nil
		return true
	}
	return ps.scanner.Scan()
}
func (ps *PeekableScanner) Text() string {
	if ps.peekedLine != nil {
		return *ps.peekedLine
	}
	return ps.scanner.Text()
}
func (ps *PeekableScanner) Err() error {
	if ps.peekErr != nil {
		return ps.peekErr
	}
	return ps.scanner.Err()
}
func (ps *PeekableScanner) PeekRecordType() (string, error) {
	if ps.peekedLine != nil {
		return ps.extractRecordType(*ps.peekedLine), ps.peekErr
	}
	if ps.scanner.Scan() {
		line := ps.scanner.Text()
		ps.peekedLine = &line
		ps.peekErr = nil
		return ps.extractRecordType(line), nil
	} else {
		ps.peekErr = ps.scanner.Err()
		ps.peekedLine = nil
		return "", ps.peekErr
	}
}
func (ps *PeekableScanner) extractRecordType(line string) string {
	trimmedLine := strings.TrimSpace(line)
	if trimmedLine == "" {
		return ""
	}
	parts := strings.SplitN(trimmedLine, ",", 2)
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}
func (ps *PeekableScanner) Buffer(buf []byte, max int) { ps.scanner.Buffer(buf, max) }
