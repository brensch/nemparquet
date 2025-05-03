package analyser

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/brensch/nemparquet/internal/config" // Use your module name
	"github.com/brensch/nemparquet/internal/util"

	_ "github.com/marcboeker/go-duckdb"
)

// RunAnalysis connects to DuckDB and performs FPP analysis.
func RunAnalysis(cfg config.Config) error {
	log.Println("--- Starting DuckDB FPP Analysis ---")
	ctx := context.Background() // Use background context for analysis

	db, err := sql.Open("duckdb", cfg.DbPath) // Use path from config
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", cfg.DbPath, err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()

	log.Println("DuckDB (Analysis): Installing and loading Parquet extension...")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
		// Allow continuing if maybe already loaded, but log warning
		log.Printf("WARN: Failed to install/load parquet extension: %v. Analysis might fail.", err)
		// Return error as analysis requires it
		return fmt.Errorf("install/load parquet extension: %w", err)
	} else {
		log.Println("DuckDB (Analysis): Parquet extension loaded.")
	}

	// DuckDB path formatting using OutputDir from config
	duckdbParquetDir := strings.ReplaceAll(cfg.OutputDir, `\`, `/`)

	// --- View Definitions (Unchanged SQL logic, uses formatted path) ---
	createCfViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW fpp_cf AS SELECT FPP_UNITID AS unit, INTERVAL_DATETIME, TRY_CAST(CONTRIBUTION_FACTOR AS DOUBLE) AS cf FROM read_parquet('%s/*CONTRIBUTION_FACTOR*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)
	createRcrViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW rcr AS SELECT INTERVAL_DATETIME, BIDTYPE AS SERVICE, TRY_CAST(RCR AS DOUBLE) AS rcr FROM read_parquet('%s/*FPP_RCR*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)
	createPriceViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW price AS SELECT INTERVAL_DATETIME, REGIONID, TRY_CAST(RAISEREGRRP AS DOUBLE) AS price_raisereg, TRY_CAST(LOWERREGRRP AS DOUBLE) AS price_lowerreg FROM read_parquet('%s/*PRICESOLUTION*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)

	// Execute View Creation
	log.Println("DuckDB (Analysis): Creating view fpp_cf...")
	if _, err = conn.ExecContext(ctx, createCfViewSQL); err != nil {
		return fmt.Errorf("create view fpp_cf: %w\nSQL:\n%s", err, createCfViewSQL)
	}
	log.Println("DuckDB (Analysis): Creating view rcr...")
	if _, err = conn.ExecContext(ctx, createRcrViewSQL); err != nil {
		return fmt.Errorf("create view rcr: %w\nSQL:\n%s", err, createRcrViewSQL)
	}
	log.Println("DuckDB (Analysis): Creating view price...")
	if _, err = conn.ExecContext(ctx, createPriceViewSQL); err != nil {
		return fmt.Errorf("create view price: %w\nSQL:\n%s", err, createPriceViewSQL)
	}
	log.Println("DuckDB (Analysis): Views created.")

	// --- Diagnostics (Unchanged logic) ---
	log.Println("DuckDB (Analysis): Checking distinct SERVICE values...")
	// ... (distinct service query and logging) ...
	distinctServiceSQL := `SELECT DISTINCT SERVICE FROM rcr ORDER BY 1 NULLS LAST LIMIT 20;`
	serviceRows, err := conn.QueryContext(ctx, distinctServiceSQL)
	if err != nil {
		log.Printf("WARN: Failed query distinct service values: %v", err)
	} else {
		log.Println("  Distinct SERVICE values (limit 20):")
		// ... (scan and print logic) ...
		serviceRows.Close()
	}

	log.Println("DuckDB (Analysis): Checking epoch date ranges (INT64)...")
	// ... (epoch check query and logging using util.GetNEMLocation() for parsing check times) ...
	epochCheckSQL := `
    WITH epochs AS (
        (SELECT 'fpp_cf' as tbl, INTERVAL_DATETIME FROM fpp_cf WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000) UNION ALL
        (SELECT 'rcr' as tbl, INTERVAL_DATETIME FROM rcr WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000) UNION ALL
        (SELECT 'price' as tbl, INTERVAL_DATETIME FROM price WHERE INTERVAL_DATETIME IS NOT NULL LIMIT 10000)
    ) SELECT tbl, COUNT(*) as sample_count, MIN(INTERVAL_DATETIME) as min_epoch_ms, MAX(INTERVAL_DATETIME) as max_epoch_ms
    FROM epochs GROUP BY tbl;`
	epochRows, err := conn.QueryContext(ctx, epochCheckSQL)
	if err != nil {
		log.Printf("WARN: Failed query epoch date ranges: %v", err)
	} else {
		log.Println("  Epoch Date Range Check Results (from sample):")
		log.Printf("    %-10s | %-12s | %-20s | %-20s | %-25s | %-25s\n", "Table", "Sample Count", "Min Epoch MS", "Max Epoch MS", "Min Approx TS (UTC)", "Max Approx TS (UTC)")
		log.Println("    " + strings.Repeat("-", 125))
		// ... (scan and print logic, converting epoch ms to time.Time for display) ...
		for epochRows.Next() {
			var tbl sql.NullString
			var sCnt sql.NullInt64
			var minE, maxE sql.NullInt64
			if err := epochRows.Scan(&tbl, &sCnt, &minE, &maxE); err != nil {
				log.Printf("    WARN: Error scanning epoch range row: %v", err)
				break
			}
			minTsUTC := time.Unix(0, 0) // Default
			maxTsUTC := time.Unix(0, 0) // Default
			if minE.Valid {
				minTsUTC = time.UnixMilli(minE.Int64).UTC()
			}
			if maxE.Valid {
				maxTsUTC = time.UnixMilli(maxE.Int64).UTC()
			}

			log.Printf("    %-10s | %-12d | %-20d | %-20d | %-25s | %-25s\n",
				tbl.String, sCnt.Int64, minE.Int64, maxE.Int64,
				minTsUTC.Format(time.RFC3339), maxTsUTC.Format(time.RFC3339))
		}
		epochRows.Close()
	}

	// --- FPP Calculation View (Unchanged SQL logic) ---
	calculateFppSQL := `
    CREATE OR REPLACE VIEW fpp_combined AS
    SELECT cf.unit, cf.INTERVAL_DATETIME, cf.cf, r.rcr, r.SERVICE,
        COALESCE(p.price_raisereg, 0.0) AS price_raisereg,
        COALESCE(p.price_lowerreg, 0.0) AS price_lowerreg,
        CASE UPPER(r.SERVICE) WHEN 'RAISEREG' THEN COALESCE(p.price_raisereg, 0.0) WHEN 'LOWERREG' THEN COALESCE(p.price_lowerreg, 0.0) ELSE 0.0 END AS service_price,
        COALESCE(cf.cf, 0.0) * COALESCE(r.rcr, 0.0) * CASE UPPER(r.SERVICE) WHEN 'RAISEREG' THEN COALESCE(p.price_raisereg, 0.0) WHEN 'LOWERREG' THEN COALESCE(p.price_lowerreg, 0.0) ELSE 0.0 END AS fpp_cost
    FROM fpp_cf cf
    JOIN rcr r ON cf.INTERVAL_DATETIME = r.INTERVAL_DATETIME
    JOIN price p ON cf.INTERVAL_DATETIME = p.INTERVAL_DATETIME
    WHERE UPPER(r.SERVICE) IN ('RAISEREG', 'LOWERREG');`
	log.Println("DuckDB (Analysis): Calculating combined FPP view...")
	if _, err = conn.ExecContext(ctx, calculateFppSQL); err != nil {
		return fmt.Errorf("calculate combined FPP view: %w\nSQL:\n%s", err, calculateFppSQL)
	}
	log.Println("DuckDB (Analysis): Combined FPP view created.")

	// Diagnostics: Check Join Count
	checkJoinSQL := `SELECT COUNT(*) FROM fpp_combined;`
	var joinRowCount int64 = -1
	err = conn.QueryRowContext(ctx, checkJoinSQL).Scan(&joinRowCount)
	if err != nil {
		log.Printf("WARN: Failed check row count of fpp_combined view: %v", err)
	} else {
		log.Printf("INFO: Row count of fpp_combined view (before date filter): %d", joinRowCount)
		if joinRowCount == 0 {
			log.Printf(">>> WARNING: Joins/SERVICE filter produced 0 rows. Check data overlap and SERVICE values. <<<")
		}
	}

	// --- Aggregation (Unchanged logic, uses util.GetNEMLocation) ---
	// Calculate Epoch Milliseconds for Date Range Filter (April 2025 NEM time)
	nemLocation := util.GetNEMLocation() // Use util func
	startFilterTime, startErr := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/01 00:00:00", nemLocation)
	endFilterTime, endErr := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/30 23:59:59", nemLocation)

	if startErr != nil || endErr != nil {
		// Log error but maybe proceed with a default? Or return error? Return error is safer.
		return fmt.Errorf("failed to parse filter dates: startErr=%v, endErr=%v", startErr, endErr)
	}

	startEpochMS := startFilterTime.UnixMilli()
	endEpochMS := endFilterTime.UnixMilli()
	log.Printf("DuckDB (Analysis): Filtering between Epoch MS: %d (%s) and %d (%s) (NEM Time)", startEpochMS, startFilterTime.Format(time.RFC3339), endEpochMS, endFilterTime.Format(time.RFC3339))

	aggregateFppSQL := fmt.Sprintf(`
    SELECT unit,
        SUM(CASE WHEN UPPER(SERVICE) = 'RAISEREG' THEN fpp_cost ELSE 0 END) AS total_raise_fpp,
        SUM(CASE WHEN UPPER(SERVICE) = 'LOWERREG' THEN fpp_cost ELSE 0 END) AS total_lower_fpp,
        SUM(fpp_cost) AS total_fpp
    FROM fpp_combined
    WHERE INTERVAL_DATETIME >= %d AND INTERVAL_DATETIME <= %d
    GROUP BY unit
    ORDER BY unit;`, startEpochMS, endEpochMS)

	log.Println("DuckDB (Analysis): Aggregating FPP results for April 2025...")
	rows, err := conn.QueryContext(ctx, aggregateFppSQL)
	if err != nil {
		return fmt.Errorf("execute final FPP aggregation: %w\nSQL:\n%s", err, aggregateFppSQL)
	}
	defer rows.Close()

	// Log results to standard log - UI won't show these directly unless piped
	log.Println("--- FPP Calculation Results (April 2025 NEM Time) ---")
	log.Printf("%-20s | %-20s | %-20s | %-20s\n", "Unit", "Total Raise FPP", "Total Lower FPP", "Total FPP")
	log.Println(strings.Repeat("-", 85))
	rowCount := 0
	var analysisErrors error
	for rows.Next() {
		var unit string
		var totalRaise, totalLower, totalFpp sql.NullFloat64
		if err := rows.Scan(&unit, &totalRaise, &totalLower, &totalFpp); err != nil {
			log.Printf("ERROR: Failed to scan result row: %v", err)
			analysisErrors = errors.Join(analysisErrors, fmt.Errorf("scan result row: %w", err))
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
		analysisErrors = errors.Join(analysisErrors, fmt.Errorf("iterate result rows: %w", err))
	}

	if rowCount == 0 {
		log.Println("No FPP results found for the specified date range (April 2025 NEM Time). Check diagnostics.")
	}
	log.Println(strings.Repeat("-", 85))
	log.Println("--- DuckDB FPP Analysis Finished ---")

	return analysisErrors // Return any errors encountered during result processing
}
