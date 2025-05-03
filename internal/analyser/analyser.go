package analyser // Use 's' spelling

import (
	"context"
	"database/sql"
	"errors" // Import errors for joining
	"fmt"
	"log/slog"
	"strings"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/util"

	_ "github.com/marcboeker/go-duckdb"
)

// Helper function to query and log row counts for diagnostics
func logViewRowCount(ctx context.Context, conn *sql.Conn, logger *slog.Logger, viewName, filterClause string) {
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s;", viewName, filterClause)
	var count int64 = -1 // Default to -1 to indicate query failure
	err := conn.QueryRowContext(ctx, countSQL).Scan(&count)
	if err != nil {
		logger.Warn("Diagnostic: Failed to get row count", slog.String("view", viewName), "error", err)
	} else {
		logger.Debug("Diagnostic: Row count", slog.String("view", viewName), slog.String("filter", filterClause), slog.Int64("count", count))
	}
}

// RunAnalysis connects to DuckDB, runs FPP analysis, uses slog.
func RunAnalysis(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	logger.Info("--- Starting DuckDB FPP Analysis ---")

	db, err := sql.Open("duckdb", cfg.DbPath) // Use configured DB path
	if err != nil {
		return fmt.Errorf("failed to open duckdb database (%s): %w", cfg.DbPath, err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection from pool: %w", err)
	}
	defer conn.Close()

	logger.Debug("Installing and loading Parquet extension for analysis.")
	setupSQL := `INSTALL parquet; LOAD parquet;`
	if _, err := conn.ExecContext(ctx, setupSQL); err != nil {
		logger.Warn("Failed to install/load parquet extension. Analysis might fail.", "error", err)
	} else {
		logger.Debug("Parquet extension loaded (or already available).")
	}

	duckdbParquetDir := strings.ReplaceAll(cfg.OutputDir, `\`, `/`)

	// --- Calculate Date Range Filter ---
	nemLocation := util.GetNEMLocation()
	startFilterTime, startErr := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/01 00:00:00", nemLocation)
	endFilterTime, endErr := time.ParseInLocation("2006/01/02 15:04:05", "2025/04/30 23:59:59", nemLocation)
	if startErr != nil || endErr != nil {
		return fmt.Errorf("failed parse filter dates: start=%v, end=%v", startErr, endErr)
	}
	startEpochMS := startFilterTime.UnixMilli()
	endEpochMS := endFilterTime.UnixMilli()
	dateFilterClause := fmt.Sprintf("INTERVAL_DATETIME >= %d AND INTERVAL_DATETIME <= %d", startEpochMS, endEpochMS)
	logger.Info("Analysis date range", slog.Time("start_nem", startFilterTime), slog.Time("end_nem", endFilterTime), slog.Int64("start_epoch_ms", startEpochMS), slog.Int64("end_epoch_ms", endEpochMS))

	// --- View Definitions ---
	createCfViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW fpp_cf AS SELECT FPP_UNITID AS unit, INTERVAL_DATETIME, TRY_CAST(CONTRIBUTION_FACTOR AS DOUBLE) AS cf FROM read_parquet('%s/*CONTRIBUTION_FACTOR*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)
	createRcrViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW rcr AS SELECT INTERVAL_DATETIME, BIDTYPE AS SERVICE, TRY_CAST(RCR AS DOUBLE) AS rcr FROM read_parquet('%s/*FPP_RCR*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)
	createPriceViewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW price AS SELECT INTERVAL_DATETIME, REGIONID, TRY_CAST(RAISEREGRRP AS DOUBLE) AS price_raisereg, TRY_CAST(LOWERREGRRP AS DOUBLE) AS price_lowerreg FROM read_parquet('%s/*PRICESOLUTION*.parquet', HIVE_PARTITIONING=0);`, duckdbParquetDir)

	// Execute View Creation
	logger.Info("Creating analysis views.")
	if _, err = conn.ExecContext(ctx, createCfViewSQL); err != nil {
		return fmt.Errorf("create view fpp_cf: %w", err)
	}
	if _, err = conn.ExecContext(ctx, createRcrViewSQL); err != nil {
		return fmt.Errorf("create view rcr: %w", err)
	}
	if _, err = conn.ExecContext(ctx, createPriceViewSQL); err != nil {
		return fmt.Errorf("create view price: %w", err)
	}
	logger.Debug("Analysis views created.")

	// --- *** ADDED DIAGNOSTICS: Check counts in base views for the target date range *** ---
	logger.Debug("--- Running Base View Diagnostics ---")
	logViewRowCount(ctx, conn, logger, "fpp_cf", dateFilterClause)
	logViewRowCount(ctx, conn, logger, "rcr", dateFilterClause)
	logViewRowCount(ctx, conn, logger, "price", dateFilterClause)
	logger.Debug("--- Finished Base View Diagnostics ---")
	// --- End Added Diagnostics ---

	// --- Diagnostics (Distinct SERVICE values - Unchanged) ---
	logger.Debug("Checking distinct SERVICE values in rcr view.")
	distinctServiceSQL := `SELECT DISTINCT SERVICE FROM rcr ORDER BY 1 NULLS LAST LIMIT 20;`
	serviceRows, err := conn.QueryContext(ctx, distinctServiceSQL)
	// ... (Rest of distinct SERVICE logging unchanged) ...
	if err != nil {
		logger.Warn("Failed query distinct service values", "error", err)
	} else {
		var services []string
		for serviceRows.Next() {
			var s sql.NullString
			if scanErr := serviceRows.Scan(&s); scanErr == nil {
				if s.Valid {
					services = append(services, s.String)
				} else {
					services = append(services, "NULL")
				}
			} else {
				logger.Warn("Failed scanning distinct service value", "error", scanErr)
			}
		}
		serviceRows.Close()
		logger.Debug("Distinct SERVICE values found", slog.Any("values_sample", services))
		contains := func(slice []string, val string) bool {
			for _, item := range slice {
				if item == val {
					return true
				}
			}
			return false
		}
		if len(services) > 0 && (!contains(services, "RAISEREG") || !contains(services, "LOWERREG")) {
			logger.Warn("Expected SERVICE values 'RAISEREG' or 'LOWERREG' might be missing from sample.")
		} else if len(services) == 0 {
			logger.Warn("No SERVICE values found in rcr view sample. Joins might yield no results.")
		}
	}

	// --- Diagnostics (Epoch date ranges - Unchanged) ---
	logger.Debug("Checking epoch date ranges (INT64) in views.")
	epochCheckSQL := `...` // Same SQL as before
	epochRows, err := conn.QueryContext(ctx, epochCheckSQL)
	// ... (Rest of epoch range logging unchanged) ...
	if err != nil {
		logger.Warn("Failed query epoch date ranges", "error", err)
	} else {
		logger.Debug("Epoch Date Range Check Results (from sample):")
		for epochRows.Next() {
			var tbl sql.NullString
			var sCnt sql.NullInt64
			var minE, maxE sql.NullInt64
			if scanErr := epochRows.Scan(&tbl, &sCnt, &minE, &maxE); scanErr != nil {
				logger.Warn("Error scanning epoch range row", "error", scanErr)
				break
			}
			minTsUTC := time.Unix(0, 0)
			maxTsUTC := time.Unix(0, 0)
			if minE.Valid {
				minTsUTC = time.UnixMilli(minE.Int64).UTC()
			}
			if maxE.Valid {
				maxTsUTC = time.UnixMilli(maxE.Int64).UTC()
			}
			logger.Debug("Range", slog.String("table", tbl.String), slog.Int64("sample_count", sCnt.Int64), slog.Int64("min_epoch_ms", minE.Int64), slog.Int64("max_epoch_ms", maxE.Int64), slog.Time("min_ts_utc", minTsUTC), slog.Time("max_ts_utc", maxTsUTC))
		}
		epochRows.Close()
	}

	// --- FPP Calculation View ---
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
	logger.Info("Calculating combined FPP view.")
	if _, err = conn.ExecContext(ctx, calculateFppSQL); err != nil {
		return fmt.Errorf("calculate combined FPP view: %w", err)
	}
	logger.Debug("Combined FPP view created.")

	// --- *** ADDED DIAGNOSTICS: Check counts in combined view *** ---
	logger.Debug("--- Running Combined View Diagnostics ---")
	logViewRowCount(ctx, conn, logger, "fpp_combined", "1=1")            // Count before date filter
	logViewRowCount(ctx, conn, logger, "fpp_combined", dateFilterClause) // Count after date filter
	logger.Debug("--- Finished Combined View Diagnostics ---")
	// --- End Added Diagnostics ---

	// --- Aggregation ---
	logger.Info("Aggregating FPP results for filter period.") // Log message moved down slightly

	aggregateFppSQL := fmt.Sprintf(`
    SELECT unit,
        SUM(CASE WHEN UPPER(SERVICE) = 'RAISEREG' THEN fpp_cost ELSE 0 END) AS total_raise_fpp,
        SUM(CASE WHEN UPPER(SERVICE) = 'LOWERREG' THEN fpp_cost ELSE 0 END) AS total_lower_fpp,
        SUM(fpp_cost) AS total_fpp
    FROM fpp_combined WHERE %s
    GROUP BY unit ORDER BY unit;`, dateFilterClause) // Use dateFilterClause variable

	rows, err := conn.QueryContext(ctx, aggregateFppSQL)
	if err != nil {
		return fmt.Errorf("execute final FPP aggregation: %w", err)
	}
	defer rows.Close()

	logger.Info("--- FPP Calculation Results (April 2025 NEM Time) ---")
	fmt.Println("--- FPP Results ---")
	fmt.Printf("%-20s | %-20s | %-20s | %-20s\n", "Unit", "Total Raise FPP", "Total Lower FPP", "Total FPP")
	fmt.Println(strings.Repeat("-", 85))

	rowCount := 0
	var analysisErrors error
	for rows.Next() { // ... (Result scanning/printing unchanged) ...
		var unit string
		var totalRaise, totalLower, totalFpp sql.NullFloat64
		if scanErr := rows.Scan(&unit, &totalRaise, &totalLower, &totalFpp); scanErr != nil {
			logger.Error("Failed to scan result row", "error", scanErr)
			analysisErrors = errors.Join(analysisErrors, fmt.Errorf("scan result: %w", scanErr))
			continue
		}
		printFloat := func(f sql.NullFloat64) string {
			if f.Valid {
				return fmt.Sprintf("%.4f", f.Float64)
			}
			return "NULL"
		}
		logger.Debug("FPP Result Row", slog.String("unit", unit), slog.Float64("raise_fpp", totalRaise.Float64), slog.Float64("lower_fpp", totalLower.Float64), slog.Float64("total_fpp", totalFpp.Float64), slog.Bool("raise_valid", totalRaise.Valid), slog.Bool("lower_valid", totalLower.Valid), slog.Bool("total_valid", totalFpp.Valid))
		fmt.Printf("%-20s | %-20s | %-20s | %-20s\n", unit, printFloat(totalRaise), printFloat(totalLower), printFloat(totalFpp))
		rowCount++
	}
	if err = rows.Err(); err != nil {
		analysisErrors = errors.Join(analysisErrors, fmt.Errorf("iterate results: %w", err))
	}
	fmt.Println(strings.Repeat("-", 85))

	if rowCount == 0 {
		logger.Warn("No FPP results found for the specified date range.") // Warning remains
		fmt.Println("No FPP results found for the specified date range.")
	} else {
		logger.Info("Analysis results generated.", slog.Int("result_rows", rowCount))
	}
	logger.Info("--- DuckDB FPP Analysis Finished ---")

	if analysisErrors != nil {
		logger.Warn("Analysis completed with errors during result processing.", "error", analysisErrors)
	}
	return analysisErrors
}
