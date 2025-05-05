package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"
	"github.com/brensch/nemparquet/internal/orchestrator" // <<< Import orchestrator

	"github.com/lmittmann/tint"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/spf13/cobra"
)

var (
	// Config flags - bound in init()
	cfgFile         string
	inputDir        string
	outputDir       string
	dbPath          string
	workers         int
	logFormat       string
	logLevel        string
	logOutput       string
	feedUrls        []string
	archiveFeedUrls []string
	schemaFile      string

	// Global instances populated in PersistentPreRunE
	rootLogger *slog.Logger
	dbConn     *sql.DB
	appConfig  config.Config
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "nemparquet",
	Short: "Download, process NEM data feeds into Parquet, and run analysis.",
	Long: `NemParquet handles fetching NEM data feeds, converting CSV sections to Parquet,
and running DuckDB analysis. It uses a DuckDB database to track file event history.

Schema definitions are provided via a JSON file using the --schema-file flag.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// --- 1. Initialize Logger (Unchanged) ---
		// ... (logger setup code as before) ...
		var level slog.Level
		switch strings.ToLower(logLevel) {
		case "debug":
			level = slog.LevelDebug
		case "info":
			level = slog.LevelInfo
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		default:
			level = slog.LevelInfo
		}
		var logWriter io.Writer = os.Stderr
		logFileHandle := io.Closer(nil)
		isTerminal := false
		if logOutput != "" && strings.ToLower(logOutput) != "stderr" {
			if strings.ToLower(logOutput) == "stdout" {
				logWriter = os.Stdout
				if fileInfo, _ := os.Stdout.Stat(); (fileInfo.Mode() & os.ModeCharDevice) != 0 {
					isTerminal = true
				}
			} else {
				f, err := os.OpenFile(logOutput, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					return fmt.Errorf("failed to open log file %s: %w", logOutput, err)
				}
				logWriter = f
				logFileHandle = f
				isTerminal = false
			}
		} else {
			if fileInfo, _ := os.Stderr.Stat(); (fileInfo.Mode() & os.ModeCharDevice) != 0 {
				isTerminal = true
			}
		}
		var handler slog.Handler
		logFormatLower := strings.ToLower(logFormat)
		if logFormatLower == "text" {
			handler = tint.NewHandler(logWriter, &tint.Options{Level: level, TimeFormat: time.Kitchen, AddSource: false, NoColor: !isTerminal})
		} else if logFormatLower == "json" {
			handler = slog.NewJSONHandler(logWriter, &slog.HandlerOptions{Level: level, AddSource: true})
		} else {
			handler = slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: level, AddSource: false})
		}
		rootLogger = slog.New(handler)
		slog.SetDefault(rootLogger)
		rootLogger.Info("Logger initialized", "level", level.String(), "format", logFormat, "output", logOutput, "colors_enabled", isTerminal && logFormatLower == "text")

		// --- 2. Load/Validate Config (from flags) ---
		// Load predefined schemas first
		loadedSchemas, schemaLoadErr := config.LoadSchemaFile(schemaFile)
		if schemaLoadErr != nil {
			rootLogger.Error("Failed to load or parse schema file.", "path", schemaFile, "error", schemaLoadErr)
			return fmt.Errorf("schema file error: %w", schemaLoadErr)
		}
		// *** Require schema file if this is the intended mode ***
		if schemaFile == "" {
			rootLogger.Error("Schema file path is required (--schema-file)")
			return fmt.Errorf("--schema-file flag must be provided")
		}
		if len(loadedSchemas) == 0 && schemaFile != "" {
			// File specified but empty or contained no valid schemas
			rootLogger.Warn("Schema file loaded but contains no valid schema definitions.", "path", schemaFile)
			// Decide if this is an error or just a warning
		} else {
			rootLogger.Info("Successfully loaded predefined schemas.", "path", schemaFile, "count", len(loadedSchemas))
		}

		appConfig = config.Config{
			InputDir:          inputDir,
			OutputDir:         outputDir,
			DbPath:            dbPath,
			NumWorkers:        workers,
			FeedURLs:          feedUrls,
			ArchiveFeedURLs:   archiveFeedUrls,
			PredefinedSchemas: loadedSchemas,
			SchemaFilePath:    schemaFile,
		}
		rootLogger.Debug("Configuration loaded", slog.Any("config_flags", appConfig))
		if appConfig.InputDir == "" || appConfig.OutputDir == "" || appConfig.DbPath == "" {
			return fmt.Errorf("--input-dir, --output-dir, and --db-path flags are required")
		}
		for _, d := range []string{appConfig.InputDir, appConfig.OutputDir} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", d, err)
			}
		}
		if appConfig.DbPath != ":memory:" {
			dbDir := filepath.Dir(appConfig.DbPath)
			if err := os.MkdirAll(dbDir, 0o755); err != nil {
				return fmt.Errorf("failed to create database directory %s: %w", dbDir, err)
			}
		}

		// --- 3. Initialize DuckDB Connection ---
		rootLogger.Info("Initializing DuckDB connection", "path", appConfig.DbPath)
		var err error
		dbConn, err = sql.Open("duckdb", appConfig.DbPath)
		if err != nil {
			return fmt.Errorf("failed to open duckdb database (%s): %w", appConfig.DbPath, err)
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err = dbConn.PingContext(pingCtx); err != nil {
			dbConn.Close()
			return fmt.Errorf("failed to ping duckdb database (%s): %w", appConfig.DbPath, err)
		}
		rootLogger.Info("DuckDB connection successful.")

		// --- 4. Initialize Event Log Schema ---
		if err := db.InitializeSchema(dbConn); err != nil {
			dbConn.Close()
			return fmt.Errorf("failed to initialize event log schema: %w", err)
		}
		rootLogger.Info("Event log schema initialized successfully.")

		// --- 5. Pre-create Schemas from Config ---
		// Use a separate context for schema creation? Or main app context? Using background for now.
		schemaCtx, schemaCancel := context.WithTimeout(context.Background(), 2*time.Minute) // Timeout for schema creation
		defer schemaCancel()
		// Call the function from the orchestrator package (or move it to db package if preferred)
		if err := orchestrator.CreateAllPredefinedSchemas(schemaCtx, dbConn, appConfig.PredefinedSchemas, rootLogger); err != nil {
			dbConn.Close()
			rootLogger.Error("Failed to create predefined schemas in database.", "error", err)
			return fmt.Errorf("failed to create predefined schemas: %w", err)
		}
		rootLogger.Info("Predefined schema creation step completed.")

		// --- 6. Setup PostRun for Cleanup (Unchanged) ---
		cmd.PersistentPostRunE = func(cmd *cobra.Command, args []string) error {
			if dbConn != nil {
				rootLogger.Info("Closing DuckDB connection.")
				if err := dbConn.Close(); err != nil {
					rootLogger.Error("Failed to close DuckDB connection cleanly", "error", err)
				}
			}
			if logFileHandle != nil {
				rootLogger.Debug("Closing log file handle.")
				if err := logFileHandle.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "Error closing log file: %v\n", err)
				}
			}
			return nil
		}

		return nil
	},
}

// Execute (Unchanged)
func Execute() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(inspectCmd)
	// rootCmd.AddCommand(analyseCmd)
	rootCmd.AddCommand(stateCmd)
	rootCmd.AddCommand(saveCmd)

	err := rootCmd.Execute()
	if err != nil {
		if rootLogger != nil {
			rootLogger.Error("Command execution failed", "error", err)
		} else {
			fmt.Fprintf(os.Stderr, "Command execution failed: %v\n", err)
		}
		os.Exit(1)
	}
}

// init (Unchanged - already has schemaFile flag)
func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.nemparquet.yaml) (Not implemented yet)")
	rootCmd.PersistentFlags().StringVarP(&inputDir, "input-dir", "i", "./input_csv", "Directory for downloaded zip files")
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output-dir", "o", "./output_parquet", "Directory for generated Parquet files")
	rootCmd.PersistentFlags().StringVarP(&dbPath, "db-path", "d", "./nemparquet_state.duckdb", "Path to DuckDB state database file (:memory: for in-memory)")
	rootCmd.PersistentFlags().IntVarP(&workers, "workers", "w", runtime.NumCPU(), "Number of concurrent processing workers")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log output format (text or json)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logOutput, "log-output", "stderr", "Log output destination (stderr, stdout, or file path)")
	rootCmd.PersistentFlags().StringSliceVar(&feedUrls, "feed-url", config.DefaultFeedURLs, "Feed URLs for current data (can specify multiple)")
	rootCmd.PersistentFlags().StringSliceVar(&archiveFeedUrls, "archive-feed-url", config.DefaultArchiveFeedURLs, "Feed URLs for archive data (can specify multiple)")
	rootCmd.PersistentFlags().StringVar(&schemaFile, "schema-file", "", "Path to JSON file containing predefined table schemas")

	rootCmd.Version = "0.4.0" // Incremented version
}

// Helper functions (Unchanged)
func getLogger() *slog.Logger {
	if rootLogger == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return rootLogger
}
func getDB() *sql.DB           { return dbConn }
func getConfig() config.Config { return appConfig }
