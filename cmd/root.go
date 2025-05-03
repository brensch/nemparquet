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

	"github.com/lmittmann/tint"         // Import the tint handler
	_ "github.com/marcboeker/go-duckdb" // DuckDB driver
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

The primary command is 'run', which orchestrates the download and processing workflow.
Other commands allow inspecting data, running analysis separately, or viewing state.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// --- 1. Initialize Logger ---
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

		var logWriter io.Writer = os.Stderr // Default to stderr
		logFileHandle := io.Closer(nil)     // Keep track of file handle if opened
		isTerminal := false                 // Flag to check if output is a terminal

		if logOutput != "" && strings.ToLower(logOutput) != "stderr" {
			if strings.ToLower(logOutput) == "stdout" {
				logWriter = os.Stdout
				// Check if stdout is a terminal
				if fileInfo, _ := os.Stdout.Stat(); (fileInfo.Mode() & os.ModeCharDevice) != 0 {
					isTerminal = true
				}
			} else {
				// Attempt to open file
				f, err := os.OpenFile(logOutput, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					return fmt.Errorf("failed to open log file %s: %w", logOutput, err)
				}
				logWriter = f
				logFileHandle = f  // Store handle for potential closing later
				isTerminal = false // File output is not a terminal
			}
		} else {
			// Defaulting to stderr, check if it's a terminal
			if fileInfo, _ := os.Stderr.Stat(); (fileInfo.Mode() & os.ModeCharDevice) != 0 {
				isTerminal = true
			}
		}

		var handler slog.Handler
		logFormatLower := strings.ToLower(logFormat)

		// *** Use tint handler for text format if output is a terminal ***
		if logFormatLower == "text" {
			handler = tint.NewHandler(logWriter, &tint.Options{
				Level:      level,
				TimeFormat: time.Kitchen, // Example time format
				AddSource:  false,        // Disable source code location for cleaner logs
				NoColor:    !isTerminal,  // Disable color if not writing to a terminal
			})
		} else if logFormatLower == "json" {
			handler = slog.NewJSONHandler(logWriter, &slog.HandlerOptions{
				Level:     level,
				AddSource: true, // Optionally add source for JSON
			})
		} else {
			// Fallback to default text handler if format is unknown
			handler = slog.NewTextHandler(logWriter, &slog.HandlerOptions{
				Level:     level,
				AddSource: false,
			})
		}

		rootLogger = slog.New(handler)
		slog.SetDefault(rootLogger)
		rootLogger.Info("Logger initialized", "level", level.String(), "format", logFormat, "output", logOutput, "colors_enabled", isTerminal && logFormatLower == "text")

		// --- 2. Load/Validate Config (from flags) ---
		appConfig = config.Config{ /* ... */
			InputDir: inputDir, OutputDir: outputDir, DbPath: dbPath, NumWorkers: workers, FeedURLs: feedUrls, ArchiveFeedURLs: archiveFeedUrls, SchemaRowLimit: config.DefaultSchemaRowLimit,
		}
		rootLogger.Debug("Configuration loaded", slog.Any("config", appConfig))
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

		// --- 3. Initialize DuckDB Connection & Schema ---
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
		if err := db.InitializeSchema(dbConn); err != nil {
			dbConn.Close()
			return fmt.Errorf("failed to initialize database schema: %w", err)
		}
		rootLogger.Info("Database schema initialized successfully.")

		// Handle closing the log file if it was opened (best effort in PostRun)
		// Note: This relies on the logFileHandle variable being accessible in PostRun.
		// A cleaner way might involve storing it in the command's context or a global registry.
		cmd.PersistentPostRunE = func(cmd *cobra.Command, args []string) error {
			if dbConn != nil {
				rootLogger.Info("Closing DuckDB connection.")
				if err := dbConn.Close(); err != nil {
					rootLogger.Error("Failed to close DuckDB connection cleanly", "error", err)
				}
			}
			if logFileHandle != nil {
				rootLogger.Debug("Closing log file handle.") // Log before closing
				if err := logFileHandle.Close(); err != nil {
					// Log error closing file, but don't fail command exit
					fmt.Fprintf(os.Stderr, "Error closing log file: %v\n", err)
				}
			}
			return nil
		}

		return nil
	},
	// Remove PersistentPostRunE from here if defined within PersistentPreRunE
	// PersistentPostRunE: func(cmd *cobra.Command, args []string) error { ... },
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	// Add child commands here
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(inspectCmd)
	rootCmd.AddCommand(analyseCmd)
	rootCmd.AddCommand(stateCmd)

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

func init() {
	// Define persistent flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.nemparquet.yaml) (Not implemented yet)")
	rootCmd.PersistentFlags().StringVarP(&inputDir, "input-dir", "i", "./input_csv", "Directory for downloaded zip files")
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output-dir", "o", "./output_parquet", "Directory for generated Parquet files")
	rootCmd.PersistentFlags().StringVarP(&dbPath, "db-path", "d", "./nemparquet_state.duckdb", "Path to DuckDB state database file (:memory: for in-memory)")
	rootCmd.PersistentFlags().IntVarP(&workers, "workers", "w", runtime.NumCPU(), "Number of concurrent workers for processing phase")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log output format (text or json)") // Keep text as default
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logOutput, "log-output", "stderr", "Log output destination (stderr, stdout, or file path)")
	rootCmd.PersistentFlags().StringSliceVar(&feedUrls, "feed-url", config.DefaultFeedURLs, "Feed URLs to fetch discovery info from (can specify multiple)")
	rootCmd.PersistentFlags().StringSliceVar(&archiveFeedUrls, "archive-feed-url", config.DefaultArchiveFeedURLs, "Feed URLs to fetch archive discovery info from (archives)")

	rootCmd.Version = "0.2.2" // Incremented version
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
