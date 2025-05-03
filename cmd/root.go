package cmd

import (
	"context" // Keep context import
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	// "time" // No longer needed directly here

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/db"

	_ "github.com/marcboeker/go-duckdb" // DuckDB driver
	"github.com/spf13/cobra"
	// "github.com/spf13/pflag" // No longer explicitly needed if not using advanced pflag features
)

var (
	// Config flags - bound in init()
	cfgFile   string
	inputDir  string
	outputDir string
	dbPath    string
	workers   int
	logFormat string
	logLevel  string
	logOutput string
	feedUrls  []string

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
		if logOutput != "" && strings.ToLower(logOutput) != "stderr" {
			if strings.ToLower(logOutput) == "stdout" {
				logWriter = os.Stdout
			} else {
				// Attempt to open file
				// Use O_APPEND|O_CREATE|O_WRONLY for appending to log file
				f, err := os.OpenFile(logOutput, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					return fmt.Errorf("failed to open log file %s: %w", logOutput, err)
				}
				// Note: File handle 'f' is not explicitly closed here.
				// For a CLI tool that exits, this is often acceptable as the OS cleans up.
				// For long-running services, proper closure is essential.
				logWriter = f
			}
		}

		opts := &slog.HandlerOptions{Level: level}
		var handler slog.Handler
		if logFormat == "json" {
			handler = slog.NewJSONHandler(logWriter, opts)
		} else {
			handler = slog.NewTextHandler(logWriter, opts)
		}
		rootLogger = slog.New(handler)
		slog.SetDefault(rootLogger) // Set for packages using global slog
		rootLogger.Info("Logger initialized", "level", level.String(), "format", logFormat, "output", logOutput)

		// --- 2. Load/Validate Config (from flags) ---
		// More sophisticated apps would use Viper or similar here, potentially loading from cfgFile
		appConfig = config.Config{
			InputDir:       inputDir,
			OutputDir:      outputDir,
			DbPath:         dbPath,
			NumWorkers:     workers,
			FeedURLs:       feedUrls,
			SchemaRowLimit: config.DefaultSchemaRowLimit, // Consider making this a flag too
		}
		rootLogger.Debug("Configuration loaded", slog.Any("config", appConfig))

		// Validate essential paths
		if appConfig.InputDir == "" || appConfig.OutputDir == "" || appConfig.DbPath == "" {
			return fmt.Errorf("--input-dir, --output-dir, and --db-path flags are required")
		}

		// Ensure directories exist
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
		// Ping to ensure connection is valid immediately
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Add timeout to ping
		defer cancel()
		if err = dbConn.PingContext(pingCtx); err != nil {
			dbConn.Close() // Close if ping fails
			return fmt.Errorf("failed to ping duckdb database (%s): %w", appConfig.DbPath, err)
		}
		rootLogger.Info("DuckDB connection successful.")

		// Initialize DB Schema
		if err := db.InitializeSchema(dbConn); err != nil {
			dbConn.Close()
			return fmt.Errorf("failed to initialize database schema: %w", err)
		}
		rootLogger.Info("Database schema initialized successfully.")

		return nil // Return nil on success
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		// Close DB connection if it was opened
		if dbConn != nil {
			rootLogger.Info("Closing DuckDB connection.")
			if err := dbConn.Close(); err != nil {
				// Log error but don't necessarily fail the command exit status
				rootLogger.Error("Failed to close DuckDB connection cleanly", "error", err)
				// return err // Optionally return error
			}
		}
		// Close log file? Still tricky with Cobra's lifecycle.
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	// Add child commands here
	rootCmd.AddCommand(runCmd)     // Combined workflow command
	rootCmd.AddCommand(inspectCmd) // Inspect parquet files
	rootCmd.AddCommand(analyseCmd) // Run FPP analysis
	rootCmd.AddCommand(stateCmd)   // View DB state log

	err := rootCmd.Execute()
	if err != nil {
		// Cobra usually prints the error, but log it just in case
		if rootLogger != nil {
			rootLogger.Error("Command execution failed", "error", err)
		} else {
			// Fallback if logger wasn't initialized
			fmt.Fprintf(os.Stderr, "Command execution failed: %v\n", err)
		}
		os.Exit(1)
	}
}

func init() {
	// Define persistent flags available to root and all subcommands
	// Use PersistentFlags() for flags common to all commands
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.nemparquet.yaml) (Not implemented yet)")
	rootCmd.PersistentFlags().StringVarP(&inputDir, "input-dir", "i", "./input_csv", "Directory for downloaded zip files")
	rootCmd.PersistentFlags().StringVarP(&outputDir, "output-dir", "o", "./output_parquet", "Directory for generated Parquet files")
	rootCmd.PersistentFlags().StringVarP(&dbPath, "db-path", "d", "./nemparquet_state.duckdb", "Path to DuckDB state database file (:memory: for in-memory)")
	rootCmd.PersistentFlags().IntVarP(&workers, "workers", "w", runtime.NumCPU(), "Number of concurrent workers for processing phase")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log output format (text or json)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logOutput, "log-output", "stderr", "Log output destination (stderr, stdout, or file path)")
	rootCmd.PersistentFlags().StringSliceVar(&feedUrls, "feed-url", config.DefaultFeedURLs, "Feed URLs to fetch discovery info from (can specify multiple)")

	// Add version flag (handled automatically by Cobra if Version field is set)
	rootCmd.Version = "0.2.0" // Update your version
	// rootCmd.Flags().BoolP("version", "v", false, "Print version information and exit") // Cobra adds this if Version is set

	// Bind persistent flags to viper config if using viper later
	// Ex: cobra.OnInitialize(initConfig)
	// func initConfig() { viper.BindPFlag(...) }
}

// Helper to get logger (could use context propagation instead)
func getLogger() *slog.Logger {
	if rootLogger == nil {
		// Fallback if PersistentPreRun hasn't run (e.g., during tests or init failures)
		// Consider returning a disabled logger or panicking based on requirements
		return slog.New(slog.NewTextHandler(io.Discard, nil)) // Discard logs if not initialized
	}
	return rootLogger
}

// Helper to get DB connection (could use context propagation instead)
func getDB() *sql.DB {
	// Add nil check? Should always be populated after PersistentPreRunE succeeds.
	return dbConn
}

// Helper to get Config (could use context propagation instead)
func getConfig() config.Config {
	// Add validation check? Should always be populated after PersistentPreRunE succeeds.
	return appConfig
}
