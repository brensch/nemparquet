package cmd

import (
	"fmt"
	"log/slog" // Import slog

	// Use your actual module path
	// Import config
	"github.com/brensch/nemparquet/internal/saver" // Import the new saver package

	"github.com/spf13/cobra"
)

// saveCmd represents the save command
var saveCmd = &cobra.Command{
	Use:   "save",
	Short: "Saves tables from the DuckDB database to Parquet files",
	Long: `Connects to the DuckDB database specified in the configuration
and saves each table found into a separate Parquet file in the configured output directory.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := getLogger() // Assuming getLogger() exists in your cmd package
		cfg := getConfig()    // Assuming getConfig() exists in your cmd package

		// Validate essential config
		if cfg.DbPath == "" {
			return fmt.Errorf("database path (DbPath) not configured")
		}
		if cfg.OutputDir == "" {
			return fmt.Errorf("output directory (OutputDir) not configured")
		}

		logger.Info("Starting table save process...",
			slog.String("db_path", cfg.DbPath),
			slog.String("output_dir", cfg.OutputDir),
		)

		err := saver.SaveTablesToParquet(cfg, logger) // Pass config and logger
		if err != nil {
			logger.Error("Save process completed with errors", "error", err)
			return fmt.Errorf("save failed: %w", err) // Wrap error for context
		}

		logger.Info("Table save process completed successfully.")
		return nil
	},
}

// Add flags specific to the save command here if needed in the future
func init() {
	// Example flag:
	// saveCmd.Flags().StringP("output", "o", "", "Override output directory (defaults to config)")
	// viper.BindPFlag("outputDirOverride", saveCmd.Flags().Lookup("output")) // Example Viper binding

	// Add saveCmd to the root command in your root.go or main application file
	// e.g., rootCmd.AddCommand(saveCmd)
}
