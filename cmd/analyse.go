package cmd

import (
	"context"
	"fmt"

	// Use your actual module path and spelling
	"github.com/brensch/nemparquet/internal/analyser"

	"github.com/spf13/cobra"
)

// analyseCmd represents the analyse command
var analyseCmd = &cobra.Command{
	Use:   "analyse", // Use 's'
	Short: "Run pre-defined FPP analysis on Parquet data using DuckDB",
	Long:  `Connects to DuckDB and executes a pre-defined analysis script (currently FPP calculation for April 2025) using the Parquet files in the output directory as input views.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := getLogger()
		// Analyser needs config for OutputDir and DbPath
		cfg := getConfig()

		logger.Info("Starting DuckDB FPP analysis...")

		// Pass logger and config
		err := analyser.RunAnalysis(context.Background(), cfg, logger) // Pass context, logger
		if err != nil {
			logger.Error("Analysis completed with errors", "error", err)
			return fmt.Errorf("analysis failed: %w", err)
		}

		logger.Info("DuckDB analysis completed successfully.")
		return nil
	},
}

// No specific flags for analyse needed currently
func init() {
}
