package cmd

import (
	"fmt"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/inspector"

	"github.com/spf13/cobra"
)

// inspectCmd represents the inspect command
var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Inspect schema and row counts of generated Parquet files using DuckDB",
	Long:  `Connects to DuckDB (using the state DB path or in-memory) and inspects all *.parquet files found in the output directory. It shows the schema and row count for each file.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := getLogger()
		// Inspector might need config for OutputDir and potentially DbPath
		cfg := getConfig()

		logger.Info("Starting Parquet file inspection...")

		// Pass logger and relevant config (DB path could be different for inspection if needed)
		err := inspector.InspectDuckDB(cfg, logger) // Pass logger
		if err != nil {
			logger.Error("Inspection completed with errors", "error", err)
			return fmt.Errorf("inspection failed: %w", err)
		}

		logger.Info("Parquet inspection completed successfully.")
		return nil
	},
}

// No specific flags for inspect needed currently
func init() {
}
