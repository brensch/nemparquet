package cmd

import (
	"context"
	"fmt"

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/orchestrator" // Use new orchestrator

	"github.com/spf13/cobra"
)

// Flags for the run command (can override persistent flags if needed)
var forceDownloadRun bool
var forceProcessRun bool

// runCmd represents the combined download and process command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the full download and process workflow",
	Long: `Performs the complete data pipeline:
1. Discovers all zip URLs from configured feeds.
2. Checks the database to identify missing downloads and pending processing jobs.
3. Sequentially downloads missing zip files to the input directory.
4. Concurrently processes downloaded zip files (extracting CSVs and converting to Parquet).
Use --force-download to re-download all discovered files.
Use --force-process to re-process all downloaded files.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := getLogger()
		db := getDB()
		cfg := getConfig()

		// Get flag values specifically for this command run
		forceDownloadFlag, _ := cmd.Flags().GetBool("force-download")
		forceProcessFlag, _ := cmd.Flags().GetBool("force-process")

		logger.Info("Starting combined run workflow...")

		// Call the orchestrator function
		err := orchestrator.RunCombinedWorkflow(
			context.Background(), // Use background context for now
			cfg,
			db,
			logger,
			forceDownloadFlag,
			forceProcessFlag,
		)

		if err != nil {
			logger.Error("Combined workflow completed with errors", "error", err)
			return fmt.Errorf("run workflow failed: %w", err)
		}

		logger.Info("Combined workflow completed successfully.")
		return nil
	},
}

func init() {
	// Add flags specific to the 'run' command
	runCmd.Flags().BoolVar(&forceDownloadRun, "force-download", false, "Force download of all discovered archives, ignoring DB state.")
	runCmd.Flags().BoolVar(&forceProcessRun, "force-process", false, "Force processing of downloaded zips, ignoring DB state.")

	// Add the run command to the root command
	// Ensure downloadCmd and processCmd are removed from root in root.go's init
	// rootCmd.AddCommand(runCmd) // This should be done in root.go's init typically, or here is fine too
}
