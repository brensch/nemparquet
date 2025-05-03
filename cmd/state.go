package cmd

import (
	"context"
	"fmt"
	"strings" // Import strings

	// Use your actual module path
	"github.com/brensch/nemparquet/internal/db"

	"github.com/spf13/cobra"
)

// Flags remain the same
var stateLimit int
var stateFilterStatus string

// stateCmd represents the command to view DB state (updated description)
var stateCmd = &cobra.Command{
	Use:   "state [filetype]",
	Short: "View the event log history for tracked files (zips or csvs)",
	Long: `Queries the DuckDB event log and displays the history for tracked files.
Specify 'zips' or 'csvs' as an optional argument to filter by file type.
Use flags to filter by event type (status) and limit the output.`,
	Args: cobra.MaximumNArgs(1), // 0 or 1 argument
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := getLogger()
		dbConn := getDB()
		fileTypeFilter := ""
		if len(args) > 0 {
			fileType := strings.ToLower(args[0])
			if fileType == "zips" || fileType == "zip" {
				fileTypeFilter = db.FileTypeZip
			} else if fileType == "csvs" || fileType == "csv" {
				fileTypeFilter = db.FileTypeCsv
			} else {
				return fmt.Errorf("invalid filetype filter: %s (use 'zips' or 'csvs')", args[0])
			}
		}

		logger.Info("Querying database event log", "type_filter", fileTypeFilter, "event_filter", stateFilterStatus, "limit", stateLimit)

		// Call the updated display function
		err := db.DisplayFileHistory(context.Background(), dbConn, fileTypeFilter, stateFilterStatus, stateLimit)

		if err != nil {
			logger.Error("Failed to display state history", "error", err)
			return err
		}

		return nil
	},
}

func init() {
	stateCmd.Flags().IntVarP(&stateLimit, "limit", "n", 50, "Limit the number of log records displayed")
	// Rename status flag to event flag for clarity
	stateCmd.Flags().StringVarP(&stateFilterStatus, "event", "e", "", "Filter records by event type (e.g., download_end, error, process_start)")
}
