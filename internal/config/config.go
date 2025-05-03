package config

// DefaultFeedURLs lists the standard NEMWEB directories to check for data.
// This can be overridden by the --feed-url flag.
var DefaultFeedURLs = []string{
	"https://nemweb.com.au/Reports/Current/FPP/",
	"https://nemweb.com.au/Reports/Current/FPPDAILY/",
	"https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	"https://nemweb.com.au/Reports/Current/PD7Day/",
	"https://nemweb.com.au/Reports/Current/P5_Reports/",
	// Add archive URLs here if needed, or pass via flags
}

const (
	// DefaultSchemaRowLimit specifies the maximum number of 'D' rows
	// to examine within a CSV section when inferring the Parquet schema,
	// especially when trying to find a row without blank values.
	DefaultSchemaRowLimit = 100
)

// Config holds application settings derived from flags or a potential config file.
// These fields are typically populated during the PersistentPreRunE phase of the root command.
type Config struct {
	// InputDir specifies the directory where downloaded zip files are stored.
	// It's also scanned by the processor for zip files to process.
	InputDir string

	// OutputDir specifies the directory where generated Parquet files are written.
	OutputDir string

	// DbPath specifies the path to the DuckDB database file used for storing
	// the event log history (e.g., download/process status).
	// Can be ":memory:" for an in-memory database (state lost on exit).
	DbPath string

	// NumWorkers determines the number of concurrent goroutines used for the
	// processing phase (unzipping and converting CSVs to Parquet).
	// Defaults to the number of logical CPUs.
	NumWorkers int

	// FeedURLs lists the base URLs to scan for discovering zip files.
	// Populated from DefaultFeedURLs or the --feed-url flag.
	FeedURLs []string

	// SchemaRowLimit corresponds to DefaultSchemaRowLimit, potentially configurable later.
	SchemaRowLimit int

	// Add other potential settings here, e.g.:
	// - Date Ranges for filtering downloads/processing
	// - Specific analysis parameters
	// - Overwrite flags (though often handled by command-specific flags like --force)
}
