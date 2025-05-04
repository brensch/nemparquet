package config

// DefaultFeedURLs lists the standard NEMWEB "Current" directories containing individual data zip files.
// Can be overridden by the --feed-url flag.
var DefaultFeedURLs = []string{
	// "https://nemweb.com.au/Reports/Current/FPP/",
	// "https://nemweb.com.au/Reports/Current/FPPDAILY/",
	// "https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	// "https://nemweb.com.au/Reports/Current/PD7Day/",
	// "https://nemweb.com.au/Reports/Current/P5_Reports/",
}

// DefaultArchiveFeedURLs lists the NEMWEB "Archive" directories potentially containing zip files of zip files.
// Can be overridden by the --archive-feed-url flag.
var DefaultArchiveFeedURLs = []string{
	// "https://nemweb.com.au/Reports/Archive/FPPDAILY/",
	// "https://nemweb.com.au/Reports/Archive/FPPRATES/",
	// "https://nemweb.com.au/Reports/Archive/FPPRUN/", // Contains historical FPP_RCR data
	// "https://nemweb.com.au/Reports/Archive/P5_Reports/",
}

const (
	// DefaultSchemaRowLimit specifies the maximum number of 'D' rows
	// to examine within a CSV section when inferring the Parquet schema,
	// especially when trying to find a row without blank values.
	DefaultSchemaRowLimit = 100
)

// Config holds application settings derived from flags or a potential config file.
type Config struct {
	// InputDir specifies the directory where downloaded individual data zip files are stored.
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

	// FeedURLs lists the base URLs for "Current" data containing individual data zips.
	FeedURLs []string

	// ArchiveFeedURLs lists the base URLs for "Archive" data potentially containing zips of zips.
	ArchiveFeedURLs []string

	// SchemaRowLimit corresponds to DefaultSchemaRowLimit, potentially configurable later.
	SchemaRowLimit int
}
