package config

import "runtime"

// Default feed URLs
var DefaultFeedURLs = []string{
	"https://nemweb.com.au/Reports/Current/FPP/",
	"https://nemweb.com.au/Reports/Current/FPPDAILY/",
	"https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	"https://nemweb.com.au/Reports/Current/PD7Day/",
	"https://nemweb.com.au/Reports/Current/P5_Reports/",
	// Add archive URLs here if needed
}

const (
	// Default maximum number of 'D' rows to check for schema inference.
	DefaultSchemaRowLimit = 100
)

var (
	// Default number of workers, often set to CPU count.
	DefaultNumWorkers = runtime.NumCPU()
)

// Config holds application settings
type Config struct {
	InputDir       string
	OutputDir      string
	DbPath         string
	NumWorkers     int
	FeedURLs       []string
	SchemaRowLimit int
	// Add other settings like overwrite flags, specific dates, etc. if needed
}

// Add functions here later to load config from file, flags, or env vars
