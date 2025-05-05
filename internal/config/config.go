package config

import (
	"encoding/json" // Import encoding/json
	"fmt"
	"os" // Import os
)

// DefaultFeedURLs lists the standard NEMWEB "Current" directories containing individual data zip files.
// Can be overridden by the --feed-url flag.
var DefaultFeedURLs = []string{
	"https://nemweb.com.au/Reports/Current/FPP/",
	"https://nemweb.com.au/Reports/Current/FPPDAILY/",
	"https://nemweb.com.au/Reports/Current/FPPRATES/",
	"https://nemweb.com.au/Reports/Current/FPPRUN/",
	"https://nemweb.com.au/Reports/Current/PD7Day/",
	"https://nemweb.com.au/Reports/Current/P5_Reports/",
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
	// NOTE: This is no longer used with predefined schemas but kept for context.
	DefaultSchemaRowLimit = 100
)

// --- New Schema Definition Structs ---

// ColumnDefinition defines the name and DuckDB type for a single column.
type ColumnDefinition struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// TableSchema defines the expected schema for a specific table identifier.
type TableSchema struct {
	// BaseIdentifier is the key (e.g., "GROUP__TABLE__VERSION")
	Columns []ColumnDefinition `json:"columns"`
	// Add other metadata if needed, e.g., primary keys, constraints
}

// SchemaConfig holds the map of predefined schemas loaded from a file.
// Key: Base table identifier (e.g., "GROUP__TABLE__VERSION")
// Value: TableSchema
type SchemaConfig map[string]TableSchema

// --- End New Schema Definition Structs ---

// Config holds application settings derived from flags or a potential config file.
type Config struct {
	InputDir        string
	OutputDir       string
	DbPath          string
	NumWorkers      int
	FeedURLs        []string
	ArchiveFeedURLs []string
	// SchemaRowLimit int // No longer needed for inference

	// --- New Field for Predefined Schemas ---
	// This map holds the schemas loaded from the schema file.
	// Key: Base table identifier (e.g., "FPP__FPP_RUN__1")
	PredefinedSchemas SchemaConfig `json:"-"` // Exclude from direct config file loading if config file != schema file

	// --- New Field for Schema File Path ---
	SchemaFilePath string `json:"-"` // Path provided by flag, not usually in main config file
}

// LoadSchemaFile loads the predefined schemas from the specified JSON file path.
func LoadSchemaFile(filePath string) (SchemaConfig, error) {
	if filePath == "" {
		// No schema file provided, return empty map and no error
		return make(SchemaConfig), nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file '%s': %w", filePath, err)
	}

	var schemas SchemaConfig
	err = json.Unmarshal(data, &schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema file '%s': %w", filePath, err)
	}

	// Optional: Validate loaded schemas (e.g., check for empty columns, valid types)
	for id, schema := range schemas {
		if len(schema.Columns) == 0 {
			return nil, fmt.Errorf("invalid schema definition for '%s': no columns defined", id)
		}
		for _, col := range schema.Columns {
			if col.Name == "" || col.Type == "" {
				return nil, fmt.Errorf("invalid column definition in schema '%s': name or type is empty", id)
			}
			// Add more type validation if needed
		}
	}

	return schemas, nil
}
