package processor

// import (
// 	"bufio"
// 	"bytes" // Needed for capturing logs
// 	"context"
// 	"fmt"
// 	"io"
// 	"log/slog"
// 	"os"
// 	"path/filepath"
// 	"strings"
// 	"sync"
// 	"testing"

// 	// Use your actual module path for config and util
// 	"github.com/brensch/nemparquet/internal/config"
// 	// "github.com/brensch/nemparquet/internal/util" // Not directly needed

// 	// Import parquet reader
// 	"github.com/xitongsys/parquet-go-source/local"
// 	"github.com/xitongsys/parquet-go/reader"
// )

// // --- Log Capturing Handler ---

// type capturingHandler struct {
// 	mu      sync.Mutex
// 	records []slog.Record
// 	next    slog.Handler
// }

// func NewCapturingHandler(next slog.Handler) *capturingHandler {
// 	if next == nil {
// 		next = slog.NewTextHandler(io.Discard, nil)
// 	}
// 	return &capturingHandler{next: next}
// }
// func (h *capturingHandler) Enabled(ctx context.Context, level slog.Level) bool {
// 	return h.next.Enabled(ctx, level)
// }
// func (h *capturingHandler) Handle(ctx context.Context, r slog.Record) error {
// 	h.mu.Lock()
// 	h.records = append(h.records, r.Clone())
// 	h.mu.Unlock()
// 	// return h.next.Handle(ctx, r) // Uncomment to also print logs during test
// 	return nil
// }
// func (h *capturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
// 	return NewCapturingHandler(h.next.WithAttrs(attrs))
// }
// func (h *capturingHandler) WithGroup(name string) slog.Handler {
// 	return NewCapturingHandler(h.next.WithGroup(name))
// }
// func (h *capturingHandler) GetRecordsByLevel(minLevel slog.Level) []slog.Record {
// 	h.mu.Lock()
// 	defer h.mu.Unlock()
// 	filtered := make([]slog.Record, 0)
// 	for _, r := range h.records {
// 		if r.Level >= minLevel {
// 			filtered = append(filtered, r.Clone())
// 		}
// 	}
// 	return filtered
// }

// // --- Main Test Function ---

// // TestProcessCSVStream_Directory reads all *.csv files in the current directory
// // and runs processCSVStream on each, verifying logs and output.
// func TestProcessCSVStream_Directory(t *testing.T) {
// 	// --- Test Setup ---
// 	ctx := context.Background()
// 	tempDir := t.TempDir() // Unique output dir for this test run
// 	t.Logf("Using temporary output directory: %s", tempDir)

// 	cfg := config.Config{OutputDir: tempDir, SchemaRowLimit: 100} // Use default limit or adjust

// 	// Find test CSV files in the current directory (".")
// 	csvFiles, err := filepath.Glob("*.csv") // Look for CSVs in the same dir as the test
// 	if err != nil {
// 		t.Fatalf("Failed to glob for csv files in current directory: %v", err)
// 	}
// 	if len(csvFiles) == 0 {
// 		t.Skipf("No *.csv files found in the current directory (%s), skipping test", getWd(t))
// 	}
// 	t.Logf("Found test files: %v", csvFiles)

// 	// --- Execute and Assert for each file ---
// 	for _, csvFilename := range csvFiles {
// 		// Use relative path for subtest name
// 		csvFilename := csvFilename // Capture loop variable for subtest

// 		t.Run(csvFilename, func(t *testing.T) {
// 			// Create a new logger and handler for each subtest
// 			logBuf := &bytes.Buffer{}
// 			// handler := NewCapturingHandler(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})) // For debugging test
// 			handler := NewCapturingHandler(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})) // Capture WARN+
// 			testLogger := slog.New(handler)

// 			// Read the specific CSV test file content
// 			csvData, err := os.ReadFile(csvFilename)
// 			if err != nil {
// 				t.Fatalf("Failed to read test file %s: %v", csvFilename, err)
// 			}
// 			csvReader := bytes.NewReader(csvData)

// 			// --- Count expected 'D' rows per section dynamically ---
// 			dRowCount := make(map[string]int64)
// 			currentSectionKey := ""
// 			// Use a simple scanner, not the PeekableScanner for counting
// 			scanner := bufio.NewScanner(strings.NewReader(string(csvData)))
// 			lineNum := 0
// 			for scanner.Scan() {
// 				lineNum++
// 				line := scanner.Text()
// 				parts := strings.Split(line, ",")
// 				if len(parts) > 0 {
// 					recordType := strings.TrimSpace(parts[0])
// 					if recordType == "I" && len(parts) >= 4 {
// 						comp := strings.TrimSpace(parts[2])
// 						ver := strings.TrimSpace(parts[3])
// 						currentSectionKey = fmt.Sprintf("%s_v%s", comp, ver)
// 						dRowCount[currentSectionKey] = 0 // Initialize/reset count
// 					} else if recordType == "D" && currentSectionKey != "" {
// 						dRowCount[currentSectionKey]++ // Increment count for current section
// 					}
// 				}
// 			}
// 			if err := scanner.Err(); err != nil {
// 				t.Fatalf("Error counting D rows in %s: %v", csvFilename, err)
// 			}
// 			t.Logf("Counted D rows for %s: %v", csvFilename, dRowCount)
// 			if len(dRowCount) == 0 {
// 				t.Logf("No 'I' sections found or no 'D' rows found in %s", csvFilename)
// 				// Decide if this is an error or just an empty/non-standard file to skip further checks
// 			}
// 			// --- End Counting ---

// 			// Define the base name for output files from the CSV filename
// 			zipBaseName := strings.TrimSuffix(csvFilename, filepath.Ext(csvFilename))

// 			// --- Execute ---
// 			processErr := processCSVStream(ctx, cfg, testLogger, csvReader, zipBaseName, cfg.OutputDir)

// 			// --- Assertions ---
// 			// 1. Check for direct processing error
// 			if processErr != nil {
// 				t.Errorf("processCSVStream for %s returned an unexpected error: %v", csvFilename, processErr)
// 				// Optionally print captured logs on error for easier debugging
// 				capturedRecordsOnError := handler.GetRecordsByLevel(slog.LevelDebug)
// 				if len(capturedRecordsOnError) > 0 {
// 					t.Logf("--- Captured Logs for %s on Error ---", csvFilename)
// 					for _, rec := range capturedRecordsOnError {
// 						t.Logf("[%s] %s", rec.Level, rec.Message) // Basic log format
// 					}
// 					t.Logf("--- End Logs ---")
// 				}
// 			}

// 			// 2. Check captured logs for unexpected WARN or ERROR level messages
// 			capturedRecords := handler.GetRecordsByLevel(slog.LevelWarn)
// 			for _, rec := range capturedRecords {
// 				// Allow the "Inferring schema from row with blanks" warning
// 				if strings.Contains(rec.Message, "Inferring schema from row with blanks") {
// 					t.Logf("Found expected 'inferring from blanks' warning for %s: %s", csvFilename, rec.Message)
// 					continue // Don't fail the test for this specific warning
// 				}
// 				// Fail on any other WARN or ERROR
// 				t.Errorf("Found unexpected WARN/ERROR log message for %s: Level=%s Message=%q", csvFilename, rec.Level, rec.Message)
// 			}

// 			// 3. Verify Parquet file existence and row counts for sections with D rows
// 			if len(dRowCount) > 0 {
// 				for sectionKey, expectedDRows := range dRowCount {
// 					// Construct expected parquet filename: zipBaseName_Comp_vVer.parquet
// 					parquetFilename := fmt.Sprintf("%s_%s.parquet", zipBaseName, sectionKey)
// 					parquetPath := filepath.Join(tempDir, parquetFilename)

// 					if _, statErr := os.Stat(parquetPath); statErr != nil {
// 						// Only fail if we expected rows for this section
// 						if expectedDRows > 0 {
// 							t.Errorf("Expected output file %s was not found for %s: %v", parquetFilename, csvFilename, statErr)
// 						} else {
// 							t.Logf("Output file %s not found, but 0 D rows were expected.", parquetFilename)
// 						}
// 						continue // Skip row count check if file doesn't exist
// 					}

// 					// If file exists but we expected 0 rows, that's also potentially an issue
// 					if expectedDRows == 0 {
// 						t.Errorf("Output file %s exists, but 0 D rows were expected for this section in %s.", parquetFilename, csvFilename)
// 						continue
// 					}

// 					// Read row count from Parquet file
// 					fr, err := local.NewLocalFileReader(parquetPath)
// 					if err != nil {
// 						t.Errorf("Failed to open generated parquet file %s: %v", parquetFilename, err)
// 						continue
// 					}
// 					pr, err := reader.NewParquetReader(fr, nil, 1)
// 					if err != nil {
// 						t.Errorf("Failed to create parquet reader for %s: %v", parquetFilename, err)
// 						fr.Close()
// 						continue
// 					}

// 					parquetRowCount := pr.GetNumRows()
// 					pr.ReadStop() // Important to stop reader goroutines
// 					fr.Close()

// 					// Compare counts
// 					if parquetRowCount != expectedDRows {
// 						t.Errorf("Row count mismatch for %s: expected %d 'D' rows, got %d Parquet rows", parquetFilename, expectedDRows, parquetRowCount)
// 					} else {
// 						t.Logf("Row count verified for %s: %d rows", parquetFilename, parquetRowCount)
// 					}
// 				}
// 			} else {
// 				// Check if any unexpected parquet files were created for this zipBaseName
// 				files, _ := filepath.Glob(filepath.Join(tempDir, zipBaseName+"_*.parquet"))
// 				if len(files) > 0 {
// 					t.Errorf("No D rows counted in %s, but Parquet files were created: %v", csvFilename, files)
// 				}
// 			}

// 		}) // End subtest
// 	} // End loop through CSV files
// }

// // Helper to get working directory for logging
// func getWd(t *testing.T) string {
// 	wd, err := os.Getwd()
// 	if err != nil {
// 		t.Logf("Warning: could not get working directory: %v", err)
// 		return "[unknown]"
// 	}
// 	return wd
// }
