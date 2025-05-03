package main

import (
	"fmt"
	"log"
	"os"

	"github.com/brensch/nemparquet/internal/app" // Use your actual module name
	"github.com/brensch/nemparquet/internal/config"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	// --- Configuration Setup ---
	// Use flags or env vars for real app, simplified here
	cfg := config.Config{
		InputDir:       "./input_csv",
		OutputDir:      "./output_parquet",
		DbPath:         ":memory:",               // Use a file for persistence: "./nem_data.duckdb"
		NumWorkers:     config.DefaultNumWorkers, // Uses runtime.NumCPU()
		FeedURLs:       config.DefaultFeedURLs,
		SchemaRowLimit: config.DefaultSchemaRowLimit,
	}

	// Ensure directories exist (moved from original main)
	for _, d := range []string{cfg.InputDir, cfg.OutputDir} {
		if d != "" && d != ":memory:" { // Don't try to create ":memory:"
			if err := os.MkdirAll(d, 0o755); err != nil {
				log.Fatalf("FATAL: Failed to create directory %s: %v", d, err)
			}
		}
	}
	// --- End Configuration Setup ---

	// --- Bubbletea Setup ---
	// Use a log file for background task logging, as bubbletea takes over stdout
	logFile, err := tea.LogToFile("debug.log", "nemparquet")
	if err != nil {
		fmt.Println("Failed to open log file:", err)
		os.Exit(1)
	}
	defer logFile.Close()
	log.SetOutput(logFile) // Redirect standard logger
	log.Println("--- Application Starting ---")
	log.Printf("Config: %+v", cfg)

	initialModel := app.NewAppModel(cfg)
	p := tea.NewProgram(initialModel, tea.WithAltScreen()) // Use AltScreen for cleaner exit

	// Run returns the final model state and error
	finalModel, err := p.Run()
	if err != nil {
		fmt.Printf("Error running Bubbletea program: %v\n", err)
		os.Exit(1)
	}

	// You can optionally inspect the finalModel if needed
	if m, ok := finalModel.(*app.AppModel); ok {
		if m.FatalErr != nil {
			fmt.Printf("\nApplication finished with fatal error: %v\n", m.FatalErr)
			// Log the error as well
			log.Printf("FATAL ERROR: %v", m.FatalErr)
			os.Exit(1)
		} else if m.Quitting {
			fmt.Println("\nExited gracefully.")
		} else {
			fmt.Println("\nFinished.") // Or handle other final states
		}
	}
}

// Helper function to create a default progress bar style
func DefaultProgressBar() progress.Model {
	// Example using the default progress bar
	p := progress.New(progress.WithDefaultGradient())
	// You might want to customize width, colors etc.
	// p.Width = // Set width based on terminal size later
	return p
}
