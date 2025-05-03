package app

// AppState represents the different views/modes of the application.
type AppState int

const (
	ShowMenu AppState = iota
	DownloadingFiles
	ProcessingFiles
	InspectingFiles
	AnalyzingData
	ShowError
	Exiting
)
