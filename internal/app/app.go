package app

import (
	"fmt"
	"log"

	// Corrected import paths based on user feedback
	"strings"
	"sync"
	"time"

	"github.com/brensch/nemparquet/internal/analyser" // Corrected spelling
	"github.com/brensch/nemparquet/internal/config"
	"github.com/brensch/nemparquet/internal/downloader"
	"github.com/brensch/nemparquet/internal/inspector"
	"github.com/brensch/nemparquet/internal/processor"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss" // For styling
)

// --- Styles --- (Unchanged)
var (
	titleStyle              = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("62"))
	menuStyle               = lipgloss.NewStyle().PaddingLeft(2)                   // Base padding for all menu items
	selectedStyle           = lipgloss.NewStyle().Foreground(lipgloss.Color("79")) // Style for the selected text only
	errorStyle              = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	infoStyle               = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))
	progressBarStyle        = lipgloss.NewStyle().Padding(0, 1)
	fileProgressHeaderStyle = lipgloss.NewStyle().Bold(true).MarginBottom(1)
	fileStatusStyle         = map[string]lipgloss.Style{
		"Processing":  lipgloss.NewStyle().Foreground(lipgloss.Color("214")),
		"Complete":    lipgloss.NewStyle().Foreground(lipgloss.Color("46")),
		"Skipped":     lipgloss.NewStyle().Foreground(lipgloss.Color("240")),
		"Error":       lipgloss.NewStyle().Foreground(lipgloss.Color("196")),
		"Queued":      lipgloss.NewStyle().Foreground(lipgloss.Color("248")),
		"Downloading": lipgloss.NewStyle().Foreground(lipgloss.Color("39")),
		"Extracting":  lipgloss.NewStyle().Foreground(lipgloss.Color("45")),
	}
)

// --- Model --- (Unchanged)
type FileProgress struct {
	FileName string
	Status   string
	Progress float64
	ErrMsg   string
	Start    time.Time
	Elapsed  time.Duration
}

type AppModel struct {
	Cfg              config.Config
	State            AppState
	menuChoices      []string
	menuCursor       int
	spinner          spinner.Model
	overallProgress  progress.Model
	progressBarWidth int

	mu             sync.RWMutex
	fileProgress   map[string]*FileProgress
	fileOrder      []string
	overallTotal   int64
	overallCurrent int64
	currentTaskTag string
	lastActivity   string
	taskStartTime  time.Time

	lastError error
	FatalErr  error
	Quitting  bool

	termWidth  int
	termHeight int

	uiMsgChan chan tea.Msg
}

func NewAppModel(cfg config.Config) *AppModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	prog := progress.New(progress.WithDefaultGradient())

	return &AppModel{
		Cfg:             cfg,
		State:           ShowMenu,
		menuChoices:     []string{"Download Feeds", "Process CSVs", "Inspect Parquet", "Run FPP Analysis", "Exit"},
		menuCursor:      0,
		spinner:         s,
		overallProgress: prog,
		fileProgress:    make(map[string]*FileProgress),
		fileOrder:       make([]string, 0),
	}
}

// --- Bubbletea Interface ---

func (m *AppModel) Init() tea.Cmd {
	return m.spinner.Tick
}

// Update remains the same as the previous version (with pointer receiver)
func (m *AppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch m.State {
		case ShowMenu:
			cmd = m.handleMenuKey(msg)
			cmds = append(cmds, cmd)
		case ShowError:
			if msg.Type == tea.KeyEnter || msg.Type == tea.KeyEsc || msg.String() == "q" {
				m.State = ShowMenu
				m.lastError = nil
			}
		case Exiting:
			return m, nil
		default: // Active task state
			if msg.String() == "ctrl+c" || msg.String() == "q" {
				log.Println("Ctrl+C / q pressed during task - initiating quit")
				m.Quitting = true
				m.State = Exiting
				return m, tea.Quit
			}
		}
	case tea.WindowSizeMsg:
		m.termWidth = msg.Width
		m.termHeight = msg.Height
		m.progressBarWidth = max(0, m.termWidth-4)
		m.overallProgress.Width = m.progressBarWidth
	case ProgressMsg:
		m.mu.Lock()
		m.currentTaskTag = msg.Tag
		m.overallCurrent = msg.Current
		m.overallTotal = msg.Total
		m.lastActivity = msg.Activity
		m.mu.Unlock()
		var percent float64
		if msg.Total > 0 {
			percent = float64(msg.Current) / float64(msg.Total)
		}
		cmd = m.overallProgress.SetPercent(percent)
		cmds = append(cmds, cmd)
	case FileProgressMsg:
		m.mu.Lock()
		if _, exists := m.fileProgress[msg.FileID]; !exists {
			m.fileProgress[msg.FileID] = &FileProgress{
				FileName: msg.FileName,
				Status:   "Queued",
				Start:    time.Now(),
			}
			m.fileOrder = append(m.fileOrder, msg.FileID)
		}
		fp := m.fileProgress[msg.FileID]
		fp.Status = msg.Status
		fp.ErrMsg = msg.ErrMsg
		if msg.Total > 0 {
			fp.Progress = float64(msg.Current) / float64(msg.Total)
		} else if msg.Status == "Complete" || msg.Status == "Skipped" {
			fp.Progress = 1.0
		}
		if msg.ElapsedTime > 0 {
			fp.Elapsed = msg.ElapsedTime
		} else if (msg.Status == "Complete" || msg.Status == "Skipped" || msg.Status == "Error") && !fp.Start.IsZero() && fp.Elapsed == 0 {
			fp.Elapsed = time.Since(fp.Start)
		}
		m.mu.Unlock()
	case TaskFinishedMsg:
		m.mu.Lock()
		log.Printf("Task '%s' finished processing. Duration: %s", msg.Tag, msg.EndTime.Sub(msg.StartTime).Round(time.Millisecond))
		m.State = ShowMenu
		m.fileProgress = make(map[string]*FileProgress)
		m.fileOrder = make([]string, 0)
		m.overallCurrent = 0
		m.overallTotal = 0
		m.currentTaskTag = ""
		m.lastActivity = ""
		m.uiMsgChan = nil
		m.mu.Unlock()
		if msg.Err != nil {
			log.Printf("Task %s finished with error: %v", msg.Tag, msg.Err)
			m.lastError = fmt.Errorf("Task '%s' failed: %w", msg.Tag, msg.Err)
			m.State = ShowError
		} else {
			log.Printf("Task %s finished successfully. Message: %s", msg.Tag, msg.Message)
		}
	case GeneralErrorMsg:
		log.Printf("General Error: %v", msg.Err)
		m.lastError = msg.Err
		m.State = ShowError
		m.uiMsgChan = nil
	case spinner.TickMsg:
		if m.State != ShowMenu && m.State != ShowError && m.State != Exiting {
			m.spinner, cmd = m.spinner.Update(msg)
			cmds = append(cmds, cmd)
		}
	case progress.FrameMsg:
		if m.State != ShowMenu && m.State != ShowError && m.State != Exiting {
			progModel, frameCmd := m.overallProgress.Update(msg)
			if newModel, ok := progModel.(progress.Model); ok {
				m.overallProgress = newModel
				cmds = append(cmds, frameCmd)
			}
		}
	}

	if m.uiMsgChan != nil {
		cmds = append(cmds, m.waitForActivityCmd(m.uiMsgChan))
	}

	return m, tea.Batch(cmds...)
}

func (m *AppModel) View() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("--- NEM Parquet Converter ---"))
	b.WriteString("\n\n")

	switch m.State {
	case ShowMenu:
		b.WriteString(m.viewMenu()) // Use updated viewMenu
	case DownloadingFiles, ProcessingFiles, InspectingFiles, AnalyzingData:
		b.WriteString(m.viewProgress())
	case ShowError:
		b.WriteString(m.viewError())
	case Exiting:
		b.WriteString(infoStyle.Render("Exiting..."))
	}

	b.WriteString("\n\n")
	if m.State == ShowMenu {
		b.WriteString(infoStyle.Render("Use up/down arrows and Enter to select. 'q' or Ctrl+C to quit."))
	} else if m.State != Exiting && m.State != ShowError {
		b.WriteString(infoStyle.Render("Task running... 'q' or Ctrl+C to force quit."))
	} else if m.State == ShowError {
		b.WriteString(infoStyle.Render("Press Enter or Esc to return to menu. 'q' or Ctrl+C to quit."))
	}

	return b.String()
}

// --- View Helpers ---

// viewMenu - Revised for better alignment control
func (m *AppModel) viewMenu() string {
	var b strings.Builder
	b.WriteString("Select an action:\n")

	// Apply base padding style to each line container
	paddedStyle := menuStyle // Includes PaddingLeft(2)

	for i, choice := range m.menuChoices {
		var lineContent string
		if m.menuCursor == i {
			// Selected line: Cursor + Styled Text
			cursor := "> "
			styledChoice := selectedStyle.Render(choice)
			lineContent = cursor + styledChoice
		} else {
			// Unselected line: Spaces for alignment + Plain Text
			cursor := "  " // Two spaces to align with "> "
			lineContent = cursor + choice
		}
		// Render the constructed line within the padded style
		b.WriteString(paddedStyle.Render(lineContent))
		b.WriteString("\n")
	}
	return b.String()
}

// viewProgress remains the same as previous version
func (m *AppModel) viewProgress() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%s Running Task: %s %s\n", m.spinner.View(), m.currentTaskTag, m.lastActivity))
	b.WriteString(progressBarStyle.Render(m.overallProgress.View()))
	b.WriteString(fmt.Sprintf(" (%d/%d)\n\n", m.overallCurrent, m.overallTotal))

	maxLines := m.termHeight - 10
	if maxLines < 1 {
		maxLines = 1
	}
	startIdx := 0
	if len(m.fileOrder) > maxLines {
		startIdx = len(m.fileOrder) - maxLines
	}

	if len(m.fileOrder) > 0 {
		b.WriteString(fileProgressHeaderStyle.Render(fmt.Sprintf("%-40s | %-15s | %s", "File", "Status", "Elapsed")))
		b.WriteString("\n")
		b.WriteString(strings.Repeat("-", m.termWidth))
		b.WriteString("\n")
		for i := startIdx; i < len(m.fileOrder); i++ {
			fileID := m.fileOrder[i]
			fp := m.fileProgress[fileID]
			if fp == nil {
				continue
			}
			statusStyled, ok := fileStatusStyle[fp.Status]
			if !ok {
				statusStyled = infoStyle
			}
			elapsedStr := ""
			if fp.Elapsed > 0 {
				elapsedStr = fp.Elapsed.Round(time.Millisecond).String()
			} else if !fp.Start.IsZero() && fp.Status != "Queued" && fp.Status != "Skipped" && fp.Status != "Complete" && fp.Status != "Error" {
				elapsedStr = time.Since(fp.Start).Round(time.Second).String() + "..."
			}
			maxFNameWidth := 40
			fileName := fp.FileName
			if len(fileName) > maxFNameWidth {
				fileName = fileName[:maxFNameWidth-3] + "..."
			}
			line := fmt.Sprintf("%-40s | %-15s | %s", fileName, statusStyled.Render(fp.Status), elapsedStr)
			if len(line) >= m.termWidth {
				line = line[:m.termWidth-1]
			}
			b.WriteString(line)
			if fp.Status == "Error" && fp.ErrMsg != "" {
				errMsg := fmt.Sprintf("  -> Error: %s", fp.ErrMsg)
				if len(errMsg) >= m.termWidth {
					errMsg = errMsg[:m.termWidth-1]
				}
				b.WriteString("\n")
				b.WriteString(errorStyle.Render(errMsg))
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

// viewError remains the same as previous version
func (m *AppModel) viewError() string {
	var b strings.Builder
	b.WriteString(errorStyle.Render("An error occurred:"))
	b.WriteString("\n\n")
	if m.lastError != nil {
		wrappedErr := wrapText(m.lastError.Error(), m.termWidth-4)
		b.WriteString(wrappedErr)
	} else {
		b.WriteString("Unknown error.")
	}
	b.WriteString("\n")
	return b.String()
}

// --- Update Helpers ---

// handleMenuKey remains the same as previous version
func (m *AppModel) handleMenuKey(msg tea.KeyMsg) tea.Cmd {
	switch msg.String() {
	case "up", "k":
		if m.menuCursor > 0 {
			m.menuCursor--
		}
	case "down", "j":
		if m.menuCursor < len(m.menuChoices)-1 {
			m.menuCursor++
		}
	case "enter":
		m.lastError = nil
		m.mu.Lock()
		m.fileProgress = make(map[string]*FileProgress)
		m.fileOrder = make([]string, 0)
		m.overallCurrent = 0
		m.overallTotal = 0
		m.currentTaskTag = ""
		m.lastActivity = ""
		m.mu.Unlock()
		m.taskStartTime = time.Now()
		m.uiMsgChan = make(chan tea.Msg)
		choice := m.menuChoices[m.menuCursor]
		log.Printf("Menu selection: %s", choice)
		var taskCmd tea.Cmd
		switch choice {
		case "Download Feeds":
			m.State = DownloadingFiles
			m.currentTaskTag = "Download"
			taskCmd = m.startDownloadTask(m.uiMsgChan)
		case "Process CSVs":
			m.State = ProcessingFiles
			m.currentTaskTag = "Process"
			taskCmd = m.startProcessTask(m.uiMsgChan)
		case "Inspect Parquet":
			m.State = InspectingFiles
			m.currentTaskTag = "Inspect"
			taskCmd = m.startInspectTask(m.uiMsgChan)
		case "Run FPP Analysis":
			m.State = AnalyzingData
			// Use correct spelling from import for consistency if needed, or keep tag simple
			m.currentTaskTag = "Analyze" // Keep tag user-friendly
			taskCmd = m.startAnalyzeTask(m.uiMsgChan)
		case "Exit":
			m.Quitting = true
			m.State = Exiting
			m.uiMsgChan = nil
			return tea.Quit
		default:
			m.uiMsgChan = nil
		}
		return tea.Batch(taskCmd, m.waitForActivityCmd(m.uiMsgChan))
	case "ctrl+c", "q":
		m.Quitting = true
		m.State = Exiting
		return tea.Quit
	}
	return nil
}

// waitForActivityCmd remains the same as previous version
func (m *AppModel) waitForActivityCmd(uiMsgChan chan tea.Msg) tea.Cmd {
	if uiMsgChan == nil {
		return nil
	}
	return func() tea.Msg {
		msg, ok := <-uiMsgChan
		if !ok {
			log.Println("waitForActivityCmd: uiMsgChan closed.")
			return nil
		}
		log.Printf("waitForActivityCmd: Received message: %T", msg)
		return msg
	}
}

// --- Task Starters ---

// startDownloadTask remains the same as previous version
func (m *AppModel) startDownloadTask(uiMsgChan chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		log.Println("Cmd Executing: Launching downloader goroutine...")
		workerProgressChan := make(chan downloader.DownloadProgress)
		go func() {
			startTime := m.taskStartTime
			var finalErr error
			defer func() {
				log.Printf("Goroutine: downloader.DownloadAndExtract finished.")
				uiMsgChan <- NewTaskFinished(m.currentTaskTag, startTime, finalErr, "Download/Extract finished.")
				close(uiMsgChan)
				log.Println("Goroutine: Closed uiMsgChan for download.")
			}()
			finalErr = downloader.DownloadAndExtract(m.Cfg, workerProgressChan)
		}()
		go func() {
			log.Println("Goroutine: Download progress translator started.")
			for p := range workerProgressChan {
				log.Printf("Translator: Received download progress: %+v", p)
				uiMsgChan <- NewProgress(m.currentTaskTag, int64(p.ArchivesProcessed), int64(p.TotalArchives), p.CurrentArchive)
				status := "Downloading"
				if p.BytesCurrent > 0 && p.BytesTotal > 0 && p.BytesCurrent < p.BytesTotal {
					status = "Downloading"
				}
				if p.BytesCurrent == p.BytesTotal && p.BytesTotal > 0 {
					status = "Extracting"
				}
				if p.Extracted {
					status = "Complete"
				}
				if p.Skipped {
					status = "Skipped"
				}
				if p.Err != nil {
					status = "Error"
				}
				errMsg := ""
				if p.Err != nil {
					errMsg = p.Err.Error()
				}
				uiMsgChan <- NewFileProgress(p.CurrentArchive, p.CurrentArchive, status, p.BytesCurrent, p.BytesTotal, 0, errMsg)
			}
			log.Println("Goroutine: Download progress translator finished (worker chan closed).")
		}()
		return nil
	}
}

// startProcessTask remains the same as previous version
func (m *AppModel) startProcessTask(uiMsgChan chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		log.Println("Cmd Executing: Launching processor goroutine...")
		workerProgressChan := make(chan processor.ProcessProgress)
		go func() {
			startTime := m.taskStartTime
			var finalErr error
			defer func() {
				log.Printf("Goroutine: processor.ProcessFiles finished.")
				uiMsgChan <- NewTaskFinished(m.currentTaskTag, startTime, finalErr, "Processing finished.")
				close(uiMsgChan)
				log.Println("Goroutine: Closed uiMsgChan for process.")
			}()
			finalErr = processor.ProcessFiles(m.Cfg, workerProgressChan)
		}()
		go func() {
			log.Println("Goroutine: Process progress translator started.")
			for p := range workerProgressChan {
				log.Printf("Translator: Received process progress: %+v", p)
				uiMsgChan <- NewProgress(m.currentTaskTag, int64(p.FilesProcessed), int64(p.TotalFiles), p.CurrentFile)
				errMsg := ""
				if p.Err != nil {
					errMsg = p.Err.Error()
				}
				status := "Queued"
				if p.Err != nil {
					status = "Error"
				} else if p.Skipped {
					status = "Skipped"
				} else if p.Complete {
					status = "Complete"
				} else if p.LinesProcessed >= 0 {
					status = "Processing"
				}
				uiMsgChan <- NewFileProgress(p.CurrentFile, p.CurrentFile, status, p.LinesProcessed, p.TotalLines, p.ElapsedTime, errMsg)
			}
			log.Println("Goroutine: Process progress translator finished (worker chan closed).")
		}()
		return nil
	}
}

// startInspectTask remains the same as previous version
func (m *AppModel) startInspectTask(uiMsgChan chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		log.Println("Cmd Executing: Launching inspector goroutine...")
		startTime := m.taskStartTime
		uiMsgChan <- NewProgress(m.currentTaskTag, 0, 1, "Running inspection...")
		var finalErr error
		defer func() {
			log.Printf("Goroutine: inspector.InspectParquet finished.")
			uiMsgChan <- NewTaskFinished(m.currentTaskTag, startTime, finalErr, "Parquet inspection finished.")
			close(uiMsgChan)
			log.Println("Goroutine: Closed uiMsgChan for inspect.")
		}()
		finalErr = inspector.InspectParquet(m.Cfg) // Use inspector package
		return nil
	}
}

// startAnalyzeTask - uses analyser package now
func (m *AppModel) startAnalyzeTask(uiMsgChan chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		log.Println("Cmd Executing: Launching analyser goroutine...") // Corrected log message
		startTime := m.taskStartTime
		uiMsgChan <- NewProgress(m.currentTaskTag, 0, 1, "Running analysis...")
		var finalErr error
		defer func() {
			log.Printf("Goroutine: analyser.RunAnalysis finished.") // Corrected log message
			// Use analyser package in message if desired, or keep generic
			uiMsgChan <- NewTaskFinished(m.currentTaskTag, startTime, finalErr, "DuckDB analysis finished.")
			close(uiMsgChan)
			log.Println("Goroutine: Closed uiMsgChan for analyse.")
		}()
		// Call the analyser package function
		finalErr = analyser.RunAnalysis(m.Cfg) // Use analyser package
		return nil
	}
}

// --- Helpers --- (Unchanged)
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func wrapText(text string, maxWidth int) string {
	if maxWidth <= 0 {
		return text
	}
	var result strings.Builder
	var currentLine strings.Builder
	words := strings.Fields(text)
	for _, word := range words {
		if currentLine.Len() > 0 && currentLine.Len()+len(word)+1 > maxWidth {
			result.WriteString(currentLine.String())
			result.WriteString("\n")
			currentLine.Reset()
		}
		if currentLine.Len() > 0 {
			currentLine.WriteString(" ")
		}
		currentLine.WriteString(word)
	}
	result.WriteString(currentLine.String())
	return result.String()
}
