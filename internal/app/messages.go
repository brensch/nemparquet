package app

import (
	"fmt"
	"time"
)

// --- Progress Messages ---

// ProgressMsg updates the overall progress bar.
type ProgressMsg struct {
	Tag      string // Identifier for the overall task (e.g., "Download", "Process")
	Current  int64  // Current progress count
	Total    int64  // Total items
	Activity string // Short description of current activity (optional)
}

// FileProgressMsg updates the progress for a specific file/worker.
type FileProgressMsg struct {
	FileID      string        // Unique ID (e.g., filename or worker ID)
	FileName    string        // Display name
	Status      string        // e.g., "Downloading", "Processing", "Complete", "Error", "Skipped"
	Current     int64         // e.g., bytes downloaded, lines processed
	Total       int64         // e.g., total bytes, total lines (if known)
	ElapsedTime time.Duration // Optional: Time taken for this file
	ErrMsg      string        // Error message if Status is "Error"
}

// TaskFinishedMsg signals the completion of a major background task.
type TaskFinishedMsg struct {
	Tag       string // Identifier (matches ProgressMsg.Tag)
	Err       error  // Error if the task failed overall
	StartTime time.Time
	EndTime   time.Time
	Message   string // Optional summary message
}

// GeneralErrorMsg signals an error that might not be tied to a specific task.
type GeneralErrorMsg struct {
	Err error
}

// --- Other Messages ---

// Represents terminal resize events. Bubbletea handles this automatically,
// but we might react to it. type WindowSizeMsg = tea.WindowSizeMsg

// --- Message Constructors (Optional but helpful) ---

func NewProgress(tag string, current, total int64, activity string) ProgressMsg {
	return ProgressMsg{Tag: tag, Current: current, Total: total, Activity: activity}
}

func NewFileProgress(fileID, fileName, status string, current, total int64, elapsed time.Duration, errMsg string) FileProgressMsg {
	return FileProgressMsg{
		FileID:      fileID,
		FileName:    fileName,
		Status:      status,
		Current:     current,
		Total:       total,
		ElapsedTime: elapsed,
		ErrMsg:      errMsg,
	}
}

func NewTaskFinished(tag string, start time.Time, err error, msg string) TaskFinishedMsg {
	return TaskFinishedMsg{
		Tag:       tag,
		StartTime: start,
		EndTime:   time.Now(),
		Err:       err,
		Message:   msg,
	}
}

func NewError(err error) GeneralErrorMsg {
	return GeneralErrorMsg{Err: err}
}

// Implement the error interface for relevant messages
func (e GeneralErrorMsg) Error() string {
	return e.Err.Error()
}
func (t TaskFinishedMsg) Error() string {
	if t.Err != nil {
		return t.Err.Error()
	}
	return ""
}

// Implement tea.Msg interface (empty methods satisfy it)
func (p ProgressMsg) String() string {
	return fmt.Sprintf("Progress %s: %d/%d", p.Tag, p.Current, p.Total)
}
func (fp FileProgressMsg) String() string {
	return fmt.Sprintf("FileProgress %s: %s", fp.FileID, fp.Status)
}
func (tf TaskFinishedMsg) String() string { return fmt.Sprintf("TaskFinished %s", tf.Tag) }
func (ge GeneralErrorMsg) String() string { return fmt.Sprintf("GeneralError: %s", ge.Err) }
