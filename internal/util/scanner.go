package util

import (
	"bufio"
	"context" // Keep context for potential future use
	"io"
	"strings"
)

// PeekableScanner wraps bufio.Scanner to allow peeking at the next line.
type PeekableScanner struct {
	scanner    *bufio.Scanner
	peekedLine *string
	peekErr    error
	context    context.Context // Not actively used for cancellation here yet
}

// NewPeekableScanner creates a new PeekableScanner.
func NewPeekableScanner(r io.Reader) *PeekableScanner {
	scanner := bufio.NewScanner(r)
	return &PeekableScanner{scanner: scanner, context: context.Background()}
}

// Scan advances the scanner, consuming the peeked line if it exists.
func (ps *PeekableScanner) Scan() bool {
	select {
	case <-ps.context.Done():
		ps.peekErr = ps.context.Err()
		return false
	default:
		if ps.peekedLine != nil {
			ps.peekedLine = nil
			ps.peekErr = nil
			return true
		}
		return ps.scanner.Scan()
	}
}

// Text returns the current line's text.
func (ps *PeekableScanner) Text() string {
	if ps.peekedLine != nil {
		return *ps.peekedLine
	}
	return ps.scanner.Text()
}

// Err returns any scanner errors.
func (ps *PeekableScanner) Err() error {
	if ps.peekErr != nil {
		return ps.peekErr
	}
	return ps.scanner.Err()
}

// PeekRecordType reads the next significant line to determine its record type without consuming it.
func (ps *PeekableScanner) PeekRecordType() (string, error) {
	if ps.peekedLine != nil {
		return ps.extractRecordType(*ps.peekedLine), ps.peekErr
	}

	for ps.scanner.Scan() {
		line := ps.scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "#") { // Skip blank/comment
			continue
		}

		ps.peekedLine = &line
		ps.peekErr = nil
		return ps.extractRecordType(line), nil
	}

	ps.peekErr = ps.scanner.Err()
	ps.peekedLine = nil
	return "", ps.peekErr
}

func (ps *PeekableScanner) extractRecordType(line string) string {
	trimmedLine := strings.TrimSpace(line)
	if trimmedLine == "" {
		return ""
	}
	parts := strings.SplitN(trimmedLine, ",", 2)
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return ""
}

// Buffer sets the internal buffer for the scanner.
func (ps *PeekableScanner) Buffer(buf []byte, max int) {
	ps.scanner.Buffer(buf, max)
}
