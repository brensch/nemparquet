package util

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	nemLocation      *time.Location
	nemDateTimeRegex *regexp.Regexp
)

func init() {
	// Define the NEM time zone location (UTC+10)
	nemLocation = time.FixedZone("NEM", 10*60*60) // 10 hours * 60 mins * 60 secs

	// Compile the regex for date format detection
	// Matches "YYYY/MM/DD HH:MI:SS" exactly, allowing for optional surrounding quotes.
	nemDateTimeRegex = regexp.MustCompile(`^"?\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}"?$`)
}

// IsNEMDateTime checks if a string matches the YYYY/MM/DD HH:MI:SS format (with optional quotes).
func IsNEMDateTime(s string) bool {
	return nemDateTimeRegex.MatchString(s)
}

// NEMDateTimeToEpochMS converts a "YYYY/MM/DD HH:MI:SS" string (in NEMtime),
// potentially surrounded by double quotes, to Unix epoch milliseconds (int64).
// Returns 0 and error if parsing fails.
func NEMDateTimeToEpochMS(s string) (int64, error) {
	// Trim surrounding double quotes before parsing
	trimmedS := strings.Trim(s, `"`)

	// Define the layout matching the input string format (without quotes)
	layout := "2006/01/02 15:04:05" // Go's reference time format matching YYYY/MM/DD HH:MI:SS
	t, err := time.ParseInLocation(layout, trimmedS, nemLocation)
	if err != nil {
		// Return the original string 's' in the error message for better debugging
		return 0, fmt.Errorf("failed to parse NEM time (after trimming quotes) from '%s': %w", s, err)
	}
	// Return milliseconds since epoch
	return t.UnixMilli(), nil
}

// GetNEMLocation returns the fixed NEM time zone location used for parsing.
func GetNEMLocation() *time.Location {
	return nemLocation
}
