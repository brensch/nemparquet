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
	nemLocation = time.FixedZone("NEM", 10*60*60) // UTC+10
	nemDateTimeRegex = regexp.MustCompile(`^"?\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}"?$`)
}

// IsNEMDateTime checks if a string matches the YYYY/MM/DD HH:MI:SS format.
func IsNEMDateTime(s string) bool {
	return nemDateTimeRegex.MatchString(s)
}

// NEMDateTimeToEpochMS converts NEM time string to Unix milliseconds.
func NEMDateTimeToEpochMS(s string) (int64, error) {
	trimmedS := strings.Trim(s, `"`)
	layout := "2006/01/02 15:04:05"
	t, err := time.ParseInLocation(layout, trimmedS, nemLocation)
	if err != nil {
		return 0, fmt.Errorf("failed parse NEM time '%s': %w", s, err)
	}
	return t.UnixMilli(), nil
}

// GetNEMLocation returns the fixed NEM time zone.
func GetNEMLocation() *time.Location {
	return nemLocation
}
