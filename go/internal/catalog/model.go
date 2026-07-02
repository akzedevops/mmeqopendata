// Package catalog validates, dedups, and date-ranges the earthquake catalog, mirroring
// src/mmeq/export/writer.py + fetcher.py. The CSV/JSON writer is a later increment; this
// package currently provides the deterministic, fully-tested core. See specs/002.
package catalog

import "time"

// Quake is one earthquake record. The essential fields are typed for validation; Extra
// preserves every other field from the API response so a future writer can reproduce the
// full CSV/JSON column set without losing data (mirrors pandas absorbing all API keys).
type Quake struct {
	ID        string
	Time      time.Time // parsed from the API "time" field, in UTC
	Latitude  float64
	Longitude float64
	Depth     float64
	Mag       float64

	// Filled by Validate:
	TimeUTC string // "2006-01-02 15:04:05" in UTC
	TimeMMT string // same instant in Asia/Yangon (UTC+06:30)

	Extra map[string]any // all other API fields, preserved verbatim
}
