package catalog

import (
	"fmt"
	"strconv"
	"time"

	"github.com/akzedevops/mmeqopendata/go/internal/config"
)

// yangon is Asia/Yangon (UTC+06:30). Loaded once; falls back to a fixed zone if the
// tzdata is unavailable (e.g. a scratch container), so time_mmt is always correct.
var yangon = func() *time.Location {
	if loc, err := time.LoadLocation("Asia/Yangon"); err == nil {
		return loc
	}
	return time.FixedZone("MMT", 6*3600+30*60)
}()

const (
	tsLayout   = "2006-01-02 15:04:05" // time_utc / time_mmt formatting (matches Python strftime)
	minTimeStr = "1950-01-01T00:00:00Z"
)

// toFloat coerces an API value (string or number) to float64, mirroring
// pandas.to_numeric(errors="coerce"): returns ok=false on anything non-numeric.
func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		if x == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// parseTime parses the API "time" field to UTC. Accepts RFC3339 (with or without
// fractional seconds / "Z") and the "2006-01-02 15:04:05" space form.
func parseTime(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok || s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z", tsLayout} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// Validate mirrors src/mmeq/export/writer.py:validate_quake_data. For each raw API
// record it: parses time to UTC; coerces latitude/longitude/depth/mag to numbers
// (dropping the record if any is missing/non-numeric — like dropna); filters to the
// valid time window [1950-01-01, endDate] and the configured coordinate/depth/mag
// bounds; and fills TimeUTC (UTC) and TimeMMT (Asia/Yangon). All other fields are
// preserved verbatim in Extra so the writer can reproduce the full column set.
// Geocoding enrichment is a separate step, exactly as in Python.
func Validate(raw []map[string]any, endDate time.Time) []Quake {
	minTime, _ := time.Parse(time.RFC3339, minTimeStr)
	endUTC := endDate.UTC()
	out := make([]Quake, 0, len(raw))

	for _, rec := range raw {
		t, ok := parseTime(rec["time"])
		if !ok {
			continue
		}
		lat, okLat := toFloat(rec["latitude"])
		lon, okLon := toFloat(rec["longitude"])
		depth, okDepth := toFloat(rec["depth"])
		mag, okMag := toFloat(rec["mag"])
		if !okLat || !okLon || !okDepth || !okMag {
			continue
		}

		// Bounds (inclusive), matching pandas Series.between.
		if t.Before(minTime) || t.After(endUTC) {
			continue
		}
		if lat < config.MinLat || lat > config.MaxLat ||
			lon < config.MinLon || lon > config.MaxLon ||
			depth < config.MinDepth || depth > config.MaxDepth ||
			mag < config.MinMag || mag > config.MaxMag {
			continue
		}

		extra := make(map[string]any, len(rec))
		for k, v := range rec {
			if k == "time" { // replaced by time_utc/time_mmt, like Python drops "time"
				continue
			}
			extra[k] = v
		}

		out = append(out, Quake{
			ID:        fmt.Sprint(rec["id"]),
			Time:      t,
			Latitude:  lat,
			Longitude: lon,
			Depth:     depth,
			Mag:       mag,
			TimeUTC:   t.Format(tsLayout),
			TimeMMT:   t.In(yangon).Format(tsLayout),
			Extra:     extra,
		})
	}
	return out
}

// Dedup removes duplicate events keyed on the stable event id, keeping the LAST
// occurrence — matching writer._dedup_frame(subset=["id"], keep="last"), so a revised
// event replaces its earlier version. Input order is otherwise preserved.
func Dedup(quakes []Quake) []Quake {
	lastIdx := make(map[string]int, len(quakes))
	for i, q := range quakes {
		lastIdx[q.ID] = i
	}
	out := make([]Quake, 0, len(lastIdx))
	for i, q := range quakes {
		if lastIdx[q.ID] == i {
			out = append(out, q)
		}
	}
	return out
}
