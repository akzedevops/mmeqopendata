package catalog

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
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

		// ID is set only for a present, non-null id (pandas leaves the id cell
		// NaN/None otherwise — it never renders a "<nil>" string). Extra["id"]
		// keeps the raw value, so the writer and Dedup can still distinguish a
		// missing key (NaN) from an explicit null (None).
		id := ""
		if v, ok := rec["id"]; ok && v != nil {
			id = fmt.Sprint(v)
		}

		out = append(out, Quake{
			ID:        id,
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

// idKey classifies a quake's id cell the way pandas classifies it in a frame
// built from the raw API records: a present, non-null id is a value key; a
// missing "id" key is NaN (pandas' drop_duplicates treats every NaN as the
// same key, so all id-less rows form ONE group); an explicit null is None,
// which pandas keeps as a group of its own, distinct from NaN.
type idKey struct {
	kind byte // 'v' value, 'm' missing (NaN), 'n' null (None)
	id   string
}

// quakeIDKey computes the dedup key for one quake. Quakes built by Validate
// carry the raw id in Extra["id"], which distinguishes missing from null; for
// hand-built quakes a non-empty ID field alone counts as a value key.
func quakeIDKey(q Quake) idKey {
	if q.ID != "" {
		return idKey{'v', q.ID}
	}
	if v, ok := q.Extra["id"]; ok {
		if v == nil {
			return idKey{'n', ""}
		}
		return idKey{'v', fmt.Sprint(v)} // e.g. an explicit empty-string id
	}
	return idKey{'m', ""}
}

// hasIDColumn reports whether a frame built from these quakes would have an
// "id" column at all: pandas materializes it only when at least one record
// carries the key (even with a null value).
func hasIDColumn(quakes []Quake) bool {
	for _, q := range quakes {
		if q.ID != "" {
			return true
		}
		if _, ok := q.Extra["id"]; ok {
			return true
		}
	}
	return false
}

// recordFingerprint canonically renders every field of a quake — the typed
// fields plus Extra with keys sorted and values tagged by their Go type, so
// the string "3.6" and the number 3.6 stay distinct exactly like pandas
// object cells. Two quakes are full-row duplicates iff fingerprints match.
func recordFingerprint(q Quake) string {
	keys := make([]string, 0, len(q.Extra))
	for k := range q.Extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "%s\x1f%s\x1f%v\x1f%v\x1f%v\x1f%v",
		q.TimeUTC, q.TimeMMT, q.Latitude, q.Longitude, q.Depth, q.Mag)
	for _, k := range keys {
		v := q.Extra[k]
		fmt.Fprintf(&b, "\x1f%s\x1e%T\x1e%v", k, v, v)
	}
	return b.String()
}

// Dedup removes duplicate events, mirroring writer._dedup_frame exactly.
//
// When at least one record carries an "id" key the frame has an id column, so
// this is drop_duplicates(subset=["id"], keep="last"): rows collapse per id
// keeping the LAST occurrence (a revised event replaces its earlier version);
// all rows whose id key is missing collapse together into one NaN group (also
// keep-last), and rows with an explicit null id form their own None group —
// both verified against pandas 2.3.3. Survivors keep the position of their
// last occurrence.
//
// When NO record has an id key (no id column) it falls back to full-row
// drop_duplicates(): rows whose every field matches collapse, keeping the
// FIRST occurrence.
func Dedup(quakes []Quake) []Quake {
	if hasIDColumn(quakes) {
		lastIdx := make(map[idKey]int, len(quakes))
		for i, q := range quakes {
			lastIdx[quakeIDKey(q)] = i
		}
		out := make([]Quake, 0, len(lastIdx))
		for i, q := range quakes {
			if lastIdx[quakeIDKey(q)] == i {
				out = append(out, q)
			}
		}
		return out
	}
	seen := make(map[string]bool, len(quakes))
	out := make([]Quake, 0, len(quakes))
	for _, q := range quakes {
		fp := recordFingerprint(q)
		if seen[fp] {
			continue
		}
		seen[fp] = true
		out = append(out, q)
	}
	return out
}
