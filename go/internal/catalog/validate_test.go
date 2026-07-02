package catalog

import (
	"testing"
	"time"
)

func end() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }

func rec(m map[string]any) map[string]any { return m }

func TestValidateHappyPathTypesAndZones(t *testing.T) {
	// String-typed numerics (v1 API shape) must coerce; time_utc/time_mmt filled.
	in := []map[string]any{rec(map[string]any{
		"id": "a1", "time": "2025-03-28T06:20:52.000Z",
		"latitude": "22.0", "longitude": "96.0", "depth": "10.0", "mag": "7.7",
		"location": "Mandalay",
	})}
	got := Validate(in, end())
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	q := got[0]
	if q.Mag != 7.7 || q.Latitude != 22.0 || q.Depth != 10.0 {
		t.Fatalf("numeric coercion wrong: %+v", q)
	}
	if q.TimeUTC != "2025-03-28 06:20:52" {
		t.Fatalf("time_utc = %q", q.TimeUTC)
	}
	// Asia/Yangon is UTC+06:30: 06:20:52Z -> 12:50:52
	if q.TimeMMT != "2025-03-28 12:50:52" {
		t.Fatalf("time_mmt = %q (want +06:30)", q.TimeMMT)
	}
	if _, ok := q.Extra["location"]; !ok {
		t.Fatal("Extra must preserve non-core fields")
	}
	if _, ok := q.Extra["time"]; ok {
		t.Fatal("raw 'time' must be dropped (replaced by time_utc/time_mmt)")
	}
}

func TestValidateDropsInvalidAndOutOfBounds(t *testing.T) {
	in := []map[string]any{
		{"id": "ok", "time": "2025-01-01T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "5", "mag": "4"},
		{"id": "nomag", "time": "2025-01-01T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "5", "mag": ""},
		{"id": "badnum", "time": "2025-01-01T00:00:00Z", "latitude": "N/A", "longitude": "96", "depth": "5", "mag": "4"},
		{"id": "badtime", "time": "not-a-date", "latitude": "21", "longitude": "96", "depth": "5", "mag": "4"},
		{"id": "predates", "time": "1949-12-31T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "5", "mag": "4"},
		{"id": "deeptoodeep", "time": "2025-01-01T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "800", "mag": "4"},
		{"id": "future", "time": "2099-01-01T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "5", "mag": "4"},
	}
	got := Validate(in, end())
	if len(got) != 1 || got[0].ID != "ok" {
		ids := make([]string, len(got))
		for i, q := range got {
			ids[i] = q.ID
		}
		t.Fatalf("want only [ok], got %v", ids)
	}
}

func TestDedupKeepsLastByID(t *testing.T) {
	in := []Quake{
		{ID: "a", Mag: 5.0},
		{ID: "b", Mag: 4.0},
		{ID: "a", Mag: 5.9}, // revised a -> should win
		{ID: "c", Mag: 3.0},
	}
	got := Dedup(in)
	if len(got) != 3 {
		t.Fatalf("want 3 unique, got %d", len(got))
	}
	byID := map[string]float64{}
	for _, q := range got {
		byID[q.ID] = q.Mag
	}
	if byID["a"] != 5.9 {
		t.Fatalf("dedup must keep last: a.mag = %v, want 5.9", byID["a"])
	}
	// order preserved by first-seen position: b, a, c
	if got[0].ID != "b" || got[2].ID != "c" {
		t.Fatalf("unexpected order: %v", []string{got[0].ID, got[1].ID, got[2].ID})
	}
}

func TestValidateAcceptsNativeNumbers(t *testing.T) {
	// v2 API shape: numbers already typed.
	in := []map[string]any{{"id": "n1", "time": "2026-06-01T00:00:00Z",
		"latitude": 24.44, "longitude": 94.45, "depth": 90.0, "mag": 3.6}}
	got := Validate(in, end())
	if len(got) != 1 || got[0].Mag != 3.6 {
		t.Fatalf("native numeric handling failed: %+v", got)
	}
}
