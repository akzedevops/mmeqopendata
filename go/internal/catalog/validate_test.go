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

func TestValidateMissingOrNullID(t *testing.T) {
	in := []map[string]any{
		{"time": "2025-01-01T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "5", "mag": "4"},
		{"id": nil, "time": "2025-01-02T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "5", "mag": "4"},
		{"id": "ev9", "time": "2025-01-03T00:00:00Z", "latitude": "21", "longitude": "96", "depth": "5", "mag": "4"},
	}
	got := Validate(in, end())
	if len(got) != 3 {
		t.Fatalf("want 3 records, got %d", len(got))
	}
	// A record missing "id" (or with a null id) must NOT get the string "<nil>":
	// pandas holds NaN/None there and renders "" in CSV.
	if got[0].ID != "" {
		t.Errorf("missing id: ID = %q, want \"\"", got[0].ID)
	}
	if _, ok := got[0].Extra["id"]; ok {
		t.Error("missing id: Extra must not grow an id key")
	}
	if got[1].ID != "" {
		t.Errorf("null id: ID = %q, want \"\"", got[1].ID)
	}
	if v, ok := got[1].Extra["id"]; !ok || v != nil {
		t.Errorf("null id: Extra[\"id\"] = %v, %v; want nil, true", v, ok)
	}
	if got[2].ID != "ev9" {
		t.Errorf("real id: ID = %q, want ev9", got[2].ID)
	}
}

// TestDedupMissingIDsCollapse mirrors measured pandas 2.3.3 behavior for
// drop_duplicates(subset=["id"], keep="last") on a frame where SOME records
// lack "id": every NaN id counts as the same key, so all id-less rows collapse
// into one group keeping the last, alongside the per-id keep-last collapse.
func TestDedupMissingIDsCollapse(t *testing.T) {
	in := []Quake{
		{ID: "ev1", Mag: 4.5, Extra: map[string]any{"id": "ev1"}},
		{Mag: 3.1, Extra: map[string]any{}},                       // missing id
		{ID: "ev1", Mag: 4.9, Extra: map[string]any{"id": "ev1"}}, // revised ev1 -> wins
		{Mag: 2.2, Extra: map[string]any{}},                       // missing id -> wins the NaN group
	}
	got := Dedup(in)
	if len(got) != 2 {
		t.Fatalf("want 2 rows (pandas keeps ev1-last and NaN-last), got %d", len(got))
	}
	if got[0].ID != "ev1" || got[0].Mag != 4.9 {
		t.Errorf("row 0 = {%q %v}, want revised ev1 (mag 4.9)", got[0].ID, got[0].Mag)
	}
	if got[1].ID != "" || got[1].Mag != 2.2 {
		t.Errorf("row 1 = {%q %v}, want last id-less row (mag 2.2)", got[1].ID, got[1].Mag)
	}
}

// TestDedupNullIDSeparateFromMissing pins the measured pandas nuance: an
// explicit null id (None) is a dedup group of its own, distinct from a missing
// id key (NaN); each group collapses keep-last.
func TestDedupNullIDSeparateFromMissing(t *testing.T) {
	in := []Quake{
		{Mag: 1.0, Extra: map[string]any{"id": nil}}, // None group
		{Mag: 2.0, Extra: map[string]any{}},          // NaN group
		{Mag: 3.0, Extra: map[string]any{"id": nil}}, // None group -> wins
		{Mag: 4.0, Extra: map[string]any{}},          // NaN group -> wins
	}
	got := Dedup(in)
	if len(got) != 2 {
		t.Fatalf("want 2 rows (None group + NaN group), got %d", len(got))
	}
	if got[0].Mag != 3.0 || got[1].Mag != 4.0 {
		t.Errorf("got mags %v, %v; want 3.0 (last None) and 4.0 (last NaN)", got[0].Mag, got[1].Mag)
	}
}

// TestDedupNoIDsFullRowFallback mirrors _dedup_frame's other branch, measured
// on pandas 2.3.3: with no "id" column at all, drop_duplicates() compares full
// rows and keeps the FIRST occurrence.
func TestDedupNoIDsFullRowFallback(t *testing.T) {
	mk := func(mag float64, loc string) Quake {
		return Quake{
			Mag: mag, Latitude: 21.0, Longitude: 96.0, Depth: 5,
			TimeUTC: "2025-01-01 00:00:00", TimeMMT: "2025-01-01 06:30:00",
			Extra: map[string]any{"location": loc},
		}
	}
	in := []Quake{mk(4.5, "x"), mk(4.5, "x"), mk(2.2, "y"), mk(4.5, "x")}
	got := Dedup(in)
	if len(got) != 2 {
		t.Fatalf("want 2 unique rows, got %d", len(got))
	}
	if got[0].Mag != 4.5 || got[1].Mag != 2.2 {
		t.Errorf("got mags %v, %v; want 4.5 (kept FIRST) then 2.2", got[0].Mag, got[1].Mag)
	}
	// Rows differing in any field are not duplicates ("3.6" != 3.6 in pandas
	// object columns; type matters).
	in2 := []Quake{mk(4.5, "x"), mk(4.5, "z")}
	in2[1].Extra["extraField"] = "3.6"
	in3 := []Quake{mk(4.5, "x"), mk(4.5, "x")}
	in3[0].Extra["v"] = "3.6"
	in3[1].Extra["v"] = 3.6
	if got := Dedup(in2); len(got) != 2 {
		t.Errorf("differing rows must both survive, got %d", len(got))
	}
	if got := Dedup(in3); len(got) != 2 {
		t.Errorf("string \"3.6\" vs float 3.6 are distinct rows (pandas object dtype), got %d", len(got))
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
