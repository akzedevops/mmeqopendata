package catalog

// Golden-file parity tests: the fixtures under testdata/ were produced by
// running the ACTUAL Python pipeline writer (src/mmeq/export/writer.py) on
// input_existing.json / input_new.json — see testdata/gen_golden.py. Every
// test asserts byte-equality between the Go writer output and that snapshot.
//
// Regenerate the goldens (from the repo root):
//
//	.venv/bin/python go/internal/catalog/testdata/gen_golden.py

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// loadFixtureQuakes decodes a testdata input file into []Quake the way the
// exporter would hold them post-Validate: the seven promoted fields typed,
// everything else (including the id/lat/lon/depth/mag copies) in Extra, with
// JSON numbers as float64 and nulls as nil — exactly what encoding/json yields.
func loadFixtureQuakes(t *testing.T, name string) []Quake {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	var records []map[string]any
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	quakes := make([]Quake, 0, len(records))
	for _, rec := range records {
		var q Quake
		q.ID, _ = rec["id"].(string)
		q.TimeUTC, _ = rec["time_utc"].(string)
		q.TimeMMT, _ = rec["time_mmt"].(string)
		q.Latitude, _ = rec["latitude"].(float64)
		q.Longitude, _ = rec["longitude"].(float64)
		q.Depth, _ = rec["depth"].(float64)
		q.Mag, _ = rec["mag"].(float64)
		q.Extra = make(map[string]any, len(rec))
		for k, v := range rec {
			if k == "time_utc" || k == "time_mmt" {
				continue // filled by Validate, never present in raw API records
			}
			q.Extra[k] = v
		}
		quakes = append(quakes, q)
	}
	return quakes
}

// assertGolden fails with the first differing line if got does not byte-match
// the golden file.
func assertGolden(t *testing.T, gotPath, goldenName string) {
	t.Helper()
	got, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("read output %s: %v", gotPath, err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", goldenName))
	if err != nil {
		t.Fatalf("read golden %s: %v", goldenName, err)
	}
	if string(got) == string(want) {
		return
	}
	gotLines := strings.Split(string(got), "\n")
	wantLines := strings.Split(string(want), "\n")
	for i := 0; i < len(gotLines) || i < len(wantLines); i++ {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		} else {
			g = "<missing line>"
		}
		if i < len(wantLines) {
			w = wantLines[i]
		} else {
			w = "<missing line>"
		}
		if g != w {
			t.Fatalf("output does not match golden %s\nfirst diff at line %d:\n got: %q\nwant: %q\n(got %d bytes, want %d bytes)",
				goldenName, i+1, g, w, len(got), len(want))
		}
	}
	t.Fatalf("output does not match golden %s (lengths: got %d, want %d)", goldenName, len(got), len(want))
}

func newFixture(t *testing.T) []Quake { return loadFixtureQuakes(t, "input_new.json") }

func TestWriteMonthlyCSVGolden(t *testing.T) {
	quakes := newFixture(t)
	out := filepath.Join(t.TempDir(), "monthly.csv")
	if err := WriteMonthlyCSV(quakes, out, ColumnsFor(quakes)); err != nil {
		t.Fatalf("WriteMonthlyCSV: %v", err)
	}
	assertGolden(t, out, "golden_monthly.csv")
}

func TestWriteJSONGolden(t *testing.T) {
	quakes := newFixture(t)
	out := filepath.Join(t.TempDir(), "monthly.json")
	if err := WriteJSON(quakes, out); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	assertGolden(t, out, "golden_monthly.json")
}

func TestWriteMergedCSVGolden(t *testing.T) {
	quakes := newFixture(t)
	existing, err := os.ReadFile(filepath.Join("testdata", "existing_combined.csv"))
	if err != nil {
		t.Fatalf("read existing fixture: %v", err)
	}
	out := filepath.Join(t.TempDir(), "combined.csv")
	if err := os.WriteFile(out, existing, 0o644); err != nil {
		t.Fatalf("seed existing file: %v", err)
	}
	if err := WriteMergedCSV(quakes, out, ColumnsFor(quakes)); err != nil {
		t.Fatalf("WriteMergedCSV: %v", err)
	}
	assertGolden(t, out, "golden_merged.csv")
}

func TestWriteMergedCSVFreshGolden(t *testing.T) {
	quakes := newFixture(t)
	out := filepath.Join(t.TempDir(), "combined.csv")
	if err := WriteMergedCSV(quakes, out, ColumnsFor(quakes)); err != nil {
		t.Fatalf("WriteMergedCSV (fresh): %v", err)
	}
	assertGolden(t, out, "golden_merged_fresh.csv")
}

func TestMergeCombinedJSONGolden(t *testing.T) {
	quakes := newFixture(t)
	out := filepath.Join(t.TempDir(), "combined.json")
	existing := filepath.Join("testdata", "existing_combined.json")
	if err := MergeCombinedJSON(existing, quakes, out); err != nil {
		t.Fatalf("MergeCombinedJSON: %v", err)
	}
	assertGolden(t, out, "golden_merged.json")
}

func TestMergeCombinedJSONFreshGolden(t *testing.T) {
	quakes := newFixture(t)
	out := filepath.Join(t.TempDir(), "combined.json")
	missing := filepath.Join(t.TempDir(), "does_not_exist.json")
	if err := MergeCombinedJSON(missing, quakes, out); err != nil {
		t.Fatalf("MergeCombinedJSON (fresh): %v", err)
	}
	assertGolden(t, out, "golden_merged_fresh.json")
}

// TestPyFloatRepr pins the float-formatting contract: CPython repr(float) =
// shortest round-trip, fixed notation with ".0" for integral values, exponent
// form only for |x| >= 1e16 or 0 < |x| < 1e-4. Expected strings verified
// against pandas 2.3.3 to_csv (see gen_golden.py header).
func TestPyFloatRepr(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{90.0, "90.0"},
		{3.6, "3.6"},
		{24.441, "24.441"},
		{0.1, "0.1"},
		{math.Copysign(0, -1), "-0.0"}, // Go has no -0.0 literal
		{0.0, "0.0"},
		{1234567.0, "1234567.0"}, // Go's 'g' would give "1.234567e+06"
		{1782667764000.0, "1782667764000.0"},
		{9999999999999998.0, "9999999999999998.0"},
		{1e16, "1e+16"},
		{2.5e16, "2.5e+16"},
		{1e-4, "0.0001"},
		{9.999e-5, "9.999e-05"},
		{1e-7, "1e-07"},
		{-3.25, "-3.25"},
	}
	for _, c := range cases {
		if got := pyFloatRepr(c.in); got != c.want {
			t.Errorf("pyFloatRepr(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCSVFieldQuoting(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"a,b", `"a,b"`},
		{`he said "hi"`, `"he said ""hi"""`},
		{"line\nbreak", "\"line\nbreak\""},
		{" leading space", " leading space"}, // Python QUOTE_MINIMAL does NOT quote this; Go's encoding/csv would
		{"", ""},
	}
	for _, c := range cases {
		if got := csvField(c.in); got != c.want {
			t.Errorf("csvField(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestEmptyInputIsNoOp mirrors the df.empty early returns in save_to_csv /
// save_to_json: nothing is written and an existing file is left untouched.
func TestEmptyInputIsNoOp(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "x.csv")
	jsonPath := filepath.Join(dir, "x.json")
	if err := WriteMonthlyCSV(nil, csvPath, DefaultColumns); err != nil {
		t.Fatalf("WriteMonthlyCSV(nil): %v", err)
	}
	if err := WriteJSON(nil, jsonPath); err != nil {
		t.Fatalf("WriteJSON(nil): %v", err)
	}
	for _, p := range []string{csvPath, jsonPath} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("empty input must not create %s", p)
		}
	}

	seed := []byte("latitude,id\n1.0,a\n")
	if err := os.WriteFile(csvPath, seed, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteMergedCSV(nil, csvPath, DefaultColumns); err != nil {
		t.Fatalf("WriteMergedCSV(nil): %v", err)
	}
	got, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(seed) {
		t.Errorf("WriteMergedCSV(nil) modified the existing file: %q", got)
	}
}

// TestProductionCombinedJSONRoundTrip is the strongest parity proof available
// locally: the production combined JSON was written by Python
// json.dump(indent=2, ensure_ascii=False) and contains tens of thousands of
// NaN literals; decodeOrdered + encodePayload must reproduce it byte for byte
// (which is exactly what a MergeCombinedJSON run with no new records does).
// Skipped in checkouts without the data directory.
func TestProductionCombinedJSONRoundTrip(t *testing.T) {
	path := filepath.Join("..", "..", "..", "quake_exports", "json", "combined", "earthquakes_combined.json")
	orig, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("production combined JSON not available: %v", err)
	}
	recs := loadCombinedRecords(path)
	if len(recs) == 0 {
		t.Fatal("decoded zero records from production combined JSON")
	}
	got := encodePayload(recs)
	if string(got) != string(orig) {
		i := 0
		for i < len(got) && i < len(orig) && got[i] == orig[i] {
			i++
		}
		lo := max(0, i-80)
		t.Fatalf("round trip differs at byte %d (got %d bytes, want %d)\n got: %q\nwant: %q",
			i, len(got), len(orig), got[lo:min(len(got), i+80)], orig[lo:min(len(orig), i+80)])
	}
	t.Logf("byte-identical round trip: %d records, %d bytes", len(recs), len(orig))
}

// TestSanitizePyLiterals covers the non-standard tokens json.dump emits (the
// production combined JSON contains tens of thousands of NaN literals).
func TestSanitizePyLiterals(t *testing.T) {
	in := []byte(`{"a": NaN, "b": "NaN in a string", "c": Infinity, "d": -Infinity, "e": 1.5}`)
	v, err := decodeOrdered(in)
	if err != nil {
		t.Fatalf("decodeOrdered: %v", err)
	}
	om, ok := v.(*omap)
	if !ok {
		t.Fatalf("decoded %T, want *omap", v)
	}
	if om.vals["a"] != pyNaNSentinel {
		t.Errorf("a = %#v, want NaN sentinel", om.vals["a"])
	}
	if om.vals["b"] != "NaN in a string" {
		t.Errorf("b = %#v, string containing NaN must be untouched", om.vals["b"])
	}
	if om.vals["c"] != pyInfSentinel || om.vals["d"] != pyNegInfSentinel {
		t.Errorf("c/d = %#v/%#v, want Infinity sentinels", om.vals["c"], om.vals["d"])
	}
	if n, ok := om.vals["e"].(json.Number); !ok || n.String() != "1.5" {
		t.Errorf("e = %#v, want json.Number 1.5", om.vals["e"])
	}

	// Round trip: the sentinels re-encode as the original literals.
	var b strings.Builder
	for _, k := range om.keys {
		b.WriteString(fmt.Sprintf("%s=", k))
	}
	if got := b.String(); got != "a=b=c=d=e=" {
		t.Errorf("key order lost: %q", got)
	}
}
