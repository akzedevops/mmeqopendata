package geocoder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/akzedevops/mmeqopendata/go/internal/catalog"
)

// repoDataDir is the real repo data/ directory relative to this package.
const repoDataDir = "../../../data"

var (
	sharedOnce sync.Once
	sharedGeo  *Geocoder
	sharedErr  error
)

// sharedGeocoder loads the real datasets once for all tests that need them.
func sharedGeocoder(t *testing.T) *Geocoder {
	t.Helper()
	if _, err := os.Stat(filepath.Join(repoDataDir, "admin", "mmr_admin1.geojson")); err != nil {
		t.Skipf("repo data dir not available: %v", err)
	}
	sharedOnce.Do(func() { sharedGeo, sharedErr = New(repoDataDir) })
	if sharedErr != nil {
		t.Fatalf("New(%q): %v", repoDataDir, sharedErr)
	}
	return sharedGeo
}

// goldenRow mirrors one entry of testdata/golden_geocode.json, produced by the
// Python reference implementation (see testdata/gen_golden.py).
type goldenRow struct {
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	State      string  `json:"state"`
	District   string  `json:"district"`
	Township   string  `json:"township"`
	City       string  `json:"city"`
	PlaceType  string  `json:"place_type"`
	DistanceKm float64 `json:"distance_km"`
}

func loadGolden(t *testing.T) []goldenRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "golden_geocode.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var rows []goldenRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(rows) < 40 {
		t.Fatalf("golden has %d rows, want >= 40", len(rows))
	}
	return rows
}

// TestGoldenParity asserts exact equality with the Python reference on all 6
// outputs for every sampled real event. Distances are compared as %.1f
// strings so float representation noise cannot mask a real mismatch.
func TestGoldenParity(t *testing.T) {
	g := sharedGeocoder(t)
	for i, row := range loadGolden(t) {
		label := fmt.Sprintf("row %d (%.4f, %.4f)", i, row.Lat, row.Lon)
		if got := g.State(row.Lat, row.Lon); got != row.State {
			t.Errorf("%s: State = %q, want %q", label, got, row.State)
		}
		if got := g.District(row.Lat, row.Lon); got != row.District {
			t.Errorf("%s: District = %q, want %q", label, got, row.District)
		}
		if got := g.Township(row.Lat, row.Lon); got != row.Township {
			t.Errorf("%s: Township = %q, want %q", label, got, row.Township)
		}
		city, placeType, dist := g.NearestPlace(row.Lat, row.Lon)
		if city != row.City {
			t.Errorf("%s: nearest city = %q, want %q", label, city, row.City)
		}
		if placeType != row.PlaceType {
			t.Errorf("%s: place_type = %q, want %q", label, placeType, row.PlaceType)
		}
		got, want := fmt.Sprintf("%.1f", dist), fmt.Sprintf("%.1f", row.DistanceKm)
		if got != want {
			t.Errorf("%s: distance_km = %s, want %s", label, got, want)
		}
	}
}

// TestEnrich checks the 6 Extra keys are set (with distance as float64) and
// that a nil Extra map is initialized.
func TestEnrich(t *testing.T) {
	g := sharedGeocoder(t)
	rows := loadGolden(t)
	row := rows[len(rows)-1]
	q := &catalog.Quake{Latitude: row.Lat, Longitude: row.Lon}
	g.Enrich(q)
	if q.Extra["state_region"] != row.State ||
		q.Extra["district"] != row.District ||
		q.Extra["township"] != row.Township ||
		q.Extra["nearest_city"] != row.City ||
		q.Extra["place_type"] != row.PlaceType {
		t.Errorf("Enrich Extra = %v, want golden row %+v", q.Extra, row)
	}
	d, ok := q.Extra["distance_km"].(float64)
	if !ok {
		t.Fatalf("distance_km is %T, want float64", q.Extra["distance_km"])
	}
	if got, want := fmt.Sprintf("%.1f", d), fmt.Sprintf("%.1f", row.DistanceKm); got != want {
		t.Errorf("distance_km = %s, want %s", got, want)
	}
}

// TestMissingData: absent data files must not error and must degrade like the
// Python module (empty strings, zero distance, Extra still populated).
func TestMissingData(t *testing.T) {
	g, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New on empty dir: %v", err)
	}
	if got := g.State(21.0, 96.0); got != "" {
		t.Errorf("State = %q, want \"\"", got)
	}
	name, placeType, dist := g.NearestPlace(21.0, 96.0)
	if name != "" || placeType != "" || dist != 0.0 {
		t.Errorf("NearestPlace = (%q, %q, %v), want (\"\", \"\", 0)", name, placeType, dist)
	}
	q := &catalog.Quake{Latitude: 21.0, Longitude: 96.0}
	g.Enrich(q)
	if q.Extra["state_region"] != "" || q.Extra["nearest_city"] != "" ||
		q.Extra["distance_km"] != 0.0 {
		t.Errorf("Enrich on empty geocoder Extra = %v, want empty values", q.Extra)
	}
}

// TestRound1BankersRounding pins the Python round(d, 1) half-to-even rule on
// values whose halves are exact in binary (0.25 -> 2.5, 0.75 -> 7.5, ...).
func TestRound1BankersRounding(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{0.25, 0.2}, // 2.5 rounds down to even 2
		{0.75, 0.8}, // 7.5 rounds up to even 8
		{1.25, 1.2}, // 12.5 -> 12
		{2.25, 2.2}, // 22.5 -> 22
		{-0.25, -0.2},
		{-0.75, -0.8},
		{0.24, 0.2},
		{0.26, 0.3},
		{53.84, 53.8},
		{0.0, 0.0},
	}
	for _, c := range cases {
		if got := round1(c.in); got != c.want {
			t.Errorf("round1(%v) = %v, want %v", c.in, got, c.want)
		}
	}
	// The rule differs from half-away-from-zero exactly at even/odd halves.
	if round1(0.25) == 0.3 {
		t.Error("round1 must not round half away from zero")
	}
}

// TestPolygonContains exercises bbox pre-check, holes, and MultiPolygon parts
// with a synthetic square-with-a-hole plus a detached island.
func TestPolygonContains(t *testing.T) {
	sq := func(x0, y0, x1, y1 float64) ring {
		return ring{{x0, y0}, {x1, y0}, {x1, y1}, {x0, y1}, {x0, y0}}
	}
	outer, hole := sq(0, 0, 10, 10), sq(4, 4, 6, 6)
	main, err := buildPolygon([][][]float64{ringCoords(outer), ringCoords(hole)})
	if err != nil {
		t.Fatal(err)
	}
	island, err := buildPolygon([][][]float64{ringCoords(sq(20, 20, 22, 22))})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		x, y float64
		want bool
	}{
		{"interior", 2, 2, true},
		{"inside hole", 5, 5, false},
		{"between hole and edge", 7, 5, true},
		{"outside bbox", 15, 5, false},
		{"outside bbox (y)", 5, -1, false},
		{"far outside", 100, 100, false},
	}
	for _, c := range cases {
		if got := main.contains(c.x, c.y); got != c.want {
			t.Errorf("main.contains(%v, %v) [%s] = %v, want %v", c.x, c.y, c.name, got, c.want)
		}
	}

	// MultiPolygon semantics: contained if in any part.
	feat := adminFeature{name: "synthetic", polys: []polygon{main, island}}
	layer := adminLayer{feats: []adminFeature{feat}}
	if got := layer.query(21, 21); got != "synthetic" { // lat=21, lon=21
		t.Errorf("query island part = %q, want \"synthetic\"", got)
	}
	if got := layer.query(5, 5); got != "" { // in the hole
		t.Errorf("query hole = %q, want \"\"", got)
	}
	if got := layer.query(2, 2); got != "synthetic" {
		t.Errorf("query interior = %q, want \"synthetic\"", got)
	}
}

// TestFirstFeatureWins: with overlapping features, file order decides.
func TestFirstFeatureWins(t *testing.T) {
	sq, err := buildPolygon([][][]float64{{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}})
	if err != nil {
		t.Fatal(err)
	}
	layer := adminLayer{feats: []adminFeature{
		{name: "first", polys: []polygon{sq}},
		{name: "second", polys: []polygon{sq}},
	}}
	if got := layer.query(5, 5); got != "first" {
		t.Errorf("query = %q, want \"first\"", got)
	}
}

// TestLoadPerformance guards the eager-load budget: ADM1+2+3 (418 features)
// plus ~74k OSM places must load in under 5 seconds. Skipped with -short.
func TestLoadPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load-performance test in -short mode")
	}
	if _, err := os.Stat(filepath.Join(repoDataDir, "admin", "mmr_admin1.geojson")); err != nil {
		t.Skipf("repo data dir not available: %v", err)
	}
	start := time.Now()
	g, err := New(repoDataDir)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if n := len(g.adm3.feats); n != 330 {
		t.Errorf("ADM3 features = %d, want 330", n)
	}
	if n := len(g.places); n < 70000 {
		t.Errorf("places = %d, want >= 70000", n)
	}
	t.Logf("loaded ADM1/2/3 (%d/%d/%d features) + %d places in %v",
		len(g.adm1.feats), len(g.adm2.feats), len(g.adm3.feats), len(g.places), elapsed)
	if elapsed > 5*time.Second {
		t.Errorf("eager load took %v, budget is 5s", elapsed)
	}
}

func ringCoords(r ring) [][]float64 {
	out := make([][]float64, len(r))
	for i, p := range r {
		out[i] = []float64{p.x, p.y}
	}
	return out
}

func BenchmarkNew(b *testing.B) {
	if _, err := os.Stat(filepath.Join(repoDataDir, "admin", "mmr_admin1.geojson")); err != nil {
		b.Skipf("repo data dir not available: %v", err)
	}
	for i := 0; i < b.N; i++ {
		if _, err := New(repoDataDir); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnrich(b *testing.B) {
	if _, err := os.Stat(filepath.Join(repoDataDir, "admin", "mmr_admin1.geojson")); err != nil {
		b.Skipf("repo data dir not available: %v", err)
	}
	g, err := New(repoDataDir)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := &catalog.Quake{Latitude: 21.9588, Longitude: 96.0891} // Mandalay
		g.Enrich(q)
	}
}
