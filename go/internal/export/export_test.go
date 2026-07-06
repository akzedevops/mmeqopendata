package export

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akzedevops/mmeqopendata/go/internal/catalog"
	"github.com/akzedevops/mmeqopendata/go/internal/config"
)

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestAddMonthClamped(t *testing.T) {
	cases := []struct{ in, want time.Time }{
		{date(2026, time.January, 31), date(2026, time.February, 28)}, // relativedelta clamp
		{date(2024, time.January, 31), date(2024, time.February, 29)}, // leap year
		{date(2026, time.March, 31), date(2026, time.April, 30)},
		{date(2026, time.January, 15), date(2026, time.February, 15)}, // no clamp needed
		{date(2026, time.December, 5), date(2027, time.January, 5)},   // year rollover
	}
	for _, c := range cases {
		if got := addMonthClamped(c.in); !got.Equal(c.want) {
			t.Errorf("addMonthClamped(%s) = %s, want %s",
				c.in.Format(dateLayout), got.Format(dateLayout), c.want.Format(dateLayout))
		}
	}
}

func TestDateRanges(t *testing.T) {
	t.Run("multi-month", func(t *testing.T) {
		// last event 2026-04-20 -> current walks Apr 21, May 21, Jun 21, Jul 21;
		// Jul 21 > 2026-07-02 so July is NOT fetched (the stepped day-of-month
		// governs inclusion, exactly like Python's `while current <= end_date`).
		got := dateRanges(date(2026, time.April, 20), date(2026, time.July, 2))
		want := []monthRange{
			{2026, 4, "2026-04-01", "2026-04-30"},
			{2026, 5, "2026-05-01", "2026-05-31"},
			{2026, 6, "2026-06-01", "2026-06-30"},
		}
		if len(got) != len(want) {
			t.Fatalf("got %d ranges %v, want %d", len(got), got, len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("range[%d] = %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("last window is not clamped to endDate", func(t *testing.T) {
		// current = Jul 1 <= Jul 2 -> the July window runs to Jul 31 even though
		// endDate is Jul 2; Python never clamps (the validation cutoff filters).
		got := dateRanges(date(2026, time.June, 30), date(2026, time.July, 2))
		want := monthRange{2026, 7, "2026-07-01", "2026-07-31"}
		if len(got) != 1 || got[0] != want {
			t.Errorf("got %v, want [%+v]", got, want)
		}
	})

	t.Run("empty when up to date", func(t *testing.T) {
		if got := dateRanges(date(2026, time.July, 2), date(2026, time.July, 2)); len(got) != 0 {
			t.Errorf("expected no ranges, got %v", got)
		}
		if got := dateRanges(date(2026, time.July, 3), date(2026, time.July, 2)); len(got) != 0 {
			t.Errorf("expected no ranges when last > end, got %v", got)
		}
	})

	t.Run("day-31 stepping does not skip February", func(t *testing.T) {
		// current starts at Jan 31; naive AddDate would jump Jan 31 -> Mar 3 and
		// emit no February window. relativedelta semantics keep Feb.
		got := dateRanges(date(2026, time.January, 30), date(2026, time.March, 1))
		var months []int
		for _, r := range got {
			months = append(months, r.Month)
		}
		if len(got) != 2 || got[0].Month != 1 || got[1].Month != 2 {
			t.Errorf("expected [Jan Feb], got months %v (%v)", months, got)
		}
	})
}

func TestLastUpdatedDate(t *testing.T) {
	fallback := date(config.StartYear, time.January, 1)

	t.Run("missing file falls back to start year", func(t *testing.T) {
		got := lastUpdatedDate(filepath.Join(t.TempDir(), "nope.csv"))
		if !got.Equal(fallback) {
			t.Errorf("got %s, want fallback %s", got, fallback)
		}
	})

	t.Run("max over unsorted rows", func(t *testing.T) {
		// The combined file is written in completion order, never sorted — the
		// newest event is NOT necessarily last.
		p := filepath.Join(t.TempDir(), "combined.csv")
		csv := "id,time_utc,mag\n" +
			"a,2026-06-15 01:02:03,4.1\n" +
			"b,2026-06-28 23:59:59,3.0\n" +
			"c,2026-06-20 12:00:00,2.5\n"
		if err := os.WriteFile(p, []byte(csv), 0o644); err != nil {
			t.Fatal(err)
		}
		if got, want := lastUpdatedDate(p), date(2026, time.June, 28); !got.Equal(want) {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("no time_utc column falls back", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "combined.csv")
		if err := os.WriteFile(p, []byte("id,mag\na,4.1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := lastUpdatedDate(p); !got.Equal(fallback) {
			t.Errorf("got %s, want fallback %s", got, fallback)
		}
	})
}

// v2Event builds a v2 API record as served by /api/v2/earthquakes.
func v2Event(id, timeUTC string, lat, lon, depth, mag float64, loc string) map[string]any {
	return map[string]any{
		"id": id, "time_utc": timeUTC,
		"latitude": lat, "longitude": lon, "depth_km": depth, "mag": mag,
		"mag_type": "mb", "location": loc, "country": "Myanmar",
	}
}

// fakeV2 serves a fixed set of events per "from" date, with correct meta pagination.
func fakeV2(t *testing.T, byFrom map[string][]map[string]any, fail map[string]bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/earthquakes" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		from := r.URL.Query().Get("from")
		if fail[from] {
			http.Error(w, `{"error":"boom"}`, http.StatusBadRequest) // permanent: no retry stall
			return
		}
		events := byFrom[from]
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		page := events
		if offset < len(events) {
			page = events[offset:]
		} else {
			page = nil
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"meta": map[string]any{
				"count": len(page), "total": len(events),
				"limit": 10000, "offset": offset,
			},
			"earthquakes": page,
		})
	}))
}

func TestRunEndToEnd(t *testing.T) {
	// Seed: last event 2026-05-15; fixed now 2026-07-02T12:00Z -> yesterday
	// 2026-07-01 -> fetch windows May, June, July.
	seed := []catalog.Quake{{
		ID: "seed1", Time: date(2026, time.May, 15),
		Latitude: 21.0, Longitude: 96.0, Depth: 10, Mag: 4.0,
		TimeUTC: "2026-05-15 08:00:00", TimeMMT: "2026-05-15 14:30:00",
		Extra: map[string]any{"location": "Seeded, Myanmar"},
	}}

	byFrom := map[string][]map[string]any{
		"2026-05-01": {
			v2Event("m1", "2026-05-20T01:00:00Z", 22.0, 96.1, 12, 4.5, "May One"),
			v2Event("m2", "2026-05-25T02:00:00Z", 20.5, 95.0, 8, 3.2, "May Two"),
		},
		"2026-06-01": {
			v2Event("j1", "2026-06-10T03:00:00Z", 19.9, 96.5, 20, 5.1, "June One"),
			// Future of the validation cutoff (now-24h = 2026-07-01T12:00Z is the
			// end bound; this event is after it) -> dropped by Validate.
			v2Event("jX", "2026-07-02T00:00:00Z", 19.9, 96.5, 20, 5.1, "Too New"),
		},
		"2026-07-01": {}, // July window: nothing yet
	}
	srv := fakeV2(t, byFrom, nil)
	defer srv.Close()

	dir := t.TempDir()
	combinedCSV := filepath.Join(dir, "csv", "combined", "earthquakes_combined.csv")
	if err := os.MkdirAll(filepath.Dir(combinedCSV), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := catalog.WriteMonthlyCSV(seed, combinedCSV, catalog.ColumnsFor(seed)); err != nil {
		t.Fatal(err)
	}
	// Pre-seed the yearly JSON with an event from an earlier fetch window: the
	// yearly JSON must MERGE (audit C5 — the old overwrite left an active
	// year's JSON holding only the latest run's frames).
	yearlyJSON := filepath.Join(dir, "json", "yearly", "earthquakes_2026.json")
	if err := os.MkdirAll(filepath.Dir(yearlyJSON), 0o755); err != nil {
		t.Fatal(err)
	}
	prior := []catalog.Quake{{
		ID: "old2026", Time: date(2026, time.January, 10),
		Latitude: 20.2, Longitude: 95.2, Depth: 15, Mag: 4.2,
		TimeUTC: "2026-01-10 04:00:00", TimeMMT: "2026-01-10 10:30:00",
	}}
	if err := catalog.WriteJSON(prior, yearlyJSON); err != nil {
		t.Fatal(err)
	}

	err := Run(Options{
		BaseURL:   srv.URL,
		ExportDir: dir,
		DataDir:   filepath.Join(dir, "no-such-data"), // geocoder skipped, like Python's try/except
		Workers:   2,
		Route:     "v2",
		Now:       func() time.Time { return time.Date(2026, time.July, 2, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	read := func(rel string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		return string(b)
	}

	// Monthly files exist for May and June (July validated to empty -> no file).
	may := read("csv/monthly/earthquakes_2026_05.csv")
	for _, id := range []string{"m1", "m2"} {
		if !strings.Contains(may, id) {
			t.Errorf("monthly May CSV missing %s:\n%s", id, may)
		}
	}
	june := read("csv/monthly/earthquakes_2026_06.csv")
	if !strings.Contains(june, "j1") || strings.Contains(june, "jX") {
		t.Errorf("monthly June CSV wrong (want j1, no jX):\n%s", june)
	}
	if _, err := os.Stat(filepath.Join(dir, "csv/monthly/earthquakes_2026_07.csv")); !os.IsNotExist(err) {
		t.Errorf("July monthly CSV should not exist (validated empty)")
	}

	// Yearly: merged CSV + merged JSON, both with the 3 new events; the JSON
	// additionally retains the pre-seeded January event.
	yearly := read("csv/yearly/earthquakes_2026.csv")
	for _, id := range []string{"m1", "m2", "j1"} {
		if !strings.Contains(yearly, id) {
			t.Errorf("yearly CSV missing %s", id)
		}
	}
	var ypayload struct {
		Earthquakes []map[string]any `json:"earthquakes"`
	}
	if err := json.Unmarshal([]byte(read("json/yearly/earthquakes_2026.json")), &ypayload); err != nil {
		t.Fatalf("yearly JSON: %v", err)
	}
	if len(ypayload.Earthquakes) != 4 {
		t.Errorf("yearly JSON has %d events, want 4 (old2026 preserved + m1,m2,j1)", len(ypayload.Earthquakes))
	}
	yjson := read("json/yearly/earthquakes_2026.json")
	if !strings.Contains(yjson, "old2026") {
		t.Errorf("yearly JSON lost the pre-existing old2026 event (overwrite instead of merge)")
	}

	// Combined CSV merges seed + new; combined JSON merges into the (absent) JSON.
	combined := read("csv/combined/earthquakes_combined.csv")
	for _, id := range []string{"seed1", "m1", "m2", "j1"} {
		if !strings.Contains(combined, id) {
			t.Errorf("combined CSV missing %s", id)
		}
	}
	var payload struct {
		Earthquakes []map[string]any `json:"earthquakes"`
	}
	if err := json.Unmarshal([]byte(read("json/combined/earthquakes_combined.json")), &payload); err != nil {
		t.Fatalf("combined JSON: %v", err)
	}
	if len(payload.Earthquakes) != 3 {
		t.Errorf("combined JSON has %d events, want 3 (m1,m2,j1)", len(payload.Earthquakes))
	}
}

func TestRunMonthFailureContinues(t *testing.T) {
	// June fails permanently; May must still be written and Run must return an
	// error (Python: has_error -> sys.exit(1) after writing what succeeded).
	byFrom := map[string][]map[string]any{
		"2026-05-01": {v2Event("m1", "2026-05-20T01:00:00Z", 22.0, 96.1, 12, 4.5, "May One")},
	}
	srv := fakeV2(t, byFrom, map[string]bool{"2026-06-01": true, "2026-07-01": true})
	defer srv.Close()

	// Pin lastUpdated via a seed combined CSV so the run fetches only May–July.
	dir := t.TempDir()
	combinedCSV := filepath.Join(dir, "csv", "combined", "earthquakes_combined.csv")
	if err := os.MkdirAll(filepath.Dir(combinedCSV), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(combinedCSV, []byte("id,time_utc\nseed1,2026-04-30 00:00:00\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(Options{
		BaseURL:   srv.URL,
		ExportDir: dir,
		DataDir:   filepath.Join(dir, "no-such-data"),
		Workers:   2,
		Route:     "v2",
		Now:       func() time.Time { return time.Date(2026, time.July, 2, 12, 0, 0, 0, time.UTC) },
	})
	if err == nil {
		t.Fatal("Run should report an error when a month fails")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "csv/monthly/earthquakes_2026_05.csv")); statErr != nil {
		t.Errorf("May monthly CSV should exist despite June failure: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "csv/monthly/earthquakes_2026_06.csv")); !os.IsNotExist(statErr) {
		t.Errorf("June monthly CSV should not exist")
	}
}

// TestRunV1Route exercises the explicit v1-compat route: raw records (string
// values, upstream key set, "time" field) flow verbatim into Validate/writer.
// (The config default is now "export"; v1 is a fallback selected here explicitly.)
func TestRunV1Route(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/myanmar-quakes" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var events []map[string]any
		if r.URL.Query().Get("from") == "2026-06-01" {
			// v1 inclusive window, raw string-typed record
			events = []map[string]any{{
				"id": "v1a", "time": "2026-06-10T03:00:00.000Z",
				"latitude": "19.9000000", "longitude": "96.5000000",
				"depth": "20.00", "mag": "5.10", "magType": "mb",
				"nst": "", "gap": "", "location": "June One, Myanmar",
			}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"earthquakes": events})
	}))
	defer srv.Close()

	dir := t.TempDir()
	combinedCSV := filepath.Join(dir, "csv", "combined", "earthquakes_combined.csv")
	if err := os.MkdirAll(filepath.Dir(combinedCSV), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(combinedCSV, []byte("id,time_utc\nseed1,2026-05-31 00:00:00\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := Run(Options{
		BaseURL:   srv.URL,
		ExportDir: dir,
		DataDir:   filepath.Join(dir, "no-such-data"),
		Workers:   2,
		Route:     "v1", // explicit: the config default is now "export"
		Now:       func() time.Time { return time.Date(2026, time.July, 2, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "csv/monthly/earthquakes_2026_06.csv"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, "v1a") || !strings.Contains(got, "2026-06-10 03:00:00") {
		t.Errorf("June monthly CSV missing v1 record or formatted time:\n%s", got)
	}
	// mag came in as the string "5.10" — Python's to_numeric turns it into 5.1.
	if !strings.Contains(got, "5.1") {
		t.Errorf("expected numeric mag 5.1 in CSV:\n%s", got)
	}
}

// TestRunExportRouteDefault exercises the config default route ("export"): raw
// v1-shaped records served through the v2 export envelope (meta.total + earthquakes[])
// flow VERBATIM into Validate/writer, just like the v1 route but paginated.
func TestRunExportRouteDefault(t *testing.T) {
	rawRecords := []map[string]any{{
		"id": "ex1", "time": "2026-06-10T03:00:00.000Z",
		"latitude": "19.9000000", "longitude": "96.5000000",
		"depth": "20.00", "mag": "5.10", "magType": "mb",
		"continent": "Asia", "location": "June One, Myanmar",
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/export" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var events []map[string]any
		if r.URL.Query().Get("from") == "2026-06-01" {
			events = rawRecords
		}
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		var page []map[string]any
		if offset < len(events) {
			page = events[offset:]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"meta": map[string]any{
				"count": len(page), "total": len(events), "limit": 10000, "offset": offset,
			},
			"earthquakes": page,
		})
	}))
	defer srv.Close()

	dir := t.TempDir()
	combinedCSV := filepath.Join(dir, "csv", "combined", "earthquakes_combined.csv")
	if err := os.MkdirAll(filepath.Dir(combinedCSV), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(combinedCSV, []byte("id,time_utc\nseed1,2026-05-31 00:00:00\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := Run(Options{
		BaseURL:   srv.URL,
		ExportDir: dir,
		DataDir:   filepath.Join(dir, "no-such-data"),
		Workers:   2,
		// Route omitted: config default is now "export"
		Now: func() time.Time { return time.Date(2026, time.July, 2, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "csv/monthly/earthquakes_2026_06.csv"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, "ex1") || !strings.Contains(got, "2026-06-10 03:00:00") {
		t.Errorf("June monthly CSV missing export record or formatted time:\n%s", got)
	}
	if !strings.Contains(got, "5.1") { // string "5.10" -> numeric 5.1, like v1
		t.Errorf("expected numeric mag 5.1 in CSV:\n%s", got)
	}
}

// TestRunInvalidRoute asserts the dispatch fails loud on an unknown route rather
// than silently falling back to a route (which would ship a wrong-fidelity artifact).
func TestRunInvalidRoute(t *testing.T) {
	dir := t.TempDir()
	err := Run(Options{
		BaseURL:   "http://unused.invalid",
		ExportDir: dir,
		DataDir:   filepath.Join(dir, "no-such-data"),
		Workers:   1,
		Route:     "bogus",
		Now:       func() time.Time { return time.Date(2026, time.July, 2, 12, 0, 0, 0, time.UTC) },
	})
	if err == nil {
		t.Fatal("Run with an unknown route should return a descriptive error, not fetch")
	}
	if !strings.Contains(err.Error(), "bogus") || !strings.Contains(err.Error(), "route") {
		t.Errorf("err = %v, want a descriptive unknown-route error naming the bad value", err)
	}
}

func TestV2BaseNormalization(t *testing.T) {
	for in, want := range map[string]string{
		"https://mmeq.akze.net":         "https://mmeq.akze.net",
		"https://mmeq.akze.net/":        "https://mmeq.akze.net",
		"https://mmeq.akze.net/api/v2":  "https://mmeq.akze.net",
		"https://mmeq.akze.net/api/v2/": "https://mmeq.akze.net",
	} {
		if got := config.V2Base(in); got != want {
			t.Errorf("V2Base(%q) = %q, want %q", in, got, want)
		}
	}
}
