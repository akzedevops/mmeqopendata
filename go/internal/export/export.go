// Package export orchestrates the fetch → validate → geocode → dedup → write
// pipeline, mirroring src/mmeq/cli.py:cmd_export (plus fetcher.get_last_updated_date
// and fetcher.generate_date_ranges). See ../../../specs/002-go-export-rewrite.md
// increment I5.
//
// Semantics replicated from the Python pipeline, exactly:
//
//   - lastUpdated: full scan of the time_utc column of the existing combined CSV,
//     taking the MAX (the file is written in completion order, never sorted); any
//     failure falls back to config.StartYear Jan 1.
//   - Date ranges: month windows from (lastUpdated+1day).replace(day=1) while
//     current <= yesterday-UTC, each window [month first, month last] — the last
//     window is NOT clamped to yesterday (Python doesn't clamp; the validation
//     cutoff below excludes future rows). Month stepping clamps the day like
//     dateutil.relativedelta (Jan 31 + 1 month = Feb 28), NOT Go's AddDate
//     normalization — otherwise a Feb window could be skipped entirely.
//   - Validation cutoff: end_date = now(UTC) - 24h, an instant (not yesterday
//     23:59:59), matching validate_quake_data's default.
//   - A month whose fetch fails is logged and skipped; the run continues and
//     Run returns a non-nil error at the end (Python sets has_error and exits 1).
//   - Monthly files are overwritten with just the new frame; yearly/combined CSVs
//     merge+dedup keyed on id keep-last; the combined JSON merges keyed on id.
//   - Yearly JSON quirk: Python calls save_to_json(ydf) where ydf is only THIS
//     run's fetched frames for the year — the yearly JSON is overwritten, not
//     merged. Replicated verbatim (WriteJSON, not a merge).
//   - "No new data" (no ranges, or every fetched month validated to empty) logs
//     and returns nil (unless a month errored).
package export

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/akzedevops/mmeqopendata/go/internal/api"
	"github.com/akzedevops/mmeqopendata/go/internal/catalog"
	"github.com/akzedevops/mmeqopendata/go/internal/config"
	"github.com/akzedevops/mmeqopendata/go/internal/geocoder"
)

// exportSubdirs mirrors config.EXPORT_SUBDIRS (created up front, even if empty).
var exportSubdirs = []string{
	"json/monthly", "json/yearly", "json/combined",
	"csv/monthly", "csv/yearly", "csv/combined",
}

const dateLayout = "2006-01-02"

// Options configures one export run. Zero values fall back to the config
// package defaults (which honor the MMEQ_* environment).
type Options struct {
	BaseURL    string           // API host base; default config.APIV2BaseURL
	ExportDir  string           // default config.ExportDir
	DataDir    string           // geocoder data dir; default "data"
	Workers    int              // concurrent month fetches; default config.MaxWorkers
	MaxRetries int              // api.Client retries; 0 = client default
	Route      string           // fetch route: "export" (full-fidelity artifact route, default), "v1" (legacy compat, same raw shape), or "v2" (typed JSON, cannot reproduce the artifact); default config.FetchRoute. Any other value fails the run.
	HTTPClient *http.Client     // override for tests
	Now        func() time.Time // override for tests; default time.Now
}

func (o *Options) fill() {
	if o.BaseURL == "" {
		o.BaseURL = config.APIV2BaseURL
	}
	if o.ExportDir == "" {
		o.ExportDir = config.ExportDir
	}
	if o.DataDir == "" {
		o.DataDir = "data"
	}
	if o.Workers <= 0 {
		o.Workers = config.MaxWorkers
	}
	if o.Route == "" {
		o.Route = config.FetchRoute
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// monthRange is one fetch window, mirroring generate_date_ranges' (year, month,
// from_date, to_date) tuples.
type monthRange struct {
	Year  int
	Month int
	From  string // "YYYY-MM-DD", first day of the month
	To    string // "YYYY-MM-DD", last day of the month (never clamped)
}

// parseTimeUTC parses a time_utc CSV cell, accepting the formats the pipeline
// has ever written (pd.to_datetime is more permissive; these cover our files).
func parseTimeUTC(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05", time.RFC3339Nano, time.RFC3339, dateLayout,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// lastUpdatedDate mirrors fetcher.get_last_updated_date: scan the FULL time_utc
// column of the combined CSV and return the max as a date (midnight UTC). Any
// failure — missing file, missing column, no parseable timestamp — falls back to
// config.StartYear Jan 1, with a warning.
func lastUpdatedDate(combinedCSV string) time.Time {
	fallback := time.Date(config.StartYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	f, err := os.Open(combinedCSV)
	if err != nil {
		log.Printf("export: fallback to start year: %v", err)
		return fallback
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	header, err := r.Read()
	if err != nil {
		log.Printf("export: fallback to start year: read %s header: %v", combinedCSV, err)
		return fallback
	}
	idx := -1
	for i, h := range header {
		if h == "time_utc" {
			idx = i
			break
		}
	}
	if idx < 0 {
		log.Printf("export: fallback to start year: %s has no time_utc column", combinedCSV)
		return fallback
	}

	var max time.Time
	found := false
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // on_bad_lines="skip"
		}
		if idx >= len(rec) {
			continue
		}
		if t, ok := parseTimeUTC(rec[idx]); ok && (!found || t.After(max)) {
			max, found = t, true
		}
	}
	if !found {
		log.Printf("export: fallback to start year: no parseable time_utc in %s", combinedCSV)
		return fallback
	}
	last := time.Date(max.Year(), max.Month(), max.Day(), 0, 0, 0, 0, time.UTC)
	log.Printf("Last updated date: %s", last.Format(dateLayout))
	return last
}

// addMonthClamped adds one calendar month keeping the day-of-month, clamping to
// the target month's last day — dateutil.relativedelta(months=1) semantics
// (Jan 31 -> Feb 28), NOT time.AddDate's normalization (Jan 31 -> Mar 3).
func addMonthClamped(t time.Time) time.Time {
	first := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
	lastDay := first.AddDate(0, 1, -1).Day()
	d := t.Day()
	if d > lastDay {
		d = lastDay
	}
	return time.Date(first.Year(), first.Month(), d, 0, 0, 0, 0, time.UTC)
}

// dateRanges mirrors fetcher.generate_date_ranges: month windows from
// lastUpdated+1day through endDate (both dates at midnight UTC, inclusive).
func dateRanges(lastUpdated, endDate time.Time) []monthRange {
	var out []monthRange
	for current := lastUpdated.AddDate(0, 0, 1); !current.After(endDate); current = addMonthClamped(current) {
		from := time.Date(current.Year(), current.Month(), 1, 0, 0, 0, 0, time.UTC)
		to := from.AddDate(0, 1, -1) // last day of the month; never clamped to endDate
		out = append(out, monthRange{
			Year:  from.Year(),
			Month: int(from.Month()),
			From:  from.Format(dateLayout),
			To:    to.Format(dateLayout),
		})
	}
	return out
}

// monthResult is the outcome of one month's fetch+validate+enrich+dedup.
type monthResult struct {
	quakes []catalog.Quake
	err    error
}

// Run executes one export cycle. It returns nil on success (including the
// no-new-data case) and an error if any month failed — the caller should exit
// non-zero, matching the Python sys.exit(1) semantics.
func Run(opts Options) error {
	opts.fill()

	client := &api.Client{
		BaseURL:    opts.BaseURL,
		HTTPClient: opts.HTTPClient,
		MaxRetries: opts.MaxRetries,
	}
	// Explicit three-way route dispatch — fail loud on anything unknown rather than
	// silently defaulting to a route (which would ship the wrong-fidelity artifact).
	var fetch func(context.Context, string, string) ([]map[string]any, error)
	switch opts.Route {
	case "export":
		fetch = client.FetchWindowExport // full-fidelity artifact route (default)
	case "v1":
		fetch = client.FetchWindowV1 // legacy-compat route
	case "v2":
		fetch = client.FetchWindow // typed route; cannot reproduce the artifact
	default:
		return fmt.Errorf("export: unknown fetch route %q (want \"export\", \"v1\", or \"v2\")", opts.Route)
	}

	now := opts.Now().UTC()

	for _, sub := range exportSubdirs {
		if err := os.MkdirAll(filepath.Join(opts.ExportDir, sub), 0o755); err != nil {
			return fmt.Errorf("export: create %s: %w", sub, err)
		}
	}

	combinedCSV := filepath.Join(opts.ExportDir, "csv", "combined", "earthquakes_combined.csv")
	combinedJSON := filepath.Join(opts.ExportDir, "json", "combined", "earthquakes_combined.json")

	yesterday := now.AddDate(0, 0, -1)
	endDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	ranges := dateRanges(lastUpdatedDate(combinedCSV), endDate)
	log.Printf("Processing %d months of earthquake data...", len(ranges))
	if len(ranges) == 0 {
		log.Printf("No new data to process.")
		return nil
	}

	// Geocoder is shared, read-only after load. A load failure mirrors Python's
	// try/except around enrich_dataframe: warn and continue WITHOUT the 6 columns.
	geo, err := geocoder.New(opts.DataDir)
	if err != nil {
		log.Printf("Geocoding enrichment skipped: %v", err)
		geo = nil
	}

	validateEnd := now.Add(-24 * time.Hour) // datetime.now(utc) - timedelta(days=1)

	// Fetch + validate months concurrently, bounded by Workers (mirrors
	// ThreadPoolExecutor). Results land at their range index so all writing below
	// is deterministic in date order (Python writes in completion order, which is
	// nondeterministic — the artifacts are keyed/deduped by id either way).
	results := make([]monthResult, len(ranges))
	sem := make(chan struct{}, opts.Workers)
	var wg sync.WaitGroup
	for i := range ranges {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			r := ranges[i]
			raw, err := fetch(context.Background(), r.From, r.To)
			if err != nil {
				results[i].err = err
				return
			}
			quakes := catalog.Validate(raw, validateEnd)
			if geo != nil {
				for j := range quakes {
					geo.Enrich(&quakes[j])
				}
			}
			results[i].quakes = catalog.Dedup(quakes)
		}(i)
	}
	wg.Wait()

	hasError := false
	var all []catalog.Quake
	yearly := make(map[int][]catalog.Quake)
	var years []int
	for i, r := range ranges {
		res := results[i]
		if res.err != nil {
			hasError = true
			log.Printf("Failed to process %d-%02d: %v", r.Year, r.Month, res.err)
			continue
		}
		if len(res.quakes) == 0 {
			continue
		}
		name := fmt.Sprintf("earthquakes_%d_%02d", r.Year, r.Month)
		monthlyCSV := filepath.Join(opts.ExportDir, "csv", "monthly", name+".csv")
		monthlyJSON := filepath.Join(opts.ExportDir, "json", "monthly", name+".json")
		if err := catalog.WriteMonthlyCSV(res.quakes, monthlyCSV, catalog.DefaultColumns); err != nil {
			return fmt.Errorf("export: write %s: %w", monthlyCSV, err)
		}
		if err := catalog.WriteJSON(res.quakes, monthlyJSON); err != nil {
			return fmt.Errorf("export: write %s: %w", monthlyJSON, err)
		}
		if _, ok := yearly[r.Year]; !ok {
			years = append(years, r.Year)
		}
		yearly[r.Year] = append(yearly[r.Year], res.quakes...)
		all = append(all, res.quakes...)
		log.Printf("Updated %d-%02d: %d records", r.Year, r.Month, len(res.quakes))
	}

	if len(all) == 0 {
		log.Printf("No new data to process.")
		if hasError {
			return errors.New("export: one or more months failed")
		}
		return nil
	}

	log.Printf("Saving yearly and combined files...")
	for _, y := range years {
		q := yearly[y]
		yearlyCSV := filepath.Join(opts.ExportDir, "csv", "yearly", fmt.Sprintf("earthquakes_%d.csv", y))
		yearlyJSON := filepath.Join(opts.ExportDir, "json", "yearly", fmt.Sprintf("earthquakes_%d.json", y))
		if err := catalog.WriteMergedCSV(q, yearlyCSV, catalog.DefaultColumns); err != nil {
			return fmt.Errorf("export: write %s: %w", yearlyCSV, err)
		}
		// Python quirk, replicated on purpose: save_to_json(ydf) OVERWRITES the
		// yearly JSON with only this run's fetched frames for the year (it is not
		// merged with the file's previous contents).
		if err := catalog.WriteJSON(q, yearlyJSON); err != nil {
			return fmt.Errorf("export: write %s: %w", yearlyJSON, err)
		}
	}

	if err := catalog.WriteMergedCSV(all, combinedCSV, catalog.DefaultColumns); err != nil {
		return fmt.Errorf("export: write %s: %w", combinedCSV, err)
	}
	if err := catalog.MergeCombinedJSON(combinedJSON, all, combinedJSON); err != nil {
		return fmt.Errorf("export: write %s: %w", combinedJSON, err)
	}

	log.Printf("All done! Earthquake data is up to date!")
	if hasError {
		return errors.New("export: one or more months failed")
	}
	return nil
}
