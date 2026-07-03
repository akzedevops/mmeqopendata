package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// shrinkBackoff makes retry waits near-instant for the duration of a test.
func shrinkBackoff(t *testing.T) {
	t.Helper()
	saved := backoffBase
	backoffBase = time.Millisecond
	t.Cleanup(func() { backoffBase = saved })
}

// serve returns a test server whose handler is fn and a Client pointed at it.
func serve(t *testing.T, fn http.HandlerFunc) (*httptest.Server, *Client) {
	t.Helper()
	srv := httptest.NewServer(fn)
	t.Cleanup(srv.Close)
	return srv, &Client{BaseURL: srv.URL}
}

// writePage writes a well-formed v2 response with the given records and total.
func writePage(w http.ResponseWriter, records []map[string]any, total, limit, offset int) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"meta": map[string]any{
			"api_version": 2, "count": len(records), "total": total,
			"limit": limit, "offset": offset, "generated_at": "2026-07-02T00:00:00Z",
		},
		"earthquakes": records,
	})
}

// event builds a minimal valid v2 record with the given id and time.
func event(id, timeUTC string) map[string]any {
	return map[string]any{
		"id": id, "time_utc": timeUTC, "latitude": 24.441, "longitude": 94.449,
		"depth_km": 90.0, "mag": 3.6, "mag_type": "ml", "location": "loc",
		"place": "place", "country": "MM", "type": "earthquake", "net": nil,
		"updated_at": "2026-06-01T03:00:00Z", "ingested_at": "2026-06-01T03:00:00Z",
	}
}

// (a) FetchWindow takes v1-INCLUSIVE dates and must query the v2 half-open window,
// i.e. to = toDate + 1 day, alongside the endpoint path and pagination params.
func TestFetchWindowConvertsInclusiveToHalfOpen(t *testing.T) {
	var gotPath string
	var gotQuery url.Values
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query()
		writePage(w, []map[string]any{event("q1", "2026-06-01T02:21:31Z")}, 1, 10000, 0)
	})

	if _, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30"); err != nil {
		t.Fatalf("FetchWindow: %v", err)
	}
	if gotPath != "/api/v2/earthquakes" {
		t.Errorf("path = %q, want /api/v2/earthquakes", gotPath)
	}
	if got := gotQuery.Get("from"); got != "2026-06-01" {
		t.Errorf("from = %q, want 2026-06-01", got)
	}
	if got := gotQuery.Get("to"); got != "2026-07-01" { // inclusive 06-30 -> half-open 07-01
		t.Errorf("to = %q, want 2026-07-01 (toDate + 1 day)", got)
	}
	if got := gotQuery.Get("limit"); got != "10000" {
		t.Errorf("limit = %q, want 10000 (default page limit)", got)
	}
	if got := gotQuery.Get("offset"); got != "0" {
		t.Errorf("offset = %q, want 0", got)
	}
}

func TestFetchWindowRejectsBadToDate(t *testing.T) {
	c := &Client{BaseURL: "http://unused.invalid"}
	if _, err := c.FetchWindow(context.Background(), "2026-06-01", "not-a-date"); err == nil {
		t.Fatal("FetchWindow with invalid toDate: want error, got nil")
	}
}

// (b) Pagination: 5 events at limit=2 -> 3 pages, each event exactly once, in order.
func TestFetchWindowPaginates(t *testing.T) {
	all := make([]map[string]any, 5)
	for i := range all {
		all[i] = event(fmt.Sprintf("q%d", i), "2026-06-01T02:21:31Z")
	}
	var requests int
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		end := offset + limit
		if end > len(all) {
			end = len(all)
		}
		writePage(w, all[offset:end], len(all), limit, offset)
	})
	c.PageLimit = 2

	got, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30")
	if err != nil {
		t.Fatalf("FetchWindow: %v", err)
	}
	if requests != 3 {
		t.Errorf("requests = %d, want 3 (5 events / limit 2)", requests)
	}
	if len(got) != 5 {
		t.Fatalf("len(records) = %d, want 5", len(got))
	}
	for i, rec := range got { // union of all pages, exactly once, in API order
		if want := fmt.Sprintf("q%d", i); rec["id"] != want {
			t.Errorf("record %d id = %v, want %s", i, rec["id"], want)
		}
	}
}

// (c) Field mapping: v2 names -> v1-ish keys, nulls -> "" for strings, and null
// numeric fields dropped (key absent) so catalog.Validate's dropna kicks in.
func TestFetchWindowMapsRecordFields(t *testing.T) {
	rec := event("q1", "2026-06-01T02:21:31Z")
	rec["mag"] = nil      // null numeric -> key must be absent
	rec["location"] = nil // null string -> ""
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		writePage(w, []map[string]any{rec}, 1, 10000, 0)
	})

	got, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30")
	if err != nil {
		t.Fatalf("FetchWindow: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(got))
	}
	m := got[0]

	// Renames: value moves to the v1 key, the v2 key disappears.
	renames := map[string]string{
		"time_utc": "time", "depth_km": "depth", "mag_type": "magType", "updated_at": "updated",
	}
	for v2k, v1k := range renames {
		if _, ok := m[v2k]; ok {
			t.Errorf("key %q should have been renamed to %q", v2k, v1k)
		}
	}
	if m["time"] != "2026-06-01T02:21:31Z" {
		t.Errorf("time = %v, want the time_utc value", m["time"])
	}
	if m["depth"] != 90.0 {
		t.Errorf("depth = %v, want 90 (from depth_km, native number)", m["depth"])
	}
	if m["magType"] != "ml" {
		t.Errorf("magType = %v, want ml", m["magType"])
	}
	if m["updated"] != "2026-06-01T03:00:00Z" {
		t.Errorf("updated = %v, want the updated_at value", m["updated"])
	}

	// Pass-through fields keep their names and native types.
	if m["id"] != "q1" || m["latitude"] != 24.441 || m["longitude"] != 94.449 ||
		m["country"] != "MM" || m["type"] != "earthquake" || m["place"] != "place" {
		t.Errorf("pass-through fields corrupted: %v", m)
	}

	// Nulls: string -> "", numeric -> absent.
	if m["net"] != "" {
		t.Errorf("net = %#v, want \"\" (null string field)", m["net"])
	}
	if m["location"] != "" {
		t.Errorf("location = %#v, want \"\" (null string field)", m["location"])
	}
	if v, ok := m["mag"]; ok {
		t.Errorf("mag = %v, want key absent (null numeric field)", v)
	}
}

// FetchUpdatedAfter uses the updated_after param and the same mapping/pagination.
func TestFetchUpdatedAfter(t *testing.T) {
	var gotQuery url.Values
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		writePage(w, []map[string]any{event("q1", "2026-06-01T02:21:31Z")}, 1, 10000, 0)
	})

	got, err := c.FetchUpdatedAfter(context.Background(), "2026-06-01T00:00:00Z")
	if err != nil {
		t.Fatalf("FetchUpdatedAfter: %v", err)
	}
	if got := gotQuery.Get("updated_after"); got != "2026-06-01T00:00:00Z" {
		t.Errorf("updated_after = %q, want 2026-06-01T00:00:00Z", got)
	}
	if len(got) != 1 || got[0]["time"] != "2026-06-01T02:21:31Z" {
		t.Errorf("records = %v, want one mapped record with a time key", got)
	}
}

// (d) Retry: two 500s then a 200 must succeed after exactly 3 attempts.
func TestRetryOn5xxThenSuccess(t *testing.T) {
	shrinkBackoff(t)
	var attempts int
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts <= 2 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		writePage(w, []map[string]any{event("q1", "2026-06-01T02:21:31Z")}, 1, 10000, 0)
	})

	got, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30")
	if err != nil {
		t.Fatalf("FetchWindow: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (500, 500, 200)", attempts)
	}
	if len(got) != 1 {
		t.Errorf("len(records) = %d, want 1", len(got))
	}
}

// Retries are bounded: MaxRetries=3 means at most 4 attempts before failing.
func TestRetryExhausted(t *testing.T) {
	shrinkBackoff(t)
	var attempts int
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "boom", http.StatusInternalServerError)
	})

	if _, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30"); err == nil {
		t.Fatal("want error after exhausting retries, got nil")
	}
	if attempts != 4 {
		t.Errorf("attempts = %d, want 4 (1 initial + 3 retries)", attempts)
	}
}

// Non-retryable statuses (4xx other than 429) fail immediately.
func TestNoRetryOn4xx(t *testing.T) {
	shrinkBackoff(t)
	var attempts int
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, "bad request", http.StatusBadRequest)
	})

	if _, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30"); err == nil {
		t.Fatal("want error on 400, got nil")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (400 is not retryable)", attempts)
	}
}

// (e) Context cancellation aborts an in-flight request without further retries.
func TestContextCancellationAborts(t *testing.T) {
	release := make(chan struct{})
	var attempts atomic.Int32 // handler goroutine is still running when we read it
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		<-release // block until the test finishes; the client must not wait for us
	})
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.FetchWindow(ctx, "2026-06-01", "2026-06-30")
		errc <- err
	}()

	time.Sleep(20 * time.Millisecond) // let the request reach the blocked handler
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FetchWindow did not return after context cancellation")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (no retry after cancellation)", got)
	}
}

// (f) A Client with no explicit HTTPClient must get a finite, non-zero timeout —
// http.DefaultClient (Timeout 0) would hang forever on a server that accepts a
// connection and never responds.
func TestDefaultHTTPClientHasTimeout(t *testing.T) {
	c := &Client{BaseURL: "http://unused.invalid"}
	if got := c.httpClient().Timeout; got <= 0 {
		t.Errorf("default httpClient().Timeout = %v, want > 0 (must not be http.DefaultClient)", got)
	}
}

// A server that accepts and never responds must fail the fetch once the client
// timeout elapses, rather than hanging the caller forever.
func TestFetchWindowTimesOutOnSilentServer(t *testing.T) {
	shrinkBackoff(t)
	release := make(chan struct{})
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		<-release // never respond; the client's Timeout must fire first
	})
	t.Cleanup(func() { close(release) })
	c.HTTPClient = &http.Client{Timeout: 50 * time.Millisecond}
	c.MaxRetries = 1

	errc := make(chan error, 1)
	go func() {
		_, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30")
		errc <- err
	}()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("want timeout error from silent server, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FetchWindow hung on a silent server; client timeout did not fire")
	}
}

// (g) Pagination must terminate against a lying server that keeps returning
// non-empty pages with meta.total > offset forever. With the default limit the
// per-page cap is total/limit+2 = 2 pages here.
func TestFetchAllCapsPagesAgainstLyingServer(t *testing.T) {
	var requests atomic.Int32
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		// Always one more record, with total forever ahead of offset.
		writePage(w, []map[string]any{event(fmt.Sprintf("q%d", offset), "2026-06-01T02:21:31Z")},
			offset+2, 10000, offset)
	})

	_, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30")
	if err == nil {
		t.Fatal("want pagination-cap error against lying server, got nil")
	}
	if !strings.Contains(err.Error(), "pagination did not terminate") {
		t.Errorf("err = %v, want a descriptive pagination-cap error", err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("requests = %d, want 2 (total/limit+2 pages with total barely ahead)", got)
	}
}

// The absolute page ceiling bounds even a server whose meta.total is astronomically
// large (so total/limit+2 would allow near-unbounded pages).
func TestFetchAllAbsolutePageCeiling(t *testing.T) {
	var requests atomic.Int32
	_, c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		writePage(w, []map[string]any{event(fmt.Sprintf("q%d", offset), "2026-06-01T02:21:31Z")},
			1<<40, 1, offset) // total so large the formula exceeds the ceiling
	})
	c.PageLimit = 1

	_, err := c.FetchWindow(context.Background(), "2026-06-01", "2026-06-30")
	if err == nil {
		t.Fatal("want pagination-cap error, got nil")
	}
	if got := requests.Load(); got != maxPagesCeiling {
		t.Errorf("requests = %d, want %d (absolute page ceiling)", got, maxPagesCeiling)
	}
}
