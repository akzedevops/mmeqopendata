// Package api fetches earthquake records from the Myanmar Earthquake API v2
// (GET {base}/api/v2/earthquakes), mirroring src/mmeq/export/fetcher.py.
//
// The v2 API serves typed JSON over half-open [from,to) windows with proper
// limit/offset pagination (max limit 10000) and an updated_after incremental-sync
// filter, so the spec-001 bisection tripwire is NOT implemented here — callers own
// that. The client retries 429/5xx and transport errors with exponential backoff,
// and remaps each record to the v1-ish keys catalog.Validate and the writer expect
// (time_utc -> time, depth_km -> depth, mag_type -> magType, updated_at -> updated).
// See ../../../specs/002-go-export-rewrite.md (increment I2).
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	endpointPath      = "/api/v2/earthquakes"
	dateLayout        = "2006-01-02"
	defaultPageLimit  = 10000 // the API's documented max limit
	defaultMaxRetries = 3
)

// backoffBase is the first retry delay; attempt n waits backoffBase*2^n. A package
// variable (not a const) so tests can shrink it.
var backoffBase = 500 * time.Millisecond

// Client is a Myanmar Earthquake API v2 client. The zero value is not usable —
// BaseURL is required; the other fields fall back to sane defaults when unset.
type Client struct {
	BaseURL    string       // e.g. "https://mmeq.akze.net" (no trailing path)
	HTTPClient *http.Client // defaults to http.DefaultClient
	PageLimit  int          // records per page; defaults to 10000 (the API max)
	MaxRetries int          // retries after the first attempt; defaults to 3
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c *Client) pageLimit() int {
	if c.PageLimit > 0 {
		return c.PageLimit
	}
	return defaultPageLimit
}

func (c *Client) maxRetries() int {
	if c.MaxRetries > 0 {
		return c.MaxRetries
	}
	return defaultMaxRetries
}

// pageMeta is the v2 response "meta" object (the fields pagination needs).
type pageMeta struct {
	Count  int `json:"count"`
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// apiPage is one v2 response page. Records are decoded as raw maps so unknown/new
// API fields survive verbatim (the permissive-schema requirement from spec 002).
type apiPage struct {
	Meta        pageMeta         `json:"meta"`
	Earthquakes []map[string]any `json:"earthquakes"`
}

// numericV2 marks the v2 fields that are JSON numbers. A JSON null on one of these
// is DROPPED (key left absent) so catalog.Validate's dropna semantics apply; a null
// on any other field becomes "" like the v1 string shape.
var numericV2 = map[string]bool{
	"latitude":  true,
	"longitude": true,
	"depth_km":  true,
	"mag":       true,
}

// v1Key renames v2 field names to the v1-ish keys catalog.Validate and the writer
// consume. Every other key (id, latitude, longitude, mag, location, place, country,
// type, net, ingested_at, future additions) passes through unchanged.
var v1Key = map[string]string{
	"time_utc":   "time", // catalog.Validate reads rec["time"]
	"depth_km":   "depth",
	"mag_type":   "magType",
	"updated_at": "updated",
}

// mapRecord converts one raw v2 record to the v1-ish shape described above.
func mapRecord(rec map[string]any) map[string]any {
	out := make(map[string]any, len(rec))
	for k, v := range rec {
		if v == nil {
			if numericV2[k] {
				continue // absent numeric -> Validate drops the record, like dropna
			}
			v = "" // null string field -> "", matching the v1 API shape
		}
		if nk, ok := v1Key[k]; ok {
			k = nk
		}
		out[k] = v
	}
	return out
}

// FetchWindow fetches every event in the v1-INCLUSIVE date window [fromDate, toDate]
// (both "YYYY-MM-DD"). The v2 API uses half-open [from, to) windows, so toDate is
// converted by adding one day. Pages of PageLimit records are fetched until
// meta.total is exhausted; records are returned oldest-page-first in API order,
// remapped to the v1-ish keys (see mapRecord).
func (c *Client) FetchWindow(ctx context.Context, fromDate, toDate string) ([]map[string]any, error) {
	to, err := time.Parse(dateLayout, toDate)
	if err != nil {
		return nil, fmt.Errorf("api: invalid to date %q: %w", toDate, err)
	}
	params := url.Values{
		"from": {fromDate},
		"to":   {to.AddDate(0, 0, 1).Format(dateLayout)}, // inclusive -> half-open
	}
	return c.fetchAll(ctx, params)
}

// FetchUpdatedAfter fetches every event created or revised after the RFC3339
// instant since (the v2 updated_after filter), paginated and remapped exactly like
// FetchWindow. This backs the incremental daily sync.
func (c *Client) FetchUpdatedAfter(ctx context.Context, since string) ([]map[string]any, error) {
	params := url.Values{"updated_after": {since}}
	return c.fetchAll(ctx, params)
}

// fetchAll paginates limit/offset over the given filter params until meta.total is
// exhausted, remapping each record. An empty page also terminates, as a guard
// against a server that under-reports pages relative to total.
func (c *Client) fetchAll(ctx context.Context, params url.Values) ([]map[string]any, error) {
	limit := c.pageLimit()
	var out []map[string]any
	for offset := 0; ; {
		p := url.Values{}
		for k, vs := range params {
			p[k] = vs
		}
		p.Set("limit", strconv.Itoa(limit))
		p.Set("offset", strconv.Itoa(offset))

		page, err := c.getPage(ctx, p)
		if err != nil {
			return nil, err
		}
		for _, rec := range page.Earthquakes {
			out = append(out, mapRecord(rec))
		}
		offset += len(page.Earthquakes)
		if len(page.Earthquakes) == 0 || offset >= page.Meta.Total {
			return out, nil
		}
	}
}

// getPage GETs one page, retrying 429/5xx and transport errors with exponential
// backoff (backoffBase*2^n) up to MaxRetries retries. Context cancellation aborts
// immediately, including mid-backoff.
func (c *Client) getPage(ctx context.Context, params url.Values) (*apiPage, error) {
	u := strings.TrimRight(c.BaseURL, "/") + endpointPath + "?" + params.Encode()
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			wait := backoffBase * (1 << (attempt - 1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
		}

		page, retryable, err := c.doRequest(ctx, u)
		if err == nil {
			return page, nil
		}
		if ctx.Err() != nil { // cancellation surfaced as a transport error
			return nil, ctx.Err()
		}
		lastErr = err
		if !retryable || attempt >= c.maxRetries() {
			return nil, lastErr
		}
	}
}

// doRequest performs a single GET and decodes the page. retryable reports whether
// the failure is transient (429/5xx or a transport error) — a 4xx other than 429 or
// a malformed body is permanent.
func (c *Client) doRequest(ctx context.Context, u string) (page *apiPage, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, false, fmt.Errorf("api: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("api: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Drain a snippet for the error, then discard so the connection is reusable.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return nil, retryable, fmt.Errorf("api: HTTP %d from %s: %s", resp.StatusCode, u, strings.TrimSpace(string(snippet)))
	}

	var p apiPage
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, false, fmt.Errorf("api: decode response from %s: %w", u, err)
	}
	return &p, false, nil
}
