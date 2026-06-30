// Package api fetches earthquake records from the Myanmar Earthquake API.
//
// It mirrors src/mmeq/export/fetcher.py: a retrying HTTP client (429 + 5xx, backoff
// with jitter, Retry-After) and Fetch(from, to) -> []Quake. It MUST implement the
// 500-record adaptive window bisection from spec 001 (the API silently caps responses
// at 500, newest-first, with no pagination). Implementation pending spec 002.
package api
