// Package config mirrors src/mmeq/config.py: constants and MMEQ_* environment
// overrides. See ../../../specs/002-go-export-rewrite.md.
package config

import (
	"os"
	"strconv"
	"strings"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Mirrors the Python constants in src/mmeq/config.py. Validation bounds are fixed;
// the rest are MMEQ_*-overridable.
var (
	APIURL    = env("MMEQ_API_URL", "https://mmeq.akze.net/api/myanmar-quakes")
	StartYear = envInt("MMEQ_START_YEAR", 1950)
	ExportDir = env("MMEQ_EXPORT_DIR", "quake_exports")

	// APIV2BaseURL is the v2 API HOST base; api.Client appends /api/v2/earthquakes
	// itself. MMEQ_API_V2_URL accepts either the bare host ("https://mmeq.akze.net")
	// or the ".../api/v2"-suffixed form the Python pipeline uses — the suffix is
	// stripped so both conventions work.
	APIV2BaseURL = V2Base(env("MMEQ_API_V2_URL", "https://mmeq.akze.net"))

	// FetchRoute picks which contract the exporter fetches from the same host:
	// "v1" — the legacy-compat route, serving the full raw upstream columns the
	// published 33-column artifact requires (parity default); or "v2" — typed
	// JSON, which drops the raw upstream columns (dmin, gap, nst, rms,
	// shakemap*, …) and so cannot reproduce the artifact byte-for-byte.
	FetchRoute = env("MMEQ_FETCH_ROUTE", "v1")

	MinLat, MaxLat     = -90.0, 90.0
	MinLon, MaxLon     = -180.0, 180.0
	MinDepth, MaxDepth = 0.0, 700.0
	MinMag, MaxMag     = 0.0, 10.0

	MaxWorkers = envInt("MMEQ_MAX_WORKERS", 10)
	// APIPageCap > 0 enables defensive window bisection. The live API is uncapped, so
	// the default is 0 (off), matching the corrected Python default. See specs/001.
	APIPageCap = envInt("MMEQ_API_PAGE_CAP", 0)
)

// V2Base normalizes a v2 API URL to the host base api.Client expects: trailing
// slashes and a trailing "/api/v2" path are stripped.
func V2Base(u string) string {
	u = strings.TrimRight(u, "/")
	u = strings.TrimSuffix(u, "/api/v2")
	return strings.TrimRight(u, "/")
}
