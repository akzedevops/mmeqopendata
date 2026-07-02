// Package config mirrors src/mmeq/config.py: constants and MMEQ_* environment
// overrides. See ../../../specs/002-go-export-rewrite.md.
package config

import (
	"os"
	"strconv"
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

	MinLat, MaxLat     = -90.0, 90.0
	MinLon, MaxLon     = -180.0, 180.0
	MinDepth, MaxDepth = 0.0, 700.0
	MinMag, MaxMag     = 0.0, 10.0

	MaxWorkers = envInt("MMEQ_MAX_WORKERS", 10)
	// APIPageCap > 0 enables defensive window bisection. The live API is uncapped, so
	// the default is 0 (off), matching the corrected Python default. See specs/001.
	APIPageCap = envInt("MMEQ_API_PAGE_CAP", 0)
)
