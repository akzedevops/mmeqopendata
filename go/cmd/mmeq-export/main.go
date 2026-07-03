// Command mmeq-export is the Go reimplementation of the Python earthquake
// fetch/validate/dedup/export pipeline (see ../../../specs/002-go-export-rewrite.md).
//
// SCOPE: this binary replaces `python dataexport.py` / `mmeq export` in the daily CI
// cron. It produces the same CSV/JSON artifacts that the Python scientific pipeline
// consumes. It does NOT reimplement any analysis or figure code — that stays in Python.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/akzedevops/mmeqopendata/go/internal/config"
	"github.com/akzedevops/mmeqopendata/go/internal/export"
)

const usage = `mmeq-export — Myanmar earthquake data fetch/export (Go)

Usage:
  mmeq-export export [flags]

Commands:
  export    Fetch new months from the API, validate, dedup, and write CSV/JSON.

Export flags:
  --export-dir DIR   Output tree (env: MMEQ_EXPORT_DIR; default "quake_exports")
  --data-dir DIR     Geocoder data dir with admin/ + osm/ (env: MMEQ_DATA_DIR; default "data")
  --workers N        Concurrent month fetches (env: MMEQ_MAX_WORKERS; default 10)
  --route v1|v2      API contract to fetch (env: MMEQ_FETCH_ROUTE; default "v1" —
                     the compat route serves the full raw columns the published
                     artifact needs; "v2" is typed JSON without them)

The API host comes from MMEQ_API_V2_URL (host base or ".../api/v2" form; default
"https://mmeq.akze.net"). Exits 0 on success (including no new data), 1 on failure.
`

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	switch flag.Arg(0) {
	case "export":
		fs := flag.NewFlagSet("export", flag.ExitOnError)
		fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
		exportDir := fs.String("export-dir", config.ExportDir, "output tree (env: MMEQ_EXPORT_DIR)")
		dataDir := fs.String("data-dir", envOr("MMEQ_DATA_DIR", "data"), "geocoder data dir (env: MMEQ_DATA_DIR)")
		workers := fs.Int("workers", config.MaxWorkers, "concurrent month fetches (env: MMEQ_MAX_WORKERS)")
		route := fs.String("route", config.FetchRoute, "API contract: v1 or v2 (env: MMEQ_FETCH_ROUTE)")
		_ = fs.Parse(flag.Args()[1:]) // ExitOnError: Parse exits on bad flags

		if *route != "v1" && *route != "v2" {
			fmt.Fprintf(os.Stderr, "mmeq-export: --route must be v1 or v2, got %q\n", *route)
			os.Exit(2)
		}
		err := export.Run(export.Options{
			BaseURL:   config.APIV2BaseURL,
			ExportDir: *exportDir,
			DataDir:   *dataDir,
			Workers:   *workers,
			Route:     *route,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "mmeq-export: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "mmeq-export: unknown command %q\n\n", flag.Arg(0))
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}
