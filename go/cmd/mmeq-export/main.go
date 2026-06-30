// Command mmeq-export is the Go reimplementation of the Python earthquake
// fetch/validate/dedup/export pipeline (see ../../../specs/002-go-export-rewrite.md).
//
// SCOPE: this binary replaces `python dataexport.py` in the daily CI cron. It produces
// the same CSV/JSON artifacts that the Python scientific pipeline consumes. It does NOT
// reimplement any analysis or figure code — that stays in Python.
//
// STATUS: scaffold only. The export logic is unimplemented pending spec 002 approval.
package main

import (
	"flag"
	"fmt"
	"os"
)

const usage = `mmeq-export — Myanmar earthquake data fetch/export (Go)

Usage:
  mmeq-export export [flags]

Commands:
  export    Fetch new months from the API, validate, dedup, and write CSV/JSON.

Status: scaffold — not yet implemented. See specs/002-go-export-rewrite.md.
`

func main() {
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	switch flag.Arg(0) {
	case "export":
		// TODO(spec-002): wire config → api.Fetch → catalog.Validate → Dedup → Writer,
		// including the 500-record adaptive window bisection from spec 001.
		fmt.Fprintln(os.Stderr, "mmeq-export: 'export' not yet implemented (see specs/002-go-export-rewrite.md)")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "mmeq-export: unknown command %q\n\n", flag.Arg(0))
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}
