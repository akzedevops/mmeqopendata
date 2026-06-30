#!/usr/bin/env python3
"""Backwards-compatible shim — the export logic now lives in the `mmeq` package.

Historically this was a ~270-line monolith duplicating src/mmeq/export/*. It had
drifted from the package and was the CI entry point. CI now runs `mmeq export`; this
shim remains only so that `python dataexport.py` keeps working for anyone with the old
habit. It delegates to the same code path (`mmeq export`), which includes the 500-record
window bisection, atomic writes, and id-keyed dedup.

See specs/001-data-fetch-upgrade.md and specs/003-project-improvements.md.
"""
import sys

from mmeq.cli import main

if __name__ == "__main__":
    # Invoke the package CLI's `export` subcommand regardless of how we were called.
    sys.argv = [sys.argv[0], "export", *sys.argv[1:]]
    main()
