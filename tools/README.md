# `tools/` — maintenance & data scripts

One-off / periodic utilities that sit outside the `mmeq` CLI. Run them from the
repo root with the venv active (`source .venv/bin/activate`). None of these are
part of the daily pipeline unless noted.

| Script | What it does | When to run |
|---|---|---|
| [`backfill_usgs.py`](backfill_usgs.py) | Fill catalog gaps from USGS FDSN with spatio-temporal cross-network dedup | Repairing a coverage hole / completeness gap |
| [`backfill.py`](backfill.py) | Re-fetch & merge specific months from the mmeq API into the monthly files | After a bad/partial daily run |
| [`build_figure_data.py`](build_figure_data.py) | Precompute the report CSVs (dam risk, Vs30, sensitivity) that the figures read | In CI before `generate_figures.py`; locally to refresh figures |
| [`diff_exports.py`](diff_exports.py) | Id-keyed, float-tolerant diff of two export trees (Python vs Go) | Verifying Go/Python export parity |
| [`fetch_vendor.py`](fetch_vendor.py) | Download & hash-pin the self-hosted JS/CSS for the maps/dashboard | When bumping folium/plotly; `--verify` in CI |
| `vendor_lock.json` | sha256 lock for the vendored assets (not a script) | — |

---

## `backfill_usgs.py` — USGS gap / completeness fill

Reconstructs missing earthquakes from **USGS FDSN** (ComCat) and merges them into
`quake_exports/` in the exact 32-column schema, via the normal
`validate_quake_data` path (`net='us'`, geocoded, byte-shaped like existing USGS
rows). **Spatio-temporal dedup is on by default**, so it can never inject a
cross-network duplicate — critical because the catalog is a multi-network union
where the same quake often sits under a `th_`/`in_`/`ems` id with a location
tens of km off USGS. Dedup is time-primary: an event is "already present" if any
catalog quake is within `--window-s` (35 s) **and** `--dup-km` (250 km).

```bash
# Fill the empty 2014 window (spec 007):
python tools/backfill_usgs.py --start 2013-09-01 --end 2015-03-01

# Complete 1970–2019 to USGS M≥4 in-Myanmar (spec 008):
python tools/backfill_usgs.py --start 1970-01-01 --end 2020-01-01

python tools/backfill_usgs.py --dry-run          # fetch + report counts, write nothing
python tools/backfill_usgs.py --no-dedup         # (unsafe) disable the dedup guard
```

Filter: box `lat 9–29, lon 92–102`, `M ≥ 4.0` (`--minmag`), epicenter inside a
Myanmar ADM1 polygon (the pipeline geocoder). Idempotent — re-running adds
nothing new. ComCat is mutable; the exact frozen id sets are in
`specs/008-backfill-manifest.csv`. See `specs/007`–`009`.

## `backfill.py` — mmeq-API monthly re-fetch

Re-fetches and merges specific months from the mmeq API into the monthly CSV/JSON
files (id-dedup), for recovering late-arriving events the daily cron missed
(the daily window starts at `last_updated + 1 day`, so a month can freeze before
stragglers land). After running it, reconcile the combined + yearly stores with
`mmeq export --rebuild`.

## `build_figure_data.py`

Computes and writes the `report/` CSVs the figures depend on (`dam_vs30.csv`,
`dam_risk_scores.csv`, `sensitivity_analysis.csv`) so `generate_figures.py` and
`mmeq report --reuse-risk` don't each recompute dam risk. CI runs it before
figure generation; run it locally after any change that moves dam-risk numbers.

## `diff_exports.py`

Compares two export trees id-by-id with a float tolerance (default 1e-9),
tolerant of row order. Used by `shadow_go_export.yml` to prove the Go exporter
stays byte-parallel with the Python one. `python tools/diff_exports.py <dirA> <dirB>`.

## `fetch_vendor.py`

Downloads the JS/CSS the interactive maps/dashboard need (leaflet, plotly, …),
reads the URLs from the installed folium/plotly defaults, and pins them by
sha256 in `vendor_lock.json` so the published site serves **zero third-party
script origins**. Regenerates `src/mmeq/visualization/vendor/`.

```bash
python tools/fetch_vendor.py            # (re)download + write the vendor tree + lock
python tools/fetch_vendor.py --verify   # check the committed tree matches the lock (CI)
```

Run it (without `--verify`) whenever you bump folium/plotly, and commit the
regenerated `vendor/` tree together with `vendor_lock.json` — the tree must
always reproduce from the tool.
