"""Backfill a catalog gap from USGS FDSN (spec 007).

The upstream mmeq API has an ~18-month ingestion hole (2013-09 … 2015-02 was
published empty, though USGS recorded ~150 M≥4 events in the Myanmar region).
This tool reconstructs that window from USGS ComCat using a transparent,
reproducible rule, maps it onto the exact 32-column artifact schema via the
normal validate/geocode path, writes the monthly files, and rebuilds the
combined + yearly artifacts.

Reproducible query (defaults below):
    service   earthquake.usgs.gov/fdsnws/event/1/query (GeoJSON)
    window    [2013-09-01, 2015-03-01)  (half-open, UTC)
    box       lat 9..29, lon 92..102
    magnitude M >= 4.0
    spatial   keep only epicenters inside a Myanmar ADM1 polygon
              (pipeline geocoder -> non-empty state_region)

Usage:
    python tools/backfill_usgs.py            # do the backfill + rebuild
    python tools/backfill_usgs.py --dry-run  # fetch + report counts, write nothing
"""
import argparse
import json
import logging
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from mmeq.config import COMBINED_CSV, EXPORT_DIR
from mmeq.export.writer import (
    validate_quake_data,
    save_to_csv,
    save_merged_json,
    rebuild_combined,
)

logger = logging.getLogger(__name__)

FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Canonical artifact column order (the combined-CSV header), so the new monthly
# files are written tidily; rebuild reconciles order from the combined store.
ARTIFACT_COLUMNS = [
    "latitude", "longitude", "depth", "mag", "magType", "nst", "gap", "dmin",
    "rms", "net", "id", "updated", "location", "place", "country", "continent",
    "type", "timeAdded", "timestamp", "locationInferred", "state",
    "initialPosition", "time_utc", "time_mmt", "state_region", "district",
    "township", "nearest_city", "place_type", "distance_km", "shakemapURL",
    "shakemapLastUpdated",
]


def fetch_usgs(start, end, minmag, box):
    """Return the raw USGS GeoJSON features for the query."""
    params = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
        "minmagnitude": minmag,
        "minlatitude": box[0], "maxlatitude": box[1],
        "minlongitude": box[2], "maxlongitude": box[3],
        "orderby": "time-asc",
        "limit": 20000,
    }
    url = FDSN_URL + "?" + urllib.parse.urlencode(params)
    logger.info("Fetching USGS FDSN: %s", url)
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.load(resp)["features"]


def _iso_ms(epoch_ms):
    """USGS epoch-milliseconds -> ISO8601 Z string, matching the catalog's
    `updated` format (e.g. 2014-11-07T01:49:44.701Z)."""
    if epoch_ms is None:
        return ""
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def features_to_raw(features):
    """Map USGS features onto the raw record shape validate_quake_data expects
    (it reads `time`/`latitude`/`longitude`/`depth`/`mag` and passes the rest
    through), matching how existing USGS-sourced catalog rows look."""
    rows = []
    for f in features:
        p = f["properties"]
        lon, lat, depth = f["geometry"]["coordinates"]
        rows.append({
            "time": _iso_ms(p.get("time")),   # origin time -> time_utc/time_mmt
            "latitude": lat,
            "longitude": lon,
            "depth": depth,
            "mag": p.get("mag"),
            "magType": p.get("magType"),
            "nst": p.get("nst"),
            "gap": p.get("gap"),
            "dmin": p.get("dmin"),
            "rms": p.get("rms"),
            "net": p.get("net"),
            "id": f.get("id"),
            "updated": _iso_ms(p.get("updated")),
            "location": None,
            "place": p.get("place"),
            "country": "MM",
            "continent": "Asia",
            "type": "earthquake",
            "timeAdded": 0,
            "timestamp": 0,
            "locationInferred": 1,
            "state": None,
            "initialPosition": None,
            "shakemapURL": None,
            "shakemapLastUpdated": None,
        })
    return pd.DataFrame(rows)


def _reorder(df):
    cols = [c for c in ARTIFACT_COLUMNS if c in df.columns]
    extra = [c for c in df.columns if c not in cols]
    return df[cols + extra]


def _drop_crossnetwork_dups(kept, window_s, dup_km):
    """Remove fetched USGS events that already exist in the combined catalog —
    by exact id OR by spatio-temporal proximity to any catalog event.

    The catalog is a multi-network union (62% Thai-network ids, 22% USGS, …);
    the same physical earthquake is often stored under a regional network's id
    with a location that disagrees with USGS by tens of km. Id-only dedup would
    therefore re-inject hundreds of duplicates. Origin *time* agrees tightly
    across networks while *location* scatters, so the match is time-primary:
    an event is a duplicate if any catalog event is within ``window_s`` seconds
    AND ``dup_km`` km (a generous location bound that absorbs inter-network
    disagreement). See spec 008.
    """
    if kept.empty:
        return kept
    if not os.path.exists(COMBINED_CSV):
        logger.warning("no combined catalog at %s — skipping dedup", COMBINED_CSV)
        return kept
    cat = pd.read_csv(COMBINED_CSV)
    cat_ids = set(cat["id"].astype(str))
    ct = pd.to_datetime(cat["time_utc"], utc=True).values.astype("datetime64[s]").astype(np.int64)
    clat = cat["latitude"].to_numpy()
    clon = cat["longitude"].to_numpy()

    kt = pd.to_datetime(kept["time_utc"], utc=True).values.astype("datetime64[s]").astype(np.int64)
    klat = kept["latitude"].to_numpy()
    klon = kept["longitude"].to_numpy()
    kid = kept["id"].astype(str).to_numpy()

    keep_mask = np.ones(len(kept), dtype=bool)
    n_id = n_st = 0
    for i in range(len(kept)):
        if kid[i] in cat_ids:
            keep_mask[i] = False
            n_id += 1
            continue
        near = np.abs(ct - kt[i]) <= window_s
        if near.any():
            dlat = clat[near] - klat[i]
            dlon = (clon[near] - klon[i]) * math.cos(math.radians(klat[i]))
            dkm = np.sqrt(dlat ** 2 + dlon ** 2) * 111.0
            if (dkm <= dup_km).any():
                keep_mask[i] = False
                n_st += 1
    logger.info(
        "Dedup vs catalog: dropped %d id-dups + %d spatio-temporal cross-network "
        "dups; %d genuinely-missing events remain",
        n_id, n_st, int(keep_mask.sum()),
    )
    return kept[keep_mask].copy()


def backfill(start, end, minmag, box, dedup=True, window_s=35, dup_km=250,
             dry_run=False):
    features = fetch_usgs(start, end, minmag, box)
    logger.info("USGS returned %d events in the box", len(features))
    raw = features_to_raw(features)

    # validate_quake_data does the time formatting, geocoding (adds the 6
    # geocoder columns + state_region), and per-window dedup — identical to the
    # normal export path. end_date well past the window so nothing is clipped.
    validated = validate_quake_data(raw, end_date=datetime.now(timezone.utc))

    # Spatial refinement: keep only epicenters inside a Myanmar ADM1 polygon
    # (non-empty state_region), the same rule every catalog row already meets.
    # validate_quake_data geocodes inside a try/except and silently skips it if
    # shapely/scipy are missing or the admin GeoJSON is unreadable — in which
    # case there is no state_region column and the polygon filter is impossible.
    # The whole backfill depends on it, so fail loudly rather than raise a
    # cryptic KeyError or (worse) write an unfiltered box.
    if "state_region" not in validated.columns:
        raise RuntimeError(
            "geocoding was skipped (no state_region column) — the Myanmar-polygon "
            "filter cannot run. Install the geocoder deps (shapely, scipy) and "
            "check data/admin/mmr_admin1.geojson before backfilling."
        )
    in_mm = validated["state_region"].astype(str).str.strip().replace("nan", "").ne("")
    kept = validated[in_mm].copy()
    logger.info("Kept %d of %d after Myanmar-polygon filter", len(kept), len(validated))

    # Remove events already in the catalog (by id or cross-network proximity),
    # so the backfill only ADDS genuinely-missing events and never duplicates.
    if dedup:
        kept = _drop_crossnetwork_dups(kept, window_s, dup_km)

    if kept.empty:
        logger.warning("Nothing to backfill")
        return kept

    kept = _reorder(kept)
    kept["_t"] = pd.to_datetime(kept["time_utc"])
    by_month = kept.groupby([kept["_t"].dt.year, kept["_t"].dt.month])

    if dry_run:
        for (y, m), g in by_month:
            logger.info("  %d-%02d: %d events", y, m, len(g))
        logger.info("DRY RUN — wrote nothing (%d total)", len(kept))
        return kept

    for (y, m), g in by_month:
        g = g.drop(columns=["_t"])
        base = f"earthquakes_{y}_{m:02d}"
        csv_path = os.path.join(EXPORT_DIR, "csv/monthly", base + ".csv")
        json_path = os.path.join(EXPORT_DIR, "json/monthly", base + ".json")
        # Both writers MERGE into any existing monthly file (id-dedup): most
        # months in 1970-2019 already hold events, so a plain overwrite would
        # destroy them. save_to_csv(dedup=True) merges the CSV; save_merged_json
        # merges the JSON (a bare save_to_json would overwrite).
        save_to_csv(g, csv_path, dedup=True)
        save_merged_json(g, json_path)
        logger.info("Merged %d events into %s", len(g), base)

    n = rebuild_combined()
    logger.info("Rebuilt combined + yearly catalog: %d events", n)
    return kept


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Backfill a catalog gap from USGS FDSN")
    ap.add_argument("--start", default="2013-09-01")
    ap.add_argument("--end", default="2015-03-01")
    ap.add_argument("--minmag", type=float, default=4.0)
    ap.add_argument("--box", default="9,29,92,102",
                    help="minlat,maxlat,minlon,maxlon")
    ap.add_argument("--no-dedup", action="store_true",
                    help="disable spatio-temporal dedup vs the existing catalog")
    ap.add_argument("--window-s", type=float, default=35,
                    help="dedup time window (s); a catalog event within this "
                         "AND --dup-km is treated as the same physical event")
    ap.add_argument("--dup-km", type=float, default=250,
                    help="dedup location bound (km); generous to absorb "
                         "inter-network epicenter disagreement")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    box = tuple(float(x) for x in args.box.split(","))
    backfill(args.start, args.end, args.minmag, box, dedup=not args.no_dedup,
             window_s=args.window_s, dup_km=args.dup_km, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
