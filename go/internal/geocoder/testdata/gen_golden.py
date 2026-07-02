#!/usr/bin/env python
"""Generate golden reverse-geocoding fixtures for the Go geocoder parity test.

Samples ~40 real events from the combined export, spanning every distinct
state/region bucket (including events outside all admin polygons: offshore or
across the border, which must yield empty strings) plus dense-city areas, then
records the Python reference outputs of mmeq.analysis.geocoder.

Run from the repo root:

    PYTHONPATH=src .venv/bin/python go/internal/geocoder/testdata/gen_golden.py

Regenerate only when the admin/OSM datasets or the Python geocoder change.
"""

import csv
import json
import os

from mmeq.analysis import geocoder

COMBINED = os.path.join("quake_exports", "csv", "combined", "earthquakes_combined.csv")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden_geocode.json")
TARGET = 44  # a handful over 40; dedup may trim


def main():
    with open(COMBINED, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    by_state = {}
    for r in rows:
        by_state.setdefault(r["state_region"], []).append(r)

    sample, seen = [], set()

    def add(r):
        key = (r["latitude"], r["longitude"])
        if key not in seen:
            seen.add(key)
            sample.append(r)

    # 2 events per state bucket ("" first = outside-all-polygons cases).
    for state in sorted(by_state):
        for r in by_state[state][:2]:
            add(r)
    # Extra border/offshore points expected to geocode to empty strings.
    for r in by_state.get("", [])[2:8]:
        add(r)
    # Dense-city areas (events whose nearest place is a city).
    for r in rows:
        if len(sample) >= TARGET:
            break
        if r["place_type"] == "city":
            add(r)

    out = []
    for r in sample:
        lat, lon = float(r["latitude"]), float(r["longitude"])
        city, place_type, dist = geocoder.get_nearest_place(lat, lon)
        out.append(
            {
                "lat": lat,
                "lon": lon,
                "state": geocoder.get_state(lat, lon),
                "district": geocoder.get_district(lat, lon),
                "township": geocoder.get_township(lat, lon),
                "city": city,
                "place_type": place_type,
                "distance_km": dist,
            }
        )

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
        f.write("\n")
    empties = sum(1 for o in out if not o["state"])
    print(f"wrote {len(out)} rows to {OUT} ({empties} outside all polygons)")


if __name__ == "__main__":
    main()
