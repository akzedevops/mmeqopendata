#!/usr/bin/env python3
"""Generate the derived ``report/`` CSVs that the paper figures depend on.

``generate_figures.py`` reads three files that the regular ``mmeq report``
pipeline does not produce on its own:

  report/dam_vs30.csv          - site Vs30 sampled at each dam from the USGS
                                 ShakeMap Vs30 grid (Wald & Allen 2007 proxy)
  report/dam_risk_scores.csv   - dam risk scores (uses dam_vs30.csv for site Vs30
                                 and declustered Gutenberg-Richter rates)
  report/sensitivity_analysis.csv - Monte-Carlo weight-sensitivity of the grades

Run this before ``generate_figures.py`` (CI does exactly that) so the figures are
fully reproducible from the committed catalog + data files. dam_vs30.csv is
written first so dam_risk_scores picks up the site-specific Vs30.

Usage:  python tools/build_figure_data.py [--report-dir report]
"""
import argparse
import logging
import os
import sys

import pandas as pd

# Allow running from the repo root without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.mmeq.analysis.dam_risk import _load_dams_df, dam_risk_scores, sensitivity_analysis
from src.mmeq.analysis.osm_exposure import _get_vs30
from src.mmeq.analysis.seismology import b_value, decluster_catalog

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_figure_data")

COMBINED_CSV = os.path.join("quake_exports", "csv", "combined", "earthquakes_combined.csv")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report-dir", default="report", help="Output directory for the CSVs")
    ap.add_argument("--catalog", default=COMBINED_CSV, help="Combined earthquake CSV")
    args = ap.parse_args()

    os.makedirs(args.report_dir, exist_ok=True)

    dams = _load_dams_df()
    if dams is None or dams.empty:
        log.error("No dam data found (myanmar_dams.geojson); cannot build figure data.")
        return 1

    # 1) Site Vs30 at each dam, sampled from the ShakeMap grid. Written first so
    #    dam_risk_scores() -> _load_vs30() picks it up for site amplification.
    vs30_rows = [
        {
            "lat": float(d["latitude"]),
            "lon": float(d["longitude"]),
            "vs30": round(_get_vs30(float(d["latitude"]), float(d["longitude"]))),
        }
        for _, d in dams.iterrows()
    ]
    vs30_df = pd.DataFrame(vs30_rows)
    vs30_path = os.path.join(args.report_dir, "dam_vs30.csv")
    vs30_df.to_csv(vs30_path, index=False)
    log.info("Wrote %s (%d dams, Vs30 %d-%d m/s)", vs30_path, len(vs30_df),
             int(vs30_df["vs30"].min()), int(vs30_df["vs30"].max()))

    # 2) Catalog + declustered Gutenberg-Richter rates for the PSHA / return periods.
    df = pd.read_csv(args.catalog, on_bad_lines="skip")
    df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce")
    df = df.dropna(subset=["time_utc"])
    df_dec = decluster_catalog(df)
    b, a, mc = b_value(df_dec["mag"])
    log.info("Declustered G-R: b=%.3f a=%.3f Mc=%.1f (N=%d of %d)",
             b, a, mc, len(df_dec), len(df))

    # _load_vs30 reads from MMEQ_REPORT_DIR (default "report"); align it with the
    # chosen report dir so dam_risk_scores reads the dam_vs30.csv we just wrote.
    os.environ["MMEQ_REPORT_DIR"] = args.report_dir

    # 3) Dam risk scores.
    risk = dam_risk_scores(df, b, a, mc)
    risk_path = os.path.join(args.report_dir, "dam_risk_scores.csv")
    risk.to_csv(risk_path, index=False)
    grades = risk["risk_grade"].value_counts().to_dict()
    log.info("Wrote %s (%d dams; grades %s)", risk_path, len(risk), grades)

    # 4) Sensitivity analysis.
    sens = sensitivity_analysis(df, b, a, mc, n_samples=100)
    sens_path = os.path.join(args.report_dir, "sensitivity_analysis.csv")
    sens.to_csv(sens_path, index=False)
    log.info("Wrote %s (%d Monte-Carlo samples)", sens_path, len(sens))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
