import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from mmeq import __version__
from mmeq.config import EXPORT_DIR, EXPORT_SUBDIRS, OUTPUT_DIR, MAX_WORKERS
from mmeq.export.fetcher import fetch_quake_data, generate_date_ranges
from mmeq.export.writer import (
    validate_quake_data,
    save_to_csv,
    save_to_json,
    load_combined_json,
    merge_combined_json,
)
from mmeq.analysis.temporal import (
    monthly_frequency,
    magnitude_distribution,
    magnitude_vs_depth,
)
from mmeq.analysis.seismology import b_value, magnitude_of_completeness, decluster_catalog
from mmeq.analysis.clustering import run_dbscan, plot_clusters_on_map, dams_in_seismic_zones


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_data(path: str = None) -> pd.DataFrame:
    if path is None:
        path = os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv")
    if not os.path.exists(path):
        logging.error(f"Data file not found: {path}. Run 'export' first.")
        sys.exit(1)
    df = pd.read_csv(path, on_bad_lines="skip")
    df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce")
    df = df.dropna(subset=["time_utc"])
    logging.info(f"Loaded {len(df)} records from {path}")
    return df


def cmd_export(args) -> None:
    for subdir in EXPORT_SUBDIRS:
        os.makedirs(os.path.join(EXPORT_DIR, subdir), exist_ok=True)

    date_ranges = generate_date_ranges()
    logging.info(f"Processing {len(date_ranges)} months of earthquake data...")

    if not date_ranges:
        logging.info("No new data to process.")
        return

    all_frames = []
    yearly_frames = {}

    def process_month(year, month, from_date, to_date):
        df_raw = fetch_quake_data(from_date, to_date)
        df_valid = validate_quake_data(df_raw)
        return year, month, df_valid

    has_error = False
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_month, y, m, fd, td): (y, m)
            for y, m, fd, td in date_ranges
        }
        for future in as_completed(futures):
            try:
                year, month, df_valid = future.result()
            except Exception:
                has_error = True
                y, m = futures[future]
                logging.error(f"Failed to process {y}-{m:02d}")
                continue

            if df_valid.empty:
                continue

            monthly_csv = os.path.join(
                EXPORT_DIR, "csv/monthly", f"earthquakes_{year}_{month:02d}.csv"
            )
            monthly_json = os.path.join(
                EXPORT_DIR, "json/monthly", f"earthquakes_{year}_{month:02d}.json"
            )
            save_to_csv(df_valid, monthly_csv, overwrite=True)
            save_to_json(df_valid, monthly_json)
            yearly_frames.setdefault(year, []).append(df_valid)
            all_frames.append(df_valid)
            logging.info(f"Updated {year}-{month:02d}: {len(df_valid)} records")

    if not all_frames:
        logging.info("No new data to process.")
        if has_error:
            sys.exit(1)
        return

    logging.info("Saving yearly and combined files...")
    for year, frames in yearly_frames.items():
        ydf = pd.concat(frames, ignore_index=True)
        save_to_csv(
            ydf,
            os.path.join(EXPORT_DIR, "csv/yearly", f"earthquakes_{year}.csv"),
            dedup=True,
        )
        save_to_json(
            ydf,
            os.path.join(EXPORT_DIR, "json/yearly", f"earthquakes_{year}.json"),
        )

    combined_df = pd.concat(all_frames, ignore_index=True)
    combined_csv = os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv")
    combined_json = os.path.join(EXPORT_DIR, "json/combined/earthquakes_combined.json")

    save_to_csv(combined_df, combined_csv, dedup=True)

    existing_records = load_combined_json(combined_json)
    new_records = combined_df.to_dict(orient="records")
    merged = merge_combined_json(existing_records, new_records)
    with open(combined_json, "w", encoding="utf-8") as f:
        import json
        json.dump({"earthquakes": merged}, f, indent=2, ensure_ascii=False)

    logging.info("All done! Earthquake data is up to date!")
    if has_error:
        sys.exit(1)


def cmd_analyze(args) -> None:
    df = load_data(args.data)
    os.makedirs(args.output, exist_ok=True)

    if not args.type or "all" in args.type:
        types = ["temporal", "clustering", "seismology"]
    else:
        types = args.type

    if args.min_mag:
        df = df[df["mag"] >= args.min_mag]
        logging.info(f"Filtered to mag >= {args.min_mag}: {len(df)} records")

    if "temporal" in types:
        monthly_frequency(df, os.path.join(args.output, "monthly_frequency_trends.png"))
        magnitude_distribution(df, os.path.join(args.output, "magnitude_distribution.png"))
        magnitude_vs_depth(df, os.path.join(args.output, "mag_vs_depth_relationship.png"))

    if "clustering" in types:
        try:
            import geopandas as gpd
            faults = None
            if os.path.exists("fault_lines.json"):
                faults = gpd.read_file("fault_lines.json").to_crs(epsg=3857)
            plot_clusters_on_map(
                df,
                title="Earthquake Clusters in Myanmar (DBSCAN)",
                filename=os.path.join(args.output, "earthquake_clusters.png"),
                faults_gdf=faults,
                show_dams=not args.no_dams,
            )
        except ImportError:
            logging.error("Install geopandas, contextily, scikit-learn for clustering")

    if "seismology" in types:
        b, a, mc = b_value(df["mag"], min_mag=args.mc)
        print(f"\nSeismological Summary:")
        print(f"  b-value: {b:.3f}")
        print(f"  a-value: {a:.3f}")
        print(f"  Mc (completeness): {mc:.1f}")
        print(f"  Total events: {len(df)}")

        if args.decluster:
            mainshocks = decluster_catalog(df)
            print(f"  Main shocks: {len(mainshocks)}")
            print(f"  Removed (aftershocks): {len(df) - len(mainshocks)}")

    if args.dam_risk or ("seismology" in types and args.dam_risk):
        risk_df = dams_in_seismic_zones(df)
        if not risk_df.empty:
            risk_csv = os.path.join(args.output, "dam_seismic_risk.csv")
            risk_df.to_csv(risk_csv, index=False)
            print(f"\nDam-Seismic Risk Analysis:")
            print(f"  Dams in seismic zones: {len(risk_df)}")
            for _, row in risk_df.head(20).iterrows():
                print(f"    {row['dam_name']}: in {row['cluster_name']} "
                      f"(max M{row['cluster_max_mag']:.1f}, {row['cluster_events']} events)")
            print(f"  Full report: {risk_csv}")
        else:
            print("\nNo dams found within seismic cluster zones.")

    print(f"\nAnalysis complete. Outputs saved to {args.output}/")


def cmd_visualize(args) -> None:
    from mmeq.visualization.map import build_earthquake_map

    df = load_data(args.data)
    os.makedirs(args.output, exist_ok=True)

    output_path = os.path.join(args.output, "enhanced_earthquake_map.html")
    build_earthquake_map(
        df,
        output_path=output_path,
        min_mag=args.min_mag,
        show_heatmap=not args.no_heatmap,
        show_markers=not args.no_markers,
        cluster_markers=not args.no_cluster,
        show_dams=not args.no_dams,
    )
    print(f"Map saved to {output_path}")


def _build_interactive_map(df, output_dir, min_mag=0.0):
    from mmeq.visualization.map import build_earthquake_map
    path = os.path.join(output_dir, "enhanced_earthquake_map.html")
    build_earthquake_map(df, output_path=path, min_mag=min_mag, show_dams=True)
    return path


def cmd_report(args) -> None:
    from mmeq.analysis.dam_risk import dam_risk_scores
    from mmeq.analysis.aftershock import modified_omori_forecast
    from mmeq.analysis.population import estimate_population_exposure
    from mmeq.visualization.dashboard import build_dashboard
    from mmeq.visualization.cross_section import depth_cross_section
    from mmeq.visualization.animated_map import build_animated_map
    from mmeq.visualization.report import generate_pdf_report

    df = load_data(args.data)
    os.makedirs(args.output, exist_ok=True)

    if args.min_mag:
        df = df[df["mag"] >= args.min_mag]
        logging.info(f"Filtered to mag >= {args.min_mag}: {len(df)} records")

    b, a, mc = b_value(df["mag"], min_mag=args.mc)
    logging.info(f"b={b:.3f}, a={a:.3f}, Mc={mc:.1f}")

    risk_df = None
    if not args.no_dams:
        logging.info("Computing dam risk scores...")
        risk_df = dam_risk_scores(df, b, a, mc)
        if not risk_df.empty:
            risk_csv = os.path.join(args.output, "dam_risk_scores.csv")
            risk_df.to_csv(risk_csv, index=False)
            logging.info(f"Dam risk scores saved -> {risk_csv}")
            grades = risk_df["risk_grade"].value_counts()
            print(f"\nDam Risk Assessment:")
            for g in ["Critical", "High", "Moderate", "Low"]:
                if g in grades.index:
                    print(f"  {g}: {grades[g]} dams")
            print(f"  Top risk: {risk_df.iloc[0]['name']} ({risk_df.iloc[0]['composite_risk']:.1f})")

    forecast_df = None
    forecast_params = None
    if not args.no_forecast:
        logging.info("Running aftershock forecast...")
        forecast_df, forecast_params = modified_omori_forecast(df, min_mag=3.0)
        if forecast_params:
            print(f"\nAftershock Forecast (p={forecast_params['p']:.2f}):")
            print(f"  Expected M>=3 in 7d: {forecast_params['expected_aftershocks_7d']:.0f}")
            print(f"  Expected M>=3 in 30d: {forecast_params['expected_aftershocks_30d']:.0f}")
            print(f"  Expected M>=3 in 90d: {forecast_params['expected_aftershocks_90d']:.0f}")

    if not args.no_dashboard:
        logging.info("Building Plotly dashboard...")
        build_dashboard(
            df,
            risk_df=risk_df,
            forecast_params=forecast_params,
            forecast_df=forecast_df,
            output_path=os.path.join(args.output, "dashboard.html"),
        )
        print(f"  Dashboard: {args.output}/dashboard.html")

    if not args.no_3d:
        logging.info("Building 3D cross-section...")
        depth_cross_section(
            df,
            output_path=os.path.join(args.output, "depth_cross_section.html"),
        )
        print(f"  3D view: {args.output}/depth_cross_section.html")

    if not args.no_animated:
        logging.info("Building animated timeline map...")
        build_animated_map(
            df,
            output_path=os.path.join(args.output, "animated_earthquake_map.html"),
            min_mag=args.min_mag,
        )
        print(f"  Animated map: {args.output}/animated_earthquake_map.html")

    if not args.no_population:
        logging.info("Computing population exposure...")
        exposure_df = estimate_population_exposure(df, radius_km=50)
        if not exposure_df.empty:
            exposure_csv = os.path.join(args.output, "population_exposure.csv")
            exposure_df.to_csv(exposure_csv, index=False)
            print(f"\nPopulation Exposure (top 5):")
            for _, row in exposure_df.head(5).iterrows():
                print(f"  {row['name']}: {row['population_within_50km']:,} people within 50km")

    if not getattr(args, 'no_buildings', False):
        try:
            from mmeq.analysis.osm_exposure import compute_building_exposure, exposure_summary
            logging.info("Computing OSM building exposure...")
            bldg_df = compute_building_exposure()
            bldg_csv = os.path.join(args.output, "building_exposure.csv")
            bldg_df.to_csv(bldg_csv, index=False)
            summary = exposure_summary(bldg_df)
            print(f"\nBuilding Exposure ({len(bldg_df)} structures):")
            for mmi in ["Severe (VIII)", "Very Strong (VII)", "Strong (VI)"]:
                if mmi in bldg_df["mmi_label"].values:
                    n = (bldg_df["mmi_label"] == mmi).sum()
                    print(f"  {mmi}: {n:,}")
            schools_high = len(bldg_df[(bldg_df["category"] == "school") & (bldg_df["pga_g"] > 0.1)])
            hospitals_high = len(bldg_df[(bldg_df["category"] == "hospital") & (bldg_df["pga_g"] > 0.1)])
            print(f"  Schools > 0.1g: {schools_high}")
            print(f"  Hospitals > 0.1g: {hospitals_high}")
        except Exception as e:
            logging.warning("OSM building exposure failed: %s", e)

    if not args.no_pdf:
        logging.info("Generating interactive map for report...")
        map_path = _build_interactive_map(df, args.output, args.min_mag)

        logging.info("Generating PDF report...")
        generate_pdf_report(
            df,
            risk_df=risk_df,
            forecast_params=forecast_params,
            b_value=b,
            a_value=a,
            mc=mc,
            output_path=os.path.join(args.output, "myanmar_earthquake_report.pdf"),
            map_image=os.path.join(args.output, "earthquake_clusters.png") if not args.no_clusters else None,
            cluster_image=os.path.join(args.output, "earthquake_clusters.png") if not args.no_clusters else None,
        )
        print(f"  PDF report: {args.output}/myanmar_earthquake_report.pdf")

    if not args.no_coulomb:
        from mmeq.analysis.coulomb import compute_coulomb_at_dams
        logging.info("Computing Coulomb stress transfer...")
        coulomb_results = compute_coulomb_at_dams()
        if coulomb_results:
            import json as _json
            coulomb_path = os.path.join(args.output, "coulomb_stress_dams.json")
            with open(coulomb_path, "w") as f:
                _json.dump(coulomb_results, f, indent=2)
            n_trig = sum(1 for r in coulomb_results if r["stress_regime"] == "Triggered")
            n_shad = sum(1 for r in coulomb_results if r["stress_regime"] == "Shadow")
            print(f"\nCoulomb Stress Transfer:")
            print(f"  Triggered (ΔCFS>0): {n_trig} dams")
            print(f"  Stress shadow (ΔCFS<0): {n_shad} dams")
            print(f"  Results: {coulomb_path}")

    if not args.no_fragility:
        from mmeq.analysis.fragility import compute_dam_fragilities
        logging.info("Computing dam fragility analysis...")
        frag_results = compute_dam_fragilities(risk_df)
        if frag_results:
            import json as _json
            frag_path = os.path.join(args.output, "dam_fragility.json")
            with open(frag_path, "w") as f:
                _json.dump(frag_results, f, indent=2)
            ds_counts = {}
            for r in frag_results:
                ds = r["most_likely_damage"]
                ds_counts[ds] = ds_counts.get(ds, 0) + 1
            print(f"\nDam Fragility Analysis:")
            for ds in ["None", "Slight", "Moderate", "Extensive", "Complete"]:
                print(f"  {ds}: {ds_counts.get(ds, 0)} dams")
            print(f"  Results: {frag_path}")

    if not args.no_montecarlo:
        from mmeq.analysis.dam_risk import monte_carlo_pga
        logging.info(f"Running Monte Carlo PGA (1000 iterations)...")
        mc_df = monte_carlo_pga(df, n_iterations=1000)
        if not mc_df.empty:
            mc_path = os.path.join(args.output, "monte_carlo_pga.csv")
            mc_df.to_csv(mc_path, index=False)
            print(f"\nMonte Carlo PGA Uncertainty (1000 iterations):")
            print(f"  Median PGA range: {mc_df['pga_p50_g'].min():.4f}-{mc_df['pga_p50_g'].max():.4f}g")
            print(f"  Dams with P(PGA>0.2g)>50%: {(mc_df['prob_pga_gt_0.2g'] > 0.5).sum()}")
            print(f"  Results: {mc_path}")

    print(f"\nFull report generated in {args.output}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mmeq",
        description="Myanmar Earthquake Open Data toolkit",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug output")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- export ---
    p_export = subparsers.add_parser("export", help="Fetch & export earthquake data")
    p_export.add_argument("--workers", type=int, default=MAX_WORKERS, help="Parallel workers (env: MMEQ_MAX_WORKERS)")

    # --- analyze ---
    p_analyze = subparsers.add_parser("analyze", help="Run analyses on earthquake data")
    p_analyze.add_argument("--data", default=None, help="Path to CSV (default: combined)")
    p_analyze.add_argument("--output", default=OUTPUT_DIR, help="Output directory")
    p_analyze.add_argument(
        "--type",
        nargs="+",
        choices=["temporal", "clustering", "seismology", "all"],
        default=None,
        help="Analysis types to run",
    )
    p_analyze.add_argument("--min-mag", type=float, default=0.0, help="Minimum magnitude filter")
    p_analyze.add_argument("--mc", type=float, default=None, help="Override magnitude of completeness")
    p_analyze.add_argument("--decluster", action="store_true", help="Run declustering")
    p_analyze.add_argument("--no-dams", action="store_true", help="Disable dam overlay on cluster map")
    p_analyze.add_argument("--dam-risk", action="store_true", help="Analyze dams in seismic zones")

    # --- visualize ---
    p_viz = subparsers.add_parser("visualize", help="Generate interactive map")
    p_viz.add_argument("--data", default=None, help="Path to CSV (default: combined)")
    p_viz.add_argument("--output", default=OUTPUT_DIR, help="Output directory")
    p_viz.add_argument("--min-mag", type=float, default=0.0, help="Minimum magnitude filter")
    p_viz.add_argument("--no-heatmap", action="store_true", help="Disable heatmap layer")
    p_viz.add_argument("--no-markers", action="store_true", help="Disable marker layer")
    p_viz.add_argument("--no-cluster", action="store_true", help="Disable marker clustering")
    p_viz.add_argument("--no-dams", action="store_true", help="Disable dam overlay")

    # --- report (full pipeline) ---
    p_report = subparsers.add_parser("report", help="Generate full report with all analyses")
    p_report.add_argument("--data", default=None, help="Path to CSV (default: combined)")
    p_report.add_argument("--output", default=OUTPUT_DIR, help="Output directory")
    p_report.add_argument("--min-mag", type=float, default=0.0, help="Minimum magnitude filter")
    p_report.add_argument("--mc", type=float, default=None, help="Override magnitude of completeness")
    p_report.add_argument("--no-dams", action="store_true", help="Skip dam risk analysis")
    p_report.add_argument("--no-forecast", action="store_true", help="Skip aftershock forecast")
    p_report.add_argument("--no-dashboard", action="store_true", help="Skip Plotly dashboard")
    p_report.add_argument("--no-3d", action="store_true", help="Skip 3D cross-section")
    p_report.add_argument("--no-animated", action="store_true", help="Skip animated map")
    p_report.add_argument("--no-population", action="store_true", help="Skip population exposure")
    p_report.add_argument("--no-buildings", action="store_true", help="Skip OSM building exposure")
    p_report.add_argument("--no-pdf", action="store_true", help="Skip PDF generation")
    p_report.add_argument("--no-clusters", action="store_true", help="Skip cluster map image")
    p_report.add_argument("--no-coulomb", action="store_true", help="Skip Coulomb stress analysis")
    p_report.add_argument("--no-fragility", action="store_true", help="Skip dam fragility analysis")
    p_report.add_argument("--no-montecarlo", action="store_true", help="Skip Monte Carlo PGA")

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.command == "export":
        cmd_export(args)
    elif args.command == "analyze":
        cmd_analyze(args)
    elif args.command == "visualize":
        cmd_visualize(args)
    elif args.command == "report":
        cmd_report(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
