"""Check workflow heartbeats and deployed catalog provenance, not earthquake age."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen


def check_heartbeat(runs: list, now: datetime, max_age_hours: int = 48) -> None:
    completed = [run for run in runs if run["status"] == "completed"]
    if not completed or completed[0]["conclusion"] != "success":
        raise ValueError("Latest completed scheduled job did not succeed")
    updated = datetime.fromisoformat(completed[0]["updated_at"].replace("Z", "+00:00"))
    if now - updated > timedelta(hours=max_age_hours):
        raise ValueError("No successful workflow heartbeat within freshness window")


def fetch_json(url: str, token: str = ""):
    headers = {"Accept": "application/json", "User-Agent": "mmeq-automation-monitor"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers), timeout=30) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "akzedevops/mmeqopendata"))
    parser.add_argument("--site", default="https://akzedevops.github.io/mmeqopendata")
    parser.add_argument("--catalog", default="quake_exports/csv/combined/earthquakes_combined.csv")
    args = parser.parse_args()
    token = os.environ.get("GH_TOKEN", "")
    now = datetime.now(timezone.utc)
    base = f"https://api.github.com/repos/{args.repo}/actions/workflows"
    for workflow in ("daily_data_fetch.yml", "shadow_go_export.yml"):
        runs = fetch_json(f"{base}/{workflow}/runs?branch=master&event=schedule&per_page=10", token)
        check_heartbeat(runs["workflow_runs"], now)
    reports = fetch_json(f"{base}/report_and_pages.yml/runs?branch=master&per_page=5", token)["workflow_runs"]
    if reports and reports[0]["status"] != "completed":
        print("Report deployment in progress; defer published-hash comparison")
        return
    if not reports or reports[0]["conclusion"] != "success":
        raise ValueError("Latest report deployment did not succeed")
    published = fetch_json(f"{args.site.rstrip('/')}/report/publication.json")
    expected = hashlib.sha256(Path(args.catalog).read_bytes()).hexdigest()
    if published.get("catalog_sha256") != expected:
        raise ValueError("Published report does not match the current catalog")
    print("Scheduled heartbeats and published catalog hash are current")


if __name__ == "__main__":
    main()
