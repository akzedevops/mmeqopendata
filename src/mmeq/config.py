import os
from datetime import datetime, timedelta, timezone
from typing import List

API_URL = os.environ.get("MMEQ_API_URL", "https://mmeq.akze.net/api/myanmar-quakes")
# mmeq API v2 base URL (e.g. "https://mmeq.akze.net/api/v2"). Empty = disabled;
# the fetcher then uses the v1 endpoint above. v2 serves typed JSON from its own
# SQLite catalog (no live third-party proxying) with half-open [from, to) windows.
API_V2_URL = os.environ.get("MMEQ_API_V2_URL", "").rstrip("/")
START_YEAR = int(os.environ.get("MMEQ_START_YEAR", "1950"))
END_DATE = datetime.now(timezone.utc) - timedelta(days=1)
EXPORT_DIR = os.environ.get("MMEQ_EXPORT_DIR", "quake_exports")
LOG_FILE = os.environ.get("MMEQ_LOG_FILE", "dataexport.log")

EXPORT_SUBDIRS: List[str] = [
    "json/monthly", "json/yearly", "json/combined",
    "csv/monthly", "csv/yearly", "csv/combined",
]

MIN_LAT, MAX_LAT = -90, 90
MIN_LON, MAX_LON = -180, 180
MIN_DEPTH, MAX_DEPTH = 0, 700
MIN_MAG, MAX_MAG = 0, 10

MAX_WORKERS = int(os.environ.get("MMEQ_MAX_WORKERS", "10"))
REQUEST_TIMEOUT = (5, 30)
RETRY_TOTAL = 3
RETRY_BACKOFF = 0.5
RETRY_STATUS_FORCELIST = [429, 502, 503, 504]

# Window-bisection tripwire. The API has NO record cap — it serves the full catalog
# in a single request (verified) — so bisection is DISABLED by default (0). It remains
# available as a defensive measure: set MMEQ_API_PAGE_CAP to a positive N and any window
# whose response returns >= N records is bisected by date, in case the API ever adds
# pagination/truncation. See specs/001.
API_PAGE_CAP = int(os.environ.get("MMEQ_API_PAGE_CAP", "0"))

DBSCAN_EPS = 0.3
DBSCAN_MIN_SAMPLES = 10

MAP_CENTER = [21.0, 96.0]
MAP_ZOOM = 6

MAG_THRESHOLDS = {
    "low": 3.0,
    "medium": 5.0,
}
MAG_COLORS = {
    "low": "green",
    "medium": "orange",
    "high": "red",
}

# --- Dam-risk scoring (spec 003 / P5) ---------------------------------------
# Composite risk weights: seismic (scenario PGA), fault (distance to nearest
# fault), proximity (distance to the major event), exposure (dam size). They
# should sum to 1.0; used by analysis.dam_risk.dam_risk_scores.
RISK_WEIGHTS = {
    "seismic": float(os.environ.get("MMEQ_RISK_W_SEISMIC", "0.35")),
    "fault": float(os.environ.get("MMEQ_RISK_W_FAULT", "0.30")),
    "proximity": float(os.environ.get("MMEQ_RISK_W_PROXIMITY", "0.20")),
    "exposure": float(os.environ.get("MMEQ_RISK_W_EXPOSURE", "0.15")),
}
# Composite-score cutoffs for the published risk grades:
# >= critical -> Critical, >= high -> High, >= moderate -> Moderate, else Low.
RISK_GRADE_THRESHOLDS = {
    "critical": float(os.environ.get("MMEQ_RISK_GRADE_CRITICAL", "7")),
    "high": float(os.environ.get("MMEQ_RISK_GRADE_HIGH", "5")),
    "moderate": float(os.environ.get("MMEQ_RISK_GRADE_MODERATE", "3")),
}
# Total aleatory standard deviation of ln(PGA) for the ASK08 GMPE, used by both
# the Cornell-McGuire PSHA integration and the Monte Carlo PGA simulation.
GMPE_SIGMA_LN = float(os.environ.get("MMEQ_GMPE_SIGMA_LN", "0.65"))

FAULT_LINES_PATH = os.environ.get("MMEQ_FAULT_LINES", "fault_lines.json")
COMBINED_CSV = os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv")
COMBINED_JSON = os.path.join(EXPORT_DIR, "json/combined/earthquakes_combined.json")

OUTPUT_DIR = os.environ.get("MMEQ_OUTPUT_DIR", ".")
