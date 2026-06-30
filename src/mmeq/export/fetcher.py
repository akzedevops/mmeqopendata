import os
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from typing import List, Tuple, Optional

from mmeq.config import (
    API_URL,
    START_YEAR,
    EXPORT_DIR,
    COMBINED_CSV,
    MAX_WORKERS,
    REQUEST_TIMEOUT,
    RETRY_TOTAL,
    RETRY_BACKOFF,
    RETRY_STATUS_FORCELIST,
    API_PAGE_CAP,
)

import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)


def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_STATUS_FORCELIST,
        respect_retry_after_header=True,  # honor 429 Retry-After
        backoff_jitter=0.5,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _build_session()
    return _session


def get_last_updated_date() -> datetime.date:
    path = COMBINED_CSV
    try:
        if not os.path.exists(path):
            raise FileNotFoundError("Combined CSV not found")
        # Read the full time column (a single column is cheap even for the whole
        # catalog) so the latest date is correct regardless of row order. The
        # combined file is concatenated in completion order and never sorted, so
        # a tail-only read could miss the newest event and re-fetch extra months.
        df = pd.read_csv(path, usecols=["time_utc"])
        df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        last_date = df["time_utc"].max().date()
        logger.info(f"Last updated date: {last_date}")
        return last_date
    except Exception as e:
        logger.warning(f"Fallback to start year due to: {e}")
        return datetime(START_YEAR, 1, 1).date()


def generate_date_ranges(
    end_date: Optional[datetime.date] = None,
) -> List[Tuple[int, int, str, str]]:
    if end_date is None:
        end_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    last_updated = get_last_updated_date()
    date_ranges = []
    current = last_updated + timedelta(days=1)
    while current <= end_date:
        dt_from = current.replace(day=1)
        dt_to = (dt_from + relativedelta(months=1)) - timedelta(days=1)
        from_date = dt_from.strftime("%Y-%m-%d")
        to_date = dt_to.strftime("%Y-%m-%d")
        date_ranges.append((dt_from.year, dt_from.month, from_date, to_date))
        current += relativedelta(months=1)
    return date_ranges


def fetch_quake_data(from_date: str, to_date: str) -> pd.DataFrame:
    url = f"{API_URL}?from={from_date}&to={to_date}"
    try:
        response = get_session().get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        earthquakes = data.get("earthquakes", [])
        if not isinstance(earthquakes, list):
            logger.error(f"Unexpected API response format for {from_date} -> {to_date}")
            raise ValueError("API response 'earthquakes' is not a list")
        logger.info(f"Data fetched for {from_date} -> {to_date}: {len(earthquakes)} records")
        return pd.DataFrame(earthquakes)
    except Exception as e:
        logger.error(f"Error fetching data ({from_date} -> {to_date}): {e}")
        raise


def fetch_quake_data_complete(from_date: str, to_date: str, _depth: int = 0) -> pd.DataFrame:
    """Fetch all records in [from_date, to_date], defeating the API's record cap.

    The API returns at most ``API_PAGE_CAP`` records, newest-first, with no pagination.
    A response that hits the cap is therefore truncated (older events in the window are
    silently dropped). When that happens we bisect the date window and recurse, so each
    sub-window comes back under the cap; the pieces are unioned (later dedup handles any
    overlap). Recursion bottoms out at single-day windows, which the API cannot subdivide.
    """
    df = fetch_quake_data(from_date, to_date)
    if len(df) < API_PAGE_CAP:
        return df

    d0 = datetime.strptime(from_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(to_date, "%Y-%m-%d").date()
    if d0 >= d1:
        logger.warning(
            "Window %s..%s hit the %d-record cap and cannot be subdivided below one day; "
            "this day's data may be incomplete.", from_date, to_date, API_PAGE_CAP,
        )
        return df

    mid = d0 + (d1 - d0) // 2
    logger.info("Window %s..%s hit cap (%d records); bisecting at %s",
                from_date, to_date, len(df), mid)
    left = fetch_quake_data_complete(from_date, mid.strftime("%Y-%m-%d"), _depth + 1)
    right = fetch_quake_data_complete(
        (mid + timedelta(days=1)).strftime("%Y-%m-%d"), to_date, _depth + 1,
    )
    return pd.concat([left, right], ignore_index=True)
