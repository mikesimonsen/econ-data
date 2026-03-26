"""Fetch CPI metro data from the BLS (Bureau of Labor Statistics) API.

Used for metro areas not available in FRED (discontinued there after 2018
geographic restructuring, but still published by BLS under new area codes).

BLS API v2: https://www.bls.gov/developers/
  - With API key (BLS_API_KEY in .env): 500 queries/day, 50 series/query, 20 years
  - Without key: 25 queries/day, 25 series/query, 10 years

Strategy: fetch most recent data first so new observations land even if we
hit the daily quota before backfilling full history.
"""

import os
import time
from datetime import date, datetime

import requests
from dotenv import load_dotenv

from econ_data.fetch import Observation

load_dotenv()

API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BATCH_DELAY = 1.5  # seconds between requests


def _parse_bls_date(year: str, period: str) -> date:
    """Convert BLS year + period (e.g. 'M01') to a date (first of month)."""
    month = int(period[1:])
    return date(int(year), month, 1)


def _make_request(bls_ids: list, start_year: int, end_year: int,
                  api_key: str = None) -> dict | None:
    """Make a single BLS API request. Returns parsed JSON or None on failure."""
    payload = {
        "seriesid": bls_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    if api_key:
        payload["registrationkey"] = api_key

    try:
        resp = requests.post(API_URL, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] BLS request failed — {e}")
        return None


def _is_quota_error(result: dict) -> bool:
    """Check if the BLS response indicates daily quota exceeded."""
    msgs = result.get("message", [])
    if isinstance(msgs, list):
        return any("threshold" in str(m).lower() or "daily" in str(m).lower()
                    for m in msgs)
    return "threshold" in str(msgs).lower()


def _extract_observations(result: dict, series_map: dict,
                          last_dates: dict) -> list:
    """Extract Observation objects from a BLS API response."""
    observations = []
    for series in result.get("Results", {}).get("series", []):
        bls_id = series["seriesID"]
        if bls_id not in series_map:
            continue
        info = series_map[bls_id]
        our_id = info["id"]
        name = info["name"]
        last = last_dates.get(our_id)

        for dp in series.get("data", []):
            period = dp.get("period", "")
            if not period.startswith("M") or period == "M13":
                continue  # skip annual averages

            try:
                obs_date = _parse_bls_date(dp["year"], period)
                value = float(dp["value"])
            except (ValueError, KeyError, TypeError):
                continue

            if last and obs_date <= last:
                continue

            observations.append(Observation(
                series_id=our_id,
                name=name,
                date=obs_date,
                value=value,
            ))
    return observations


def fetch_bls(series_map: dict, last_dates: dict = None,
              start_year: int = None) -> dict:
    """
    Fetch observations from BLS for the given series.

    Fetches recent data first (current year + prior year), then backfills
    history in chunks, stopping if we hit the daily API quota.

    series_map: {bls_series_id: {"id": our_series_id, "name": display_name}, ...}
    last_dates: {our_series_id: date} of most recent observation in DB.
    start_year: earliest year to backfill to (default: 10 years back, or 20 with key).

    Returns {"new": [Observation, ...], "counts": {our_series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    api_key = os.environ.get("BLS_API_KEY")
    max_span = 20 if api_key else 10
    batch_size = 50 if api_key else 25

    current_year = date.today().year
    if start_year is None:
        start_year = current_year - max_span + 1

    all_new = []
    counts = {info["id"]: 0 for info in series_map.values()}
    bls_ids = list(series_map.keys())
    quota_hit = False

    # Build year ranges: recent first, then backfill in chunks
    # Each BLS request can span up to max_span years
    year_ranges = []

    # First priority: last 2 years (gets us current + recent data)
    year_ranges.append((current_year - 1, current_year))

    # Then backfill in max_span-year chunks, most recent first
    backfill_end = current_year - 2
    while backfill_end >= start_year:
        chunk_start = max(start_year, backfill_end - max_span + 1)
        year_ranges.append((chunk_start, backfill_end))
        backfill_end = chunk_start - 1

    for range_start, range_end in year_ranges:
        if quota_hit:
            break

        # Split series into batches if needed
        for i in range(0, len(bls_ids), batch_size):
            if quota_hit:
                break
            if i > 0 or year_ranges.index((range_start, range_end)) > 0:
                time.sleep(BATCH_DELAY)

            batch = bls_ids[i:i + batch_size]
            result = _make_request(batch, range_start, range_end, api_key)

            if result is None:
                for bls_id in batch:
                    counts[series_map[bls_id]["id"]] = -1
                continue

            if result.get("status") != "REQUEST_SUCCEEDED":
                if _is_quota_error(result):
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{ts}] BLS daily quota reached — stopping. "
                          f"Got {len(all_new)} observations so far.")
                    quota_hit = True
                else:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    msgs = result.get("message", [])
                    print(f"[{ts}] BLS API error: {msgs}")
                    for bls_id in batch:
                        counts[series_map[bls_id]["id"]] = -1
                continue

            new_obs = _extract_observations(result, series_map, last_dates)
            all_new.extend(new_obs)
            for obs in new_obs:
                counts[obs.series_id] = counts.get(obs.series_id, 0) + 1

    return {"new": all_new, "counts": counts}


def build_series_map(cfg: dict) -> dict:
    """Build BLS series map from config.

    Looks for series with a `bls_id` field in config.yaml.
    Returns {bls_series_id: {"id": our_series_id, "name": name}, ...}
    """
    result = {}
    for s in cfg.get("series", []):
        if s.get("bls_id"):
            result[s["bls_id"]] = {"id": s["id"], "name": s["name"]}
    for group in cfg.get("groups", {}).values():
        for s in group.get("series", []):
            if s.get("bls_id"):
                result[s["bls_id"]] = {"id": s["id"], "name": s["name"]}
    return result
