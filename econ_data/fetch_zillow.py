"""Fetch Zillow Research monthly housing metrics from their public CSVs.

Source: Zillow Research Data (https://www.zillow.com/research/data/)
Files are wide-format CSVs hosted on files.zillowstatic.com — one row per
region, one column per month. We filter to the United States row
(RegionID=102001) and pivot the date columns into long-format observations.

Frequency: Monthly. Zillow updates these files mid-month (around the 16th)
with the prior month's reading.

We pull *levels* only; period_pct (MoM) and yoy_pct are derived downstream by
calculations.py — same convention as every other series in this repo.
"""

import csv
import io
from datetime import date, datetime
from typing import List, Optional, Tuple

import requests

from econ_data.fetch import Observation

US_REGION_ID = "102001"
COOLDOWN_DAYS = 7

SERIES = {
    "ZILLOW_ZHVI": {
        "name": "Typical Home Value (Zillow ZHVI, SA)",
        "url": (
            "https://files.zillowstatic.com/research/public_csvs/zhvi/"
            "Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
        ),
    },
    "ZILLOW_ZORI": {
        "name": "Typical Rent (Zillow ZORI, All Homes + Multifamily)",
        "url": (
            "https://files.zillowstatic.com/research/public_csvs/zori/"
            "Metro_zori_uc_sfrcondomfr_sm_month.csv"
        ),
    },
    "ZILLOW_INVENTORY": {
        "name": "For-Sale Inventory (Zillow, smoothed)",
        "url": (
            "https://files.zillowstatic.com/research/public_csvs/invt_fs/"
            "Metro_invt_fs_uc_sfrcondo_sm_month.csv"
        ),
    },
    "ZILLOW_SALES_NOWCAST": {
        "name": "Sales Count Nowcast (Zillow)",
        "url": (
            "https://files.zillowstatic.com/research/public_csvs/sales_count_now/"
            "Metro_sales_count_now_uc_sfrcondo_month.csv"
        ),
    },
}


def _parse_date(iso: str) -> date:
    """Convert '2026-03-31' to a date object."""
    return date.fromisoformat(iso)


def _fetch_one(series_id: str, spec: dict, last: Optional[date]) -> Tuple[List[Observation], int]:
    """Pull one Zillow CSV, extract the US row, return new observations."""
    try:
        resp = requests.get(spec["url"], timeout=30)
        resp.raise_for_status()
    except Exception as e:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED Zillow {series_id} — {e}")
        return [], -1

    reader = csv.reader(io.StringIO(resp.text))
    rows = list(reader)
    if not rows:
        return [], 0

    header = rows[0]
    # First 5 columns are metadata: RegionID, SizeRank, RegionName, RegionType, StateName
    date_cols = header[5:]

    us_row = next((r for r in rows[1:] if r and r[0] == US_REGION_ID), None)
    if us_row is None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED Zillow {series_id} — no United States row")
        return [], 0

    new: List[Observation] = []
    for date_str, raw in zip(date_cols, us_row[5:]):
        if not raw:
            continue
        try:
            obs_date = _parse_date(date_str)
            value = float(raw)
        except ValueError:
            continue
        if last and obs_date <= last:
            continue
        new.append(Observation(
            series_id=series_id,
            name=spec["name"],
            date=obs_date,
            value=value,
        ))

    return new, len(new)


def fetch_zillow(last_dates: dict = None) -> dict:
    """
    Download Zillow Research CSVs and extract national-level observations.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    all_new: List[Observation] = []
    counts = {sid: 0 for sid in SERIES}

    # All four files refresh together — cooldown off the most-active one.
    last = last_dates.get("ZILLOW_ZHVI")
    if last and (date.today() - last).days <= COOLDOWN_DAYS:
        return {"new": all_new, "counts": counts}

    for series_id, spec in SERIES.items():
        last = last_dates.get(series_id)
        new, count = _fetch_one(series_id, spec, last)
        all_new.extend(new)
        counts[series_id] = count

    return {"new": all_new, "counts": counts}
