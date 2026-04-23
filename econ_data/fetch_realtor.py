"""Fetch Realtor.com bonus metrics from their public S3 CSV.

Supplements the 9 FRED series (ACTLISCOUUS, etc.) with metrics only
available in the direct CSV download: pending_ratio, price_reduced_share,
total_listing_count.

Source: Realtor.com Research Data Library
URL: https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/
     RDC_Inventory_Core_Metrics_Country_History.csv
Frequency: Monthly (updated first week of each month)
"""

import csv
import io
from datetime import date, datetime

import requests

from econ_data.fetch import Observation

CSV_URL = (
    "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/"
    "RDC_Inventory_Core_Metrics_Country_History.csv"
)

COOLDOWN_DAYS = 7

# Series sourced from the CSV that are NOT on FRED
SERIES = {
    "REALTOR_PENDING_RATIO": {
        "name": "Pending Ratio (Realtor.com)",
        "column": "pending_ratio",
    },
    "REALTOR_PRICE_REDUCED_SHARE": {
        "name": "Price Reduced Share (Realtor.com)",
        "column": "price_reduced_share",
    },
    "REALTOR_TOTAL_LISTINGS": {
        "name": "Total Listing Count (Realtor.com)",
        "column": "total_listing_count",
    },
}


def _parse_date(yyyymm: str) -> date:
    """Convert '202603' to date(2026, 3, 1)."""
    return date(int(yyyymm[:4]), int(yyyymm[4:6]), 1)


def fetch_realtor(last_dates: dict = None) -> dict:
    """
    Download the Realtor.com national history CSV and extract bonus metrics.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    all_new = []
    counts = {sid: 0 for sid in SERIES}

    # Cooldown: check any series — they all update together
    last = last_dates.get("REALTOR_PENDING_RATIO")
    if last and (date.today() - last).days <= COOLDOWN_DAYS:
        return {"new": all_new, "counts": counts}

    try:
        resp = requests.get(CSV_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED Realtor.com CSV — {e}")
        for sid in SERIES:
            counts[sid] = -1
        return {"new": all_new, "counts": counts}

    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        yyyymm = row.get("month_date_yyyymm", "")
        if not yyyymm:
            continue
        obs_date = _parse_date(yyyymm)

        for series_id, spec in SERIES.items():
            last = last_dates.get(series_id)
            if last and obs_date <= last:
                continue

            raw = row.get(spec["column"], "")
            if not raw:
                continue

            try:
                value = float(raw)
            except ValueError:
                continue

            all_new.append(Observation(
                series_id=series_id,
                name=spec["name"],
                date=obs_date,
                value=value,
            ))
            counts[series_id] = counts.get(series_id, 0) + 1

    return {"new": all_new, "counts": counts}
