"""Fetch Redfin housing market data from their public S3 data download.

Source: Redfin Data Center (public, no auth required)
URL: https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/
     us_national_market_tracker.tsv000.gz
Frequency: Monthly (updated weekly, but data is monthly periods)

We filter to PROPERTY_TYPE='All Residential' and pull both NSA and SA
variants for key metrics.
"""

import csv
import gzip
import io
from datetime import date, datetime

import requests

from econ_data.fetch import Observation

TSV_URL = (
    "https://redfin-public-data.s3.us-west-2.amazonaws.com/"
    "redfin_market_tracker/us_national_market_tracker.tsv000.gz"
)

COOLDOWN_DAYS = 3  # weekly updates

# Series definitions: map series_id -> TSV column + display name
# We pull the NSA (not seasonally adjusted) data for direct comparison
# with other sources. SA data uses separate series IDs.
SERIES = {
    # --- Not Seasonally Adjusted ---
    "REDFIN_PENDING": {
        "name": "Pending Sales (Redfin)",
        "column": "PENDING_SALES",
        "sa": False,
    },
    "REDFIN_SOLD": {
        "name": "Homes Sold (Redfin)",
        "column": "HOMES_SOLD",
        "sa": False,
    },
    "REDFIN_NEW_LISTINGS": {
        "name": "New Listings (Redfin)",
        "column": "NEW_LISTINGS",
        "sa": False,
    },
    "REDFIN_INVENTORY": {
        "name": "Active Inventory (Redfin)",
        "column": "INVENTORY",
        "sa": False,
    },
    "REDFIN_MONTHS_SUPPLY": {
        "name": "Months of Supply (Redfin)",
        "column": "MONTHS_OF_SUPPLY",
        "sa": False,
    },
    "REDFIN_MEDIAN_DOM": {
        "name": "Median Days on Market (Redfin)",
        "column": "MEDIAN_DOM",
        "sa": False,
        "inverted": True,
    },
    "REDFIN_SALE_TO_LIST": {
        "name": "Sale-to-List Ratio (Redfin)",
        "column": "AVG_SALE_TO_LIST",
        "sa": False,
    },
    "REDFIN_PRICE_DROPS": {
        "name": "Price Drop Share (Redfin)",
        "column": "PRICE_DROPS",
        "sa": False,
    },
    "REDFIN_MEDIAN_PRICE": {
        "name": "Median Sale Price (Redfin)",
        "column": "MEDIAN_SALE_PRICE",
        "sa": False,
    },
    "REDFIN_OFF_MARKET_2WK": {
        "name": "Off Market in Two Weeks (Redfin)",
        "column": "OFF_MARKET_IN_TWO_WEEKS",
        "sa": False,
    },
    # --- Seasonally Adjusted ---
    "REDFIN_PENDING_SA": {
        "name": "Pending Sales SA (Redfin)",
        "column": "PENDING_SALES",
        "sa": True,
    },
    "REDFIN_SOLD_SA": {
        "name": "Homes Sold SA (Redfin)",
        "column": "HOMES_SOLD",
        "sa": True,
    },
    "REDFIN_NEW_LISTINGS_SA": {
        "name": "New Listings SA (Redfin)",
        "column": "NEW_LISTINGS",
        "sa": True,
    },
    "REDFIN_INVENTORY_SA": {
        "name": "Active Inventory SA (Redfin)",
        "column": "INVENTORY",
        "sa": True,
    },
}


def _parse_date(period_end: str) -> date:
    """Convert '2026-02-28' to a date object."""
    return date.fromisoformat(period_end)


def fetch_redfin(last_dates: dict = None) -> dict:
    """
    Download Redfin national market tracker TSV and extract key metrics.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    all_new = []
    counts = {sid: 0 for sid in SERIES}

    # Cooldown check
    last = last_dates.get("REDFIN_PENDING")
    if last and (date.today() - last).days <= COOLDOWN_DAYS:
        return {"new": all_new, "counts": counts}

    try:
        resp = requests.get(TSV_URL, timeout=30)
        resp.raise_for_status()
        text = gzip.decompress(resp.content).decode("utf-8")
    except Exception as e:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED Redfin download — {e}")
        for sid in SERIES:
            counts[sid] = -1
        return {"new": all_new, "counts": counts}

    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    for row in reader:
        # Only "All Residential" property type
        if row.get("PROPERTY_TYPE") != "All Residential":
            continue

        is_sa = row.get("IS_SEASONALLY_ADJUSTED", "").lower() == "true"
        period_end = row.get("PERIOD_END", "")
        if not period_end:
            continue

        obs_date = _parse_date(period_end)

        for series_id, spec in SERIES.items():
            if spec["sa"] != is_sa:
                continue

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
