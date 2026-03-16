"""Fetch mortgage rates from Mortgage News Daily."""

import sys
from datetime import date, datetime

import requests

from econ_data.fetch import Observation

API_ENDPOINT = "https://feeds.mortgagenewsdaily.com/partners/compass/mortgage-rates"

# Map MND productKey to our series_id and name
PRODUCTS = {
    "30YRFRM": ("MND_30YR_FIXED", "30-Year Fixed Mortgage Rate"),
    "15YRFRM": ("MND_15YR_FIXED", "15-Year Fixed Mortgage Rate"),
    "FHA30YRFIX": ("MND_30YR_FHA", "30-Year FHA Mortgage Rate"),
    "JUMBO30YRFIX": ("MND_30YR_JUMBO", "30-Year Jumbo Mortgage Rate"),
    "5YRARM": ("MND_7YR_ARM", "7/6 SOFR ARM Rate"),
    "30YRVA": ("MND_30YR_VA", "30-Year VA Mortgage Rate"),
}


def fetch_mnd(last_dates: dict = None) -> dict:
    """
    Fetch latest mortgage rates from Mortgage News Daily.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    matching the same contract as fetch.fetch_all.
    """
    if last_dates is None:
        last_dates = {}

    all_new = []
    counts = {series_id: 0 for series_id, _ in PRODUCTS.values()}

    try:
        resp = requests.get(API_ENDPOINT, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED MND fetch — {e}", file=sys.stderr)
        for series_id, _ in PRODUCTS.values():
            counts[series_id] = -1
        return {"new": all_new, "counts": counts}

    for rate_group in data.get("rates", []):
        for rate in rate_group.get("rates", []):
            product_key = rate.get("productKey")
            if product_key not in PRODUCTS:
                continue

            series_id, name = PRODUCTS[product_key]
            rate_date = datetime.fromisoformat(rate["rateDate"]).date()
            value = float(rate["rate"])

            last = last_dates.get(series_id)
            if last and rate_date <= last:
                continue

            all_new.append(Observation(
                series_id=series_id,
                name=name,
                date=rate_date,
                value=value,
            ))
            counts[series_id] = 1

    return {"new": all_new, "counts": counts}
