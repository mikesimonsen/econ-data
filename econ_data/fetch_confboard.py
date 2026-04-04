"""Fetch Conference Board Consumer Confidence Index from MacroMicro.me.

The Conference Board CCI is not available on FRED. MacroMicro.me embeds the
full historical series as base64-encoded JSON in the page HTML.

Three series:
  - Headline CCI (1985=100)
  - Present Situation Index
  - Expectations Index
"""

import base64
import json
import re
from datetime import date, datetime

import requests

from econ_data.fetch import Observation

# MacroMicro series pages
SERIES = {
    "CB_CCI": {
        "url": "https://en.macromicro.me/series/7054/consumer-confidence",
        "name": "Consumer Confidence Index (Conference Board)",
    },
    "CB_CCI_PRESENT": {
        "url": "https://en.macromicro.me/series/28770/consumer-confidence-current-situation",
        "name": "Consumer Confidence - Present Situation",
    },
    "CB_CCI_EXPECT": {
        "url": "https://en.macromicro.me/series/28771/consumer-confidence-expectations",
        "name": "Consumer Confidence - Expectations",
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


def _parse_page(url: str) -> list:
    """Fetch a MacroMicro series page and extract [timestamp_ms, value] pairs."""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    matches = re.findall(r'atob\("([^"]+)"\)', resp.text)
    if not matches:
        raise ValueError(f"No atob data found at {url}")

    data = json.loads(base64.b64decode(matches[0]).decode("utf-8"))
    return data


def _ts_to_date(ts_ms: int) -> date:
    """Convert millisecond timestamp to first-of-month date."""
    dt = datetime.utcfromtimestamp(ts_ms / 1000)
    return date(dt.year, dt.month, 1)


COOLDOWN_DAYS = 28  # wait ~1 month after last observation before checking again


def fetch_confboard(last_dates: dict = None) -> dict:
    """
    Fetch Conference Board CCI data from MacroMicro.me.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    matching the same contract as other fetchers.
    """
    if last_dates is None:
        last_dates = {}

    all_new = []
    counts = {sid: 0 for sid in SERIES}

    # Check cooldown against headline series — all three release together
    last = last_dates.get("CB_CCI")
    if last and (date.today() - last).days <= COOLDOWN_DAYS:
        return {"new": all_new, "counts": counts}

    for series_id, info in SERIES.items():
        last = last_dates.get(series_id)
        try:
            data = _parse_page(info["url"])
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] SKIPPED {series_id} — {e}")
            counts[series_id] = -1
            continue

        new_count = 0
        for ts_ms, value in data:
            if value is None:
                continue
            obs_date = _ts_to_date(ts_ms)
            if last and obs_date <= last:
                continue

            all_new.append(Observation(
                series_id=series_id,
                name=info["name"],
                date=obs_date,
                value=float(value),
            ))
            new_count += 1

        counts[series_id] = new_count

    return {"new": all_new, "counts": counts}
