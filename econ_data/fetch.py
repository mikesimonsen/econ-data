import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from fredapi import Fred

load_dotenv()

# Delay between FRED API calls to avoid rate limiting (120 req/min)
API_DELAY = 0.6  # seconds

# After receiving new data, wait this many days before checking again
COOLDOWN_DAYS = {
    "daily": 0,       # always check
    "weekly": 4,      # wait 4 days after last observation
    "monthly": 21,    # wait 3 weeks after last observation
    "quarterly": 70,  # wait ~10 weeks after last observation
}


@dataclass
class Observation:
    series_id: str
    name: str
    date: date
    value: float


def _detect_frequency(series_id: str) -> str:
    """Guess frequency from the series_id."""
    if series_id.startswith("DGS"):
        return "daily"
    if series_id in ("ICSA", "IC4WSA", "CCSA", "CC4WSA", "IURSA"):
        return "weekly"
    return "monthly"


def _should_fetch(series_id: str, last_obs: date = None,
                  last_checked: date = None) -> bool:
    """Decide if it's time to check FRED for new data.

    Logic per frequency:
      - After receiving new data, wait COOLDOWN_DAYS before checking again.
      - Once the cooldown expires, check daily until new data arrives.
      - Never re-check on the same day we already checked.
    """
    if last_obs is None:
        return True  # never fetched — always check

    if last_checked is not None and last_checked >= date.today():
        return False  # already checked today

    freq = _detect_frequency(series_id)
    cooldown = COOLDOWN_DAYS.get(freq, 21)
    days_since_obs = (date.today() - last_obs).days

    # Still in cooldown period after last observation — skip
    if days_since_obs <= cooldown:
        return False

    # Cooldown expired — check daily until new data arrives
    return True


def fetch_series(series_id: str, name: str, since: date = None) -> list:
    """
    Fetch observations for a FRED series.

    If since is provided, passes it as observation_start to minimize the FRED payload,
    then filters client-side to only return dates strictly newer than since.
    (FRED's period-based filtering can return the since date itself for monthly series.)
    """
    fred = Fred(api_key=os.environ["FRED_API_KEY"])
    kwargs = {}
    if since:
        kwargs["observation_start"] = since.isoformat()
    data: pd.Series = fred.get_series(series_id, **kwargs)
    return [
        Observation(series_id=series_id, name=name, date=d.date(), value=float(v))
        for d, v in data.items()
        if pd.notna(v) and (since is None or d.date() > since)
    ]


def fetch_all(series: list, last_dates: dict = None,
              last_checked: dict = None) -> dict:
    """
    Fetch updates for all (series_id, name) pairs.

    Uses smart scheduling based on observation recency:
      - Recently updated series sleep for a cooldown period
      - Series past their cooldown get checked daily until new data arrives

    last_dates: {series_id: date} of the most recent observation in the DB.
    last_checked: {series_id: date} of when each series was last checked.
    Returns {"new": [Observation, ...], "counts": {series_id: int},
             "checked": [series_id, ...]}
    where counts is:  >0 = new observations,  0 = no new data or skipped,  -1 = error
    and checked is the list of series that were actually queried (for updating fetch_log).
    """
    if last_dates is None:
        last_dates = {}
    if last_checked is None:
        last_checked = {}

    all_new = []
    counts = {}
    checked = []
    fetched = 0

    for series_id, name in series:
        last_obs = last_dates.get(series_id)
        lc = last_checked.get(series_id)

        if not _should_fetch(series_id, last_obs, lc):
            counts[series_id] = 0
            continue

        # Rate limiting
        if fetched > 0:
            time.sleep(API_DELAY)

        try:
            results = fetch_series(series_id, name, since=last_obs)
            counts[series_id] = len(results)
            all_new.extend(results)
            checked.append(series_id)
            fetched += 1
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] SKIPPED {series_id} — {e}")
            counts[series_id] = -1
            fetched += 1

    return {"new": all_new, "counts": counts, "checked": checked}
