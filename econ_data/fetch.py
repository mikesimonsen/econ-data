import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from fredapi import Fred

load_dotenv()


@dataclass
class Observation:
    series_id: str
    name: str
    date: date
    value: float


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


def fetch_all(series: list, last_dates: dict = None) -> dict:
    """
    Fetch updates for all (series_id, name) pairs.

    last_dates: {series_id: date} of the most recent observation already in the DB.
    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    where counts is the number of new observations per series (0 = already up to date).
    """
    if last_dates is None:
        last_dates = {}

    all_new = []
    counts = {}

    for series_id, name in series:
        since = last_dates.get(series_id)
        try:
            results = fetch_series(series_id, name, since=since)
            counts[series_id] = len(results)
            all_new.extend(results)
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] SKIPPED {series_id} — {e}", file=sys.stderr)
            counts[series_id] = -1  # sentinel for error

    return {"new": all_new, "counts": counts}
