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


@dataclass
class Observation:
    series_id: str
    name: str
    date: date
    value: float


def _detect_frequency(series_id: str) -> str:
    """Guess frequency from the series_id. Used by the revision-window logic
    to decide how to fetch (daily series skip the lookback window)."""
    if (series_id.startswith("DGS")
            or series_id in ("WTI_CRUDE", "T5YIE", "T10YIE", "T5YIFR", "SP500")):
        return "daily"
    if series_id in ("ICSA", "IC4WSA", "CCSA", "CC4WSA", "IURSA"):
        return "weekly"
    return "monthly"


REVISION_LOOKBACK_MONTHS = 4  # re-fetch this many months to catch revisions


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


def fetch_series_with_revisions(series_id: str, name: str,
                                last_obs: date = None) -> list:
    """Fetch recent observations including the revision window.

    Returns ALL observations from (last_obs - REVISION_LOOKBACK_MONTHS) forward,
    so the caller can compare against stored values to detect revisions.
    """
    fred = Fred(api_key=os.environ["FRED_API_KEY"])
    if last_obs:
        lookback = last_obs - timedelta(days=REVISION_LOOKBACK_MONTHS * 31)
        start = lookback.isoformat()
    else:
        start = None

    kwargs = {}
    if start:
        kwargs["observation_start"] = start
    data: pd.Series = fred.get_series(series_id, **kwargs)
    return [
        Observation(series_id=series_id, name=name, date=d.date(), value=float(v))
        for d, v in data.items()
        if pd.notna(v)
    ]


def fetch_all(series: list, last_dates: dict = None,
              force: bool = False, **_compat) -> dict:
    """Fetch updates for all (series_id, name) pairs.

    Scheduling is driven by the release_schedule table — a series is only
    queried if it has a PENDING/OVERDUE row whose scheduled_release <= today.
    On capture, mark_captured advances the schedule. Series whose latest
    release has already been captured are skipped without an API call.

    last_dates: {series_id: date} of the most recent observation in the DB.
    force: if True, bypass schedule check (intraday retry of fetch_errors).
    Returns {"new": [Observation, ...], "counts": {series_id: int},
             "checked": [series_id, ...], "all_fetched": [Observation, ...]}
    counts:  >0 = new observations,  0 = no new data or skipped,  -1 = error

    Also maintains the fetch_errors table: records a row on exception, deletes
    it on success. The intraday run reads that table to decide what to retry.
    """
    # Local imports to avoid a circular import at module load time.
    from econ_data.store import clear_fetch_error, record_fetch_error
    from econ_data.release_schedule import series_due_now, mark_captured

    if last_dates is None:
        last_dates = {}

    all_new = []
    all_fetched = []
    counts = {}
    checked = []
    fetched = 0

    for series_id, name in series:
        last_obs = last_dates.get(series_id)

        if not force and not series_due_now(series_id):
            # No PENDING release scheduled — skip without an API call.
            counts[series_id] = 0
            continue

        # Rate limiting
        if fetched > 0:
            time.sleep(API_DELAY)

        try:
            freq = _detect_frequency(series_id)
            if freq == "daily":
                # Daily series: just fetch new data, no revision tracking
                results = fetch_series(series_id, name, since=last_obs)
                new_only = results
            else:
                # Weekly/monthly: fetch revision window to catch changes
                results = fetch_series_with_revisions(series_id, name,
                                                     last_obs=last_obs)
                new_only = [o for o in results
                            if last_obs is None or o.date > last_obs]

            counts[series_id] = len(new_only)
            all_new.extend(new_only)
            all_fetched.extend(results)
            checked.append(series_id)
            fetched += 1
            clear_fetch_error(series_id)

            # Advance the schedule for each new period that arrived. If FRED
            # had no new data, nothing was captured — the PENDING row stays
            # so the next cron firing retries.
            for obs in new_only:
                mark_captured(series_id, obs.date)
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] SKIPPED {series_id} — {e}")
            counts[series_id] = -1
            fetched += 1
            try:
                record_fetch_error(series_id, str(e))
            except Exception as rec_err:
                # Don't let an error-recording failure mask the original error.
                print(f"[{ts}] (could not record fetch_error for {series_id}: {rec_err})")

    return {"new": all_new, "counts": counts, "checked": checked,
            "all_fetched": all_fetched}
