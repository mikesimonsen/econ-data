"""
Compute mortgage rate spread series (30-year fixed minus 10-year Treasury).

Three derived series:
  SPREAD_30Y_10Y      — daily spread (MND_30YR_FIXED - DGS10)
  SPREAD_30Y_10Y_WK   — weekly average spread (Mon-Fri avg, dated Saturday)
  SPREAD_30Y_10Y_4WK  — 4-week rolling average of the weekly spread
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from econ_data.fetch import Observation
from econ_data.store_sqlite import DB_PATH

MORTGAGE_ID = "MND_30YR_FIXED"
TREASURY_ID = "DGS10"

DAILY_ID = "SPREAD_30Y_10Y"
WEEKLY_ID = "SPREAD_30Y_10Y_WK"
ROLLING_ID = "SPREAD_30Y_10Y_4WK"

DAILY_NAME = "Mortgage Rate Spread (30Y - 10Y)"
WEEKLY_NAME = "Mortgage Rate Spread Weekly Avg"
ROLLING_NAME = "Mortgage Rate Spread 4-Week Avg"

ALL_IDS = [DAILY_ID, WEEKLY_ID, ROLLING_ID]


def compute_spread(last_dates: dict = None,
                   db_path: Path = DB_PATH) -> dict:
    """Compute all spread series from existing MND and DGS10 observations.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    con = sqlite3.connect(db_path)

    # Load both underlying series
    mnd = dict(con.execute(
        "SELECT date, value FROM observations WHERE series_id = ? ORDER BY date",
        (MORTGAGE_ID,),
    ).fetchall())

    dgs = dict(con.execute(
        "SELECT date, value FROM observations WHERE series_id = ? ORDER BY date",
        (TREASURY_ID,),
    ).fetchall())

    con.close()

    all_new = []
    counts = {sid: 0 for sid in ALL_IDS}

    # ── Daily spread ──────────────────────────────────────────
    daily_last = last_dates.get(DAILY_ID)
    daily_spreads = {}  # date_str -> spread

    for date_str in sorted(mnd.keys() & dgs.keys()):
        spread = round(mnd[date_str] - dgs[date_str], 2)
        daily_spreads[date_str] = spread

        obs_date = date.fromisoformat(date_str)
        if daily_last and obs_date <= daily_last:
            continue

        all_new.append(Observation(DAILY_ID, DAILY_NAME, obs_date, spread))
        counts[DAILY_ID] += 1

    # ── Weekly spread (Mon-Fri avg, dated Saturday) ───────────
    weekly_last = last_dates.get(WEEKLY_ID)
    weeks = _group_by_week(daily_spreads)
    weekly_values = []  # [(saturday_date, avg_spread), ...] sorted

    for saturday, values in sorted(weeks.items()):
        if len(values) < 3:
            continue  # need at least 3 trading days for a meaningful week
        avg = round(sum(values) / len(values), 2)
        weekly_values.append((saturday, avg))

        if weekly_last and saturday <= weekly_last:
            continue

        all_new.append(Observation(WEEKLY_ID, WEEKLY_NAME, saturday, avg))
        counts[WEEKLY_ID] += 1

    # ── 4-week rolling average of weekly spread ───────────────
    rolling_last = last_dates.get(ROLLING_ID)

    for i in range(3, len(weekly_values)):
        saturday, _ = weekly_values[i]
        window = [v for _, v in weekly_values[i - 3 : i + 1]]
        avg = round(sum(window) / len(window), 2)

        if rolling_last and saturday <= rolling_last:
            continue

        all_new.append(Observation(ROLLING_ID, ROLLING_NAME, saturday, avg))
        counts[ROLLING_ID] += 1

    return {"new": all_new, "counts": counts}


def _group_by_week(daily: dict[str, float]) -> dict[date, list[float]]:
    """Group daily values into Mon-Fri weeks, keyed by Saturday date."""
    weeks: dict[date, list[float]] = defaultdict(list)
    for date_str, value in daily.items():
        d = date.fromisoformat(date_str)
        # Saturday = end of week (days_ahead: Mon=5, Tue=4, ... Fri=1)
        days_ahead = 5 - d.weekday()
        if days_ahead <= 0:
            continue  # skip weekends (shouldn't happen but defensive)
        saturday = d + timedelta(days=days_ahead)
        weeks[saturday].append(value)
    return weeks
