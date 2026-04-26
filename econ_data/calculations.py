"""
Compute derived time series from raw observations.

Calculation types:
  period_pct — percentage change from the prior observation (for level series)
  yoy_pct   — percentage change from ~1 year earlier (for level series)
  period_pp  — percentage-point difference from the prior observation (for percent/rate series)
  yoy_pp     — percentage-point difference from ~1 year earlier (for percent/rate series)

YoY matching is frequency-agnostic: for each observation, we find the
nearest observation to the date exactly one year prior.  A tolerance of
half the series' median observation gap ensures we don't match across
gaps that are too large (e.g. a 6-month hiatus).
"""
import bisect
from datetime import date as date_type, timedelta
from pathlib import Path

from econ_data.config import load, percent_series
from econ_data.db import connect
from econ_data.store_sqlite import DB_PATH  # signature compat for unmigrated callers

UPSERT_SQL = (
    "INSERT INTO calculated (series_id, calc_type, date, value) "
    "VALUES (%s, %s, %s, %s) "
    "ON CONFLICT (series_id, calc_type, date) DO UPDATE SET value = EXCLUDED.value"
)


def compute_all(db_path: Path = DB_PATH) -> int:
    """Compute all derived series and save to the calculated table. Returns rows written."""
    con = connect()

    cfg = load()
    pct_ids = percent_series(cfg)

    series_ids = [r[0] for r in con.execute(
        "SELECT DISTINCT series_id FROM observations ORDER BY series_id"
    ).fetchall()]

    total = 0
    for series_id in series_ids:
        rows = con.execute(
            "SELECT date::text, value FROM observations WHERE series_id = %s ORDER BY date",
            (series_id,),
        ).fetchall()

        if series_id in pct_ids:
            total += _compute_period_pp(con, series_id, rows)
            total += _compute_yoy_pp(con, series_id, rows)
        else:
            total += _compute_period_pct(con, series_id, rows)
            total += _compute_yoy_pct(con, series_id, rows)

    con.commit()
    return total


def _median_gap_days(rows) -> int:
    """Return the median gap in days between consecutive observations."""
    if len(rows) < 2:
        return 30
    gaps = []
    for i in range(1, min(len(rows), 50)):  # sample recent observations
        d1 = date_type.fromisoformat(rows[-(i + 1)][0])
        d2 = date_type.fromisoformat(rows[-i][0])
        gaps.append((d2 - d1).days)
    gaps.sort()
    return gaps[len(gaps) // 2]


def _find_yoy_match(dates: list[date_type], values: list[float],
                    target: date_type, tolerance_days: int):
    """Find the observation nearest to target within tolerance.

    Uses binary search for efficiency. Returns the value or None.
    """
    idx = bisect.bisect_left(dates, target)

    best_val = None
    best_gap = tolerance_days + 1

    # Check the candidate at idx and idx-1 (the two nearest)
    for i in (idx - 1, idx):
        if 0 <= i < len(dates):
            gap = abs((dates[i] - target).days)
            if gap < best_gap:
                best_gap = gap
                best_val = values[i]

    return best_val


def _compute_period_pct(con, series_id, rows):
    """Percentage change from prior observation."""
    calcs = []
    for i in range(1, len(rows)):
        prev_val = rows[i - 1][1]
        cur_date, cur_val = rows[i]
        if prev_val != 0:
            pct = (cur_val - prev_val) / prev_val * 100
            calcs.append((series_id, "period_pct", cur_date, round(pct, 2)))

    with con.cursor() as cur:
        cur.executemany(UPSERT_SQL, calcs)
    return len(calcs)


def _compute_yoy_pct(con, series_id, rows):
    """Percentage change from ~1 year ago (nearest observation)."""
    if len(rows) < 2:
        return 0

    dates = [date_type.fromisoformat(r[0]) for r in rows]
    values = [r[1] for r in rows]
    tolerance = max(_median_gap_days(rows), 7)  # at least 7 days

    calcs = []
    for i, (date_str, value) in enumerate(rows):
        target = _year_ago(dates[i])
        prev_val = _find_yoy_match(dates, values, target, tolerance)
        if prev_val is not None and prev_val != 0:
            pct = (value - prev_val) / prev_val * 100
            calcs.append((series_id, "yoy_pct", date_str, round(pct, 2)))

    with con.cursor() as cur:
        cur.executemany(UPSERT_SQL, calcs)
    return len(calcs)


def _compute_period_pp(con, series_id, rows):
    """Percentage-point difference from prior observation."""
    calcs = []
    for i in range(1, len(rows)):
        prev_val = rows[i - 1][1]
        cur_date, cur_val = rows[i]
        diff = cur_val - prev_val
        calcs.append((series_id, "period_pp", cur_date, round(diff, 2)))

    with con.cursor() as cur:
        cur.executemany(UPSERT_SQL, calcs)
    return len(calcs)


def _compute_yoy_pp(con, series_id, rows):
    """Percentage-point difference from ~1 year ago (nearest observation)."""
    if len(rows) < 2:
        return 0

    dates = [date_type.fromisoformat(r[0]) for r in rows]
    values = [r[1] for r in rows]
    tolerance = max(_median_gap_days(rows), 7)

    calcs = []
    for i, (date_str, value) in enumerate(rows):
        target = _year_ago(dates[i])
        prev_val = _find_yoy_match(dates, values, target, tolerance)
        if prev_val is not None:
            diff = value - prev_val
            calcs.append((series_id, "yoy_pp", date_str, round(diff, 2)))

    with con.cursor() as cur:
        cur.executemany(UPSERT_SQL, calcs)
    return len(calcs)


def _year_ago(d: date_type) -> date_type:
    """Return the date exactly 1 year before d, handling leap years."""
    try:
        return d.replace(year=d.year - 1)
    except ValueError:
        # Feb 29 -> Feb 28
        return d.replace(year=d.year - 1, day=d.day - 1)
