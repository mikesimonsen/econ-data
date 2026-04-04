"""
Seasonal adjustment for non-seasonally-adjusted series (e.g., Altos Research).

Computes weekly seasonal factors using the ratio-to-centered-moving-average
method (simplified X-11).  For each ISO week (1-52), the factor represents
the typical ratio of that week's value to the local trend.

  factor > 1.0  →  above-average activity (e.g., spring listings)
  factor < 1.0  →  below-average activity (e.g., holiday weeks)

Seasonally adjusted value = raw_value / factor[iso_week]

Housing-specific notes:
  - Easter (moveable, Mar-Apr), July 4th, Labor Day, Thanksgiving,
    Christmas/New Year all suppress activity.  The model captures these
    implicitly from 10+ years of history; a 5-week smoothing window
    reduces noise from Easter's year-to-year drift.
"""
from __future__ import annotations

import sqlite3
import statistics
from collections import defaultdict
from datetime import date as date_type
from pathlib import Path

from econ_data.store_sqlite import DB_PATH

MIN_WEEKS = 104        # need ≥ 2 full years
MA_HALF_WINDOW = 26    # 52-week centered moving average (26 each side)
SMOOTH_WINDOW = 5      # centered smoothing for per-week factors

# Module-level cache: {series_id: {iso_week: factor} or None}
_cache: dict[str, dict[int, float] | None] = {}


def compute_seasonal_factors(
    series_id: str, db_path: Path = DB_PATH
) -> dict[int, float] | None:
    """Return {iso_week (1-52): multiplicative factor} or None if < 2 years."""
    if series_id in _cache:
        return _cache[series_id]

    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT date, value FROM observations WHERE series_id = ? ORDER BY date",
        (series_id,),
    ).fetchall()
    con.close()

    if len(rows) < MIN_WEEKS:
        _cache[series_id] = None
        return None

    dates = [date_type.fromisoformat(r[0]) for r in rows]
    values = [r[1] for r in rows]

    # ── Centered 52-week moving average ───────────────────────
    n = len(values)
    ma = [0.0] * n
    for i in range(n):
        lo = max(0, i - MA_HALF_WINDOW)
        hi = min(n, i + MA_HALF_WINDOW + 1)
        ma[i] = sum(values[lo:hi]) / (hi - lo)

    # ── Seasonal ratios by ISO week ───────────────────────────
    ratios_by_week: dict[int, list[float]] = defaultdict(list)
    for i in range(n):
        if ma[i] == 0:
            continue
        iso_week = min(dates[i].isocalendar()[1], 52)  # fold 53 → 52
        ratios_by_week[iso_week].append(values[i] / ma[i])

    # ── Raw factor = median ratio per week ────────────────────
    raw: dict[int, float] = {}
    for week in range(1, 53):
        r = ratios_by_week.get(week, [])
        raw[week] = statistics.median(r) if r else 1.0

    # ── Smooth with centered window (handles Easter drift) ────
    half = SMOOTH_WINDOW // 2
    smoothed: dict[int, float] = {}
    for week in range(1, 53):
        vals = [raw[((week - 1 + offset) % 52) + 1]
                for offset in range(-half, half + 1)]
        smoothed[week] = sum(vals) / len(vals)

    # ── Normalize to mean ≈ 1.0 ──────────────────────────────
    mean_f = sum(smoothed.values()) / len(smoothed)
    if mean_f > 0:
        factors = {w: f / mean_f for w, f in smoothed.items()}
    else:
        factors = smoothed

    _cache[series_id] = factors
    return factors


def sa_period_changes(
    rows: list[tuple], factors: dict[int, float]
) -> dict[str, float]:
    """Compute seasonally-adjusted WoW % changes from raw observations.

    rows: [(date_str, value), ...] ordered by date.
    factors: {iso_week: factor} from compute_seasonal_factors.

    Returns {date_str: sa_pct_change}.  SA value = raw / factor[week].
    """
    result: dict[str, float] = {}
    for i in range(1, len(rows)):
        prev_date, prev_val = rows[i - 1]
        cur_date, cur_val = rows[i]
        prev_week = min(date_type.fromisoformat(prev_date).isocalendar()[1], 52)
        cur_week = min(date_type.fromisoformat(cur_date).isocalendar()[1], 52)
        sa_prev = prev_val / factors.get(prev_week, 1.0)
        sa_cur = cur_val / factors.get(cur_week, 1.0)
        if sa_prev != 0:
            result[cur_date] = (sa_cur - sa_prev) / sa_prev * 100
    return result


def clear_cache():
    """Clear cached seasonal factors (call after new data is ingested)."""
    _cache.clear()
