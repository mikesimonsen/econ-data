"""
Compute rolling average series from existing weekly observations.

Each entry in ROLLING_SERIES defines a derived series computed as an
N-week rolling average of a source series. Runs after all fetchers,
before compute_all().
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from econ_data.db import connect
from econ_data.fetch import Observation
from econ_data.store_sqlite import DB_PATH  # signature compat for unmigrated callers

# Each entry: (source_series_id, window, derived_series_id, name)
ROLLING_SERIES = [
    ("XACTUS_MII", 4, "XACTUS_MII_4WK", "Xactus MII 4-Week Avg"),
    ("XACTUS_MII", 13, "XACTUS_MII_13WK", "Xactus MII 13-Week Avg"),
    ("MBA_PURCHASE", 4, "MBA_PURCHASE_4WK", "MBA Purchase Index 4-Week Avg"),
    ("MBA_PURCHASE", 13, "MBA_PURCHASE_13WK", "MBA Purchase Index 13-Week Avg"),
]


def compute_rolling(last_dates: dict = None,
                    db_path: Path = DB_PATH) -> dict:
    """Compute all rolling average series.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    con = connect()
    all_new = []
    counts = {}

    for source_id, window, derived_id, name in ROLLING_SERIES:
        last = last_dates.get(derived_id)

        rows = con.execute(
            "SELECT date::text, value FROM observations WHERE series_id = %s "
            "ORDER BY date",
            (source_id,),
        ).fetchall()

        if len(rows) < window:
            counts[derived_id] = 0
            continue

        dates = [date.fromisoformat(r[0]) for r in rows]
        values = [r[1] for r in rows]
        new_obs = []

        for i in range(window - 1, len(dates)):
            obs_date = dates[i]
            if last and obs_date <= last:
                continue
            avg = sum(values[i - window + 1 : i + 1]) / window
            new_obs.append(Observation(
                series_id=derived_id,
                name=name,
                date=obs_date,
                value=round(avg, 2),
            ))

        all_new.extend(new_obs)
        counts[derived_id] = len(new_obs)

    return {"new": all_new, "counts": counts}
