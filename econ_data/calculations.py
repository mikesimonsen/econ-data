"""
Compute derived time series from raw observations.

Calculation types:
  period_pct — percentage change from the prior observation (for level series)
  yoy_pct   — percentage change from the same month one year earlier (for level series)
  period_pp  — percentage-point difference from the prior observation (for percent/rate series)
  yoy_pp     — percentage-point difference from the same month one year earlier (for percent/rate series)
"""
import sqlite3
from pathlib import Path

from econ_data.config import load, percent_series
from econ_data.store_sqlite import DB_PATH


def compute_all(db_path: Path = DB_PATH) -> int:
    """Compute all derived series and save to the calculated table. Returns rows written."""
    con = sqlite3.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS calculated (
            series_id   TEXT    NOT NULL,
            calc_type   TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            value       REAL    NOT NULL,
            PRIMARY KEY (series_id, calc_type, date)
        )
    """)

    cfg = load()
    pct_ids = percent_series(cfg)

    series_ids = [r[0] for r in con.execute(
        "SELECT DISTINCT series_id FROM observations ORDER BY series_id"
    ).fetchall()]

    total = 0
    for series_id in series_ids:
        rows = con.execute(
            "SELECT date, value FROM observations WHERE series_id = ? ORDER BY date",
            (series_id,),
        ).fetchall()

        if series_id in pct_ids:
            total += _compute_period_pp(con, series_id, rows)
            total += _compute_yoy_pp(con, series_id, rows)
        else:
            total += _compute_period_pct(con, series_id, rows)
            total += _compute_yoy_pct(con, series_id, rows)

    con.commit()
    con.close()
    return total


def _compute_period_pct(con, series_id, rows):
    """Percentage change from prior observation."""
    calcs = []
    for i in range(1, len(rows)):
        prev_val = rows[i - 1][1]
        cur_date, cur_val = rows[i]
        if prev_val != 0:
            pct = (cur_val - prev_val) / prev_val * 100
            calcs.append((series_id, "period_pct", cur_date, round(pct, 2)))

    con.executemany(
        "INSERT OR REPLACE INTO calculated (series_id, calc_type, date, value) VALUES (?, ?, ?, ?)",
        calcs,
    )
    return len(calcs)


def _compute_yoy_pct(con, series_id, rows):
    """Percentage change from the same month one year ago."""
    by_month = {}
    for date_str, value in rows:
        y, m = date_str[:4], date_str[5:7]
        by_month[(int(y), int(m))] = (date_str, value)

    calcs = []
    for date_str, value in rows:
        y, m = int(date_str[:4]), int(date_str[5:7])
        prev = by_month.get((y - 1, m))
        if prev and prev[1] != 0:
            pct = (value - prev[1]) / prev[1] * 100
            calcs.append((series_id, "yoy_pct", date_str, round(pct, 2)))

    con.executemany(
        "INSERT OR REPLACE INTO calculated (series_id, calc_type, date, value) VALUES (?, ?, ?, ?)",
        calcs,
    )
    return len(calcs)


def _compute_period_pp(con, series_id, rows):
    """Percentage-point difference from prior observation."""
    calcs = []
    for i in range(1, len(rows)):
        prev_val = rows[i - 1][1]
        cur_date, cur_val = rows[i]
        diff = cur_val - prev_val
        calcs.append((series_id, "period_pp", cur_date, round(diff, 2)))

    con.executemany(
        "INSERT OR REPLACE INTO calculated (series_id, calc_type, date, value) VALUES (?, ?, ?, ?)",
        calcs,
    )
    return len(calcs)


def _compute_yoy_pp(con, series_id, rows):
    """Percentage-point difference from the same month one year ago."""
    by_month = {}
    for date_str, value in rows:
        y, m = date_str[:4], date_str[5:7]
        by_month[(int(y), int(m))] = (date_str, value)

    calcs = []
    for date_str, value in rows:
        y, m = int(date_str[:4]), int(date_str[5:7])
        prev = by_month.get((y - 1, m))
        if prev is not None:
            diff = value - prev[1]
            calcs.append((series_id, "yoy_pp", date_str, round(diff, 2)))

    con.executemany(
        "INSERT OR REPLACE INTO calculated (series_id, calc_type, date, value) VALUES (?, ?, ?, ?)",
        calcs,
    )
    return len(calcs)
