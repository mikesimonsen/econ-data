"""Fetch Altos Research weekly housing data from drop-folder CSV files.

Altos delivers a weekly national trends CSV (every Friday).
Save the file to import_files/ and this module will ingest it.

File naming convention: altos_trends_national_*.csv
Key columns: date, res_type_master_id, quartile, window_size
"""
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from econ_data.fetch import Observation

IMPORT_DIR = Path("import_files")

# Series definitions: each specifies filters and which value column to sum/extract.
# "window_size" matches the CSV's window_size column (6=weekly, 90=13-week rolling).
# "rolling" defines a computed rolling average from the weekly (window_size=6) data.
ALTOS_SERIES = {
    "ALTOS_INVENTORY": {
        "name": "Altos Total Inventory (SFR + Condo)",
        "column": "count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 6,
    },
    "ALTOS_NEW_LISTINGS": {
        "name": "Altos New Listings (SFR + Condo)",
        "column": "new_count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 6,
    },
    "ALTOS_NEW_LISTINGS_13WK": {
        "name": "Altos New Listings 13-Week Avg (SFR + Condo)",
        "column": "new_count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 90,
    },
    "ALTOS_NEW_LISTINGS_4WK": {
        "name": "Altos New Listings 4-Week Avg (SFR + Condo)",
        "column": "new_count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 6,
        "rolling": 4,
    },
    "ALTOS_NEW_PENDING": {
        "name": "Altos New Pending Sales (SFR + Condo)",
        "column": "pending_new_count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 6,
    },
    "ALTOS_NEW_PENDING_13WK": {
        "name": "Altos New Pending Sales 13-Week Avg (SFR + Condo)",
        "column": "pending_new_count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 90,
    },
    "ALTOS_NEW_PENDING_4WK": {
        "name": "Altos New Pending Sales 4-Week Avg (SFR + Condo)",
        "column": "pending_new_count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 6,
        "rolling": 4,
    },
    "ALTOS_PENDING_DOM": {
        "name": "Altos Pending Days on Market (SFR + Condo)",
        "column": "pending_dom_mean",
        "weight_column": "pending_count",
        "res_types": [100, 200],
        "quartile": "__ALL",
        "window_size": 6,
    },
}


def fetch_altos(last_dates: dict = None) -> dict:
    """
    Scan import_files/ for altos_trends_national_*.csv and ingest new observations.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    all_new = []
    counts = {sid: 0 for sid in ALTOS_SERIES}

    if not IMPORT_DIR.exists():
        return {"new": all_new, "counts": counts}

    files = sorted(IMPORT_DIR.glob("altos_trends_national_*.csv"))
    if not files:
        return {"new": all_new, "counts": counts}

    # Each file contains the full history — use the latest one
    fpath = files[-1]

    try:
        rows = _read_csv(fpath)
    except Exception as e:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED Altos file {fpath.name} — {e}")
        return {"new": all_new, "counts": counts}

    for series_id, spec in ALTOS_SERIES.items():
        last = last_dates.get(series_id)
        if "rolling" in spec:
            obs = _build_rolling_series(rows, series_id, spec, last)
        else:
            obs = _build_series(rows, series_id, spec, last)
        all_new.extend(obs)
        counts[series_id] = len(obs)

    return {"new": all_new, "counts": counts}


def _read_csv(fpath: Path) -> list[dict]:
    """Read CSV and return list of row dicts."""
    with open(fpath, newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _build_series(
    rows: list[dict],
    series_id: str,
    spec: dict,
    last_date: date | None,
) -> list[Observation]:
    """Filter rows by spec, aggregate across res_types per date."""
    if "weight_column" in spec:
        date_totals = _weighted_avg(rows, spec)
    else:
        date_totals = _aggregate(rows, spec)

    observations = []
    for obs_date in sorted(date_totals):
        if last_date and obs_date <= last_date:
            continue
        observations.append(Observation(
            series_id=series_id,
            name=spec["name"],
            date=obs_date,
            value=date_totals[obs_date],
        ))

    return observations


def _build_rolling_series(
    rows: list[dict],
    series_id: str,
    spec: dict,
    last_date: date | None,
) -> list[Observation]:
    """Build a rolling average from the weekly (window_size=6) data."""
    window = spec["rolling"]
    # Get the raw weekly totals (always from window_size=6)
    weekly_spec = {**spec, "window_size": 6}
    del weekly_spec["rolling"]
    date_totals = _aggregate(rows, weekly_spec)

    sorted_dates = sorted(date_totals)
    sorted_values = [date_totals[d] for d in sorted_dates]

    observations = []
    for i in range(window - 1, len(sorted_dates)):
        obs_date = sorted_dates[i]
        if last_date and obs_date <= last_date:
            continue
        avg = sum(sorted_values[i - window + 1 : i + 1]) / window
        observations.append(Observation(
            series_id=series_id,
            name=spec["name"],
            date=obs_date,
            value=round(avg, 1),
        ))

    return observations


def _aggregate(rows: list[dict], spec: dict) -> dict[date, float]:
    """Filter CSV rows by spec and sum values across res_types per date."""
    col = spec["column"]
    res_types = {str(rt) for rt in spec["res_types"]}
    quartile = spec["quartile"]
    window_size = str(spec["window_size"])

    date_totals: dict[date, float] = defaultdict(float)
    for row in rows:
        if (
            row["res_type_master_id"] in res_types
            and row["quartile"] == quartile
            and row["window_size"] == window_size
        ):
            val = row.get(col)
            if val is None or val == "":
                continue
            obs_date = date.fromisoformat(row["date"])
            date_totals[obs_date] += float(val)

    return date_totals


def _weighted_avg(rows: list[dict], spec: dict) -> dict[date, float]:
    """Filter CSV rows by spec and compute weighted average across res_types per date."""
    col = spec["column"]
    weight_col = spec["weight_column"]
    res_types = {str(rt) for rt in spec["res_types"]}
    quartile = spec["quartile"]
    window_size = str(spec["window_size"])

    # Accumulate (sum of value*weight, sum of weight) per date
    date_num: dict[date, float] = defaultdict(float)
    date_den: dict[date, float] = defaultdict(float)
    for row in rows:
        if (
            row["res_type_master_id"] in res_types
            and row["quartile"] == quartile
            and row["window_size"] == window_size
        ):
            val = row.get(col)
            wt = row.get(weight_col)
            if not val or not wt or val == "" or wt == "":
                continue
            obs_date = date.fromisoformat(row["date"])
            v, w = float(val), float(wt)
            date_num[obs_date] += v * w
            date_den[obs_date] += w

    return {d: round(date_num[d] / date_den[d], 2)
            for d in date_num if date_den[d] > 0}


if __name__ == "__main__":
    # Standalone import of the latest weekly Altos drop.
    # Reads import_files/altos_trends_national_*.csv (newest), upserts
    # observations, recomputes derived values, refreshes the sheets_data
    # CSVs + manifest, and re-renders docs/index.html. Skips the LLM
    # analyses and the git commit/push — those stay manual.
    from pathlib import Path

    from econ_data.briefing import generate_briefing
    from econ_data.calculations import compute_all
    from econ_data.config import load
    from econ_data.export_sheets import (
        export_all_groups, export_all_groups_calcs, write_manifest,
    )
    from econ_data.store import get_last_dates, get_series_captured_today, save

    result = fetch_altos(last_dates=get_last_dates())
    saved = save(result["new"])
    print(f"Altos: {saved} new observations")
    for sid, n in result["counts"].items():
        if n:
            print(f"  {sid:30} +{n}")
    print(f"Recomputed {compute_all()} derived values")

    cfg = load()
    updated_ids = get_series_captured_today()
    paths = export_all_groups(cfg, updated_ids=updated_ids)
    calc_paths = export_all_groups_calcs(cfg, updated_ids=updated_ids)
    changed_groups = [
        gid for gid, gdata in cfg.get("groups", {}).items()
        if {s["id"] for s in gdata["series"]} & updated_ids
    ]
    write_manifest(changed_groups)
    print(f"Exported {len(paths)} value CSVs + {len(calc_paths)} calc CSVs "
          f"({len(changed_groups)} groups changed)")

    Path("docs").mkdir(exist_ok=True)
    Path("docs/index.html").write_text(
        generate_briefing(cfg, updated_ids=updated_ids))
    print("Re-rendered docs/index.html")
