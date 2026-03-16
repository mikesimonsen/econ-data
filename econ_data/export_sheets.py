"""Export group data as wide-format CSVs for Google Sheets consumption.

Each group gets one CSV: dates as rows, series as columns.
"""

import csv
import sqlite3
from pathlib import Path

from econ_data.store_sqlite import DB_PATH

SHEETS_DIR = Path(__file__).parent.parent / "sheets_data"


def export_group_csv(group_id: str, group_name: str, series_ids: list,
                     output_dir: Path = SHEETS_DIR,
                     db_path: Path = DB_PATH) -> Path:
    """Export a group as a wide-format CSV (dates as rows, series as columns)."""
    con = sqlite3.connect(db_path)

    # Get series names
    names = {}
    for sid in series_ids:
        row = con.execute(
            "SELECT DISTINCT name FROM observations WHERE series_id = ?", (sid,)
        ).fetchone()
        names[sid] = row[0] if row else sid

    # Build {series_id: {date: value}} and collect all dates
    all_dates = set()
    series_data = {}
    for sid in series_ids:
        rows = con.execute(
            "SELECT date, value FROM observations WHERE series_id = ? ORDER BY date",
            (sid,),
        ).fetchall()
        series_data[sid] = {d: v for d, v in rows}
        all_dates.update(d for d, _ in rows)

    con.close()

    dates_sorted = sorted(all_dates)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{group_id}.csv"

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date"] + [names[sid] for sid in series_ids])
        for d in dates_sorted:
            row = [d]
            for sid in series_ids:
                val = series_data[sid].get(d, "")
                row.append(val)
            writer.writerow(row)

    return path


def export_all_groups(cfg: dict, output_dir: Path = SHEETS_DIR,
                      db_path: Path = DB_PATH) -> list:
    """Export all groups as wide-format CSVs. Returns list of paths written."""
    paths = []
    for gid, gdata in cfg.get("groups", {}).items():
        series_ids = [s["id"] for s in gdata["series"]]
        path = export_group_csv(gid, gdata["name"], series_ids, output_dir, db_path)
        paths.append(path)
    return paths
