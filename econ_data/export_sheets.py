"""Export group data as wide-format CSVs for Google Sheets consumption.

Each group gets one CSV: dates as rows, series as columns.
Calc exports produce unified CSVs (period/yoy) that use the right calc type
per series — percentage-point diff for rate series, percent change for levels.
Writes a manifest (last_updated.json) so consumers can skip unchanged groups.
"""

import csv
import json
from datetime import datetime
from pathlib import Path

from econ_data.config import load as load_config, percent_series
from econ_data.db import connect
from econ_data.store import DB_PATH

SHEETS_DIR = Path(__file__).parent.parent / "sheets_data"
SHEETS_CALC_DIR = Path(__file__).parent.parent / "sheets_data_calcs"
MANIFEST_PATH = Path(__file__).parent.parent / "sheets_data" / "last_updated.json"


def _export_group(group_id: str, series_ids: list, output_dir: Path,
                  table: str, calc_type: str = None,
                  suffix: str = "", db_path: Path = DB_PATH) -> Path:
    """Export a group as a wide-format CSV.

    table: "observations" for raw values, "calculated" for derived series.
    calc_type: calc type string, or None for observations.
    suffix: appended to column names (e.g. " Period Chg").
    """
    con = connect()

    # Get series names
    names = {}
    for sid in series_ids:
        row = con.execute(
            "SELECT name FROM observations WHERE series_id = %s "
            "ORDER BY date LIMIT 1",
            (sid,)
        ).fetchone()
        names[sid] = row[0] if row else sid

    # Build {series_id: {date: value}} and collect all dates
    all_dates = set()
    series_data = {}
    for sid in series_ids:
        if table == "observations":
            rows = con.execute(
                "SELECT date::text, value FROM observations WHERE series_id = %s ORDER BY date",
                (sid,),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT date::text, value FROM calculated "
                "WHERE series_id = %s AND calc_type = %s ORDER BY date",
                (sid, calc_type),
            ).fetchall()
        series_data[sid] = {d: v for d, v in rows}
        all_dates.update(d for d, _ in rows)

    dates_sorted = sorted(all_dates)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{group_id}.csv"

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date"] + [names[sid] + suffix for sid in series_ids])
        for d in dates_sorted:
            row = [d]
            for sid in series_ids:
                val = series_data[sid].get(d, "")
                row.append(val)
            writer.writerow(row)

    return path


def _export_group_unified(group_id: str, series_ids: list, output_dir: Path,
                          calc_family: str, pct_ids: set,
                          suffix: str = "", db_path: Path = DB_PATH) -> Path:
    """Export a group as a wide-format CSV, picking the right calc type per series.

    calc_family: "period" or "yoy" — resolves to period_pct/period_pp or yoy_pct/yoy_pp
                 based on whether each series is a percent-unit series.
    """
    con = connect()

    names = {}
    for sid in series_ids:
        row = con.execute(
            "SELECT name FROM observations WHERE series_id = %s "
            "ORDER BY date LIMIT 1",
            (sid,)
        ).fetchone()
        names[sid] = row[0] if row else sid

    all_dates = set()
    series_data = {}
    for sid in series_ids:
        ct = f"{calc_family}_pp" if sid in pct_ids else f"{calc_family}_pct"
        rows = con.execute(
            "SELECT date::text, value FROM calculated "
            "WHERE series_id = %s AND calc_type = %s ORDER BY date",
            (sid, ct),
        ).fetchall()
        series_data[sid] = {d: v for d, v in rows}
        all_dates.update(d for d, _ in rows)

    dates_sorted = sorted(all_dates)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{group_id}.csv"

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date"] + [names[sid] + suffix for sid in series_ids])
        for d in dates_sorted:
            row = [d]
            for sid in series_ids:
                val = series_data[sid].get(d, "")
                row.append(val)
            writer.writerow(row)

    return path


def export_group_csv(group_id: str, group_name: str, series_ids: list,
                     output_dir: Path = SHEETS_DIR,
                     db_path: Path = DB_PATH) -> Path:
    """Export raw values for a group."""
    return _export_group(group_id, series_ids, output_dir,
                         table="observations", db_path=db_path)


def _groups_with_updates(cfg: dict, updated_ids: set) -> list:
    """Return list of (group_id, group_data) where at least one series was updated."""
    result = []
    for gid, gdata in cfg.get("groups", {}).items():
        series_ids = {s["id"] for s in gdata["series"]}
        if series_ids & updated_ids:
            result.append((gid, gdata))
    return result


def export_all_groups(cfg: dict, updated_ids: set = None,
                      output_dir: Path = SHEETS_DIR,
                      db_path: Path = DB_PATH) -> list:
    """Export groups as raw-value CSVs. Only exports groups with updated series.

    If updated_ids is None, exports all groups (for initial/full export).
    """
    paths = []
    if updated_ids is not None:
        groups = _groups_with_updates(cfg, updated_ids)
    else:
        groups = list(cfg.get("groups", {}).items())

    for gid, gdata in groups:
        series_ids = [s["id"] for s in gdata["series"]]
        path = export_group_csv(gid, gdata["name"], series_ids, output_dir, db_path)
        paths.append(path)
    return paths


def export_all_groups_calcs(cfg: dict, updated_ids: set = None,
                            output_dir: Path = SHEETS_CALC_DIR,
                            db_path: Path = DB_PATH) -> list:
    """Export unified period and yoy CSVs per group.

    Each CSV picks the right calc type per series (pp for rates, pct for levels).
    """
    pct_ids = percent_series(cfg)
    paths = []
    if updated_ids is not None:
        groups = _groups_with_updates(cfg, updated_ids)
    else:
        groups = list(cfg.get("groups", {}).items())

    for gid, gdata in groups:
        series_ids = [s["id"] for s in gdata["series"]]
        for calc_family, suffix in [("period", " Period Chg"), ("yoy", " YoY Chg")]:
            sub_dir = output_dir / calc_family
            path = _export_group_unified(gid, series_ids, sub_dir,
                                         calc_family, pct_ids,
                                         suffix=suffix, db_path=db_path)
            paths.append(path)
    return paths


def write_manifest(updated_groups: list, manifest_path: Path = MANIFEST_PATH):
    """Write/update manifest with timestamps for changed groups.

    Apps Script reads this to decide which tabs to re-import.
    """
    # Load existing manifest
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
    else:
        manifest = {}

    now = datetime.now().isoformat(timespec="seconds")
    for gid in updated_groups:
        manifest[gid] = now

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
