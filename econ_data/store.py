"""Postgres-backed persistence layer for econ-data.

Replaces econ_data/store_sqlite.py at Step 4 cutover. Same function
signatures so callers don't need to change anything except the import.
The db_path argument on every function is ignored — kept only for
backward compatibility with call sites that still thread it through.

Schema is owned by schema.sql; this module assumes the tables exist.
"""
from __future__ import annotations

from datetime import date as date_type, datetime, timedelta
from pathlib import Path

from econ_data.db import connect

# Kept as an exported constant so legacy `from econ_data.store import DB_PATH`
# imports don't crash. Path is no longer authoritative — DATABASE_URL is.
DB_PATH = Path(__file__).parent.parent / "econ_data.db"


# ─────────────────────────────────────────────────────────────────────────
# Observations
# ─────────────────────────────────────────────────────────────────────────

_UPSERT_OBSERVATION = """
INSERT INTO observations (series_id, name, date, value, captured_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (series_id, date) DO UPDATE SET
    name = EXCLUDED.name,
    value = EXCLUDED.value,
    captured_at = CASE
        WHEN observations.value IS DISTINCT FROM EXCLUDED.value
        THEN EXCLUDED.captured_at
        ELSE observations.captured_at
    END
"""


def save(observations: list, db_path: Path = DB_PATH) -> int:
    """Upsert observations. captured_at is set on new rows or value changes,
    preserved when only the name changes."""
    if not observations:
        return 0
    now = datetime.now()
    rows = [
        (o.series_id, o.name, o.date, o.value, now)
        for o in observations
    ]
    con = connect()
    with con.transaction(), con.cursor() as cur:
        cur.executemany(_UPSERT_OBSERVATION, rows)
    return len(observations)


def get_last_dates(db_path: Path = DB_PATH) -> dict:
    """Return {series_id: date} of the most recent observation for each series."""
    rows = connect().execute(
        "SELECT series_id, MAX(date) FROM observations GROUP BY series_id"
    ).fetchall()
    return dict(rows)  # psycopg returns date objects for DATE columns


def get_series_captured_today(db_path: Path = DB_PATH) -> set:
    """Return set of series_ids that had observations captured today."""
    rows = connect().execute(
        "SELECT DISTINCT series_id FROM observations "
        "WHERE captured_at::date = CURRENT_DATE"
    ).fetchall()
    return {r[0] for r in rows}


def get_recent_observations(series_id: str, months: int = 4,
                            db_path: Path = DB_PATH) -> dict:
    """Return {date_str: value} for the last N months of a series."""
    cutoff = date_type.today() - timedelta(days=months * 31)
    rows = connect().execute(
        "SELECT date::text, value FROM observations "
        "WHERE series_id = %s AND date >= %s ORDER BY date",
        (series_id, cutoff),
    ).fetchall()
    return dict(rows)


# ─────────────────────────────────────────────────────────────────────────
# Revisions
# ─────────────────────────────────────────────────────────────────────────

def detect_and_save_revisions(observations: list,
                              db_path: Path = DB_PATH) -> list:
    """Compare incoming observations against stored values. Save and return revisions.

    Returns list of dicts: {series_id, name, date, old_value, new_value, pct_change}.
    """
    if not observations:
        return []

    con = connect()
    now = datetime.now()

    revisions = []
    rev_rows = []
    for obs in observations:
        row = con.execute(
            "SELECT value FROM observations WHERE series_id = %s AND date = %s",
            (obs.series_id, obs.date),
        ).fetchone()

        if row is None:
            continue  # new observation, not a revision

        old_value = row[0]
        if old_value == obs.value:
            continue  # unchanged

        if old_value != 0:
            pct_change = round((obs.value - old_value) / abs(old_value) * 100, 4)
        else:
            pct_change = 0.0

        revisions.append({
            "series_id": obs.series_id,
            "name": obs.name,
            "date": obs.date.isoformat(),
            "old_value": old_value,
            "new_value": obs.value,
            "pct_change": pct_change,
        })
        rev_rows.append(
            (obs.series_id, obs.date, old_value, obs.value, pct_change, now)
        )

    if rev_rows:
        with con.transaction(), con.cursor() as cur:
            cur.executemany(
                "INSERT INTO revisions "
                "(series_id, date, old_value, new_value, pct_change, detected_at) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (series_id, date, detected_at) DO NOTHING",
                rev_rows,
            )

    return revisions


def get_recent_revisions(days: int = 7, db_path: Path = DB_PATH) -> list:
    """Return revisions detected in the last N days, ordered by magnitude."""
    cutoff = date_type.today() - timedelta(days=days)
    rows = connect().execute(
        """
        SELECT r.series_id, o.name, r.date::text,
               r.old_value, r.new_value, r.pct_change,
               to_char(r.detected_at, 'YYYY-MM-DD"T"HH24:MI:SS') AS detected_at
        FROM revisions r
        LEFT JOIN (
            SELECT DISTINCT ON (series_id) series_id, name
            FROM observations
            ORDER BY series_id, date
        ) o ON r.series_id = o.series_id
        WHERE r.detected_at::date >= %s
        ORDER BY abs(r.pct_change) DESC
        """,
        (cutoff,),
    ).fetchall()
    return [
        {"series_id": r[0], "name": r[1], "date": r[2], "old_value": r[3],
         "new_value": r[4], "pct_change": r[5], "detected_at": r[6]}
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────────
# Export log
# ─────────────────────────────────────────────────────────────────────────

def get_export_log(db_path: Path = DB_PATH) -> dict:
    """Return {export_key: {"last_date": str, "exported_at": str}}."""
    rows = connect().execute(
        "SELECT export_key, last_date::text, "
        "to_char(exported_at, 'YYYY-MM-DD\"T\"HH24:MI:SS') "
        "FROM export_log"
    ).fetchall()
    return {key: {"last_date": ld, "exported_at": ea} for key, ld, ea in rows}


def save_export_log(export_key: str, last_date: str,
                    db_path: Path = DB_PATH) -> None:
    """Record that an export was performed with data through last_date."""
    connect().execute(
        "INSERT INTO export_log (export_key, last_date, exported_at) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (export_key) DO UPDATE SET "
        "last_date = EXCLUDED.last_date, exported_at = EXCLUDED.exported_at",
        (export_key, last_date, datetime.now()),
    )


# ─────────────────────────────────────────────────────────────────────────
# Fetch log
# ─────────────────────────────────────────────────────────────────────────

def get_fetch_log(db_path: Path = DB_PATH) -> dict:
    """Return {series_id: date} of when each series was last checked for new data."""
    rows = connect().execute(
        "SELECT series_id, last_checked FROM fetch_log"
    ).fetchall()
    return dict(rows)


def save_fetch_log(series_ids: list, db_path: Path = DB_PATH) -> None:
    """Record that these series were checked for new data today."""
    today = date_type.today()
    rows = [(sid, today) for sid in series_ids]
    con = connect()
    with con.transaction(), con.cursor() as cur:
        cur.executemany(
            "INSERT INTO fetch_log (series_id, last_checked) VALUES (%s, %s) "
            "ON CONFLICT (series_id) DO UPDATE SET last_checked = EXCLUDED.last_checked",
            rows,
        )


# ─────────────────────────────────────────────────────────────────────────
# Groups
# ─────────────────────────────────────────────────────────────────────────

def save_groups(cfg: dict, db_path: Path = DB_PATH) -> None:
    """Upsert group definitions and memberships from config."""
    con = connect()
    with con.transaction():
        for group_id, group in cfg.get("groups", {}).items():
            con.execute(
                "INSERT INTO groups (group_id, name) VALUES (%s, %s) "
                "ON CONFLICT (group_id) DO UPDATE SET name = EXCLUDED.name",
                (group_id, group["name"]),
            )
            for s in group["series"]:
                con.execute(
                    "INSERT INTO group_members (group_id, series_id) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (group_id, s["id"]),
                )
