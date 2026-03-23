import sqlite3
from pathlib import Path

from econ_data.fetch import Observation

DB_PATH = Path(__file__).parent.parent / "econ_data.db"

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS observations (
    series_id   TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    date        TEXT    NOT NULL,
    value       REAL    NOT NULL,
    captured_at TEXT,
    PRIMARY KEY (series_id, date)
);
CREATE TABLE IF NOT EXISTS groups (
    group_id    TEXT    PRIMARY KEY,
    name        TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS group_members (
    group_id    TEXT    NOT NULL,
    series_id   TEXT    NOT NULL,
    PRIMARY KEY (group_id, series_id)
);
CREATE TABLE IF NOT EXISTS export_log (
    export_key  TEXT    PRIMARY KEY,
    last_date   TEXT    NOT NULL,
    exported_at TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS fetch_log (
    series_id   TEXT    PRIMARY KEY,
    last_checked TEXT   NOT NULL
);
CREATE TABLE IF NOT EXISTS revisions (
    series_id    TEXT    NOT NULL,
    date         TEXT    NOT NULL,
    old_value    REAL    NOT NULL,
    new_value    REAL    NOT NULL,
    pct_change   REAL    NOT NULL,
    detected_at  TEXT    NOT NULL,
    PRIMARY KEY (series_id, date, detected_at)
);
"""


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.executescript(CREATE_TABLES)
    _migrate(con)
    return con


def _migrate(con: sqlite3.Connection):
    """Add columns that may not exist in older databases."""
    cols = {row[1] for row in con.execute("PRAGMA table_info(observations)")}
    if "captured_at" not in cols:
        con.execute("ALTER TABLE observations ADD COLUMN captured_at TEXT")
        con.commit()


def save(observations: list, db_path: Path = DB_PATH) -> int:
    """Upsert observations with capture timestamp. Returns number of rows inserted/replaced."""
    from datetime import datetime
    now = datetime.now().isoformat(timespec="seconds")
    con = _connect(db_path)
    rows = [(o.series_id, o.name, o.date.isoformat(), o.value, now) for o in observations]
    cur = con.executemany(
        "INSERT OR REPLACE INTO observations (series_id, name, date, value, captured_at) "
        "VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    con.commit()
    con.close()
    return cur.rowcount


def get_last_dates(db_path: Path = DB_PATH) -> dict:
    """Return {series_id: date} of the most recent observation for each series."""
    from datetime import date as date_type
    con = _connect(db_path)
    rows = con.execute(
        "SELECT series_id, MAX(date) FROM observations GROUP BY series_id"
    ).fetchall()
    con.close()
    return {series_id: date_type.fromisoformat(d) for series_id, d in rows}


def get_export_log(db_path: Path = DB_PATH) -> dict:
    """Return {export_key: {"last_date": str, "exported_at": str}}."""
    con = _connect(db_path)
    rows = con.execute("SELECT export_key, last_date, exported_at FROM export_log").fetchall()
    con.close()
    return {key: {"last_date": ld, "exported_at": ea} for key, ld, ea in rows}


def save_export_log(export_key: str, last_date: str, db_path: Path = DB_PATH) -> None:
    """Record that an export was performed with data through last_date."""
    from datetime import datetime
    con = _connect(db_path)
    con.execute(
        "INSERT OR REPLACE INTO export_log (export_key, last_date, exported_at) VALUES (?, ?, ?)",
        (export_key, last_date, datetime.now().isoformat(timespec="seconds")),
    )
    con.commit()
    con.close()


def get_fetch_log(db_path: Path = DB_PATH) -> dict:
    """Return {series_id: date} of when each series was last checked for new data."""
    from datetime import date as date_type
    con = _connect(db_path)
    rows = con.execute("SELECT series_id, last_checked FROM fetch_log").fetchall()
    con.close()
    return {sid: date_type.fromisoformat(d) for sid, d in rows}


def save_fetch_log(series_ids: list, db_path: Path = DB_PATH) -> None:
    """Record that these series were checked for new data today."""
    from datetime import date
    today = date.today().isoformat()
    con = _connect(db_path)
    con.executemany(
        "INSERT OR REPLACE INTO fetch_log (series_id, last_checked) VALUES (?, ?)",
        [(sid, today) for sid in series_ids],
    )
    con.commit()
    con.close()


def get_recent_observations(series_id: str, months: int = 4,
                            db_path: Path = DB_PATH) -> dict:
    """Return {date_str: value} for the last N months of a series."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=months * 31)).isoformat()
    con = _connect(db_path)
    rows = con.execute(
        "SELECT date, value FROM observations "
        "WHERE series_id = ? AND date >= ? ORDER BY date",
        (series_id, cutoff),
    ).fetchall()
    con.close()
    return {d: v for d, v in rows}


def detect_and_save_revisions(observations: list,
                              db_path: Path = DB_PATH) -> list:
    """Compare incoming observations against stored values. Save and return revisions.

    Returns list of dicts: {series_id, name, date, old_value, new_value, pct_change}
    """
    from datetime import datetime

    if not observations:
        return []

    now = datetime.now().isoformat(timespec="seconds")
    con = _connect(db_path)

    # Build lookup of existing values for the series/dates we're about to save
    revisions = []
    for obs in observations:
        date_str = obs.date.isoformat()
        row = con.execute(
            "SELECT value FROM observations WHERE series_id = ? AND date = ?",
            (obs.series_id, date_str),
        ).fetchone()

        if row is None:
            continue  # new observation, not a revision

        old_value = row[0]
        if old_value == obs.value:
            continue  # unchanged

        # Calculate % change of the revision
        if old_value != 0:
            pct_change = round((obs.value - old_value) / abs(old_value) * 100, 4)
        else:
            pct_change = 0.0

        revisions.append({
            "series_id": obs.series_id,
            "name": obs.name,
            "date": date_str,
            "old_value": old_value,
            "new_value": obs.value,
            "pct_change": pct_change,
        })

        con.execute(
            "INSERT OR IGNORE INTO revisions "
            "(series_id, date, old_value, new_value, pct_change, detected_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (obs.series_id, date_str, old_value, obs.value, pct_change, now),
        )

    con.commit()
    con.close()
    return revisions


def get_recent_revisions(days: int = 7, db_path: Path = DB_PATH) -> list:
    """Return revisions detected in the last N days."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    con = _connect(db_path)
    rows = con.execute(
        "SELECT r.series_id, o.name, r.date, r.old_value, r.new_value, "
        "r.pct_change, r.detected_at "
        "FROM revisions r "
        "LEFT JOIN (SELECT DISTINCT series_id, name FROM observations) o "
        "ON r.series_id = o.series_id "
        "WHERE r.detected_at >= ? "
        "ORDER BY abs(r.pct_change) DESC",
        (cutoff,),
    ).fetchall()
    con.close()
    return [
        {"series_id": r[0], "name": r[1], "date": r[2], "old_value": r[3],
         "new_value": r[4], "pct_change": r[5], "detected_at": r[6]}
        for r in rows
    ]


def save_groups(cfg: dict, db_path: Path = DB_PATH) -> None:
    """Upsert group definitions and memberships from config."""
    con = _connect(db_path)
    for group_id, group in cfg.get("groups", {}).items():
        con.execute(
            "INSERT OR REPLACE INTO groups (group_id, name) VALUES (?, ?)",
            (group_id, group["name"]),
        )
        for s in group["series"]:
            con.execute(
                "INSERT OR IGNORE INTO group_members (group_id, series_id) VALUES (?, ?)",
                (group_id, s["id"]),
            )
    con.commit()
    con.close()
