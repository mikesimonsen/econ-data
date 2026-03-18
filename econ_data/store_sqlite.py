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
