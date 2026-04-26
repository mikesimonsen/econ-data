"""One-shot migration of econ_data.db (SQLite) to Postgres.

Usage:
    1. Apply schema.sql to an empty Postgres DB:
         psql "$DATABASE_URL" -f schema.sql
    2. Set DATABASE_URL in .env (Neon pooled connection string).
    3. Run:
         python migrate_to_postgres.py

Idempotent in the sense that re-running on an already-populated DB will fail
on PK conflicts — that's intentional. To re-run, drop and recreate the schema.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv

SQLITE_PATH = Path(__file__).parent / "econ_data.db"


def parse_date(s: str | None) -> date | None:
    return date.fromisoformat(s) if s else None


def parse_ts(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def migrate_observations(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute(
        "SELECT series_id, name, date, value, captured_at FROM observations"
    ).fetchall()
    data = [(sid, name, parse_date(d), v, parse_ts(c)) for sid, name, d, v, c in rows]
    with pg.cursor() as cur:
        with cur.copy(
            "COPY observations (series_id, name, date, value, captured_at) FROM STDIN"
        ) as copy:
            for row in data:
                copy.write_row(row)
    return len(data)


def migrate_calculated(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute(
        "SELECT series_id, calc_type, date, value FROM calculated"
    ).fetchall()
    data = [(sid, ct, parse_date(d), v) for sid, ct, d, v in rows]
    with pg.cursor() as cur:
        with cur.copy(
            "COPY calculated (series_id, calc_type, date, value) FROM STDIN"
        ) as copy:
            for row in data:
                copy.write_row(row)
    return len(data)


def migrate_revisions(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute(
        "SELECT series_id, date, old_value, new_value, pct_change, detected_at "
        "FROM revisions"
    ).fetchall()
    data = [
        (sid, parse_date(d), ov, nv, pc, parse_ts(dt))
        for sid, d, ov, nv, pc, dt in rows
    ]
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO revisions "
            "(series_id, date, old_value, new_value, pct_change, detected_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            data,
        )
    return len(data)


def migrate_export_log(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute(
        "SELECT export_key, last_date, exported_at FROM export_log"
    ).fetchall()
    data = [(k, parse_date(ld), parse_ts(ea)) for k, ld, ea in rows]
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO export_log (export_key, last_date, exported_at) "
            "VALUES (%s, %s, %s)",
            data,
        )
    return len(data)


def migrate_fetch_log(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute("SELECT series_id, last_checked FROM fetch_log").fetchall()
    data = [(sid, parse_date(d)) for sid, d in rows]
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO fetch_log (series_id, last_checked) VALUES (%s, %s)",
            data,
        )
    return len(data)


def migrate_expectations(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute(
        "SELECT series_id, period, expected, compare_type, source_text, fetched_at "
        "FROM expectations"
    ).fetchall()
    data = [(sid, p, e, ct, st, parse_ts(ft)) for sid, p, e, ct, st, ft in rows]
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO expectations "
            "(series_id, period, expected, compare_type, source_text, fetched_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            data,
        )
    return len(data)


def migrate_fed_expectations(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute(
        "SELECT meeting_date, outcome_bps, probability, captured_at "
        "FROM fed_expectations"
    ).fetchall()
    data = [(parse_date(md), ob, p, parse_ts(ca)) for md, ob, p, ca in rows]
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO fed_expectations "
            "(meeting_date, outcome_bps, probability, captured_at) "
            "VALUES (%s, %s, %s, %s)",
            data,
        )
    return len(data)


def migrate_release_calendar(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute(
        "SELECT release_date, report, series_ids, confirmed, updated_at "
        "FROM release_calendar"
    ).fetchall()
    data = [
        (parse_date(rd), rep, sids, bool(c), parse_ts(ua))
        for rd, rep, sids, c, ua in rows
    ]
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO release_calendar "
            "(release_date, report, series_ids, confirmed, updated_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            data,
        )
    return len(data)


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set in environment or .env", file=sys.stderr)
        return 1
    if not SQLITE_PATH.exists():
        print(f"SQLite DB not found at {SQLITE_PATH}", file=sys.stderr)
        return 1

    sq = sqlite3.connect(SQLITE_PATH)
    with psycopg.connect(url) as pg:
        # Note: groups / group_members are intentionally NOT migrated —
        # save_groups() in store.py rebuilds them from config.yaml on first run.
        n_obs = migrate_observations(sq, pg)
        print(f"observations: {n_obs:,}")
        n_calc = migrate_calculated(sq, pg)
        print(f"calculated:   {n_calc:,}")
        n_rev = migrate_revisions(sq, pg)
        print(f"revisions:    {n_rev:,}")
        n_exp = migrate_export_log(sq, pg)
        print(f"export_log:   {n_exp:,}")
        n_fet = migrate_fetch_log(sq, pg)
        print(f"fetch_log:        {n_fet:,}")
        n_exp2 = migrate_expectations(sq, pg)
        print(f"expectations:     {n_exp2:,}")
        n_fed = migrate_fed_expectations(sq, pg)
        print(f"fed_expectations: {n_fed:,}")
        n_rc = migrate_release_calendar(sq, pg)
        print(f"release_calendar: {n_rc:,}")
        pg.commit()
    sq.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
