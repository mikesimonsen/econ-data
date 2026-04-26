"""Truncate-and-reload Postgres from local SQLite.

Runs after each daily cron during the SQLite → Postgres migration window.
After the cutover (Step 4), this script is deleted.

The strategy is the dumbest possible thing that guarantees parity: wipe every
table in Postgres, then refill from SQLite. ~470k rows reload in seconds.

Tables:
  observations, calculated, revisions, export_log, fetch_log,
  expectations, fed_expectations, release_calendar,
  groups, group_members
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from migrate_to_postgres import (
    migrate_observations, migrate_calculated, migrate_revisions,
    migrate_export_log, migrate_fetch_log,
    migrate_expectations, migrate_fed_expectations, migrate_release_calendar,
)

SQLITE_PATH = Path(__file__).parent / "econ_data.db"

# Order matters for TRUNCATE only because group_members references groups,
# but TRUNCATE ... CASCADE handles it. Listing all tables in one statement is
# atomic.
ALL_TABLES = [
    "observations", "calculated", "revisions",
    "export_log", "fetch_log",
    "expectations", "fed_expectations", "release_calendar",
    "groups", "group_members",
]


def migrate_groups(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute("SELECT group_id, name FROM groups").fetchall()
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO groups (group_id, name) VALUES (%s, %s)", rows
        )
    return len(rows)


def migrate_group_members(sq: sqlite3.Connection, pg: psycopg.Connection) -> int:
    rows = sq.execute("SELECT group_id, series_id FROM group_members").fetchall()
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO group_members (group_id, series_id) VALUES (%s, %s)",
            rows,
        )
    return len(rows)


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set", file=sys.stderr)
        return 1
    if not SQLITE_PATH.exists():
        print(f"SQLite DB not found at {SQLITE_PATH}", file=sys.stderr)
        return 1

    sq = sqlite3.connect(SQLITE_PATH)
    with psycopg.connect(url) as pg:
        # Atomic wipe of all tables. Restart identity is irrelevant (no
        # serial columns), but CASCADE handles the group_members → groups FK.
        with pg.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {', '.join(ALL_TABLES)} CASCADE")

        n = {}
        n["observations"]     = migrate_observations(sq, pg)
        n["calculated"]       = migrate_calculated(sq, pg)
        n["revisions"]        = migrate_revisions(sq, pg)
        n["export_log"]       = migrate_export_log(sq, pg)
        n["fetch_log"]        = migrate_fetch_log(sq, pg)
        n["expectations"]     = migrate_expectations(sq, pg)
        n["fed_expectations"] = migrate_fed_expectations(sq, pg)
        n["release_calendar"] = migrate_release_calendar(sq, pg)
        n["groups"]           = migrate_groups(sq, pg)
        n["group_members"]    = migrate_group_members(sq, pg)
        # Commit on `with` exit; everything is one transaction.

    sq.close()
    total = sum(n.values())
    print(f"Replicated {total:,} rows: " +
          ", ".join(f"{k}={v:,}" for k, v in n.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
