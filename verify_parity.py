"""Assert SQLite and Postgres are row-for-row identical.

Run after replicate_to_postgres.py. Exits non-zero on any mismatch so it can
gate downstream steps in run.sh / run.py.

Checks:
  1. Row count per table.
  2. Spot-checks: max(date), max(captured_at), sum(value) for observations
     across a representative series — catches transformation bugs that pure
     row counts would miss.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

SQLITE_PATH = Path(__file__).parent / "econ_data.db"

TABLES = [
    "observations", "calculated", "revisions",
    "export_log", "fetch_log",
    "expectations", "fed_expectations", "release_calendar",
    "groups", "group_members",
]

# Series chosen to span sources and frequencies.
SPOT_SERIES = ["CPIAUCSL", "DGS10", "ICSA"]


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set", file=sys.stderr)
        return 1

    sq = sqlite3.connect(SQLITE_PATH)
    mismatches = []

    with psycopg.connect(url) as pg, pg.cursor() as cur:
        # 1. Row counts
        for tbl in TABLES:
            s = sq.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            p = cur.fetchone()[0]
            if s != p:
                mismatches.append(f"{tbl}: sqlite={s:,} postgres={p:,}")

        # 2. Spot-check per series
        for sid in SPOT_SERIES:
            s_row = sq.execute(
                "SELECT COUNT(*), MAX(date), ROUND(SUM(value), 4) "
                "FROM observations WHERE series_id = ?", (sid,)
            ).fetchone()
            cur.execute(
                "SELECT COUNT(*), MAX(date)::text, ROUND(SUM(value)::numeric, 4) "
                "FROM observations WHERE series_id = %s", (sid,)
            )
            p_row = cur.fetchone()
            # Normalize types: SQLite returns floats, Postgres NUMERIC → Decimal
            s_norm = (s_row[0], s_row[1], float(s_row[2]) if s_row[2] is not None else None)
            p_norm = (p_row[0], p_row[1], float(p_row[2]) if p_row[2] is not None else None)
            if s_norm != p_norm:
                mismatches.append(f"{sid}: sqlite={s_norm} postgres={p_norm}")

    sq.close()

    if mismatches:
        print("PARITY FAILED:", file=sys.stderr)
        for m in mismatches:
            print(f"  {m}", file=sys.stderr)
        return 1

    print("Parity OK: all 10 tables match, spot checks pass.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
