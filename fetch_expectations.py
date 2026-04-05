#!/usr/bin/env python3
"""Fetch consensus expectations for upcoming economic releases.

Run this 1-3 days before expected releases. Can be scheduled via launchd
separately from the main pipeline (e.g., Monday and Wednesday evenings).

Usage:
    python fetch_expectations.py              # fetch for releases in next 3 days
    python fetch_expectations.py --days 5     # look ahead 5 days
    python fetch_expectations.py --all        # fetch for all tracked series regardless of schedule
    python fetch_expectations.py --show       # show stored expectations
"""
import argparse
from datetime import datetime

from econ_data.expectations import (
    TRACKED, fetch_expectations, get_expectations,
    _reference_period, _search_consensus, _already_fetched, _connect,
)


def log(msg=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def fetch_all():
    """Fetch consensus for all tracked series regardless of schedule.

    Tries both the current period (retroactive capture for recently released
    data) and the upcoming period (pre-release forecast).
    """
    import time
    from datetime import datetime as dt

    results = []
    con = _connect()
    now = dt.now().isoformat(timespec="seconds")

    for spec in TRACKED:
        # Try current period first (retroactive), then upcoming
        for upcoming in (False, True):
            period = _reference_period(spec["series_id"], upcoming=upcoming)

            if _already_fetched(spec["series_id"], period):
                log(f"  {spec['name']} ({period}) — already fetched, skipping")
                continue

            log(f"  Searching consensus for {spec['name']} ({period})...")
            result = _search_consensus(spec, period)

            if result and result["expected"] is not None:
                con.execute(
                    "INSERT OR REPLACE INTO expectations "
                    "(series_id, period, expected, compare_type, source_text, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (spec["series_id"], period, result["expected"],
                     spec["compare"], result["source_text"], now),
                )
                results.append(spec["name"])
                log(f"    → {result['expected']} ({result['source_text'][:80]})")
            else:
                log(f"    → No consensus found")

            time.sleep(1)

    con.commit()
    con.close()
    return results


def show_expectations():
    """Display all stored expectations."""
    expectations = get_expectations()
    if not expectations:
        print("No expectations stored.")
        return

    print(f"{'Series':<30} {'Period':<12} {'Expected':>12} {'Type':<10} Source")
    print("─" * 90)
    for sid, exp in sorted(expectations.items()):
        val = exp["expected"]
        if val >= 1000:
            val_str = f"{val:,.0f}"
        else:
            val_str = f"{val:.2f}"
        source = (exp.get("source_text") or "")[:30]
        print(f"{sid:<30} {exp['period']:<12} {val_str:>12} {exp['compare_type']:<10} {source}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch consensus expectations")
    parser.add_argument("--days", type=int, default=3,
                        help="Look ahead N days for upcoming releases (default: 3)")
    parser.add_argument("--all", action="store_true",
                        help="Fetch for all tracked series regardless of schedule")
    parser.add_argument("--show", action="store_true",
                        help="Show stored expectations")
    args = parser.parse_args()

    if args.show:
        show_expectations()
    elif args.all:
        log("=== Fetching consensus for ALL tracked series ===")
        results = fetch_all()
        log(f"Fetched {len(results)} expectations")
    else:
        log(f"=== Fetching consensus (lookahead: {args.days} days) ===")
        results = fetch_expectations(lookahead_days=args.days)
        log(f"Fetched {len(results)} expectations")
