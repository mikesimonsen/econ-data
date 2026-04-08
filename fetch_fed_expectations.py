#!/usr/bin/env python3
"""Fetch CME FedWatch probabilities for upcoming FOMC meetings.

Run weekly (or before/after FOMC meetings) to capture market-implied
rate change probabilities. Stored in the fed_expectations table.

Usage:
    python fetch_fed_expectations.py              # next 6 meetings
    python fetch_fed_expectations.py --limit 4    # next 4 meetings
    python fetch_fed_expectations.py --show       # show stored expectations
"""
import argparse
from datetime import datetime

from econ_data.fed_expectations import (
    fetch_all_upcoming, get_expectations, get_future_meetings,
)


def show():
    expectations = get_expectations()
    if not expectations:
        print("No Fed expectations stored.")
        return

    for meeting, data in sorted(expectations.items()):
        print(f"\n{meeting}  (captured {data['captured_at'][:10]})")
        for bps, prob in sorted(data["probabilities"].items()):
            sign = "+" if bps > 0 else ""
            label = "no change" if bps == 0 else f"{sign}{bps}bp"
            bar = "█" * int(prob * 40)
            print(f"  {label:>10}  {prob*100:5.1f}%  {bar}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=6,
                        help="Number of upcoming meetings to fetch (default: 6)")
    parser.add_argument("--show", action="store_true",
                        help="Show stored expectations")
    args = parser.parse_args()

    if args.show:
        show()
    else:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] Fetching FedWatch probabilities for next {args.limit} meetings")
        upcoming = get_future_meetings(limit=args.limit)
        print(f"Meetings: {[m.isoformat() for m in upcoming]}")
        n = fetch_all_upcoming(limit=args.limit)
        print(f"\nUpdated {n} meetings")
