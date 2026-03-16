"""
Latest data summary — at-a-glance view of trends, reversals, and unusual moves.

  python summary.py                    # all series
  python summary.py --group jolts      # one group
  python summary.py --signals          # only series with signals (reversals, unusual moves)
"""
import argparse
from datetime import date

from econ_data.config import load
from econ_data.summary import generate_summary, format_summary


def main():
    parser = argparse.ArgumentParser(description="Latest data summary.")
    parser.add_argument("--group", metavar="GROUP_ID",
                        help="Show only one group")
    parser.add_argument("--signals", action="store_true",
                        help="Only show series with signals (reversals, unusual moves)")
    args = parser.parse_args()

    cfg = load()

    # If filtering to one group, narrow the config
    if args.group:
        groups = cfg.get("groups", {})
        if args.group not in groups:
            print(f"Unknown group: {args.group}")
            print(f"Available: {', '.join(groups)}")
            return
        cfg = {"series": [], "groups": {args.group: groups[args.group]}}

    print(f"\n  === Latest Data Summary — {date.today()} ===")

    summary = generate_summary(cfg)

    if args.signals:
        # Filter to only series with signals
        summary["standalone"] = [a for a in summary["standalone"] if a["signals"]]
        for gid in list(summary["groups"]):
            gdata = summary["groups"][gid]
            gdata["series"] = [a for a in gdata["series"] if a["signals"]]
            if not gdata["series"]:
                del summary["groups"][gid]

        if not summary["standalone"] and not summary["groups"]:
            print("\n  No signals — all series are following their recent trends.\n")
            return

    print(format_summary(summary))
    print()


if __name__ == "__main__":
    main()
