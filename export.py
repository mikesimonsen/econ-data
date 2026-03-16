"""
Export economic data from the local SQLite database.

Run with no arguments for interactive mode, or use CLI flags:
  python export.py --format csv --group case-shiller --data yoy_pct
  python export.py --format excel --group jolts --data all
"""
import argparse
from pathlib import Path

from econ_data.config import load
from econ_data.export import (
    DATA_TYPES,
    available_groups,
    available_series,
    freshness,
    to_csv,
    to_excel,
)
from econ_data.store_sqlite import save_export_log


def do_export(fmt, series_ids, output_dir, label="econ_data", group_id=None,
              data_type="values"):
    """Run the export and record it in the export log."""
    from econ_data.store_sqlite import get_last_dates

    last_dates = get_last_dates()

    if fmt in ("csv", "both"):
        paths = to_csv(series_ids, output_dir, data_type=data_type)
        for p in paths:
            print(f"  Wrote {p}")

    if fmt in ("excel", "both"):
        path = to_excel(series_ids, output_dir / f"{label}.xlsx", data_type=data_type)
        print(f"  Wrote {path}")

    # Record what we exported
    for sid in series_ids:
        d = last_dates.get(sid)
        if d:
            key = f"{group_id}:{sid}" if group_id else sid
            save_export_log(key, d.isoformat())
    if group_id:
        latest = max((last_dates[s].isoformat() for s in series_ids if s in last_dates), default=None)
        if latest:
            save_export_log(group_id, latest)



def interactive_menu():
    """Show what's available and what's new, let the user choose what to export."""
    fresh = freshness()
    standalone = fresh["standalone"]
    groups = fresh["groups"]
    cfg = load()
    export_cfg = cfg.get("export", {})
    default_fmt = export_cfg.get("format", "excel")
    default_output = Path(export_cfg.get("output_dir", "exports"))

    if not standalone and not groups:
        print("No data in database. Run: python run.py")
        return

    while True:
        # ── Step 1: What to export ──────────────────────────────
        items = []
        print("\n=== Econ Data Export ===\n")

        if standalone:
            print("  Standalone Series:")
            for sid, name, latest, prev in standalone:
                tag = _status_tag(latest, prev)
                items.append({"type": "series", "id": sid, "name": name,
                              "series_ids": [sid], "label": sid, "latest": latest})
                idx = len(items)
                print(f"    {idx}) {sid:<12} {name:<35} thru {latest}  {tag}")

        if groups:
            print("\n  Groups:")
            for gid, gdata in groups.items():
                items.append({"type": "group", "id": gid, "name": gdata["name"],
                              "series_ids": [s[0] for s in gdata["series"]],
                              "label": gid, "latest": gdata["latest"]})
                idx = len(items)
                print(f"    {idx}) {gid:<12} {gdata['name']}")
                for sid, sname, slatest, sprev in gdata["series"]:
                    stag = _status_tag(slatest, sprev)
                    print(f"       {'':<12} {sname:<35} thru {slatest}  {stag}")

        any_new = any(_is_new(s[2], s[3]) for s in standalone)
        any_new = any_new or any(_is_new(g["latest"], g["prev"]) for g in groups.values())

        print()
        print("  a) Export ALL")
        if any_new:
            print("  n) Export only NEW data")
        print("  q) Quit")

        choice = input("\n  What to export: ").strip().lower()

        if choice == "q" or choice == "":
            return

        # Resolve selection to a list of export targets
        if choice == "a":
            targets = items
        elif choice == "n":
            targets = [i for i in items if _is_new(i["latest"], _get_prev(i, fresh))]
            if not targets:
                print("\n  Everything is already up to date.\n")
                continue
        elif choice.isdigit() and 0 < int(choice) <= len(items):
            targets = [items[int(choice) - 1]]
        else:
            print(f"  Invalid choice: {choice}")
            continue

        target_names = ", ".join(t["name"] for t in targets)

        # ── Step 2: Data type ───────────────────────────────────
        print(f"\n  Selected: {target_names}")
        print()
        print("  Data to export:")
        print("    v) Values only (raw index/rate)")
        print("    p) Period-over-period % change")
        print("    y) Year-over-year % change")
        print("    a) All (values + both % changes)")

        dt_choice = input("\n  Data type [v]: ").strip().lower()
        data_type = {"p": "period_pct", "y": "yoy_pct", "a": "all"}.get(dt_choice, "values")
        dt_label = DATA_TYPES[data_type]["label"]

        # ── Step 3: Format ──────────────────────────────────────
        print()
        print("  Format:")
        print(f"    1) excel")
        print(f"    2) csv")
        print(f"    3) both")

        fmt_choice = input(f"\n  Format [{default_fmt}]: ").strip().lower()
        fmt = {"1": "excel", "2": "csv", "3": "both",
               "excel": "excel", "csv": "csv", "both": "both"}.get(fmt_choice, default_fmt)

        # ── Export ──────────────────────────────────────────────
        print(f"\n  Exporting {target_names} — {dt_label} ({fmt})...\n")

        for item in targets:
            out = default_output / item["id"] if item["type"] == "group" else default_output
            do_export(fmt, item["series_ids"], out, item["label"],
                      group_id=item["id"] if item["type"] == "group" else None,
                      data_type=data_type)

        print("\n  Done.")

        # ── Another export? ─────────────────────────────────────
        again = input("\n  Export more? (y/n) [n]: ").strip().lower()
        if again != "y":
            return

        # Refresh freshness data for next loop
        fresh = freshness()
        standalone = fresh["standalone"]
        groups = fresh["groups"]


def _is_new(latest, prev):
    return latest is not None and (prev is None or latest > prev)


def _status_tag(latest, prev):
    if latest is None:
        return ""
    if prev is None:
        return "** NEW **"
    if latest > prev:
        return f"** NEW (was {prev}) **"
    return ""


def _get_prev(item, fresh):
    if item["type"] == "group":
        return fresh["groups"][item["id"]]["prev"]
    for sid, _, _, prev in fresh["standalone"]:
        if sid == item["id"]:
            return prev
    return None


def cli_mode():
    """Handle explicit CLI flags (non-interactive)."""
    parser = argparse.ArgumentParser(description="Export econ data to CSV or Excel.")
    parser.add_argument("--format", choices=["csv", "excel", "both"],
                        help="Export format (overrides config default)")
    parser.add_argument("--series", nargs="+", metavar="SERIES_ID",
                        help="Specific series to export")
    parser.add_argument("--group", metavar="GROUP_ID",
                        help="Export all series in a group")
    parser.add_argument("--data", choices=["values", "period_pct", "yoy_pct", "all"],
                        default="values",
                        help="Data type: values, period_pct, yoy_pct, or all (default: values)")
    parser.add_argument("--output", metavar="PATH",
                        help="Output directory")
    parser.add_argument("--list", action="store_true",
                        help="List available series and groups")
    args = parser.parse_args()

    # If no flags at all, go interactive
    if not any([args.format, args.series, args.group, args.list,
                args.data != "values"]):
        interactive_menu()
        return

    if args.list:
        series = available_series()
        groups = available_groups()
        if not series and not groups:
            print("No data in database. Run: python run.py")
            return
        grouped_ids = {sid for gdata in groups.values() for sid, _ in gdata["series"]}
        print("Standalone Series:")
        for sid, name in series:
            if sid not in grouped_ids:
                print(f"  {sid:<12} {name}")
        if groups:
            print("\nGroups:")
            for group_id, gdata in groups.items():
                print(f"  {group_id:<12} {gdata['name']}")
                for sid, sname in gdata["series"]:
                    print(f"    {sid:<12} {sname}")
        return

    cfg = load()
    export_cfg = cfg.get("export", {})
    fmt = args.format or export_cfg.get("format", "excel")
    default_output = Path(export_cfg.get("output_dir", "exports"))

    if args.group:
        groups = available_groups()
        if args.group not in groups:
            print(f"Unknown group: {args.group}")
            print(f"Available groups: {', '.join(groups)}")
            return
        series_ids = [sid for sid, _ in groups[args.group]["series"]]
        label = args.group
        output_dir = Path(args.output) if args.output else default_output / args.group
        do_export(fmt, series_ids, output_dir, label, group_id=args.group,
                  data_type=args.data)
    elif args.series:
        series_ids = args.series
        output_dir = Path(args.output) if args.output else default_output
        do_export(fmt, series_ids, output_dir, data_type=args.data)
    else:
        series_ids = [sid for sid, _ in available_series()]
        output_dir = Path(args.output) if args.output else default_output
        do_export(fmt, series_ids, output_dir, data_type=args.data)


if __name__ == "__main__":
    cli_mode()
