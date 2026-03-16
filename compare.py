"""
Compare multiple data series side by side.

Examples:
  python compare.py CUURA101SA0 CUURA316SA0 --data yoy_pct
  python compare.py DAXRSA CUURA316SA0 --data yoy_pct --name "Dallas Housing vs Inflation"
  python compare.py -i                                  # interactive mode
"""
import argparse
from pathlib import Path

from econ_data.compare import search_series, to_csv, to_excel


OUTPUT_DIR = Path("comparisons")


def interactive():
    """Interactive mode: search for series, select, and export."""
    selected = []

    print("\n  === Compare Series ===")
    print("  Search for series by keyword, then select which to compare.")
    print("  Type 'done' when you've selected all series.\n")

    while True:
        query = input("  Search (or 'done'): ").strip()
        if query.lower() == "done":
            break
        if not query:
            continue

        results = search_series(query)
        if not results:
            print(f"    No matches for '{query}'")
            continue

        for i, (sid, name) in enumerate(results, 1):
            already = " [selected]" if sid in [s[0] for s in selected] else ""
            print(f"    {i}) {sid:<16} {name}{already}")

        picks = input("  Select (numbers, comma-separated, or 'all'): ").strip().lower()
        if picks == "all":
            for sid, name in results:
                if sid not in [s[0] for s in selected]:
                    selected.append((sid, name))
                    print(f"    + {sid} ({name})")
        elif picks:
            for p in picks.replace(",", " ").split():
                if p.isdigit():
                    idx = int(p) - 1
                    if 0 <= idx < len(results):
                        sid, name = results[idx]
                        if sid not in [s[0] for s in selected]:
                            selected.append((sid, name))
                            print(f"    + {sid} ({name})")

        print(f"\n  Selected so far ({len(selected)}):")
        for sid, name in selected:
            print(f"    {sid:<16} {name}")
        print()

    if len(selected) < 2:
        print("  Need at least 2 series to compare.")
        return

    # Data type
    print("  Data type:")
    print("    v) Values (raw)")
    print("    p) Period-over-period % change")
    print("    y) Year-over-year % change")
    dt_choice = input("\n  Data type [v]: ").strip().lower()
    data_type = {"p": "period_pct", "y": "yoy_pct"}.get(dt_choice, "values")

    # Format
    print("\n  Format:")
    print("    1) excel")
    print("    2) csv")
    fmt_choice = input("\n  Format [excel]: ").strip().lower()
    fmt = {"2": "csv", "csv": "csv"}.get(fmt_choice, "excel")

    # Name
    name = input("\n  Name for this comparison (or Enter for default): ").strip()
    if not name:
        name = " vs ".join(s[1] for s in selected[:3])
        if len(selected) > 3:
            name += f" +{len(selected) - 3} more"

    series_ids = [s[0] for s in selected]

    print(f"\n  Generating comparison...\n")
    if fmt == "csv":
        path = to_csv(series_ids, data_type, OUTPUT_DIR / "placeholder.csv", name=name)
    else:
        path = to_excel(series_ids, data_type, OUTPUT_DIR / "placeholder.xlsx", name=name)

    if path:
        print(f"  Wrote {path}")
    else:
        print("  No common dates found between these series.")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="Compare multiple data series side by side.",
        epilog="Examples:\n"
               "  python compare.py CUURA101SA0 CUURA316SA0 --data yoy_pct\n"
               "  python compare.py -i",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("series", nargs="*", help="FRED series IDs to compare")
    parser.add_argument("-i", "--interactive", action="store_true",
                        help="Interactive mode with keyword search")
    parser.add_argument("--data", choices=["values", "period_pct", "yoy_pct"],
                        default="values", help="Data type (default: values)")
    parser.add_argument("--name", help="Name for the comparison output file")
    parser.add_argument("--format", choices=["excel", "csv"], default="excel",
                        help="Output format (default: excel)")

    args = parser.parse_args()

    if args.interactive or not args.series:
        interactive()
        return

    if len(args.series) < 2:
        print("Need at least 2 series IDs to compare.")
        return

    name = args.name or " vs ".join(args.series[:3])

    if args.format == "csv":
        path = to_csv(args.series, args.data, OUTPUT_DIR / "placeholder.csv", name=name)
    else:
        path = to_excel(args.series, args.data, OUTPUT_DIR / "placeholder.xlsx", name=name)

    if path:
        print(f"Wrote {path}")
    else:
        print("No common dates found between these series.")


if __name__ == "__main__":
    main()
