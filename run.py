from datetime import datetime
from pathlib import Path

from econ_data.calculations import compute_all
from econ_data.config import load, all_series, fred_series
from econ_data.fetch import fetch_all
from econ_data.fetch_mnd import fetch_mnd
from econ_data.store_sqlite import get_last_dates, save, save_groups
from econ_data.daily_analysis import generate_daily_analysis
from econ_data.export_sheets import export_all_groups, export_all_groups_calcs
from econ_data.summary import generate_summary, format_summary, format_signals_by_recency


def log(msg=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


if __name__ == "__main__":
    cfg = load()
    series = all_series(cfg)

    log(f"=== Econ Data Fetch ===\n")

    last_dates = get_last_dates()

    # Fetch FRED series
    fred = fred_series(cfg)
    result = fetch_all(fred, last_dates=last_dates)

    # Fetch Mortgage News Daily
    mnd_result = fetch_mnd(last_dates=last_dates)
    result["new"].extend(mnd_result["new"])
    result["counts"].update(mnd_result["counts"])

    counts = result["counts"]

    updated, current, errors = [], [], []
    for series_id, name in series:
        n = counts.get(series_id, 0)
        if n > 0:
            updated.append((series_id, name, n))
        elif n == 0:
            current.append(series_id)
        else:
            errors.append(series_id)

    if updated:
        log("Updated:")
        for series_id, name, n in updated:
            log(f"  {series_id:<12} {name}  (+{n} observation{'s' if n != 1 else ''})")

    if current:
        log(f"Up to date ({len(current)}): {', '.join(current)}")

    if errors:
        log(f"Errors ({len(errors)}): {', '.join(errors)}")

    new_obs = result["new"]
    if new_obs:
        saved = save(new_obs)
        log(f"Saved {saved} new rows to SQLite.")
    else:
        log("No new data — nothing written.")

    save_groups(cfg)

    calc_rows = compute_all()
    log(f"Computed {calc_rows} calculated values (period %, YoY %).")

    # Generate and save summary when new data arrived
    if new_obs:
        log("Generating data summary...")
        summary = generate_summary(cfg)
        report = format_summary(summary)
        updated_ids = {sid for sid, _, _ in updated}

        today = datetime.now().strftime("%Y-%m-%d")
        summary_dir = Path("summaries")
        summary_dir.mkdir(exist_ok=True)

        # Full summary
        summary_path = summary_dir / f"summary {today}.txt"
        summary_header = f"  === Latest Data Summary — {today} ===\n"
        summary_path.write_text(summary_header + report + "\n")
        log(f"Summary saved to {summary_path}")

        # Signals split by recency
        signals_report = format_signals_by_recency(summary, updated_ids)
        signals_path = summary_dir / f"signals {today}.txt"
        signals_header = f"  === Signals — {today} ===\n\n"
        signals_path.write_text(signals_header + signals_report + "\n")
        log(f"Signals saved to {signals_path}")

        # Generate LLM daily analysis
        log("Generating daily analysis...")
        try:
            analysis = generate_daily_analysis(
                signals_text=signals_header + signals_report,
                summary_text=summary_header + report,
            )
            analysis_path = summary_dir / f"daily analysis {today}.txt"
            analysis_header = f"  === Daily Analysis — {today} ===\n\n"
            analysis_content = (
                analysis_header + analysis
                + "\n\n" + "─" * 72 + "\n\n"
                + signals_header + signals_report
                + "\n\n" + summary_header + report + "\n"
            )
            analysis_path.write_text(analysis_content)
            log(f"Daily analysis saved to {analysis_path}")
        except Exception as e:
            log(f"Daily analysis failed: {e}")

    # Export CSVs for Google Sheets
    if new_obs:
        log("Exporting Sheets data...")
        paths = export_all_groups(cfg)
        calc_paths = export_all_groups_calcs(cfg)
        log(f"Exported {len(paths)} value CSVs + {len(calc_paths)} calc CSVs")

        # Auto-commit and push to GitHub
        import subprocess
        try:
            subprocess.run(
                ["git", "add", "sheets_data/", "sheets_data_calcs/"],
                cwd=Path(__file__).parent, check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "commit", "-m", f"data: update sheets export {today}"],
                cwd=Path(__file__).parent, check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "push"],
                cwd=Path(__file__).parent, check=True, capture_output=True,
            )
            log("Pushed sheets_data/ to GitHub.")
        except subprocess.CalledProcessError as e:
            log(f"Git push failed: {e.stderr.decode().strip() if e.stderr else e}")
        except FileNotFoundError:
            log("Git not available — skipping push.")

    log("Done.")
