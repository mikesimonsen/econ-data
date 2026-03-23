import subprocess
from datetime import datetime
from pathlib import Path

from econ_data.calculations import compute_all
from econ_data.config import load, all_series, fred_series
from econ_data.fetch import fetch_all
from econ_data.fetch_mnd import fetch_mnd
from econ_data.store_sqlite import get_last_dates, get_fetch_log, save, save_fetch_log, save_groups
from econ_data.daily_analysis import generate_daily_analysis
from econ_data.export_sheets import export_all_groups, export_all_groups_calcs, write_manifest
from econ_data.summary import generate_summary, format_summary, format_signals_by_recency


def log(msg=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


if __name__ == "__main__":
    cfg = load()
    series = all_series(cfg)
    today = datetime.now().strftime("%Y-%m-%d")

    log(f"=== Econ Data Fetch ===\n")

    last_dates = get_last_dates()
    last_checked = get_fetch_log()

    # Fetch FRED series (smart scheduling: only hits API when due)
    fred = fred_series(cfg)
    result = fetch_all(fred, last_dates=last_dates, last_checked=last_checked)

    # Record which series were checked
    if result.get("checked"):
        save_fetch_log(result["checked"])

    # Fetch Mortgage News Daily
    mnd_result = fetch_mnd(last_dates=last_dates)
    result["new"].extend(mnd_result["new"])
    result["counts"].update(mnd_result["counts"])

    counts = result["counts"]

    updated, current, skipped, errors = [], [], [], []
    for series_id, name in series:
        n = counts.get(series_id, 0)
        if n > 0:
            updated.append((series_id, name, n))
        elif n == 0:
            current.append(series_id)
        elif n == -1:
            errors.append(series_id)

    if updated:
        log("Updated:")
        for series_id, name, n in updated:
            log(f"  {series_id:<20} {name}  (+{n} observation{'s' if n != 1 else ''})")

    if current:
        log(f"Up to date / not due ({len(current)})")

    if errors:
        log(f"Errors ({len(errors)}): {', '.join(errors)}")

    new_obs = result["new"]
    if new_obs:
        saved = save(new_obs)
        log(f"Saved {saved} new rows to SQLite.")
    else:
        log("No new data.")

    save_groups(cfg)

    calc_rows = compute_all()
    log(f"Computed {calc_rows} calculated values (period %, YoY %).")

    # ── Always generate summary, signals, and daily analysis ──────
    log("Generating data summary...")
    summary = generate_summary(cfg)
    report = format_summary(summary)
    updated_ids = {sid for sid, _, _ in updated}

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

    # LLM daily analysis
    log("Generating daily analysis...")
    try:
        analysis = generate_daily_analysis(
            signals_text=signals_header + signals_report,
            summary_text=summary_header + report,
        )
        analysis_path = summary_dir / f"daily analysis {today}.md"
        analysis_content = (
            f"# Daily Analysis — {today}\n\n"
            + analysis
            + "\n\n---\n\n"
            + f"## Signals\n\n```\n{signals_report}\n```\n\n"
            + f"## Full Summary\n\n```\n{report}\n```\n"
        )
        analysis_path.write_text(analysis_content)
        log(f"Daily analysis saved to {analysis_path}")
    except Exception as e:
        log(f"Daily analysis failed: {e}")

    # ── Export and push to GitHub when new data arrived ────────────
    if new_obs:
        log("Exporting Sheets data...")
        paths = export_all_groups(cfg, updated_ids=updated_ids)
        calc_paths = export_all_groups_calcs(cfg, updated_ids=updated_ids)
        updated_groups = [gid for gid, gdata in cfg.get("groups", {}).items()
                          if {s["id"] for s in gdata["series"]} & updated_ids]
        write_manifest(updated_groups)
        log(f"Exported {len(paths)} value CSVs + {len(calc_paths)} calc CSVs ({len(updated_groups)} groups changed)")

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
