import subprocess
from datetime import datetime
from pathlib import Path

from econ_data.calc_rolling import compute_rolling
from econ_data.calc_spread import compute_spread
from econ_data.calculations import compute_all
from econ_data.config import load, all_series, fred_series
from econ_data.fetch import fetch_all
from econ_data.fetch_bls import fetch_bls, build_series_map
from econ_data.fetch_confboard import fetch_confboard
from econ_data.fetch_mnd import fetch_mnd
from econ_data.fetch_nar import fetch_nar
from econ_data.fetch_realtor import fetch_realtor
from econ_data.fetch_redfin import fetch_redfin
from econ_data.fetch_web import fetch_web
from econ_data.fetch_altos import fetch_altos
from econ_data.fetch_xactus import fetch_xactus
from econ_data.store import (
    get_last_dates, get_fetch_log, save, save_fetch_log, save_groups,
    detect_and_save_revisions, get_recent_revisions, get_series_captured_today,
)
from econ_data.briefing import generate_briefing
from econ_data.daily_analysis import generate_daily_analysis
from econ_data.housing_analysis import generate_housing_analysis
from econ_data.export_sheets import export_all_groups, export_all_groups_calcs, write_manifest
from econ_data.summary import generate_summary, format_summary, format_signals_by_recency


def log(msg=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def format_revisions(revisions: list) -> str:
    """Format revisions into a readable block for signals/analysis."""
    if not revisions:
        return ""
    lines = ["  ┌─────────────────────────────┐",
             "  │     DATA REVISIONS           │",
             "  └─────────────────────────────┘", ""]
    for r in revisions:
        direction = "↑" if r["new_value"] > r["old_value"] else "↓"
        lines.append(
            f"    {r['series_id']:<20} {r['date']}  "
            f"{r['old_value']:>10,.1f} → {r['new_value']:>10,.1f}  "
            f"{direction} {r['pct_change']:+.2f}%"
        )
        if r.get("name"):
            lines.append(f"    {'':20} {r['name']}")
    return "\n".join(lines)


if __name__ == "__main__":
    cfg = load()
    series = all_series(cfg)
    today = datetime.now().strftime("%Y-%m-%d")

    log(f"=== Econ Data Fetch ===\n")

    last_dates = get_last_dates()
    last_checked = get_fetch_log()

    # Fetch NAR Existing Home Sales first (primary source, faster than FRED)
    log("Fetching NAR existing home sales...")
    nar_result = fetch_nar(last_dates=last_dates)
    if nar_result["new"]:
        save(nar_result["new"])
        last_dates = get_last_dates()  # refresh so FRED skips these series
        log(f"NAR scraper: {len(nar_result['new'])} observations")

    # Fetch FRED series (smart scheduling: only hits API when due)
    # NAR series already captured above will be skipped via cooldown
    fred = fred_series(cfg)
    log(f"Fetching FRED ({len(fred)} series, only those due)...")
    result = fetch_all(fred, last_dates=last_dates, last_checked=last_checked)

    # Merge NAR counts into result (NAR-sourced series show in summary)
    for sid, n in nar_result["counts"].items():
        if n > 0:
            result["counts"][sid] = n
            result["new"].extend(
                o for o in nar_result["new"] if o.series_id == sid
            )

    # Record which series were checked
    if result.get("checked"):
        save_fetch_log(result["checked"])

    # Detect revisions BEFORE saving new data (compare fetched vs stored)
    all_fetched = result.get("all_fetched", [])
    revisions = detect_and_save_revisions(all_fetched)
    if revisions:
        log(f"Revisions detected ({len(revisions)}):")
        for r in revisions:
            log(f"  {r['series_id']:<20} {r['date']}  {r['old_value']:.1f} → {r['new_value']:.1f}  ({r['pct_change']:+.2f}%)")

    # Save all fetched data (new + revised values)
    if all_fetched:
        save(all_fetched)

    # Fetch Mortgage News Daily
    log("Fetching MND mortgage rates...")
    mnd_result = fetch_mnd(last_dates=last_dates)
    result["new"].extend(mnd_result["new"])
    result["counts"].update(mnd_result["counts"])

    if mnd_result["new"]:
        save(mnd_result["new"])

    # Fetch BLS series (metro CPI not available in FRED)
    bls_map = build_series_map(cfg)
    if bls_map:
        log(f"Fetching BLS metro CPI ({len(bls_map)} series)...")
        bls_result = fetch_bls(bls_map, last_dates=last_dates)
        result["new"].extend(bls_result["new"])
        result["counts"].update(bls_result["counts"])
        if bls_result["new"]:
            save(bls_result["new"])

    # Fetch Altos Research weekly inventory (drop-folder CSV)
    log("Fetching Altos weekly inventory...")
    altos_result = fetch_altos(last_dates=last_dates)
    result["new"].extend(altos_result["new"])
    result["counts"].update(altos_result["counts"])
    if altos_result["new"]:
        save(altos_result["new"])

    # Fetch Xactus Mortgage Intent Index (drop-folder spreadsheets)
    log("Fetching Xactus MII...")
    xactus_result = fetch_xactus(last_dates=last_dates)
    result["new"].extend(xactus_result["new"])
    result["counts"].update(xactus_result["counts"])
    if xactus_result["new"]:
        save(xactus_result["new"])

    # Fetch web-scraped series (MBA Purchase Index, etc.)
    log("Fetching web-scraped series (MBA, etc.)...")
    web_result = fetch_web(last_dates=last_dates)
    result["new"].extend(web_result["new"])
    result["counts"].update(web_result["counts"])
    if web_result["new"]:
        save(web_result["new"])

    # Fetch Conference Board Consumer Confidence (scraped from MacroMicro.me)
    log("Fetching Conference Board confidence...")
    cb_result = fetch_confboard(last_dates=last_dates)
    result["new"].extend(cb_result["new"])
    result["counts"].update(cb_result["counts"])
    if cb_result["new"]:
        save(cb_result["new"])

    # Fetch Realtor.com bonus metrics (S3 CSV)
    log("Fetching Realtor.com bonus metrics...")
    realtor_result = fetch_realtor(last_dates=last_dates)
    result["new"].extend(realtor_result["new"])
    result["counts"].update(realtor_result["counts"])
    if realtor_result["new"]:
        save(realtor_result["new"])

    # Fetch Redfin housing market data (S3 TSV)
    log("Fetching Redfin housing data...")
    redfin_result = fetch_redfin(last_dates=last_dates)
    result["new"].extend(redfin_result["new"])
    result["counts"].update(redfin_result["counts"])
    if redfin_result["new"]:
        save(redfin_result["new"])

    # Compute mortgage rate spread (derived from MND + DGS10)
    log("Computing mortgage rate spread...")
    spread_result = compute_spread(last_dates=get_last_dates())
    result["new"].extend(spread_result["new"])
    result["counts"].update(spread_result["counts"])
    if spread_result["new"]:
        save(spread_result["new"])

    # Compute rolling averages (MBA, Xactus)
    log("Computing rolling averages...")
    rolling_result = compute_rolling(last_dates=get_last_dates())
    result["new"].extend(rolling_result["new"])
    result["counts"].update(rolling_result["counts"])
    if rolling_result["new"]:
        save(rolling_result["new"])

    counts = result["counts"]

    updated, current, errors = [], [], []
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
    if not new_obs and not revisions:
        log("No new data or revisions.")

    save_groups(cfg)

    log("Computing period % and YoY % for all series...")
    calc_rows = compute_all()
    log(f"Computed {calc_rows} calculated values (period %, YoY %).")

    # ── Always generate summary, signals, and daily analysis ──────
    log("Generating data summary...")
    summary = generate_summary(cfg)
    report = format_summary(summary)
    updated_ids = get_series_captured_today()

    summary_dir = Path("summaries")
    summary_dir.mkdir(exist_ok=True)

    # Full summary
    summary_path = summary_dir / f"summary {today}.txt"
    summary_header = f"  === Latest Data Summary — {today} ===\n"
    summary_path.write_text(summary_header + report + "\n")
    log(f"Summary saved to {summary_path}")

    # Signals split by recency + revisions
    signals_report = format_signals_by_recency(summary, updated_ids)
    recent_revisions = get_recent_revisions(days=7)
    revisions_block = format_revisions(recent_revisions)
    if revisions_block:
        signals_report = revisions_block + "\n\n" + signals_report

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

    # Housing impact analysis
    log("Generating housing analysis...")
    try:
        housing_md = generate_housing_analysis(cfg)
        housing_path = summary_dir / f"housing analysis {today}.md"
        housing_path.write_text(f"# Housing Analysis — {today}\n\n{housing_md}\n")
        log(f"Housing analysis saved to {housing_path}")
    except Exception as e:
        log(f"Housing analysis failed: {e}")

    # Editorial briefing (HTML)
    log("Generating editorial briefing...")
    try:
        briefing_html = generate_briefing(cfg, updated_ids=updated_ids)
        briefing_path = summary_dir / f"briefing {today}.html"
        briefing_path.write_text(briefing_html)
        # Also publish to docs/ for GitHub Pages
        docs_dir = Path("docs")
        docs_dir.mkdir(exist_ok=True)
        (docs_dir / "index.html").write_text(briefing_html)
        log(f"Briefing saved to {briefing_path} + docs/index.html")
    except Exception as e:
        log(f"Briefing generation failed: {e}")

    # ── Export and push to GitHub when new data arrived ────────────
    has_changes = new_obs or revisions
    if has_changes:
        log("Exporting Sheets data...")
        # Include groups with revisions in the export
        revision_ids = {r["series_id"] for r in revisions}
        export_ids = updated_ids | revision_ids
        paths = export_all_groups(cfg, updated_ids=export_ids)
        calc_paths = export_all_groups_calcs(cfg, updated_ids=export_ids)
        changed_groups = [gid for gid, gdata in cfg.get("groups", {}).items()
                          if {s["id"] for s in gdata["series"]} & export_ids]
        write_manifest(changed_groups)
        log(f"Exported {len(paths)} value CSVs + {len(calc_paths)} calc CSVs ({len(changed_groups)} groups changed)")

    # Always push docs/ (briefing) + sheets if changed
    repo_dir = Path(__file__).parent
    try:
        subprocess.run(
            ["git", "add", "sheets_data/", "sheets_data_calcs/", "docs/"],
            cwd=repo_dir, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "commit", "-m", f"data: update sheets export {today}"],
            cwd=repo_dir, check=True, capture_output=True,
        )

        # Push with rebase-on-conflict retry. If someone (e.g. a developer
        # commit) pushes to main during the ~10 min cron window, the first
        # push gets rejected; rebase onto the new main and retry.
        for attempt in range(1, 4):
            push = subprocess.run(
                ["git", "push"], cwd=repo_dir, capture_output=True,
            )
            if push.returncode == 0:
                log("Pushed to GitHub.")
                break
            stderr = push.stderr.decode() if push.stderr else ""
            if "rejected" in stderr or "fetch first" in stderr:
                log(f"Push rejected (attempt {attempt}/3); rebasing onto origin/main...")
                rebase = subprocess.run(
                    ["git", "pull", "--rebase", "origin", "main"],
                    cwd=repo_dir, capture_output=True,
                )
                if rebase.returncode != 0:
                    log(f"Rebase failed: {rebase.stderr.decode().strip()}")
                    break
            else:
                log(f"Push failed with non-rejection error: {stderr.strip()}")
                break
        else:
            log("Push still failing after 3 rebase attempts; giving up.")
    except subprocess.CalledProcessError as e:
        # No changes to commit is fine (exit code 1 from git commit)
        if "nothing to commit" not in (e.stdout or b"").decode():
            log(f"Git commit failed: {e.stderr.decode().strip() if e.stderr else e}")
    except FileNotFoundError:
        log("Git not available — skipping push.")

    log("Done.")
