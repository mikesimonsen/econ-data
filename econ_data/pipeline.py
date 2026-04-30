"""Pipeline orchestration shared by run.py (morning) and run_intraday.py.

Three building blocks:
  fetch_morning  — full 7 AM ET cohort: NAR, FRED, MND, BLS, Altos, Xactus,
                   web (MBA), Conference Board, Realtor, Redfin + spread/rolling.
  fetch_intraday — 4 PM ET cohort: MND + retry of FRED series in fetch_errors,
                   then recompute spread/rolling.
  post_process   — calculations, summaries, analyses, briefing, sheets export,
                   git commit/push. Called after either fetch.
"""
from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from econ_data.briefing import generate_briefing
from econ_data.calc_rolling import compute_rolling
from econ_data.calc_spread import compute_spread
from econ_data.calculations import compute_all
from econ_data.config import fred_series
from econ_data.daily_analysis import generate_daily_analysis
from econ_data.export_sheets import (
    export_all_groups, export_all_groups_calcs, write_manifest,
)
from econ_data.fetch import fetch_all
from econ_data.fetch_altos import fetch_altos
from econ_data.fetch_bls import build_series_map, fetch_bls
from econ_data.fetch_confboard import fetch_confboard
from econ_data.fetch_mnd import fetch_mnd
from econ_data.fetch_nar import fetch_nar
from econ_data.fetch_realtor import fetch_realtor
from econ_data.fetch_redfin import fetch_redfin
from econ_data.fetch_web import fetch_web
from econ_data.fetch_xactus import fetch_xactus
from econ_data.housing_analysis import generate_housing_analysis
from econ_data.store import (
    detect_and_save_revisions, get_failed_series, get_last_dates,
    get_recent_revisions, get_series_captured_today, save, save_fetch_log,
    save_groups,
)
from econ_data.summary import (
    format_signals_by_recency, format_summary, generate_summary,
)


def log(msg: str = "") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _format_revisions(revisions: list) -> str:
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


def _empty_result() -> dict:
    return {"new": [], "counts": {}, "checked": [], "all_fetched": []}


def _merge(into: dict, other: dict) -> None:
    """Merge a fetcher's return dict into the running pipeline result."""
    into["new"].extend(other.get("new", []))
    into["counts"].update(other.get("counts", {}))
    if "checked" in other:
        into.setdefault("checked", []).extend(other["checked"])
    if "all_fetched" in other:
        into.setdefault("all_fetched", []).extend(other["all_fetched"])


# ─────────────────────────────────────────────────────────────────────────
# Morning fetch — full cohort
# ─────────────────────────────────────────────────────────────────────────

def fetch_morning(cfg: dict, last_dates: dict, last_checked: dict) -> tuple[dict, list]:
    """7 AM ET cohort. Returns (result, revisions)."""
    result = _empty_result()

    log("Fetching NAR existing home sales...")
    nar = fetch_nar(last_dates=last_dates)
    if nar["new"]:
        save(nar["new"])
        last_dates = get_last_dates()  # refresh so FRED skips these series
        log(f"NAR scraper: {len(nar['new'])} observations")

    fred = fred_series(cfg)
    log(f"Fetching FRED ({len(fred)} series, only those due)...")
    fred_result = fetch_all(fred, last_dates=last_dates, last_checked=last_checked)

    # Fold NAR counts into the FRED result so summary picks them up.
    for sid, n in nar["counts"].items():
        if n > 0:
            fred_result["counts"][sid] = n
            fred_result["new"].extend(o for o in nar["new"] if o.series_id == sid)

    if fred_result.get("checked"):
        save_fetch_log(fred_result["checked"])

    # Detect revisions BEFORE saving (compare fetched vs stored).
    all_fetched = fred_result.get("all_fetched", [])
    revisions = detect_and_save_revisions(all_fetched)
    if revisions:
        log(f"Revisions detected ({len(revisions)}):")
        for r in revisions:
            log(f"  {r['series_id']:<20} {r['date']}  "
                f"{r['old_value']:.1f} → {r['new_value']:.1f}  "
                f"({r['pct_change']:+.2f}%)")
    if all_fetched:
        save(all_fetched)

    _merge(result, fred_result)

    log("Fetching MND mortgage rates...")
    mnd = fetch_mnd(last_dates=last_dates)
    if mnd["new"]:
        save(mnd["new"])
    _merge(result, mnd)

    bls_map = build_series_map(cfg)
    if bls_map:
        log(f"Fetching BLS metro CPI ({len(bls_map)} series)...")
        bls = fetch_bls(bls_map, last_dates=last_dates)
        if bls["new"]:
            save(bls["new"])
        _merge(result, bls)

    log("Fetching Altos weekly inventory...")
    altos = fetch_altos(last_dates=last_dates)
    if altos["new"]:
        save(altos["new"])
    _merge(result, altos)

    log("Fetching Xactus MII...")
    xactus = fetch_xactus(last_dates=last_dates)
    if xactus["new"]:
        save(xactus["new"])
    _merge(result, xactus)

    log("Fetching web-scraped series (MBA, etc.)...")
    web = fetch_web(last_dates=last_dates)
    if web["new"]:
        save(web["new"])
    _merge(result, web)

    log("Fetching Conference Board confidence...")
    cb = fetch_confboard(last_dates=last_dates)
    if cb["new"]:
        save(cb["new"])
    _merge(result, cb)

    log("Fetching Realtor.com bonus metrics...")
    realtor = fetch_realtor(last_dates=last_dates)
    if realtor["new"]:
        save(realtor["new"])
    _merge(result, realtor)

    log("Fetching Redfin housing data...")
    redfin = fetch_redfin(last_dates=last_dates)
    if redfin["new"]:
        save(redfin["new"])
    _merge(result, redfin)

    log("Computing mortgage rate spread...")
    spread = compute_spread(last_dates=get_last_dates())
    if spread["new"]:
        save(spread["new"])
    _merge(result, spread)

    log("Computing rolling averages...")
    rolling = compute_rolling(last_dates=get_last_dates())
    if rolling["new"]:
        save(rolling["new"])
    _merge(result, rolling)

    return result, revisions


# ─────────────────────────────────────────────────────────────────────────
# Intraday fetch — 4 PM ET cohort
# ─────────────────────────────────────────────────────────────────────────

def fetch_intraday(cfg: dict, last_dates: dict) -> tuple[dict, list]:
    """4 PM ET cohort: MND + retry of FRED series in fetch_errors.
    Returns (result, revisions)."""
    result = _empty_result()

    log("Fetching MND mortgage rates...")
    mnd = fetch_mnd(last_dates=last_dates)
    if mnd["new"]:
        save(mnd["new"])
    _merge(result, mnd)

    failed_ids = get_failed_series()
    revisions: list = []
    if failed_ids:
        all_fred = fred_series(cfg)
        retry_subset = [(sid, name) for sid, name in all_fred if sid in failed_ids]
        missing = failed_ids - {sid for sid, _ in retry_subset}
        if missing:
            log(f"Skipping non-FRED series in fetch_errors: {sorted(missing)}")
        if retry_subset:
            log(f"Retrying {len(retry_subset)} failed FRED series: "
                f"{', '.join(sid for sid, _ in retry_subset)}")
            retry = fetch_all(retry_subset, last_dates=last_dates, force=True)

            all_fetched = retry.get("all_fetched", [])
            revisions = detect_and_save_revisions(all_fetched)
            if revisions:
                log(f"Revisions detected ({len(revisions)}):")
                for r in revisions:
                    log(f"  {r['series_id']:<20} {r['date']}  "
                        f"{r['old_value']:.1f} → {r['new_value']:.1f}  "
                        f"({r['pct_change']:+.2f}%)")
            if all_fetched:
                save(all_fetched)
            if retry.get("checked"):
                save_fetch_log(retry["checked"])
            _merge(result, retry)
    else:
        log("No FRED retries pending.")

    # Recompute derived series — MND just changed, and a successful retry of
    # DGS10 would change the spread input as well.
    log("Recomputing mortgage rate spread...")
    spread = compute_spread(last_dates=get_last_dates())
    if spread["new"]:
        save(spread["new"])
    _merge(result, spread)

    log("Recomputing rolling averages...")
    rolling = compute_rolling(last_dates=get_last_dates())
    if rolling["new"]:
        save(rolling["new"])
    _merge(result, rolling)

    return result, revisions


# ─────────────────────────────────────────────────────────────────────────
# Post-processing — calculations, summaries, briefing, export, push
# ─────────────────────────────────────────────────────────────────────────

def post_process(cfg: dict, series: list, result: dict, revisions: list) -> None:
    """Run calculations, regenerate summaries/analyses/briefing, export sheets,
    commit and push. Both entrypoints call this; the intraday entrypoint skips
    this entirely when nothing changed."""
    today = datetime.now().strftime("%Y-%m-%d")
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
            log(f"  {series_id:<20} {name}  "
                f"(+{n} observation{'s' if n != 1 else ''})")
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

    summary_dir = Path("summaries")
    summary_dir.mkdir(exist_ok=True)

    log("Generating data summary...")
    summary = generate_summary(cfg)
    report = format_summary(summary)
    updated_ids = get_series_captured_today()

    summary_path = summary_dir / f"summary {today}.txt"
    summary_header = f"  === Latest Data Summary — {today} ===\n"
    summary_path.write_text(summary_header + report + "\n")
    log(f"Summary saved to {summary_path}")

    signals_report = format_signals_by_recency(summary, updated_ids)
    recent_revisions = get_recent_revisions(days=7)
    revisions_block = _format_revisions(recent_revisions)
    if revisions_block:
        signals_report = revisions_block + "\n\n" + signals_report

    signals_path = summary_dir / f"signals {today}.txt"
    signals_header = f"  === Signals — {today} ===\n\n"
    signals_path.write_text(signals_header + signals_report + "\n")
    log(f"Signals saved to {signals_path}")

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

    log("Generating housing analysis...")
    try:
        housing_md = generate_housing_analysis(cfg)
        housing_path = summary_dir / f"housing analysis {today}.md"
        housing_path.write_text(f"# Housing Analysis — {today}\n\n{housing_md}\n")
        log(f"Housing analysis saved to {housing_path}")
    except Exception as e:
        log(f"Housing analysis failed: {e}")

    log("Generating editorial briefing...")
    try:
        briefing_html = generate_briefing(cfg, updated_ids=updated_ids)
        briefing_path = summary_dir / f"briefing {today}.html"
        briefing_path.write_text(briefing_html)
        docs_dir = Path("docs")
        docs_dir.mkdir(exist_ok=True)
        (docs_dir / "index.html").write_text(briefing_html)
        log(f"Briefing saved to {briefing_path} + docs/index.html")
    except Exception as e:
        log(f"Briefing generation failed: {e}")

    has_changes = bool(new_obs) or bool(revisions)
    if has_changes:
        log("Exporting Sheets data...")
        revision_ids = {r["series_id"] for r in revisions}
        export_ids = updated_ids | revision_ids
        paths = export_all_groups(cfg, updated_ids=export_ids)
        calc_paths = export_all_groups_calcs(cfg, updated_ids=export_ids)
        changed_groups = [
            gid for gid, gdata in cfg.get("groups", {}).items()
            if {s["id"] for s in gdata["series"]} & export_ids
        ]
        write_manifest(changed_groups)
        log(f"Exported {len(paths)} value CSVs + {len(calc_paths)} calc CSVs "
            f"({len(changed_groups)} groups changed)")

    repo_dir = Path(__file__).resolve().parent.parent
    try:
        subprocess.run(
            ["git", "add", "sheets_data/", "sheets_data_calcs/", "docs/"],
            cwd=repo_dir, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "commit", "-m", f"data: update sheets export {today}"],
            cwd=repo_dir, check=True, capture_output=True,
        )

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
        if "nothing to commit" not in (e.stdout or b"").decode():
            log(f"Git commit failed: "
                f"{e.stderr.decode().strip() if e.stderr else e}")
    except FileNotFoundError:
        log("Git not available — skipping push.")
