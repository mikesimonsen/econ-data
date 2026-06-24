"""Intraday entrypoint (fired 1 / 3 / 4 PM ET). Picks up MND's daily mortgage
rate posting and sweeps any FRED series still due-but-uncaptured today — FRED
often posts a 10 AM ET release to its API only in the early afternoon, after
the morning cohort's last (GitHub-Actions-delayed) fetch.

Skips post-processing entirely when nothing changed — no point regenerating
the briefing and pushing a no-op commit on quiet afternoons.
"""
from econ_data.config import all_series, load
from econ_data.pipeline import fetch_intraday, log, post_process
from econ_data.store import get_last_dates


if __name__ == "__main__":
    cfg = load()
    series = all_series(cfg)

    log("=== Econ Data Fetch (intraday) ===\n")

    last_dates = get_last_dates()

    result, revisions = fetch_intraday(cfg, last_dates)

    if not result["new"] and not revisions:
        log("Nothing new — skipping post-processing.")
    else:
        post_process(cfg, series, result, revisions)

    log("Done.")
