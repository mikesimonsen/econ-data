"""Morning (7 AM ET) entrypoint. Runs the full fetch cohort + post-processing.

The intraday counterpart is run_intraday.py (4 PM ET). Both share
econ_data/pipeline.py.
"""
from econ_data.config import all_series, load
from econ_data.pipeline import fetch_morning, log, post_process
from econ_data.store import get_fetch_log, get_last_dates


if __name__ == "__main__":
    cfg = load()
    series = all_series(cfg)

    log("=== Econ Data Fetch (morning) ===\n")

    last_dates = get_last_dates()
    last_checked = get_fetch_log()

    result, revisions = fetch_morning(cfg, last_dates, last_checked)
    post_process(cfg, series, result, revisions)

    log("Done.")
