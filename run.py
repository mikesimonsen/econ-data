"""Morning entrypoint. Fired at 07:00 / 09:00 / 10:30 ET to align with the
US economic release calendar (overnight markets, 08:30 BLS, 10:00 Census/ISM).

The schedule-driven fetcher (release_schedule table) ensures each firing only
does work for series that are actually due — subsequent firings are cheap
no-ops on non-release days.

Skips post-processing entirely when nothing changed — no point regenerating
the briefing and pushing a no-op commit on quiet mornings.
"""
from econ_data.config import all_series, load
from econ_data.pipeline import fetch_morning, log, post_process
from econ_data.store import get_last_dates


if __name__ == "__main__":
    cfg = load()
    series = all_series(cfg)

    log("=== Econ Data Fetch (morning) ===\n")

    last_dates = get_last_dates()

    result, revisions = fetch_morning(cfg, last_dates, last_checked={})

    # Always run post_process — sweep_overdue, briefing's "captured today"
    # panel, and the schedule-status summary need to reflect the current
    # state even on quiet runs. LLM analyses are gated inside post_process
    # on `has_new_data`, so quiet 09:00/10:30 runs cost ~nothing.
    post_process(cfg, series, result, revisions)

    log("Done.")
