"""Weekly release_schedule refresh entrypoint (Sundays 6 AM ET).

Pulls upcoming release dates from FRED's release/dates API for every FRED
series in config, computes scheduled dates from declared cadence rules for
non-FRED groups, and synthesizes from observation history for FRED series
that lack a forward calendar (Cleveland Fed expectations, Atlanta Fed wage
tracker, etc.).

Idempotent: rerunning is safe. Existing CAPTURED/OVERDUE rows are not
overwritten — only PENDING rows get refreshed schedule metadata.
"""
from econ_data.config import fred_series, load
from econ_data.pipeline import log
from econ_data import release_schedule as rs


if __name__ == "__main__":
    cfg = load()
    fred = [sid for sid, _name in fred_series(cfg)]

    log("=== Release-schedule weekly refresh ===\n")

    log(f"Refreshing FRED calendar for {len(fred)} series...")
    n_fred = rs.refresh_fred_calendar(fred)
    log(f"  → {n_fred} FRED schedule rows touched")

    log("Refreshing declared (non-FRED) cadence calendar...")
    n_decl = rs.refresh_declared_calendar(cfg)
    log(f"  → {n_decl} declared schedule rows touched")

    # Synthesize for any FRED series with observations but no PENDING schedule
    # (FRED's release/dates API doesn't always return future dates for
    # irregular series like Cleveland Fed expectations or Atlanta Fed wage
    # tracker). Use observation history to estimate the next release.
    from econ_data.db import connect
    rows = connect().execute(
        """
        SELECT DISTINCT o.series_id FROM observations o
        WHERE o.series_id = ANY(%s)
          AND NOT EXISTS (
              SELECT 1 FROM release_schedule rs
              WHERE rs.series_id = o.series_id AND rs.status = 'PENDING'
                AND rs.scheduled_release > CURRENT_DATE
          )
        """,
        (fred,),
    ).fetchall()
    orphans = [r[0] for r in rows]
    if orphans:
        log(f"Synthesizing schedule from history for {len(orphans)} orphans...")
        n_syn = rs.synthesize_from_history(orphans)
        log(f"  → {n_syn} synthesized schedule rows")

    log("Done.")
