"""
Compute multi-metro regional employment aggregates from component MSA series.

Some regions people report on are not a single BLS metro area. The San
Francisco Bay Area is the main one: BLS publishes it as five separate MSAs,
and San Jose — ~29% of regional employment — is not part of the "San
Francisco" MSA at all. Summing the components gives the 9-county Bay Area
figure that matches how the region is normally discussed.

Derived series:
  BAY_AREA_TOTAL — 9-county San Francisco Bay Area total nonfarm (000s, SA)

Two correctness rules, both deliberate:

1. A month is only emitted when *every* component has reported. BLS does not
   always publish all five metros in the same batch, and a partial sum would
   show up as a sharp fake decline in the total.

2. The full history is recomputed every run and compared against what's
   stored, so BLS annual benchmark revisions to any component propagate into
   the total. Only new or genuinely changed values are emitted, which keeps
   the pipeline's "did anything arrive" counts honest.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from econ_data.db import connect
from econ_data.fetch import Observation
from econ_data.store import DB_PATH

# region_id -> (display name, [component series_ids])
REGIONS: dict[str, tuple[str, list[str]]] = {
    "BAY_AREA_TOTAL": (
        "Payrolls - San Francisco Bay Area, 9-county (000s)",
        [
            "SANF806NA",  # San Francisco-Oakland-Fremont MSA
            "SANJ906NA",  # San Jose-Sunnyvale-Santa Clara MSA
            "SANT206NA",  # Santa Rosa-Petaluma MSA
            "VALL706NA",  # Vallejo MSA
            "NAPA906NA",  # Napa MSA
        ],
    ),
}

ALL_IDS = list(REGIONS)


def compute_regional(last_dates: dict = None,
                     db_path: Path = DB_PATH) -> dict:
    """Compute regional aggregates from their component MSA series.

    `last_dates` is accepted for signature consistency with the other calc_*
    modules but is intentionally unused — see rule 2 in the module docstring.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    con = connect()
    all_new: list[Observation] = []
    counts = {sid: 0 for sid in ALL_IDS}

    for region_id, (region_name, components) in REGIONS.items():
        rows = con.execute(
            "SELECT series_id, date, value FROM observations "
            "WHERE series_id = ANY(%s) ORDER BY date",
            (components,),
        ).fetchall()

        # date -> {series_id: value}
        by_date: dict[date, dict[str, float]] = {}
        for sid, obs_date, value in rows:
            if value is None:
                continue
            by_date.setdefault(obs_date, {})[sid] = value

        existing = dict(con.execute(
            "SELECT date, value FROM observations WHERE series_id = %s",
            (region_id,),
        ).fetchall())

        for obs_date, total in complete_month_totals(by_date, components):
            prior = existing.get(obs_date)
            if prior is not None and abs(prior - total) < 0.05:
                continue  # unchanged; nothing to write

            all_new.append(Observation(region_id, region_name, obs_date, total))
            counts[region_id] += 1

    return {"new": all_new, "counts": counts}


def complete_month_totals(by_date: dict[date, dict[str, float]],
                          components: list[str]) -> list[tuple[date, float]]:
    """Sum components per month, skipping months where any component is absent.

    Split out from compute_regional so the aggregation rule can be tested
    without a database. See rule 1 in the module docstring for why partial
    months are dropped rather than summed over whatever is present.
    """
    needed = set(components)
    out = []
    for obs_date in sorted(by_date):
        parts = by_date[obs_date]
        if needed - parts.keys():
            continue
        out.append((obs_date, round(sum(parts[c] for c in components), 1)))
    return out
