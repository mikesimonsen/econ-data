# Regional Payrolls — Metro Employment Group

Built 2026-08-07/08. Status: **shipped and backfilled.** Commits `67f73fc`, `233aae2`.

Tracks monthly total nonfarm employment at the metro level so regional employment
change can be reported alongside the national numbers — the San Francisco Bay Area
in particular.

---

## What was added

**`regional-payrolls`** — 40 major metros. BLS State and Area Employment (SAE)
program, delivered via FRED, so FRED's release calendar drives fetching.
Seasonally adjusted, monthly, thousands of persons. History back to 1990
(438 observations per series), current through June 2026 at time of writing.

**`regional-payrolls-derived`** — `BAY_AREA_TOTAL`, a computed sum of the five
MSAs that make up the 9-county Bay Area. Declared with `source: calculated` so
`fred_series()` skips it — that's the mechanism that keeps a derived series out
of the FRED fetch path (same pattern as `mortgage-spread`).

**`econ_data/calc_regional.py`** — the aggregation, wired into `pipeline.py` in
both the morning and intraday paths. **`tests/test_calc_regional.py`** — 6 tests.

---

## The Bay Area definition

"San Francisco Bay Area" is not a single BLS metro. It is five separate MSAs, and
**San Jose is not part of the San Francisco MSA** — it is ~29% of regional
employment on its own. Any "Bay Area" figure sourced from `SANF806NA` alone
undercounts the region by roughly a third.

The chosen definition is the official 9-county Bay Area (ABAG/MTC): San Francisco,
San Mateo, Alameda, Contra Costa, Marin, Santa Clara, Sonoma, Solano, Napa.

| Component | Series | June 2026 |
|---|---|---|
| San Francisco-Oakland-Fremont | `SANF806NA` | 2,426.3k |
| San Jose-Sunnyvale-Santa Clara | `SANJ906NA` | 1,171.0k |
| Santa Rosa-Petaluma | `SANT206NA` | 206.1k |
| Vallejo | `VALL706NA` | 146.4k |
| Napa | `NAPA906NA` | 75.6k |
| **BAY_AREA_TOTAL** | derived | **4,025.4k** |

Verified: the stored total reconciles exactly against the component sum.

### Bay Area trend as of the June 2026 print

```
2026-01   4,026.3k   +8.9k
2026-02   4,028.3k   +2.0k
2026-03   4,029.9k   +1.6k
2026-04   4,030.0k   +0.1k     <- regional peak
2026-05   4,028.9k   -1.1k
2026-06   4,025.4k   -3.5k
```

YoY **+0.80%**; summary flags it *falling 2mo*. The region peaked in April and has
given back ground since while remaining positive year over year.

---

## Two deliberate correctness rules in calc_regional.py

**1. A month is only published once every component has reported.** BLS does not
always release all five Bay Area metros in the same batch. Summing whatever is
present would render as a sharp fake decline exactly where someone is reading the
month-over-month change. Incomplete months are skipped entirely rather than
partially summed.

**2. Full history is recomputed every run, not just appended.** BLS annually
benchmarks metro employment, revising prior months. An append-only aggregate would
leave the total silently stale against revised components. The module recomputes
everything, compares against stored values, and emits only new-or-changed rows —
which also keeps the pipeline's "did anything arrive" counts honest.

---

## Operational lessons worth keeping

### New FRED series are invisible to the scheduler until the calendar is seeded

This is the one that would have quietly broken the whole thing.

`fetch_all` gates on `series_due_now()`, which returns False when a series has **no
`release_schedule` rows at all**. A newly added FRED series has none. It would have
been skipped on every run, forever, **reporting no error** — the fetch would simply
report nothing due.

The fix, and the required step for any future series addition:

```python
from econ_data.release_schedule import refresh_fred_calendar
refresh_fred_calendar(new_series_ids)   # seeds series_release + PENDING rows
```

then force-fetch history with `fetch_all(series, force=True)` to bypass the schedule
gate for the initial backfill. Seeding these 40 produced 120 schedule rows across 2
FRED releases.

The weekly `run_calendar.py` (Sunday 06:00 ET cohort) would eventually have seeded
them, but nothing would have flagged the gap in the meantime.

### fetch.py swallows per-series errors, so callers can't detect failure

`DALL148NA` returned zero observations on the otherwise-successful backfill pass.
`fetch.py` catches fetch exceptions internally and records a `-1` count rather than
raising, so a caller's `try/except` retry loop counts the series as a success. It was
only caught by verifying row counts per series afterward.

**Always verify populated-series counts after a bulk fetch. Do not trust the absence
of an exception.** This is the same swallow-the-failure shape as the Redfin dead-URL
bug documented in `HANDOFF.md` — worth considering whether the fetch path should
surface `-1` counts more loudly.

### Local backfills need per-series saves

The first two backfill attempts fetched all 40 series (~15 min) and then died on the
final `save()` when DNS resolution to Neon failed, losing everything. Rewritten to
fetch-and-save one series at a time with retries, so a network blip costs one series
instead of the whole run. All runs logged to `logs/run.log` per the manual-run rule.

---

## Notable finding in the data

**Washington DC came in at YoY -2.19%** and the summary auto-flagged it as a group
outlier — every peer metro is positive. That is the federal workforce showing up in
metro employment data. Worth speaking to when reporting.

---

## How the pieces connect

- **Exports** pick up new groups automatically (`export_all_groups` iterates config),
  producing `sheets_data/regional-payrolls.csv` and the calc variants on the next
  cloud run.
- **Briefing** shows the groups in the All Data tab automatically; both were added to
  the search keyword map in `briefing.py`. They are **not** on the Employment tab,
  which is built from explicit chart configs in `_render_employment_charts` — adding
  a regional chart there is an open option, not done.
- **`compute_all()`** was run manually after the backfill (410,534 rows) so MoM/YoY
  were available immediately rather than waiting for the next cron.

---

## Open options, not done

- Add a regional payrolls chart to the briefing's Employment tab.
- Extend the metro list — Milwaukee, Jacksonville, Memphis, Oklahoma City, etc. Each
  is a few config lines, but **re-run the calendar seed and a force backfill** for any
  additions (see the lesson above).
- Add more multi-metro aggregates. `REGIONS` in `calc_regional.py` is a dict keyed by
  region id; adding an entry is all that's required, plus a config series entry in
  `regional-payrolls-derived`.
- Consider NSA variants. Every metro series here has an NSA twin — same friendly ID
  with an `N` suffix (`SANF806NA` → `SANF806NAN`). SA is correct for reporting
  month-over-month change, which is why it was chosen.
