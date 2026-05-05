# Release-Schedule-Driven Fetch & Staleness Detection

## Problem

The current pipeline guesses when data is available via a polling heuristic
(`_should_fetch` in `econ_data/fetch.py:43`):
1. Wait `cooldown_days` after last observation (21d for monthly).
2. Once cooldown expires, query FRED daily.
3. After any check today, skip until tomorrow regardless of result.

This has three failure modes, all observed in production:

1. **Release-day races (today, 2026-05-05).** Cron at 7 AM ET, JOLTS/New Home Sales
   publish at 10 AM ET. The 7 AM run queried, found nothing, marked `last_checked = today`.
   The user-triggered re-run at 10:50 AM ET — *after* FRED published — short-circuited
   on `last_checked >= today` and skipped both releases. Bug semantics: "asked once today"
   is treated as "we have today's truth."
2. **No staleness signal.** Census's New Residential Sales report was 6 weeks late
   (Feb data didn't ship until 2026-05-05 alongside Mar). The pipeline kept polling
   silently. We had no signal that something was wrong — no alert, no log, no flag in
   the briefing. It looked the same as a normal Tuesday.
3. **Wasted polling.** Series past cooldown get queried daily until something arrives,
   even on days when no release is scheduled. Most days, most queries return nothing.

The deeper problem: **we have a finite set of series, each on a known release schedule.
This should not be a guessing game.**

## Goals

1. **Determinism.** Every series declares (or derives) when its next observation is
   expected. Fetching is driven by that schedule, not by daily polling.
2. **Universal coverage.** Same model applies to every source — FRED, direct BLS,
   NAR scraper, MND, Conference Board, Realtor.com, Redfin, Xactus, web scrapers,
   and Altos manual uploads.
3. **Staleness as a first-class signal.** "Expected data that wasn't captured" is
   surfaced in the briefing and logs. Causes can be diagnosed: government shutdown,
   manual upload pending, source down, FRED ingestion lag.
4. **Capture the day-of release, not the day after.** When user opens laptop at
   7:30 AM PT (10:30 AM ET), the morning's releases are already in the data.

## Architecture

### 1. Per-series release schedule (replaces `_should_fetch` cooldown logic)

New table `release_schedule`:

```sql
CREATE TABLE release_schedule (
  series_id           TEXT     NOT NULL,
  period_end          DATE     NOT NULL,  -- the data period (e.g. 2026-03-01 for Mar JOLTS)
  expected_release    DATE     NOT NULL,  -- when source publishes (e.g. 2026-04-29)
  expected_time_et    TIME,               -- e.g. '08:30' or '10:00'; NULL if unknown
  status              TEXT     NOT NULL,  -- PENDING | CAPTURED | OVERDUE | DELAYED
  captured_at         TIMESTAMPTZ,        -- when our pipeline saved the obs
  source_last_updated TIMESTAMPTZ,        -- what FRED reports as last_updated
  notes               TEXT,               -- human/agent annotations (e.g. "gov shutdown")
  PRIMARY KEY (series_id, period_end)
);
CREATE INDEX ON release_schedule (status, expected_release);
```

For each series, the table holds:
- One row per past period (history of what we expected vs received)
- One row per upcoming period (next N expected releases)

Status transitions:
- `PENDING` → `CAPTURED` when observation lands.
- `PENDING` → `OVERDUE` when `expected_release + grace_days < today` and still no data.
- `OVERDUE` → `CAPTURED` when late data finally arrives.
- `OVERDUE` → `DELAYED` (manual): user marks "we know this is delayed, stop alerting"
  with a note like "gov shutdown until further notice."

### 2. Cadence specification per source

FRED is authoritative for itself — pull the calendar from FRED's release API.
Other sources need a declared cadence in `config.yaml` per group/series:

```yaml
groups:
  - name: altos
    source: altos
    cadence:
      frequency: weekly
      day_of_week: monday          # uploaded Mondays
      grace_days: 3                # tolerate up to Thursday
      release_time_et: "12:00"     # rough — manual
    series: [...]

  - name: existing-home-sales
    source: nar                     # NAR scraper
    cadence:
      frequency: monthly
      day_of_month_rule: "around-20th"   # NAR releases ~20th
      grace_days: 5
      release_time_et: "10:00"
    series: [...]
```

Cadence rules supported:
- `daily`: every weekday (M–F).
- `weekly`: `day_of_week: <name>`.
- `monthly`: `day_of_month: <int>` | `nth_weekday: "first-friday" | "last-tuesday" | ...` | `around-Nth: <int>`.
- `quarterly`: `month_of_quarter: <1|2|3>` + a monthly rule.
- `irregular`: derive from observation history (median inter-arrival gap).

### 3. Calendar refresher (weekly cron)

`econ_data/release_calendar.py`:

- `refresh_fred_calendar()`:
  - For each FRED series in config, query `fred/series/release` once (cached) to learn
    its `release_id`. Persist `(series_id, release_id)` mapping in `series_release` table.
  - For each unique `release_id`, query `fred/release/dates?release_id=X&include_release_dates_with_no_data=true&realtime_start=today&realtime_end=today+90d`
    to get the next ~3 months of expected dates.
  - Upsert `release_schedule` rows for each (series_id, period_end) with status `PENDING`.
- `refresh_declared_calendar()`:
  - For each non-FRED group with `cadence:` block, compute next N expected dates from rules.
  - Upsert into `release_schedule`.
- Runs **weekly** via a new GH Actions cron job (Sunday 6 AM ET).

### 4. Fetch logic (replaces `_should_fetch`)

```python
def series_due_now(series_id) -> bool:
    """Is this series expecting data today (or recently)?"""
    row = db.fetch_one("""
      SELECT period_end, expected_release, status
      FROM release_schedule
      WHERE series_id = %s AND status IN ('PENDING', 'OVERDUE')
      ORDER BY expected_release ASC LIMIT 1
    """, (series_id,))
    if not row: return False
    return row.expected_release <= date.today()
```

Per cron run:
1. Find all series with `status = PENDING` and `expected_release <= today`.
2. For FRED series in this set: batch-query `fred/series/info` for `last_updated`. If
   `last_updated > our_last_observation`, fetch. Otherwise skip — `last_updated` is
   ground truth, replacing today's `last_checked` heuristic.
3. For non-FRED series in this set: invoke the source-specific fetcher (existing
   `fetch_nar`, `fetch_mnd`, etc.).
4. For each successfully fetched series:
   - Insert observation rows (existing `save()`).
   - Update `release_schedule.status = CAPTURED`, set `captured_at = now()`.
   - Insert next expected row from the calendar.
5. After all fetches: any row still `PENDING` past `expected_release + grace_days`
   transitions to `OVERDUE`. Pipeline logs:

   ```
   ⚠ OVERDUE (3 series past expected release):
     HSN1F                expected 2026-04-29 (6d late) — Census
     ALTOS_NEW_LISTINGS   expected 2026-05-04 (1d late) — manual upload
     CONFBOARD_CCI        expected 2026-04-28 (7d late) — source site error?
   ```

### 5. Cron firings aligned to release windows

Replace the current `7 AM + 4 PM` schedule with release-aware firings. Times in ET:

| Time   | Cohort        | Purpose                                                         |
|--------|---------------|-----------------------------------------------------------------|
| 07:00  | overnight     | Daily series (Treasury, S&P, oil, MND if posted), prior-day ↻   |
| 09:00  | morning-bls   | 8:30 ET releases (CPI, PPI, payrolls, retail sales, claims, GDP)|
| 10:30  | morning-census| 10:00 ET releases (JOLTS, ISM, Census, NAR, Conf Board)         |
| 16:00  | intraday      | Late-day MND, retry FRED `fetch_errors` (existing)              |
| Sun 06 | calendar      | Refresh `release_schedule` from FRED + cadence rules            |

Each firing only does work for series whose schedule says they're due. The 9 AM and
10:30 AM runs are *cheap* on non-release days — they touch zero series and exit fast.
Briefing/analysis regen is gated on "did any new observation land?" so a no-op run
costs only Fly boot time (~$0.0002).

Optional polish (Phase 2): on a known release day for a specific series, poll every
10 min during the 30-min window after expected release. Drops capture latency from
~30 min worst-case to ~5 min. Skipped in v1.

### 6. Staleness surfaced in the briefing

Extend the existing "Upcoming" tab into a unified release-status panel:

```
RELEASES — Tuesday May 5, 2026

✓ Captured today (3)
    JTSJOL    JOLTS Job Openings           Mar: 6,866K  (-0.8% MoM)
    HSN1F     New Home Sales (SAAR)        Mar: 682K    (+7.4% MoM)
    JTSHIR    JOLTS Hires Rate             Mar: 3.5%    (+0.4 pp)
    [...]

⚠ Overdue (1)
    ALTOS_*   Altos weekly inventory       expected 2026-05-04 (1d late)
              → manual upload; check inbox

📅 Upcoming this week
    Wed 05-06  ADP Employment              expected 08:15 ET
    Thu 05-07  Initial Claims              expected 08:30 ET
    Fri 05-08  Nonfarm Payrolls (Apr)      expected 08:30 ET
```

This makes the user's question "is the data right?" answerable at a glance.

## Data flow summary

```
weekly:  refresh_calendar.py
         └─→ FRED API (release/dates)  ─┐
         └─→ config.yaml cadence rules ─┼─→  release_schedule table (forward N periods)
                                         │
daily:   pipeline.py (each cron firing)
         ├─→ query release_schedule for PENDING + due
         ├─→ FRED last_updated check  ─→ fetch + save + mark CAPTURED
         ├─→ source-specific fetchers ─→ fetch + save + mark CAPTURED
         └─→ overdue sweep            ─→ mark OVERDUE, surface in briefing
```

## Implementation phases

### Phase 1 — Foundation (1 day)
1. Create `release_schedule` and `series_release` tables (Postgres migration).
2. Add `cadence:` schema to `config.yaml` for each non-FRED group.
3. Write `econ_data/release_calendar.py` with:
   - `refresh_fred_calendar()`
   - `refresh_declared_calendar()`
   - `compute_next_expected(cadence, last_period_end)` rule engine
4. One-time backfill script: populate `release_schedule` for the next 90 days.

### Phase 2 — Fetch path migration (1 day)
5. Add `series_due_now(series_id)` and `mark_captured(series_id, period_end)` helpers.
6. Refactor `fetch.py:_should_fetch` to use `release_schedule` instead of cooldown.
   FRED `last_updated` becomes the truth signal, not `last_checked`.
7. Wire each non-FRED fetcher (`fetch_nar`, `fetch_altos`, etc.) to update
   `release_schedule` on capture.
8. Add overdue sweep at end of pipeline.

### Phase 3 — Cron + briefing (½ day)
9. Update `.github/workflows/cron.yml`: 4 daily firings (07/09/10:30/16 ET) +
   weekly calendar refresh. Cohort dispatch already exists.
10. Add release-status section to `briefing.py` driven by `release_schedule`.
11. Gate briefing/analysis LLM regen on "new captures since last regen."

### Phase 4 — Cleanup (½ day)
12. Remove old cooldown constants, `last_checked` semantics from `_should_fetch`.
    Keep `fetch_log` as a debug/trace, not a control signal.
13. Add `summary.py --overdue` flag to list overdue series on demand.
14. Document the schedule model in `CLAUDE.md`.

Total: ~3 days of focused work.

## Open questions

1. **Altos manual uploads.** How is the file delivered today — local path, email, S3?
   The fetcher needs a way to detect "the new file is here." Probably: scan a known
   inbox path; if no new file by `expected_release + grace_days`, mark OVERDUE with
   note "awaiting manual upload."
2. **Holidays.** US federal holidays shift releases (no payrolls on July 4 week,
   etc.). FRED's API handles this for FRED series; for declared-cadence non-FRED
   series we need a holiday calendar shift rule. Phase 1 punts: declared dates can
   slip a day without alerting.
3. **Calculated series.** `compute_spread`, `compute_affordability`, `compute_rolling`
   depend on inputs landing. They should run after their inputs and inherit the most
   recent input's `period_end`. Probably auto-derived, not declared.
4. **OVERDUE notification channel.** Today the briefing is the only surface. Want
   email/Slack? Phase 1 keeps it in briefing + pipeline log; add channels later.
5. **Bootstrapping `release_id` for all 220 FRED series.** One-time API loop, ~2 min,
   cached forever. Trivial.

## Why this is the right answer

- **Today's failure becomes structurally impossible.** The control signal flips from
  "did we ask FRED today?" (a guess) to "is FRED's `last_updated` past our last obs?"
  (ground truth).
- **Universal model.** The same `release_schedule` table holds FRED, BLS, NAR, MND,
  Altos, web scrapers — every series has expected dates and capture status. Adding
  a new source means adding a `cadence:` block, not new pipeline logic.
- **Staleness is observable.** "Expected but not captured" is queryable, alertable,
  and shows up in the briefing — gov shutdowns, manual upload lag, and source
  outages all become visible the same way.
- **Cheap to operate.** Fewer FRED calls than today (only when something is due),
  minimal LLM regen (gated on new captures), and the cron firings are cheap no-ops
  on non-release days.
