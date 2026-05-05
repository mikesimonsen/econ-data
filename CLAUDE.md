# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Fetches U.S. economic data from FRED (Federal Reserve Economic Data), stores it in a local SQLite database, computes derived series (period % change, YoY % change), and exports to CSV/Excel. Runs daily via launchd to capture new releases automatically.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env  # then add your FRED_API_KEY
```

## Commands

```bash
# Run the full pipeline (fetch → SQLite → calculations)
python run.py

# Export data (interactive menu)
python export.py

# Export data (CLI)
python export.py --list                            # show available series
python export.py --format csv                      # exports/SERIES_ID.csv per series
python export.py --format excel                    # exports/econ_data.xlsx
python export.py --format csv --series CPIAUCSL    # single series
python export.py --format excel --output my.xlsx   # custom output path
python export.py --format excel --group case-shiller --data yoy_pct
python export.py --format excel --group jolts --data all

# Latest data summary
python summary.py                    # all series
python summary.py --group jolts      # one group
python summary.py --signals          # only series with notable signals

# Run tests
pytest
```

## Architecture

- `config.yaml` — defines all series, groups, export defaults, and per-group `cadence:` blocks for non-FRED sources. Single source of truth.
- `econ_data/config.py` — loads and parses config.yaml.
- `econ_data/fetch.py` — pulls FRED observations via `fredapi`. Driven by `release_schedule` (see below) — series only fetched when due.
- `econ_data/store.py` — Postgres-backed CRUD over `observations`, `groups`, `revisions`, etc. (`store_sqlite.py` is the legacy SQLite path, retained but unused.)
- `econ_data/release_schedule.py` — release-calendar-driven scheduling (see "Scheduling" below).
- `econ_data/calculations.py` — computes `period_pct` and `yoy_pct` derived series for every tracked series.
- `econ_data/pipeline.py` — orchestrates fetch → save → mark_captured → sweep_overdue → analyses → briefing → push.
- `run.py` / `run_intraday.py` / `run_calendar.py` — entrypoints. The cron in `.github/workflows/cron.yml` fires Fly machines for each cohort.

`FRED_API_KEY` and `DATABASE_URL` are loaded from `.env` via `python-dotenv`.

## Scheduling

Every series has a known publication schedule, stored in the `release_schedule`
table as one row per `(series_id, scheduled_release)` with status
`PENDING | CAPTURED | OVERDUE | DELAYED`. The fetch path queries this table
instead of guessing via cooldowns:

- **FRED series**: schedule comes from FRED's `release/dates` API
  (`series_release` table maps `series_id → release_id`).
- **Non-FRED groups** (Altos, MND, BLS direct, Conf Board, Redfin, Realtor,
  Xactus, web): each group declares a `cadence:` block in `config.yaml` with
  `frequency`, `rule` (e.g. `last-tuesday`, `wednesday`, `weekday`,
  `around-25`), `release_time_et`, and `grace_days`.
- **Irregular FRED series** with no forward calendar (Cleveland Fed
  expectations, Atlanta Fed wage tracker): synthesized from observation
  history (median inter-arrival gap projected forward).

State transitions:

- `PENDING` → `CAPTURED` when a new observation lands (via `mark_captured()`).
- `PENDING` → `OVERDUE` when `scheduled_release + grace_days < today` and no
  data arrived (`sweep_overdue()` runs at end of pipeline).
- `OVERDUE` → `CAPTURED` when late data finally arrives.
- `OVERDUE` → `DELAYED` when a human acknowledges (e.g. gov shutdown). DELAYED
  rows are skipped by alerting.

The briefing's "Upcoming" tab shows the live status: captured today, overdue
(expected data not yet captured), upcoming next 7 days.

## Cron schedule

Defined in `.github/workflows/cron.yml`. Each firing triggers a Fly machine in
`econ-data-cron`. Times in ET (each has EDT/EST cron pair):

| Time          | Cohort   | Purpose                                                           |
|---------------|----------|-------------------------------------------------------------------|
| 07:00         | morning  | Overnight markets (Treasury, S&P, oil), prior-day catchup         |
| 09:00         | morning  | Captures 08:30 ET releases (CPI, PPI, payrolls, claims, etc.)     |
| 10:30         | morning  | Captures 10:00 ET releases (JOLTS, ISM, Census, NAR, Conf Board)  |
| 16:00         | intraday | Late-day MND, retry FRED `fetch_errors`                           |
| Sun 06:00     | calendar | Weekly refresh of `release_schedule` (FRED API + cadence rules)   |

The morning cohort fires 3x/day. Each firing only does work for series whose
`scheduled_release <= today` and status `PENDING|OVERDUE` — subsequent firings
are cheap no-ops on non-release days. LLM analyses (daily + housing) are
gated on "did any new observation arrive this run?" so they regen at most
once per day-of-release.

## Commands

```bash
# Run the full pipeline (fetch → save → analyses → briefing → push)
python run.py

# Weekly schedule refresh (also runs as Sunday cron)
python run_calendar.py

# Export data (interactive menu)
python export.py

# Export data (CLI)
python export.py --list
python export.py --format csv
python export.py --format excel
python export.py --format excel --group case-shiller --data yoy_pct

# Latest data summary
python summary.py
python summary.py --group jolts
python summary.py --signals

# Run tests
pytest
```
