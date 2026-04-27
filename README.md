# econ-data

Daily U.S. economic data pipeline. Pulls from FRED, BLS, NAR, Mortgage News Daily,
Altos Research, Realtor.com, Redfin, Xactus, Conference Board; stores in managed
Postgres; computes derived series; generates an editorial HTML briefing; publishes
to GitHub Pages.

**Live briefing:** https://mikesimonsen.github.io/econ-data/

## Architecture

```
            ┌─────────────────┐
            │  Fly.io cron    │  daily at 10:00 AM Eastern (14:00 UTC during DST)
            │  python run.py  │
            └────────┬────────┘
                     │
       ┌─────────────┼──────────────┬──────────────┐
       ▼             ▼              ▼              ▼
   FRED API     BLS / NAR /     Anthropic API    Neon Postgres
                MND / scrapers  (LLM analysis)   (read + write)
                     │
                     ▼
            briefing.html → docs/index.html → git push → GitHub Pages
```

- **Compute:** Fly.io machine (`shared-cpu-1x`, 2 GB), scheduled daily.
  No always-on services — the machine wakes once per day, runs ~10 minutes, and stops.
- **Data:** Neon Postgres. 10 tables, ~470k rows. Single source of truth.
- **Briefing:** Static HTML generated each day, pushed to `docs/index.html`,
  served by GitHub Pages.
- **LLM analysis:** Claude generates the daily narrative + housing analysis;
  consensus expectations fetched separately via Claude web search.

## Daily flow

`run.py` orchestrates the pipeline:
1. Fetchers run (FRED, BLS, NAR, MND, Altos, Xactus, web scrapers, Conference
   Board, Realtor, Redfin) → `observations` table
2. Derived series — mortgage spread, rolling averages — computed and saved
3. `compute_all()` rebuilds `calculated` (period %, YoY %) for every series
4. Summary, signals, daily LLM analysis, housing LLM analysis, editorial briefing
   generated to `summaries/` and `docs/index.html`
5. Sheets exports written to `sheets_data/` and `sheets_data_calcs/`
6. All outputs committed to `main` and pushed to GitHub

## Local development

Requires Python 3.11+ and a `.env` with credentials.

```bash
python -m venv .venv
source .venv/bin/activate
pip install "anthropic>=0.40" "beautifulsoup4>=4.12" "fredapi>=0.5" \
  "openpyxl>=3.1" "psycopg[binary]>=3.2" "python-dotenv>=1.0" "pyyaml>=6.0" \
  "requests>=2.31"

cp .env.example .env  # fill in credentials
```

`.env` needs:
- `FRED_API_KEY` — fredaccount.stlouisfed.org
- `BLS_API_KEY` — bls.gov/developers
- `ANTHROPIC_API_KEY` — for LLM analysis
- `DATABASE_URL` — Neon pooled connection string

## CLIs

```bash
python run.py                          # full daily pipeline (normally Fly does this)

python summary.py                      # latest data summary
python summary.py --signals            # signals only
python summary.py --group jolts        # one group

python compare.py SERIES_A SERIES_B    # side-by-side Excel/CSV
python compare.py -i                   # interactive search + select

python export.py                       # interactive export menu
python export.py --format excel --group cpi --data all
python export.py --list                # available series + groups
```

## Deploy

The cron runs on Fly.io. Image rebuilds are needed only when Python deps change
(code is cloned fresh from `main` every cron run).

```bash
fly deploy -a econ-data-cron
# After deploy, Fly auto-creates an idle "app" machine — destroy it:
fly machines list -a econ-data-cron
fly machines destroy <id> -a econ-data-cron --force
# (only the scheduled machine should remain)
```

To change the daily fire time (e.g., for DST adjustments), destroy the scheduled
machine and recreate at the desired UTC moment:

```bash
fly machines destroy <scheduled-id> -a econ-data-cron --force
fly machines run . --schedule daily --vm-memory 2048 -a econ-data-cron
```

The new daily fire time = the moment you ran `fly machines run`.

## Project structure

```
econ-data/
├── run.py                          # daily pipeline entry point
├── summary.py / compare.py / export.py  # CLI wrappers
├── fetch_expectations.py           # consensus forecasts (separate cron)
├── fetch_fed_expectations.py       # FedWatch probabilities (separate cron)
├── config.yaml                     # series definitions, groups, export defaults
├── schema.sql                      # Postgres DDL
├── Dockerfile / fly.toml / fly_run.sh   # Fly.io deployment
├── pyproject.toml                  # Python deps
├── econ_data/
│   ├── store.py                    # Postgres store layer (save, get_*, etc.)
│   ├── db.py                       # connection singleton
│   ├── config.py                   # config.yaml loader
│   ├── fetch.py + fetch_*.py       # 10 source fetchers
│   ├── calculations.py             # period_pct, yoy_pct, period_pp, yoy_pp
│   ├── calc_spread.py              # mortgage spread (30Y - 10Y)
│   ├── calc_rolling.py             # rolling averages
│   ├── seasonal.py                 # seasonal adjustment factors
│   ├── summary.py                  # signal detection, formatting
│   ├── briefing.py                 # HTML editorial briefing
│   ├── housing_analysis.py         # LLM housing narrative
│   ├── daily_analysis.py           # LLM daily summary
│   ├── expectations.py             # consensus forecasts (read + write)
│   ├── fed_expectations.py         # FedWatch probabilities (read + write)
│   ├── export.py                   # CSV / Excel exports
│   ├── export_sheets.py            # wide-format CSVs for downstream consumers
│   └── compare.py                  # multi-series comparison
├── docs/index.html                 # the briefing webpage (GitHub Pages)
├── summaries/                      # daily text summaries + signal logs
├── sheets_data/                    # wide-format CSVs (one per group)
├── sheets_data_calcs/              # period/yoy CSVs (one per group)
├── exports/                        # ad-hoc xlsx/csv exports
├── comparisons/                    # series comparison exports
└── prompts/                        # LLM system prompts
```

## Troubleshooting

**Cron didn't run.** Check `fly machines list -a econ-data-cron` — there should be
exactly one scheduled machine. View recent logs with `fly logs -a econ-data-cron`.
GitHub commit feed should show a "data: update sheets export YYYY-MM-DD" commit
each day, authored by `econ-data cron`.

**Cron crashed mid-run.** `fly logs -a econ-data-cron` shows the traceback. Common
causes: a fetcher endpoint changed format, an API key expired, Postgres latency
spike. After fixing, force a rerun with `fly machines start <scheduled-id>`.

**Stale briefing.** GitHub Pages caches `docs/index.html`. After a fresh push,
allow ~1 minute for the new HTML to propagate. Hard refresh in browser if still stale.

**Local launchd fallback.** If Fly is unavailable for a stretch, re-enable the
local cron: `launchctl load ~/Library/LaunchAgents/com.mikesimonsen.econ-data.plist`.
The plist is preserved exactly as it was.
