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

- `config.yaml` — defines all series, groups, and export defaults. Single source of truth for what gets tracked.
- `econ_data/config.py` — loads and parses config.yaml.
- `econ_data/fetch.py` — pulls observations from FRED using `fredapi`. Incremental: only fetches data newer than what's in the DB.
- `econ_data/store_sqlite.py` — upserts observations into `econ_data.db`. Tables: `observations`, `groups`, `group_members`, `calculated`, `export_log`.
- `econ_data/calculations.py` — computes `period_pct` and `yoy_pct` derived series for every tracked series.
- `econ_data/export.py` — queries SQLite and writes CSV or Excel. Supports exporting raw values, period % change, YoY % change, or all three. Excel exports include a Contents tab.
- `econ_data/summary.py` — analyzes latest data for trend direction, reversals, and unusual moves.
- `run.py` — orchestrates fetch → store → calculate. Runs daily at 7 AM via launchd.
- `export.py` — interactive menu or CLI for exporting.
- `summary.py` — CLI for at-a-glance latest data analysis.

`FRED_API_KEY` is loaded from `.env` via `python-dotenv`.
