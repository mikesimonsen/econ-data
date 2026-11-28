"""Real GDP growth forecasts, stored by vintage in the gdp_forecasts table.

Sources:
  atlanta   Atlanta Fed GDPNow (FRED GDPNOW, every vintage)
  newyork   New York Fed Staff Nowcast (weekly spreadsheet)
  stlouis   St. Louis Fed Economic News Index nowcast (FRED STLENI, every vintage)
  philly    Philadelphia Fed Survey of Professional Forecasters, median
            (quarterly spreadsheet: current quarter + next four)

Values are stored as published: percent change at a compounded annual rate.
Converting to continuously compounded happens at display time (gdp_render.py).

Forecasts are not observations: a quarter gets many forecasts, so each one is
keyed by (source, target_quarter, forecast_date) and they stay off the
release_schedule.
"""
from __future__ import annotations

import io
import re
from datetime import date, datetime, timedelta

import openpyxl
import requests

from econ_data.db import connect
from econ_data.release_schedule import _fred_get

SPF_BASE = ("https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/"
            "survey-of-professional-forecasters/")
SPF_MEDIAN_URL = SPF_BASE + "data-files/files/Median_RGDP_Growth.xlsx"
SPF_DATES_URL = SPF_BASE + "spf-release-dates.txt"
NYFED_URL = ("https://www.newyorkfed.org/medialibrary/research/interactives/data/"
             "nowcast/downloads/New-York-Fed-Staff-Nowcast_download_data.xlsx")

# Oldest target quarter kept.
HISTORY_START = date(2022, 1, 1)
# BEA's advance estimate lands about a month after quarter end. Later vintages
# of a model are re-estimates of a known number, not forecasts.
ADVANCE_LAG_DAYS = 31


def _add_quarters(q: date, n: int) -> date:
    months = q.year * 12 + (q.month - 1) + 3 * n
    return date(months // 12, months % 12 + 1, 1)


def _is_forecast(target_quarter: date, forecast_date: date) -> bool:
    quarter_end = _add_quarters(target_quarter, 1) - timedelta(days=1)
    return forecast_date <= quarter_end + timedelta(days=ADVANCE_LAG_DAYS)


def _get(url: str) -> requests.Response:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp


def _sheet_rows(content: bytes, sheet: str) -> list:
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    return list(wb[sheet].iter_rows(values_only=True))


# ── Parsers (pure, so they can be tested without the network) ──────────

def parse_fred_vintages(payload: dict) -> list:
    """[(target_quarter, forecast_date, value)] from FRED series/observations
    queried across all realtime periods."""
    out = []
    for o in payload.get("observations", []):
        if o["value"] == ".":
            continue
        target = date.fromisoformat(o["date"])
        made = date.fromisoformat(o["realtime_start"])
        if _is_forecast(target, made):
            out.append((target, made, float(o["value"])))
    return out


def parse_nyfed(rows: list) -> list:
    """Parse the 'Forecasts By Quarter' sheet: a 'Forecast Date' header row
    followed by one column per quarter ('2026Q4')."""
    quarters, out = None, []
    for row in rows:
        if not row:
            continue
        if row[0] == "Forecast Date":
            quarters = []
            for cell in row[1:]:
                m = re.fullmatch(r"(\d{4}):?Q([1-4])", str(cell or "").strip())
                quarters.append(
                    date(int(m[1]), 3 * int(m[2]) - 2, 1) if m else None)
            continue
        if quarters is None or not isinstance(row[0], (date, datetime)):
            continue
        made = row[0].date() if isinstance(row[0], datetime) else row[0]
        for target, value in zip(quarters, row[1:]):
            if target and isinstance(value, (int, float)) and target >= HISTORY_START:
                out.append((target, made, float(value)))
    return out


def parse_spf_release_dates(text: str) -> dict:
    """{(year, quarter): news release date}. The year appears only on its Q1
    (or first listed) line; each line is quarter, deadline date, release date."""
    out, year = {}, None
    pattern = re.compile(
        r"^\s*(\d{4})?\s*Q([1-4])\s+[\d/]+\**\s+(\d{1,2})/(\d{1,2})/(\d{2})")
    for line in text.splitlines():
        m = pattern.match(line)
        if not m:
            continue
        year = int(m[1]) if m[1] else year
        if year is None:
            continue
        yy = int(m[5])
        full_year = 2000 + yy if yy < 90 else 1900 + yy
        out[(year, int(m[2]))] = date(full_year, int(m[3]), int(m[4]))
    return out


def parse_spf(rows: list, release_dates: dict) -> list:
    """Median_RGDP_Growth sheet: DRGDP2 is the survey quarter, DRGDP3..6 the
    four after it. A survey without a listed release date is skipped."""
    header = [str(c or "").upper() for c in rows[0]]
    out = []
    for row in rows[1:]:
        rec = dict(zip(header, row))
        if not isinstance(rec.get("YEAR"), (int, float)):
            continue
        year, quarter = int(rec["YEAR"]), int(rec["QUARTER"])
        made = release_dates.get((year, quarter))
        if made is None:
            continue
        survey_q = date(year, 3 * quarter - 2, 1)
        for offset in range(5):
            value = rec.get(f"DRGDP{offset + 2}")
            target = _add_quarters(survey_q, offset)
            if isinstance(value, (int, float)) and target >= HISTORY_START:
                out.append((target, made, float(value)))
    return out


# ── Fetchers ───────────────────────────────────────────────────────────

def _fetch_fred(series_id: str) -> list:
    return parse_fred_vintages(_fred_get(
        "series/observations", series_id=series_id,
        realtime_start="1776-07-04", realtime_end="9999-12-31",
        observation_start=HISTORY_START.isoformat()))


def _fetch_nyfed() -> list:
    return parse_nyfed(_sheet_rows(_get(NYFED_URL).content, "Forecasts By Quarter"))


def _fetch_spf() -> list:
    dates = parse_spf_release_dates(_get(SPF_DATES_URL).text)
    return parse_spf(
        _sheet_rows(_get(SPF_MEDIAN_URL).content, "Median_Growth"), dates)


FETCHERS = {
    "atlanta": lambda: _fetch_fred("GDPNOW"),
    "newyork": _fetch_nyfed,
    "stlouis": lambda: _fetch_fred("STLENI"),
    "philly": _fetch_spf,
}

_UPSERT = """
INSERT INTO gdp_forecasts (source, target_quarter, forecast_date, value, captured_at)
VALUES (%s, %s, %s, %s, NOW())
ON CONFLICT (source, target_quarter, forecast_date) DO UPDATE SET
    value = EXCLUDED.value, captured_at = EXCLUDED.captured_at
WHERE gdp_forecasts.value IS DISTINCT FROM EXCLUDED.value
"""


def refresh(log=print) -> int:
    """Fetch every source and upsert. Returns rows inserted or changed.
    One source failing doesn't stop the others."""
    total = 0
    con = connect()
    for source, fetch in FETCHERS.items():
        try:
            rows = fetch()
            if not rows:
                log(f"  → {source}: no forecasts parsed (source format changed?)")
                continue
            with con.transaction(), con.cursor() as cur:
                cur.executemany(_UPSERT, [(source, *r) for r in rows])
                changed = cur.rowcount
            total += changed
            log(f"  → {source}: {changed} new/changed of {len(rows)}")
        except Exception as e:
            # FRED errors carry the request URL, which includes the API key.
            msg = str(e).split("api_key")[0]
            log(f"  → {source} FAILED: {type(e).__name__}: {msg}")
    return total
