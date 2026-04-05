"""
Fetch and store consensus expectations for upcoming economic releases.

Uses Claude's web search to find consensus forecasts before each release.
Compares actual vs expected after release and flags beats/misses.

Release schedule is hardcoded based on well-known federal agency calendars.
The fetcher runs separately from the daily pipeline (e.g., Mon/Wed evenings)
so consensus is captured before the data drops.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from econ_data.store_sqlite import DB_PATH

load_dotenv()

# ── Series with trackable consensus ──────────────────────────
# Each entry: series_id, display name, what to search for,
# how to compare (level, change, mom_pct), and the release schedule.
#
# schedule types:
#   first_friday  — 1st Friday of month (Employment Situation)
#   mid_month     — around 10th-14th (CPI)
#   late_month    — around 25th-30th (PCE)
#   weekly_thu    — every Thursday (claims)
#   early_month   — around 1st-10th (JOLTS)
#   mid_month_late — around 17th-19th (housing starts/permits)
#   third_week    — around 20th-23rd (existing home sales)
#   fourth_week   — around 24th-27th (new home sales)
#   last_tuesday  — last Tuesday of month (consumer confidence)

TRACKED = [
    {
        "series_id": "PAYEMS",
        "name": "Nonfarm Payrolls",
        "search_term": "nonfarm payrolls",
        "compare": "change",  # consensus is for MoM change in thousands
        "schedule": "first_friday",
    },
    {
        "series_id": "UNRATE",
        "name": "Unemployment Rate",
        "search_term": "unemployment rate",
        "compare": "level",
        "schedule": "first_friday",
    },
    {
        "series_id": "CES0500000003",
        "name": "Average Hourly Earnings",
        "search_term": "average hourly earnings month-over-month",
        "compare": "mom_pct",
        "schedule": "first_friday",
    },
    {
        "series_id": "CPIAUCSL",
        "name": "CPI Headline",
        "search_term": "CPI consumer price index month-over-month",
        "compare": "mom_pct",
        "schedule": "mid_month",
    },
    {
        "series_id": "CPILFESL",
        "name": "Core CPI",
        "search_term": "core CPI less food and energy month-over-month",
        "compare": "mom_pct",
        "schedule": "mid_month",
    },
    {
        "series_id": "PCEPI",
        "name": "PCE Price Index",
        "search_term": "PCE price index month-over-month",
        "compare": "mom_pct",
        "schedule": "late_month",
    },
    {
        "series_id": "PCEPILFE",
        "name": "Core PCE",
        "search_term": "core PCE price index month-over-month",
        "compare": "mom_pct",
        "schedule": "late_month",
    },
    {
        "series_id": "ICSA",
        "name": "Initial Jobless Claims",
        "search_term": "initial jobless claims weekly",
        "compare": "level",
        "schedule": "weekly_thu",
    },
    {
        "series_id": "JTSJOL",
        "name": "JOLTS Job Openings",
        "search_term": "JOLTS job openings",
        "compare": "level",
        "schedule": "early_month",
    },
    {
        "series_id": "HOUST",
        "name": "Housing Starts",
        "search_term": "housing starts",
        "compare": "level",
        "schedule": "mid_month_late",
    },
    {
        "series_id": "PERMIT",
        "name": "Building Permits",
        "search_term": "building permits",
        "compare": "level",
        "schedule": "mid_month_late",
    },
    {
        "series_id": "EXHOSLUSM495S",
        "name": "Existing Home Sales",
        "search_term": "existing home sales",
        "compare": "level",
        "schedule": "third_week",
    },
    {
        "series_id": "HSN1F",
        "name": "New Home Sales",
        "search_term": "new single family home sales",
        "compare": "level",
        "schedule": "fourth_week",
    },
    {
        "series_id": "CB_CCI",
        "name": "Consumer Confidence",
        "search_term": "conference board consumer confidence index",
        "compare": "level",
        "schedule": "last_tuesday",
    },
]

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS expectations (
    series_id    TEXT    NOT NULL,
    period       TEXT    NOT NULL,
    expected     REAL,
    compare_type TEXT    NOT NULL,
    source_text  TEXT,
    fetched_at   TEXT    NOT NULL,
    PRIMARY KEY (series_id, period)
)
"""


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute(CREATE_TABLE)
    return con


def _next_release_soon(schedule: str, lookahead_days: int = 3) -> bool:
    """Check if a release is expected within the next N days."""
    today = date.today()
    target_dates = _estimate_release_dates(schedule, today, lookahead_days)
    return any(0 <= (d - today).days <= lookahead_days for d in target_dates)


def _estimate_release_dates(schedule: str, ref: date,
                            lookahead: int) -> list[date]:
    """Estimate upcoming release dates based on schedule type."""
    results = []
    window_end = ref + timedelta(days=lookahead)

    if schedule == "first_friday":
        # First Friday of the current and next month
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            d = date(y, m, 1)
            while d.weekday() != 4:  # Friday
                d += timedelta(days=1)
            results.append(d)

    elif schedule == "weekly_thu":
        # Every Thursday
        d = ref
        while d <= window_end:
            if d.weekday() == 3:  # Thursday
                results.append(d)
            d += timedelta(days=1)

    elif schedule == "mid_month":
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            for day in range(10, 15):
                results.append(date(y, m, day))

    elif schedule == "late_month":
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            for day in range(25, 32):
                try:
                    results.append(date(y, m, day))
                except ValueError:
                    pass

    elif schedule == "early_month":
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            for day in range(1, 11):
                results.append(date(y, m, day))

    elif schedule == "mid_month_late":
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            for day in range(16, 21):
                results.append(date(y, m, day))

    elif schedule == "third_week":
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            for day in range(19, 25):
                results.append(date(y, m, day))

    elif schedule == "fourth_week":
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            for day in range(23, 29):
                try:
                    results.append(date(y, m, day))
                except ValueError:
                    pass

    elif schedule == "last_tuesday":
        for month_offset in range(0, 2):
            m = ref.month + month_offset
            y = ref.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            # Find last Tuesday
            if m == 12:
                last_day = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                last_day = date(y, m + 1, 1) - timedelta(days=1)
            d = last_day
            while d.weekday() != 1:  # Tuesday
                d -= timedelta(days=1)
            results.append(d)

    return results


def _reference_period(series_id: str, upcoming: bool = True,
                      db_path: Path = DB_PATH) -> str:
    """Determine the reference period for consensus lookup.

    upcoming=True: next period (consensus for data not yet released)
    upcoming=False: current latest period (retroactive consensus capture)
    """
    con = sqlite3.connect(db_path)
    row = con.execute(
        "SELECT MAX(date) FROM observations WHERE series_id = ?",
        (series_id,),
    ).fetchone()
    con.close()

    if not row or not row[0]:
        return date.today().strftime("%Y-%m")

    latest = date.fromisoformat(row[0])

    if series_id == "ICSA":
        if upcoming:
            next_week = latest + timedelta(days=7)
            return next_week.isoformat()
        return latest.isoformat()

    if upcoming:
        m = latest.month + 1
        y = latest.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        return f"{y}-{m:02d}"
    else:
        return f"{latest.year}-{latest.month:02d}"


def _already_fetched(series_id: str, period: str,
                     db_path: Path = DB_PATH) -> bool:
    """Check if we already have an expectation for this series/period."""
    con = _connect(db_path)
    row = con.execute(
        "SELECT 1 FROM expectations WHERE series_id = ? AND period = ?",
        (series_id, period),
    ).fetchone()
    con.close()
    return row is not None


def _search_consensus(spec: dict, period: str) -> dict | None:
    """Use Claude web search to find the consensus forecast.

    Returns {"expected": float, "source_text": str} or None.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    # Build a focused search prompt
    # Convert period to readable form
    if len(period) == 7:  # YYYY-MM
        from datetime import datetime
        dt = datetime.strptime(period, "%Y-%m")
        period_label = dt.strftime("%B %Y")
    else:
        period_label = period

    compare = spec["compare"]
    if compare == "change":
        ask = (f"What is the Wall Street consensus forecast for US "
               f"{spec['search_term']} for {period_label}? "
               f"I need the expected change in jobs (e.g. +150,000). "
               f"Return ONLY the number in thousands.")
    elif compare == "mom_pct":
        ask = (f"What is the consensus forecast for US "
               f"{spec['search_term']} for {period_label}? "
               f"I need the expected month-over-month percent change "
               f"(e.g. 0.3%). Return ONLY the percentage.")
    else:  # level
        ask = (f"What is the consensus forecast for US "
               f"{spec['search_term']} for {period_label}? "
               f"I need the expected level/value. Return ONLY the number.")

    client = anthropic.Anthropic(api_key=api_key, timeout=60.0)

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 3,
            }],
            system=(
                "You are a research assistant finding economic consensus "
                "forecasts. Search the web and extract the consensus/forecast "
                "number that economists predicted BEFORE the data was released. "
                "This is the 'expected' or 'forecast' or 'consensus' value, "
                "NOT the actual result.\n\n"
                "Respond with ONLY a JSON object, no other text:\n"
                '{"value": <number>, "context": "<one line: source and what the number means>"}\n\n'
                "Rules for the value field:\n"
                "- Nonfarm payrolls change: thousands (e.g. 150 means +150,000 jobs)\n"
                "- Month-over-month percentages: the percentage (e.g. 0.3 means +0.3%)\n"
                "- Unemployment rate: the rate (e.g. 4.2)\n"
                "- Initial claims: thousands (e.g. 225 means 225,000)\n"
                "- Job openings, home sales, housing starts: thousands (e.g. 4000 means 4,000,000 for JOLTS)\n"
                "- Consumer confidence: index value (e.g. 92.5)\n\n"
                "If you truly cannot find a pre-release consensus forecast, respond:\n"
                '{"value": null, "context": "not found"}'
            ),
            messages=[{"role": "user", "content": ask}],
        )
    except Exception as e:
        print(f"  Web search failed for {spec['series_id']}: {e}")
        return None

    # Extract JSON from the response — may be embedded in prose or code blocks
    import re
    for block in msg.content:
        if not hasattr(block, "text"):
            continue
        text = block.text.strip()

        # Try to find JSON in code blocks first
        code_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if code_match:
            text_to_parse = code_match.group(1)
        else:
            # Try to find a bare JSON object anywhere in the text
            json_match = re.search(r'\{"value":\s*[^}]+\}', text)
            if json_match:
                text_to_parse = json_match.group()
            else:
                continue

        try:
            result = json.loads(text_to_parse)
            if result.get("value") is not None:
                return {
                    "expected": float(result["value"]),
                    "source_text": result.get("context", ""),
                }
        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    return None


def fetch_expectations(lookahead_days: int = 3,
                       db_path: Path = DB_PATH) -> list[dict]:
    """Fetch consensus expectations for releases coming in the next N days.

    Returns list of {series_id, period, expected, compare_type, source_text}.
    """
    from datetime import datetime

    results = []
    con = _connect(db_path)
    now = datetime.now().isoformat(timespec="seconds")

    for spec in TRACKED:
        if not _next_release_soon(spec["schedule"], lookahead_days):
            continue

        period = _reference_period(spec["series_id"], upcoming=True,
                                   db_path=db_path)

        if _already_fetched(spec["series_id"], period, db_path):
            continue

        print(f"  Searching consensus for {spec['name']} ({period})...")
        result = _search_consensus(spec, period)

        if result and result["expected"] is not None:
            con.execute(
                "INSERT OR REPLACE INTO expectations "
                "(series_id, period, expected, compare_type, source_text, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (spec["series_id"], period, result["expected"],
                 spec["compare"], result["source_text"], now),
            )
            results.append({
                "series_id": spec["series_id"],
                "name": spec["name"],
                "period": period,
                "expected": result["expected"],
                "compare_type": spec["compare"],
                "source_text": result["source_text"],
            })
            print(f"    → {result['expected']} ({result['source_text'][:80]})")
        else:
            print(f"    → No consensus found")

        time.sleep(1)  # rate limit between searches

    con.commit()
    con.close()
    return results


def get_expectations(db_path: Path = DB_PATH) -> dict:
    """Return all stored expectations as {series_id: {period, expected, compare_type}}."""
    con = _connect(db_path)
    rows = con.execute(
        "SELECT series_id, period, expected, compare_type, source_text "
        "FROM expectations ORDER BY period DESC"
    ).fetchall()
    con.close()

    result = {}
    for sid, period, expected, ctype, source in rows:
        if sid not in result:  # keep most recent period per series
            result[sid] = {
                "period": period,
                "expected": expected,
                "compare_type": ctype,
                "source_text": source,
            }
    return result


# Series where lower actual = better outcome (beat).
# For these, actual < expected means "beat", not "missed".
_INVERTED_EXPECTATIONS = {"UNRATE", "ICSA"}


def check_surprise(series_id: str, actual_value: float,
                   period_pct: float | None,
                   db_path: Path = DB_PATH) -> str | None:
    """Compare actual vs expected and return a signal string if surprised.

    Returns something like "Beat expectations (actual 178K vs expected 60K)"
    or None if no expectation stored or within normal range.
    """
    con = _connect(db_path)
    row = con.execute(
        "SELECT expected, compare_type FROM expectations "
        "WHERE series_id = ? ORDER BY period DESC LIMIT 1",
        (series_id,),
    ).fetchone()
    con.close()

    if not row:
        return None

    expected, compare_type = row
    inverted = series_id in _INVERTED_EXPECTATIONS

    if compare_type == "change":
        if period_pct is None:
            return None
        con = sqlite3.connect(db_path)
        rows = con.execute(
            "SELECT value FROM observations WHERE series_id = ? "
            "ORDER BY date DESC LIMIT 2",
            (series_id,),
        ).fetchall()
        con.close()
        if len(rows) < 2:
            return None
        actual_change = rows[0][0] - rows[1][0]
        diff = actual_change - expected
        if abs(diff) < 10:
            return None
        direction = "Beat" if diff > 0 else "Missed"
        return (f"{direction} expectations "
                f"(actual {actual_change:+,.0f}K vs expected {expected:+,.0f}K)")

    elif compare_type == "mom_pct":
        if period_pct is None:
            return None
        diff = period_pct - expected
        if abs(diff) < 0.05:
            return None
        direction = "Hotter" if diff > 0 else "Cooler"
        return (f"{direction} than expected "
                f"(actual {period_pct:+.1f}% vs expected {expected:+.1f}%)")

    elif compare_type == "level":
        # Normalize units: if expected is in thousands and actual is in
        # units (e.g., ICSA: expected=212, actual=202000), scale expected up.
        exp = expected
        if exp > 0 and actual_value / exp > 500:
            exp = exp * 1000  # expected was in thousands

        diff_pct = (actual_value - exp) / exp * 100 if exp else 0
        if abs(diff_pct) < 1.0:
            return None

        higher = actual_value > exp
        if inverted:
            direction = "Beat" if not higher else "Missed"
        else:
            direction = "Beat" if higher else "Missed"

        if actual_value >= 1000:
            return (f"{direction} expectations "
                    f"(actual {actual_value:,.0f} vs expected {exp:,.0f})")
        else:
            return (f"{direction} expectations "
                    f"(actual {actual_value:.1f} vs expected {exp:.1f})")

    return None
