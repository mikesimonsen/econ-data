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
from datetime import date, datetime, timedelta
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
);
CREATE TABLE IF NOT EXISTS release_calendar (
    release_date TEXT    NOT NULL,
    report       TEXT    NOT NULL,
    series_ids   TEXT    NOT NULL,
    confirmed    INTEGER DEFAULT 1,
    updated_at   TEXT    NOT NULL,
    PRIMARY KEY (release_date, report)
);
"""

# Confirmed release dates from agency websites (seeded once, refreshed weekly).
# Claims (ICSA) is every Thursday and doesn't need calendar entries.
SEED_CALENDAR = [
    # CPI
    ("2026-04-10", "CPI", "CPIAUCSL,CPILFESL"),
    ("2026-05-12", "CPI", "CPIAUCSL,CPILFESL"),
    # Employment Situation
    ("2026-05-08", "Employment Situation", "PAYEMS,UNRATE,CES0500000003"),
    # JOLTS
    ("2026-05-05", "JOLTS", "JTSJOL"),
    # PCE / Personal Income
    ("2026-04-30", "Personal Income/Outlays", "PCEPI,PCEPILFE"),
    # Housing Starts & Permits
    ("2026-04-29", "Housing Starts & Permits", "HOUST,PERMIT"),
    # Existing Home Sales
    ("2026-04-13", "Existing Home Sales", "EXHOSLUSM495S"),
    # New Home Sales
    ("2026-05-05", "New Home Sales", "HSN1F"),
    # Consumer Confidence
    ("2026-04-28", "Consumer Confidence", "CB_CCI"),
]


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.executescript(CREATE_TABLE)
    return con


def seed_calendar(db_path: Path = DB_PATH) -> int:
    """Insert seed release dates if the calendar is empty."""
    con = _connect(db_path)
    existing = con.execute("SELECT COUNT(*) FROM release_calendar").fetchone()[0]
    if existing > 0:
        con.close()
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    for release_date, report, series_ids in SEED_CALENDAR:
        con.execute(
            "INSERT OR IGNORE INTO release_calendar "
            "(release_date, report, series_ids, confirmed, updated_at) "
            "VALUES (?, ?, ?, 1, ?)",
            (release_date, report, series_ids, now),
        )
    con.commit()
    count = con.execute("SELECT COUNT(*) FROM release_calendar").fetchone()[0]
    con.close()
    return count


def get_calendar_releases(days_ahead: int = 7,
                          db_path: Path = DB_PATH) -> list[dict]:
    """Return upcoming releases from the calendar within the next N days.

    Also includes weekly claims (every Thursday, no calendar entry needed).
    """
    today = date.today()
    cutoff = (today + timedelta(days=days_ahead)).isoformat()

    seed_calendar(db_path)

    con = _connect(db_path)
    rows = con.execute(
        "SELECT release_date, report, series_ids FROM release_calendar "
        "WHERE release_date >= ? AND release_date <= ? "
        "ORDER BY release_date",
        (today.isoformat(), cutoff),
    ).fetchall()
    con.close()

    results = []
    for release_date, report, series_ids in rows:
        results.append({
            "release_date": release_date,
            "report": report,
            "series_ids": series_ids.split(","),
        })

    # Add weekly claims Thursdays
    d = today
    while d <= date.fromisoformat(cutoff):
        if d.weekday() == 3:  # Thursday
            results.append({
                "release_date": d.isoformat(),
                "report": "Weekly Jobless Claims",
                "series_ids": ["ICSA"],
            })
        d += timedelta(days=1)

    results.sort(key=lambda r: r["release_date"])
    return results


def refresh_release_calendar(db_path: Path = DB_PATH) -> int:
    """Use Claude web search to discover/update release dates.

    Searches for upcoming release dates for each tracked report and
    upserts into the release_calendar table. Run weekly (e.g., Mondays).
    Returns count of dates added/updated.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return 0

    # Group series into reports
    reports = {}
    for spec in TRACKED:
        if spec["schedule"] == "weekly_thu":
            continue
        sid = spec["series_id"]
        if sid in ("PAYEMS", "UNRATE", "CES0500000003"):
            name = "Employment Situation"
        elif sid in ("CPIAUCSL", "CPILFESL"):
            name = "CPI"
        elif sid in ("PCEPI", "PCEPILFE"):
            name = "Personal Income/Outlays"
        elif sid in ("HOUST", "PERMIT"):
            name = "Housing Starts & Permits"
        else:
            name = spec["name"]
        if name not in reports:
            reports[name] = []
        if sid not in reports[name]:
            reports[name].append(sid)

    client = anthropic.Anthropic(api_key=api_key, timeout=60.0)
    con = _connect(db_path)
    now = datetime.now().isoformat(timespec="seconds")
    updated = 0

    for report_name, series_ids in reports.items():
        series_str = ",".join(series_ids)
        print(f"  Checking calendar for {report_name}...")

        try:
            msg = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=400,
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": 2,
                }],
                system=(
                    "Search for the next official release date for the given "
                    "U.S. economic report. Return ONLY a JSON array of dates "
                    "in YYYY-MM-DD format. Include all confirmed upcoming "
                    "dates you can find (up to 3). Example: [\"2026-05-12\", "
                    "\"2026-06-10\"]. If you cannot find any confirmed dates, "
                    "return []."
                ),
                messages=[{
                    "role": "user",
                    "content": f"What are the next confirmed release dates "
                               f"for the U.S. {report_name} report? "
                               f"Search the official agency website.",
                }],
            )
        except Exception as e:
            print(f"    Search failed: {e}")
            continue

        import re
        full_text = "\n".join(
            b.text for b in msg.content if hasattr(b, "text")
        )
        match = re.search(r'\[([^\[\]]*"20\d{2}-\d{2}-\d{2}"[^\[\]]*)\]',
                          full_text)
        if not match:
            print(f"    No dates found")
            time.sleep(1)
            continue

        try:
            dates = json.loads(match.group())
        except (json.JSONDecodeError, ValueError):
            print(f"    Parse error")
            time.sleep(1)
            continue

        for d in dates:
            try:
                date.fromisoformat(d)
            except ValueError:
                continue
            con.execute(
                "INSERT OR REPLACE INTO release_calendar "
                "(release_date, report, series_ids, confirmed, updated_at) "
                "VALUES (?, ?, ?, 1, ?)",
                (d, report_name, series_str, now),
            )
            updated += 1
            print(f"    → {d}")

        time.sleep(1)

    con.commit()
    con.close()
    return updated


def get_upcoming_releases(days_ahead: int = 7,
                          db_path: Path = DB_PATH) -> list[dict]:
    """Return list of upcoming releases within the next N days.

    Each entry: {series_id, name, release_date, period, prior_value,
                 prior_date, expected, source_text, compare_type}
    """
    today = date.today()
    cutoff = today + timedelta(days=days_ahead)

    con = _connect(db_path)
    expectations = {}
    rows = con.execute(
        "SELECT series_id, period, expected, compare_type, source_text "
        "FROM expectations"
    ).fetchall()
    for sid, period, exp, ctype, source in rows:
        expectations.setdefault(sid, {})[period] = {
            "expected": exp,
            "compare_type": ctype,
            "source_text": source,
        }

    # Use the release calendar (not the old schedule estimator)
    calendar = get_calendar_releases(days_ahead, db_path)

    # Build a lookup: series_id → TRACKED spec
    spec_by_sid = {s["series_id"]: s for s in TRACKED}

    results = []
    seen = set()  # avoid duplicates (same series from multiple calendar entries)

    for entry in calendar:
        release_date_str = entry["release_date"]
        for sid in entry["series_ids"]:
            if sid in seen:
                continue
            seen.add(sid)
            spec = spec_by_sid.get(sid)
            if not spec:
                continue

            period = _reference_period(sid, upcoming=True, db_path=db_path)

            prior_row = con.execute(
                "SELECT date, value FROM observations "
                "WHERE series_id = ? ORDER BY date DESC LIMIT 1",
                (sid,),
            ).fetchone()

            prior_value = prior_row[1] if prior_row else None
            prior_date = prior_row[0] if prior_row else None

            exp_data = expectations.get(sid, {}).get(period, {})

            results.append({
                "series_id": sid,
                "name": spec["name"],
                "release_date": release_date_str,
                "period": period,
                "prior_value": prior_value,
                "prior_date": prior_date,
                "expected": exp_data.get("expected"),
                "source_text": exp_data.get("source_text", ""),
                "compare_type": spec["compare"],
            })

    con.close()
    results.sort(key=lambda r: r["release_date"])
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
        # Compute the prior month label for clarity
        from datetime import datetime, timedelta
        try:
            dt = datetime.strptime(period, "%Y-%m")
            prior_dt = (dt.replace(day=1) - timedelta(days=1)).replace(day=1)
            prior_label = prior_dt.strftime("%B")
        except (ValueError, TypeError):
            prior_label = "the prior month"

        ask = (f"What is the consensus forecast for US "
               f"{spec['search_term']} for {period_label}?\n\n"
               f"I need the MONTH-OVER-MONTH percent change — meaning the "
               f"percent the index is expected to rise from {prior_label} "
               f"to {period_label}. This is typically a small number "
               f"between -0.5% and +1.0% (e.g. 0.2%, 0.3%, 0.4%).\n\n"
               f"DO NOT return the year-over-year (YoY) change, which is "
               f"typically 2%-5%. I need MoM ONLY.\n\n"
               f"Return ONLY the MoM percentage as a decimal "
               f"(e.g. 0.3 for 0.3%).")
    else:  # level
        ask = (f"What is the consensus forecast for US "
               f"{spec['search_term']} for {period_label}? "
               f"I need the expected level/value. Return ONLY the number.")

    client = anthropic.Anthropic(api_key=api_key, timeout=60.0)

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-6",
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
                "- Month-over-month percentages: typically -0.5 to +1.0 "
                "(e.g. 0.3 means +0.3%). NEVER return year-over-year (YoY) "
                "values here — those are typically 2-5 and would be wrong.\n"
                "- Unemployment rate: the rate (e.g. 4.2)\n"
                "- Initial claims: thousands (e.g. 225 means 225,000)\n"
                "- Job openings, home sales, housing starts: thousands (e.g. 4000 means 4,000,000 for JOLTS)\n"
                "- Consumer confidence: index value (e.g. 92.5)\n\n"
                "SANITY CHECK: For MoM inflation (CPI, PCE), if your value "
                "is greater than 1.0, you probably grabbed YoY by mistake. "
                "Search again for the MoM number specifically.\n\n"
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
                value = float(result["value"])
                # Sanity check: MoM percentages should never exceed ~1.5%
                if compare == "mom_pct" and abs(value) > 1.5:
                    print(f"  Rejecting MoM value {value} (likely YoY)")
                    return None
                return {
                    "expected": value,
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

    # Use the release calendar to determine what's coming
    calendar = get_calendar_releases(lookahead_days, db_path)
    upcoming_sids = set()
    for entry in calendar:
        for sid in entry["series_ids"]:
            upcoming_sids.add(sid)

    spec_by_sid = {s["series_id"]: s for s in TRACKED}

    for sid in upcoming_sids:
        spec = spec_by_sid.get(sid)
        if not spec:
            continue

        period = _reference_period(sid, upcoming=True, db_path=db_path)

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
