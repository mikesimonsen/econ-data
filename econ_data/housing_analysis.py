"""Generate LLM-written housing market impact analysis.

Gathers all housing-related data plus select economic indicators
(labor, inflation, rates) and asks Claude to synthesize a housing-
focused narrative.  The prompt is tunable via prompts/housing_analysis.txt.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from econ_data.summary import analyze_series, _format_value, _format_pct, _format_pp

load_dotenv()

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "housing_analysis.txt"

# Groups whose full data feeds into the housing analysis
HOUSING_GROUPS = [
    "case-shiller",
    "existing-home-sales",
    "existing-home-sales-nsa",
    "new-home-sales",
    "housing-starts",
    "building-permits",
    "housing-under-construction",
    "housing-completions",
    "construction-spending",
    "construction-employment",
    "households",
    "altos-inventory",
    "altos-new-listings",
    "altos-new-pending",
    "mortgage-rates",
    "mortgage-spread",
    "mba-applications",
    "mortgage-intent",
]

# Individual series from non-housing groups that impact housing
EXTRA_SERIES = [
    ("UNRATE", "Unemployment Rate"),
    ("JTSHIL", "Hires: Total Nonfarm"),
    ("JTSHIR", "Hires Rate"),
    ("JTSQUL", "Quits: Total Nonfarm"),
    ("JTSQUR", "Quits Rate"),
    ("UEMP27OV", "Unemployed 27+ Weeks"),
    ("UEMPMED", "Median Duration of Unemployment"),
    ("PAYEMS", "Total Nonfarm Payrolls"),
    ("CES0500000003", "Average Hourly Earnings"),
    ("CPIAUCSL", "CPI All Items"),
    ("CPILFESL", "CPI Core"),
    ("CUSR0000SAH1", "CPI Shelter"),
    ("PCEPILFE", "Core PCE"),
    ("DGS10", "10-Year Treasury Yield"),
    ("CB_CCI", "Consumer Confidence Index"),
    ("ICSA", "Initial Jobless Claims"),
]


def _build_data_snapshot(cfg: dict, db_path=None) -> str:
    """Build a text snapshot of all housing-relevant data for the LLM."""
    from econ_data.store_sqlite import DB_PATH
    if db_path is None:
        db_path = DB_PATH

    lines = []

    # Housing groups
    for gid in HOUSING_GROUPS:
        gdata = cfg.get("groups", {}).get(gid)
        if not gdata:
            continue

        lines.append(f"\n=== {gdata['name']} ===")
        for s in gdata["series"]:
            a = analyze_series(s["id"], s["name"], db_path)
            lines.append(_format_analysis_line(a))

    # Extra non-housing series
    lines.append("\n=== Economic Indicators (Housing Impact) ===")
    for sid, name in EXTRA_SERIES:
        a = analyze_series(sid, name, db_path)
        lines.append(_format_analysis_line(a))

    # Add expectations beat/miss context
    try:
        from econ_data.expectations import get_expectations
        expectations = get_expectations(db_path)
        exp_lines = []
        for sid, exp in expectations.items():
            # Only include housing-relevant expectations
            relevant = any(sid == s[0] for s in EXTRA_SERIES)
            relevant = relevant or any(
                sid in [s["id"] for s in cfg.get("groups", {}).get(gid, {}).get("series", [])]
                for gid in HOUSING_GROUPS
            )
            if relevant and exp.get("expected") is not None:
                exp_lines.append(
                    f"  {sid}: expected={exp['expected']} "
                    f"({exp['compare_type']}) — {exp.get('source_text', '')[:60]}"
                )
        if exp_lines:
            lines.append("\n=== Consensus Expectations ===")
            lines.extend(exp_lines)
    except Exception:
        pass

    return "\n".join(lines)


def _format_analysis_line(a: dict) -> str:
    """Format a single series analysis as a compact text line."""
    if a["latest_date"] is None:
        return f"  {a['series_id']:<25} No data"

    is_pct = a.get("is_percent", False)
    val = _format_value(a["latest_value"])
    pch = _format_pp(a["period_pct"]) if is_pct else _format_pct(a["period_pct"])
    yoy = _format_pp(a["yoy_pct"]) if is_pct else _format_pct(a["yoy_pct"])

    freq = a.get("frequency", "monthly")
    date_str = a["latest_date"] if freq in ("daily", "weekly") else a["latest_date"][:7]

    trend = ""
    if a["trend_dir"] and a["trend_periods"] > 1:
        unit = {"daily": "d", "weekly": "wk", "monthly": "mo"}.get(freq, "mo")
        trend = f"  {a['trend_dir']} {a['trend_periods']}{unit}"

    signals = ""
    if a["signals"]:
        signals = "  ⚑ " + " | ".join(a["signals"])

    captured = ""
    if a.get("captured_at"):
        captured = f"  (data from {a['captured_at']})"

    return (f"  {a['name']:<45} {date_str}  {val:>10}  "
            f"period {pch:>8}  YoY {yoy:>8}{trend}{signals}{captured}")


def generate_housing_analysis(cfg: dict, db_path=None) -> str:
    """Generate the housing impact analysis via Claude."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in .env")

    snapshot = _build_data_snapshot(cfg, db_path)
    system = PROMPT_PATH.read_text()

    client = anthropic.Anthropic(api_key=api_key, timeout=120.0)

    last_err = None
    for attempt in range(1, 3):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                system=system,
                messages=[{"role": "user", "content": f"DATA SNAPSHOT:\n{snapshot}"}],
            )
            return message.content[0].text
        except (anthropic.APITimeoutError, anthropic.APIConnectionError,
                anthropic.APIStatusError) as e:
            last_err = e
            if attempt < 2:
                time.sleep(5)

    raise last_err


def load_housing_analysis(today: str) -> str:
    """Load today's housing analysis from disk, falling back to most recent."""
    import re
    summaries_dir = Path(__file__).parent.parent / "summaries"
    path = summaries_dir / f"housing analysis {today}.md"
    if not path.exists():
        candidates = sorted(summaries_dir.glob("housing analysis *.md"), reverse=True)
        if not candidates:
            return ""
        path = candidates[0]
    text = path.read_text()
    # Strip the title line
    lines = text.split("\n")
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    return "\n".join(lines).strip()
