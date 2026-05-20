"""Generate the Monthly Housing Summary — the two-paragraph synthesis
that sits at the top of the Housing tab and goes out as Mike's
monthly email to Compass International Holdings agents.

Reuses the housing-analysis data snapshot, but swaps in a different
system prompt (prompts/monthly_housing_summary.txt) and a longer
"past four weeks + coming month" framing. Persisted via the
daily_analyses table with date_key = first-of-month so there is one
row per month per kind.
"""
from __future__ import annotations

import os
import re
import time
from datetime import date
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from econ_data.housing_analysis import _build_data_snapshot

load_dotenv()

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "monthly_housing_summary.txt"
OUTPUT_DIR = Path(__file__).parent.parent / "monthly_summaries"

_MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _month_label(today: str) -> str:
    """Return 'Month YYYY' for a YYYY-MM-DD string."""
    y, m, _ = today.split("-")
    return f"{_MONTH_NAMES[int(m)]} {y}"


def _yyyy_mm(today: str) -> str:
    return today[:7]


def generate_monthly_summary(cfg: dict, today: str, db_path=None) -> str:
    """Generate the monthly housing summary via Claude.

    Returns just the two-paragraph body. The caller is responsible for
    wrapping it in a markdown title and persisting it to disk/DB.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in .env")

    snapshot = _build_data_snapshot(cfg, db_path)
    system = PROMPT_PATH.read_text()
    label = _month_label(today)

    user_msg = (
        f"TODAY IS: {today}\n"
        f"WRITE THE MONTHLY HOUSING SUMMARY FOR: {label}\n\n"
        f"Cover the four weeks ending on or near {today}. "
        f"Frame leading indicators for the coming month.\n\n"
        f"DATA SNAPSHOT:\n{snapshot}"
    )

    client = anthropic.Anthropic(api_key=api_key, timeout=120.0)

    last_err = None
    for attempt in range(1, 3):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1200,
                system=system,
                messages=[{"role": "user", "content": user_msg}],
            )
            return message.content[0].text.strip()
        except (anthropic.APITimeoutError, anthropic.APIConnectionError,
                anthropic.APIStatusError) as e:
            last_err = e
            if attempt < 2:
                time.sleep(5)

    raise last_err


def write_monthly_summary(today: str, body: str) -> Path:
    """Write the monthly summary markdown file to monthly_summaries/.

    Returns the path written. Overwrites if a file for the same month
    already exists (so a mid-month regen replaces the prior draft).
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / f"housing-{_yyyy_mm(today)}.md"
    label = _month_label(today)
    content = (
        f"# Monthly Housing Summary — {label}\n\n"
        f"*For Compass International Holdings. Prompt source: "
        f"`prompts/monthly_housing_summary.txt`.*\n\n"
        f"---\n\n"
        f"{body}\n"
    )
    path.write_text(content)
    return path


def load_monthly_summary(today: str) -> str:
    """Load the monthly summary for the given date's month from disk.

    Falls back to the most recent prior month if the current month's
    file doesn't exist yet. Returns markdown (including the title) or
    an empty string if no monthly summary has ever been produced.
    """
    path = OUTPUT_DIR / f"housing-{_yyyy_mm(today)}.md"
    if not path.exists():
        candidates = sorted(OUTPUT_DIR.glob("housing-*.md"), reverse=True)
        if not candidates:
            return ""
        path = candidates[0]
    text = path.read_text()
    # Strip the metadata block (italic line + horizontal rule) — keep
    # the title and paragraphs. The metadata line is for git-readers,
    # not the briefing page.
    lines = text.split("\n")
    out = []
    skip_metadata = False
    for ln in lines:
        stripped = ln.strip()
        if stripped.startswith("*For Compass") or stripped.startswith("*Prompt"):
            skip_metadata = True
            continue
        if skip_metadata and stripped in ("", "---"):
            if stripped == "---":
                skip_metadata = False
            continue
        skip_metadata = False
        out.append(ln)
    return "\n".join(out).strip()
