"""Generate LLM-written daily analysis from signals data."""

import os
import time
from datetime import date
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "daily_analysis.txt"

MAX_RETRIES = 2
TIMEOUT_SECS = 120.0


def _load_prompt() -> str:
    """Load the system prompt from the prompts directory."""
    return PROMPT_PATH.read_text()


def generate_daily_analysis(signals_text: str, summary_text: str = None) -> str:
    """Call Claude to generate a written daily analysis from signals data.

    The signals report already contains all values, arrows, YoY, and flags
    with explicit UPDATED TODAY / UPDATED THIS WEEK grouping.  The full
    summary is intentionally excluded — it adds 500+ lines with no temporal
    context, which causes the LLM to lose track of what is new today.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key, timeout=TIMEOUT_SECS)

    today = date.today()
    day_of_week = today.strftime("%A")
    date_header = f"TODAY IS: {today.isoformat()} ({day_of_week})\n\n"

    user_content = date_header + signals_text
    system = _load_prompt()

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1024,
                system=system,
                messages=[{"role": "user", "content": user_content}],
            )
            return message.content[0].text
        except (anthropic.APITimeoutError, anthropic.APIConnectionError, anthropic.APIStatusError) as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(5)

    raise last_err
