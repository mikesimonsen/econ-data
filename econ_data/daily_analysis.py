"""Generate LLM-written daily analysis from summary and signals data."""

import os
import time
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


def generate_daily_analysis(signals_text: str, summary_text: str) -> str:
    """Call Claude to generate a written daily analysis from signals and summary data."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key, timeout=TIMEOUT_SECS)

    user_content = f"SIGNALS:\n{signals_text}\n\nFULL SUMMARY:\n{summary_text}"
    system = _load_prompt()

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
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
