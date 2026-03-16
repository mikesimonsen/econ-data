"""Generate LLM-written daily analysis from summary and signals data."""

import os
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "daily_analysis.txt"


def _load_prompt() -> str:
    """Load the system prompt from the prompts directory."""
    return PROMPT_PATH.read_text()


def generate_daily_analysis(signals_text: str, summary_text: str) -> str:
    """Call Claude to generate a written daily analysis from signals and summary data."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key)

    user_content = f"SIGNALS:\n{signals_text}\n\nFULL SUMMARY:\n{summary_text}"

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=_load_prompt(),
        messages=[{"role": "user", "content": user_content}],
    )

    return message.content[0].text
