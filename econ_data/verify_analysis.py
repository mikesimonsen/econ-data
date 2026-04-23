"""Numeric fact-check for LLM-generated analyses.

Extracts percentage / dollar / thousand-separated numeric claims from the
LLM output and checks each one appears in the source data the LLM was given.
Used to catch hallucinated numbers like "pending sales surging 28.61%" when
the real figure was +6.26%.
"""
from __future__ import annotations

import re

# Patterns match claims the LLM makes.  We intentionally require a decimal
# or an obvious unit so bare integers like "2026" or "10-year" don't
# trigger the check.
_PCT_RE = re.compile(r"([+-]?\d+\.\d+)\s?%")
_DOLLAR_RE = re.compile(r"\$\s?(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d+)")
_THOUSANDS_RE = re.compile(r"\b(\d{1,3}(?:,\d{3})+(?:\.\d+)?)\b")

# Numbers we find in source data (broader — any decimal or thousands form).
_SOURCE_NUM_RE = re.compile(
    r"[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?|[+-]?\d+\.\d+"
)

# Absolute tolerance when matching percentages (handles rounding:
# "+6.26%" in source → "6.3%" in prose).
_PCT_TOL = 0.06
# Relative tolerance for dollar and count values.
_REL_TOL = 0.005


def _to_float(s: str) -> float | None:
    try:
        return float(s.replace(",", "").replace("+", ""))
    except ValueError:
        return None


def _source_values(source: str) -> list[float]:
    vals = []
    for m in _SOURCE_NUM_RE.finditer(source):
        v = _to_float(m.group(0))
        if v is not None:
            vals.append(v)
    return vals


def _matches_any(value: float, source_vals: list[float], *, is_pct: bool) -> bool:
    target = abs(value)
    for sv in source_vals:
        svA = abs(sv)
        if is_pct:
            if abs(svA - target) <= _PCT_TOL:
                return True
        else:
            if svA == 0:
                if target == 0:
                    return True
                continue
            if abs(svA - target) / svA <= _REL_TOL:
                return True
            if abs(svA - target) < 1 and target < 100:
                return True
    return False


def verify_numbers(analysis: str, source: str) -> list[str]:
    """Return tokens in `analysis` whose numeric value does not appear in `source`.

    An empty list means every cited number is traceable back to the data.
    """
    source_vals = _source_values(source)
    mismatches: list[str] = []
    seen: set[str] = set()

    for m in _PCT_RE.finditer(analysis):
        tok = m.group(0)
        if tok in seen:
            continue
        v = _to_float(m.group(1))
        if v is None:
            continue
        if not _matches_any(v, source_vals, is_pct=True):
            mismatches.append(tok)
            seen.add(tok)

    for m in _DOLLAR_RE.finditer(analysis):
        tok = m.group(0)
        if tok in seen:
            continue
        v = _to_float(m.group(1))
        if v is None:
            continue
        if not _matches_any(v, source_vals, is_pct=False):
            mismatches.append(tok)
            seen.add(tok)

    for m in _THOUSANDS_RE.finditer(analysis):
        tok = m.group(0)
        # Skip if already flagged as part of a $-prefixed or %-suffixed token.
        start, end = m.span()
        context_before = analysis[max(0, start - 1):start]
        context_after = analysis[end:end + 1]
        if context_before == "$" or context_after == "%":
            continue
        if tok in seen:
            continue
        v = _to_float(m.group(1))
        if v is None:
            continue
        if not _matches_any(v, source_vals, is_pct=False):
            mismatches.append(tok)
            seen.add(tok)

    return mismatches
