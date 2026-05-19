"""Fetch S&P 500 forward earnings metrics from FactSet's Earnings Insight PDF.

Source: FactSet publishes a free weekly "Earnings Insight" report on Fridays.
URL pattern: advantage.factset.com/.../EarningsInsight_MMDDYY.pdf

We extract four headline metrics from the "Forward Estimates & Valuation"
section (page 14 in current editions): forward 12M P/E, trailing 12M P/E,
current-calendar-year earnings growth estimate, and next-quarter earnings
growth estimate. These are the most-cited bottom-up consensus numbers on
Wall Street and complement Multpl's monthly trailing reported EPS.

The PDF date stamp is taken from the filename, not the document body —
filename is unambiguous, body text varies in format.
"""
from __future__ import annotations

import io
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import pypdf
import requests

from econ_data.fetch import Observation

PDF_URL_TEMPLATE = (
    "https://advantage.factset.com/hubfs/Website/Resources%20Section/"
    "Research%20Desk/Earnings%20Insight/EarningsInsight_{date_stamp}.pdf"
)
HEADERS = {"User-Agent": "Mozilla/5.0"}
COOLDOWN_DAYS = 4
LOOKBACK_FRIDAYS = 4  # how far back to scan if the latest week is missing

SERIES = {
    "FACTSET_FWD_PE_12M": "S&P 500 Forward 12M P/E Ratio",
    "FACTSET_TTM_PE_12M": "S&P 500 Trailing 12M P/E Ratio",
    "FACTSET_CY_EARNINGS_GROWTH": "S&P 500 Current Year Earnings Growth Estimate",
    "FACTSET_NEXT_Q_EARNINGS_GROWTH": "S&P 500 Next-Quarter Earnings Growth Estimate",
}


def _recent_fridays(today: date, n: int) -> List[date]:
    """Return the n most recent Fridays on-or-before `today`, newest first."""
    out: List[date] = []
    d = today
    while len(out) < n:
        if d.weekday() == 4:
            out.append(d)
        d -= timedelta(days=1)
    return out


def _fetch_pdf(pub_date: date) -> Optional[bytes]:
    """Try to download the FactSet Earnings Insight PDF for one Friday."""
    url = PDF_URL_TEMPLATE.format(date_stamp=pub_date.strftime("%m%d%y"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    if "application/pdf" not in r.headers.get("Content-Type", "").lower():
        return None
    return r.content


def _normalize_pdf_text(text: str) -> str:
    """pypdf occasionally splits numbers like '18.\n9' or '18. 9'. Glue back."""
    # Collapse multiple whitespace into single spaces so regex \s+ is reliable.
    text = re.sub(r"\s+", " ", text)
    # Heal numbers split by a space: "18. 9" → "18.9"
    text = re.sub(r"(\d)\.\s+(\d)", r"\1.\2", text)
    return text


def _parse_metrics(pdf_bytes: bytes) -> dict:
    """Extract the four metrics from the PDF text. Returns {sid: float}.

    Targets the "Forward Estimates & Valuation" section, which exists in
    every Earnings Insight edition. We search the full document text rather
    than a specific page so layout changes don't break us.
    """
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    full_text = "".join(p.extract_text() or "" for p in reader.pages)
    text = _normalize_pdf_text(full_text)

    # `12_month` matches "12-month", "12 -month", "12 - month", "12month", etc.
    # PDF text extraction is inconsistent about hyphen/space placement.
    twelve_month = r"12\s*-?\s*month"
    # Strict number capture — won't gobble a trailing sentence-ending period.
    num = r"(-?\d+(?:\.\d+)?)"
    patterns = {
        "FACTSET_FWD_PE_12M": (
            r"forward\s+" + twelve_month + r"\s+P/E\s+ratio\s+for\s+the\s+"
            r"S&P\s+500\s+is\s+" + num
        ),
        "FACTSET_TTM_PE_12M": (
            r"trailing\s+" + twelve_month + r"\s+P/E\s+ratio\s+is\s+" + num
        ),
        # "For CY 2026, analysts are projecting earnings growth of 21.0%"
        "FACTSET_CY_EARNINGS_GROWTH": (
            r"For\s+CY\s+\d{4},?\s+analysts\s+are\s+projecting\s+earnings\s+"
            r"growth\s+of\s+" + num + r"\s*%"
        ),
        # First "For Q# 20YY, analysts are projecting earnings growth of X%"
        # — this is the next reporting quarter (the "current quarter" section
        # uses "are reporting" not "are projecting").
        "FACTSET_NEXT_Q_EARNINGS_GROWTH": (
            r"For\s+Q[1-4]\s+\d{4},?\s+analysts\s+are\s+projecting\s+earnings\s+"
            r"growth\s+of\s+" + num + r"\s*%"
        ),
    }

    out: dict = {}
    for sid, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                out[sid] = float(m.group(1))
            except ValueError:
                pass
    return out


def fetch_factset(last_dates: dict = None) -> dict:
    """Download the most recent FactSet Earnings Insight PDF and extract metrics.

    Returns the standard {"new": [Observation, ...], "counts": {sid: int}}
    contract. Uses the latest series's last_date for cooldown.
    """
    if last_dates is None:
        last_dates = {}

    counts = {sid: 0 for sid in SERIES}
    all_new: List[Observation] = []

    # Cooldown: skip if the most recent capture is fresh enough.
    most_recent_capture = max(
        (last_dates.get(sid) for sid in SERIES if last_dates.get(sid)),
        default=None,
    )
    if most_recent_capture and (date.today() - most_recent_capture).days <= COOLDOWN_DAYS:
        return {"new": all_new, "counts": counts}

    # Walk back week by week until we find a published PDF.
    pdf_bytes: Optional[bytes] = None
    pub_date: Optional[date] = None
    for friday in _recent_fridays(date.today(), LOOKBACK_FRIDAYS):
        if most_recent_capture and friday <= most_recent_capture:
            break  # already have this edition
        pdf_bytes = _fetch_pdf(friday)
        if pdf_bytes:
            pub_date = friday
            break

    if not pdf_bytes or not pub_date:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED FactSet — no PDF found in last {LOOKBACK_FRIDAYS} weeks")
        return {"new": all_new, "counts": counts}

    try:
        metrics = _parse_metrics(pdf_bytes)
    except Exception as e:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] SKIPPED FactSet parse — {e}")
        return {"new": all_new, "counts": counts}

    for sid, name in SERIES.items():
        if sid not in metrics:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] FactSet {sid}: no value extracted from {pub_date}")
            continue
        last = last_dates.get(sid)
        if last and pub_date <= last:
            continue
        all_new.append(Observation(
            series_id=sid,
            name=name,
            date=pub_date,
            value=metrics[sid],
        ))
        counts[sid] = 1

    return {"new": all_new, "counts": counts}
