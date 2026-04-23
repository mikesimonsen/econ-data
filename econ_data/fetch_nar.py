"""Fetch NAR Existing Home Sales from nar.realtor press releases.

Scrapes the latest EHS press release directly from NAR's website, providing
a faster and more reliable source than FRED (which can lag by days).

Covers seasonally adjusted series:
  - Total + regional sales volume (SAAR)
  - National + regional median prices
  - Housing inventory (units)
  - Months supply of inventory

NSA sales volume series are NOT available in press releases — those
continue to come from FRED.

Redundancy: run.py calls this before FRED. If NAR scraping succeeds,
FRED's cooldown logic will skip these series. If it fails, FRED fetches
them as usual.
"""

import re
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

from econ_data.fetch import Observation

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

# NAR EHS landing page — always links to the latest press release
EHS_PAGE = "https://www.nar.realtor/research-and-statistics/housing-statistics/existing-home-sales"

# Maps parsed data keys to FRED series IDs
SERIES_MAP = {
    # Sales volume (SAAR)
    "sales_total": ("EXHOSLUSM495S", "Total Existing Home Sales"),
    "sales_northeast": ("EXHOSLUSNEM495S", "Existing Home Sales - Northeast"),
    "sales_midwest": ("EXHOSLUSMWM495S", "Existing Home Sales - Midwest"),
    "sales_south": ("EXHOSLUSSOM495S", "Existing Home Sales - South"),
    "sales_west": ("EXHOSLUSWTM495S", "Existing Home Sales - West"),
    # Median price
    "price_national": ("HOSMEDUSM052N", "Median Sales Price - National"),
    "price_northeast": ("HOSMEDUSNEM052N", "Median Sales Price - Northeast"),
    "price_midwest": ("HOSMEDUSMWM052N", "Median Sales Price - Midwest"),
    "price_south": ("HOSMEDUSSOM052N", "Median Sales Price - South"),
    "price_west": ("HOSMEDUSWTM052N", "Median Sales Price - West"),
    # Inventory
    "inventory": ("HOSINVUSM495N", "Housing Inventory"),
    "months_supply": ("HOSSUPUSM673N", "Months Supply of Inventory"),
}

MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Cooldown: wait 28 days after last observation before checking again
COOLDOWN_DAYS = 28


def _find_press_release_url(soup: BeautifulSoup) -> Optional[str]:
    """Find the latest EHS press release link on the landing page."""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/newsroom/" in href and "existing-home-sales" in href:
            if href.startswith("/"):
                return "https://www.nar.realtor" + href
            return href
    return None


def _parse_number(text: str) -> Optional[float]:
    """Parse a number that may include commas, dollar signs, or 'million'."""
    text = text.strip().replace(",", "").replace("$", "")
    m = re.match(r"([\d.]+)\s*million", text, re.IGNORECASE)
    if m:
        return float(m.group(1)) * 1_000_000
    try:
        return float(text)
    except ValueError:
        return None


def _parse_reference_month(text: str) -> Optional[date]:
    """Extract the reference month from the press release text.

    Looks for patterns like 'annual rate of X.XX million in March' to
    find the data month. Returns the first of that month.
    """
    # Pattern: "in [Month]" near the SAAR number
    m = re.search(
        r"annual rate of [\d.,]+ (?:million|thousand)?\s+in\s+(\w+)",
        text, re.IGNORECASE,
    )
    if not m:
        # Fallback: "Regional Snapshot for Existing-Home Sales in [Month]"
        m = re.search(
            r"Existing-Home Sales in (\w+)",
            text, re.IGNORECASE,
        )
    if not m:
        return None

    month_name = m.group(1).lower()
    month_num = MONTH_NAMES.get(month_name)
    if month_num is None:
        return None

    # Infer year: if month is ahead of current month, it's the previous year
    today = date.today()
    year = today.year
    if month_num > today.month:
        year -= 1

    return date(year, month_num, 1)


def _parse_press_release(text: str) -> dict:
    """Parse all EHS data from press release text.

    Returns {key: value} where keys match SERIES_MAP and values are
    in FRED-compatible units (sales in units, prices in dollars,
    months supply as float).
    """
    data = {}

    # --- National SAAR ---
    # "annual rate of 3.98 million in March"
    m = re.search(
        r"annual rate of ([\d.,]+\s*million)\s+in\s+\w+",
        text, re.IGNORECASE,
    )
    if m:
        data["sales_total"] = _parse_number(m.group(1))

    # --- National median price ---
    # "$408,800: Median existing-home price"
    m = re.search(
        r"\$([\d,]+):\s*Median existing-home price",
        text, re.IGNORECASE,
    )
    if m:
        data["price_national"] = _parse_number(m.group(1))

    # --- Inventory ---
    # "1.36 million units: Total housing inventory"
    m = re.search(
        r"([\d.,]+\s*million)\s*units:\s*Total housing inventory",
        text, re.IGNORECASE,
    )
    if m:
        data["inventory"] = _parse_number(m.group(1))

    # --- Months supply ---
    # "4.1-month supply of unsold inventory"
    m = re.search(
        r"([\d.]+)-month supply",
        text, re.IGNORECASE,
    )
    if m:
        data["months_supply"] = float(m.group(1))

    # --- Regional data ---
    # Each region has:
    #   "annual rate of [NUMBER]" (may be "X million" or "XXX,000")
    #   "$XXX,XXX: Median price"
    regions = {
        "northeast": ("sales_northeast", "price_northeast"),
        "midwest": ("sales_midwest", "price_midwest"),
        "south": ("sales_south", "price_south"),
        "west": ("sales_west", "price_west"),
    }

    # Split text into regional sections using the "Regional Snapshot" heading
    regional_match = re.search(
        r"Regional Snapshot.*?(?=##|\Z)",
        text, re.IGNORECASE | re.DOTALL,
    )
    if not regional_match:
        # Try without section boundary — look for regions in full text
        regional_text = text
    else:
        regional_text = regional_match.group(0)

    for region, (sales_key, price_key) in regions.items():
        # Find the region's section by looking for the region name as a heading
        # or bold text, then extracting data from the following text
        region_pattern = re.compile(
            rf"(?:^|\n)\s*\**{region}\**\s*\n(.*?)(?=\n\s*\**(?:northeast|midwest|south|west)\**\s*\n|\Z)",
            re.IGNORECASE | re.DOTALL,
        )
        region_match = region_pattern.search(regional_text)
        if not region_match:
            # Try a looser match: find text between this region name and the next
            region_pattern2 = re.compile(
                rf"{region}[:\s]+(.*?)(?=(?:northeast|midwest|south|west)[:\s]+|\Z)",
                re.IGNORECASE | re.DOTALL,
            )
            region_match = region_pattern2.search(regional_text)

        if region_match:
            section = region_match.group(1)
        else:
            # Last resort: search the whole regional text for this region's data
            section = regional_text

        # Regional SAAR: "annual rate of [NUMBER]"
        # Numbers can be "1.86 million" or "430,000" or "770,000"
        m = re.search(
            r"annual rate of ([\d.,]+\s*(?:million)?)",
            section, re.IGNORECASE,
        )
        if m:
            val = _parse_number(m.group(1))
            if val is not None:
                # If the number doesn't say "million" and is small, it's already in units
                data[sales_key] = val

        # Regional median price: "$XXX,XXX: Median price"
        m = re.search(
            r"\$([\d,]+):\s*Median price",
            section, re.IGNORECASE,
        )
        if m:
            data[price_key] = _parse_number(m.group(1))

    return data


def fetch_nar(last_dates: dict = None) -> dict:
    """Fetch latest EHS data from NAR's website.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    matching the contract used by other fetchers.
    """
    if last_dates is None:
        last_dates = {}

    all_series_ids = [sid for sid, _ in SERIES_MAP.values()]
    counts = {sid: 0 for sid in all_series_ids}

    # Cooldown check: use the national total as the reference
    last = last_dates.get("EXHOSLUSM495S")
    if last and (date.today() - last).days <= COOLDOWN_DAYS:
        return {"new": [], "counts": counts}

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Step 1: Find the latest press release URL
    try:
        resp = requests.get(EHS_PAGE, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        landing_soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"[{ts}] NAR scraper: failed to load EHS page — {e}")
        return {"new": [], "counts": {sid: -1 for sid in all_series_ids}}

    pr_url = _find_press_release_url(landing_soup)
    if not pr_url:
        print(f"[{ts}] NAR scraper: no press release link found on EHS page")
        return {"new": [], "counts": {sid: -1 for sid in all_series_ids}}

    # Step 2: Fetch and parse the press release
    try:
        resp = requests.get(pr_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        pr_soup = BeautifulSoup(resp.text, "html.parser")
        pr_text = pr_soup.get_text(separator="\n")
    except Exception as e:
        print(f"[{ts}] NAR scraper: failed to load press release — {e}")
        return {"new": [], "counts": {sid: -1 for sid in all_series_ids}}

    # Step 3: Parse the reference month
    ref_date = _parse_reference_month(pr_text)
    if ref_date is None:
        print(f"[{ts}] NAR scraper: could not determine reference month")
        return {"new": [], "counts": {sid: -1 for sid in all_series_ids}}

    # Step 4: Parse all data
    parsed = _parse_press_release(pr_text)
    if not parsed:
        print(f"[{ts}] NAR scraper: no data extracted from press release")
        return {"new": [], "counts": {sid: -1 for sid in all_series_ids}}

    # Step 5: Build observations, skipping data we already have
    new_obs = []
    for key, (series_id, name) in SERIES_MAP.items():
        value = parsed.get(key)
        if value is None:
            continue

        last_obs = last_dates.get(series_id)
        if last_obs is not None and ref_date <= last_obs:
            continue  # already have this or newer data

        new_obs.append(Observation(
            series_id=series_id,
            name=name,
            date=ref_date,
            value=float(value),
        ))
        counts[series_id] = 1

    if new_obs:
        print(f"[{ts}] NAR scraper: {len(new_obs)} new observations for {ref_date}")
    else:
        print(f"[{ts}] NAR scraper: data for {ref_date} already captured")

    return {"new": new_obs, "counts": counts}
