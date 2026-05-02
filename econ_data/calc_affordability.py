"""
Compute housing affordability series (price-to-income ratio).

  PRICE_TO_INCOME — monthly: HOSMEDUSM052N / MEHOINUSA646N
                    Median household income is annual; the most recent
                    annual value is carried forward to subsequent months
                    until the next annual release.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from econ_data.db import connect
from econ_data.fetch import Observation
from econ_data.store import DB_PATH

PRICE_ID = "HOSMEDUSM052N"
INCOME_ID = "MEHOINUSA646N"

RATIO_ID = "PRICE_TO_INCOME"
RATIO_NAME = "Median Existing-Home Price / Median Household Income"

ALL_IDS = [RATIO_ID]


def compute_affordability(last_dates: dict = None,
                          db_path: Path = DB_PATH) -> dict:
    """Compute price-to-income ratio for every month where price is known.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    """
    if last_dates is None:
        last_dates = {}

    con = connect()

    prices = con.execute(
        "SELECT date, value FROM observations WHERE series_id = %s ORDER BY date",
        (PRICE_ID,),
    ).fetchall()

    incomes = con.execute(
        "SELECT date, value FROM observations WHERE series_id = %s ORDER BY date",
        (INCOME_ID,),
    ).fetchall()

    counts = {RATIO_ID: 0}
    if not prices or not incomes:
        return {"new": [], "counts": counts}

    ratio_last = last_dates.get(RATIO_ID)
    all_new: list[Observation] = []

    income_idx = 0
    current_income: float | None = None
    for price_date, price in prices:
        # Advance income pointer: pick the most recent annual income
        # whose date is <= price_date
        while (income_idx < len(incomes)
               and incomes[income_idx][0] <= price_date):
            current_income = incomes[income_idx][1]
            income_idx += 1

        if current_income is None or current_income == 0:
            continue  # no income value yet; skip months before 1984

        if ratio_last and price_date <= ratio_last:
            continue

        ratio = round(price / current_income, 3)
        all_new.append(Observation(RATIO_ID, RATIO_NAME, price_date, ratio))
        counts[RATIO_ID] += 1

    return {"new": all_new, "counts": counts}
