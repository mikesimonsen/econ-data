"""One-off import of NAR EHS national history (1968 → 2026) from TradingEconomics CSVs.

Maps:
  Existing Home Sales (thousands SAAR)    → EXHOSLUSM495S  (× 1000)
  Single Family Home Prices ($, all-EHS)  → HOSMEDUSM052N
"""
import csv
from datetime import date
from pathlib import Path

from econ_data.calculations import compute_all
from econ_data.fetch import Observation
from econ_data.store import save

ROOT = Path(__file__).parent
SALES_CSV = ROOT / "import_files/historical_country_United_States_indicator_Existing_Home_Sales (3).csv"
PRICE_CSV = ROOT / "import_files/historical_country_United_States_indicator_Single_Family_Home_Prices.csv"


def _read(path: Path):
    with open(path) as f:
        next(f)  # header
        for row in csv.reader(f):
            iso = row[2]                         # "1968-01-31T00:00:00"
            y, m, _ = iso.split("T")[0].split("-")
            yield date(int(y), int(m), 1), float(row[3])


def main():
    obs = []
    for d, v in _read(SALES_CSV):
        obs.append(Observation("EXHOSLUSM495S", "Total Existing Home Sales",
                               d, v * 1000))
    for d, v in _read(PRICE_CSV):
        obs.append(Observation("HOSMEDUSM052N", "Median Sales Price - National",
                               d, v))

    print(f"Parsed {len(obs)} observations")
    saved = save(obs)
    print(f"Saved {saved} rows")

    print("Recomputing derived series…")
    print(f"Computed {compute_all()} calculated values")


if __name__ == "__main__":
    main()
