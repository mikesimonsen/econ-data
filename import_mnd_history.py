"""One-time import of historical Mortgage News Daily rates from TSV data."""

import csv
import io
from datetime import datetime
from pathlib import Path

from econ_data.fetch import Observation
from econ_data.store import save
from econ_data.calculations import compute_all

# Column index → (series_id, name)
COLUMNS = {
    1: ("MND_30YR_FIXED", "30-Year Fixed Mortgage Rate"),
    2: ("MND_15YR_FIXED", "15-Year Fixed Mortgage Rate"),
    3: ("MND_30YR_FHA", "30-Year FHA Mortgage Rate"),
    4: ("MND_30YR_JUMBO", "30-Year Jumbo Mortgage Rate"),
    5: ("MND_7YR_ARM", "7/6 SOFR ARM Rate"),
    6: ("MND_30YR_VA", "30-Year VA Mortgage Rate"),
}

DATA_FILE = Path(__file__).parent / "mnd_history.tsv"


def parse_and_import():
    observations = []

    with open(DATA_FILE) as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)  # skip header

        for row in reader:
            if not row or not row[0].strip():
                continue

            dt = datetime.strptime(row[0].strip(), "%m/%d/%y").date()

            for col_idx, (series_id, name) in COLUMNS.items():
                if col_idx < len(row) and row[col_idx].strip():
                    value = float(row[col_idx].strip())
                    observations.append(Observation(
                        series_id=series_id,
                        name=name,
                        date=dt,
                        value=value,
                    ))

    print(f"Parsed {len(observations)} observations")
    saved = save(observations)
    print(f"Saved {saved} rows to SQLite")

    calc_rows = compute_all()
    print(f"Computed {calc_rows} calculated values")


if __name__ == "__main__":
    parse_and_import()
