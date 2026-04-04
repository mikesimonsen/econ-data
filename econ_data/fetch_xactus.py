"""Fetch Xactus Mortgage Intent Index (XMII) from drop-folder spreadsheets.

Xactus delivers a weekly XMII spreadsheet via email on Mondays.
Save the .xlsx file to import_files/ and this module will ingest it.

File naming convention: XMII_MM_DD_YYYY.xlsx
Sheet: "XMII Weekly Data"
Columns: WEEK_ENDING, XMII_WEEKLY_INDEX, WOW_CHANGE, MOM_CHANGE, YOY_CHANGE
"""

from datetime import date, datetime
from pathlib import Path

import openpyxl

from econ_data.fetch import Observation

IMPORT_DIR = Path("import_files")
SERIES_ID = "XACTUS_MII"
SERIES_NAME = "Xactus Mortgage Intent Index"


def fetch_xactus(last_dates: dict = None) -> dict:
    """
    Scan import_files/ for XMII_*.xlsx files and ingest new observations.

    Returns {"new": [Observation, ...], "counts": {series_id: int}}
    matching the same contract as other fetchers.
    """
    if last_dates is None:
        last_dates = {}

    last = last_dates.get(SERIES_ID)
    all_new = []
    counts = {SERIES_ID: 0}

    if not IMPORT_DIR.exists():
        return {"new": all_new, "counts": counts}

    files = sorted(IMPORT_DIR.glob("XMII_*.xlsx"))
    if not files:
        return {"new": all_new, "counts": counts}

    # Process all files (each file contains the full history,
    # so the latest file is sufficient, but we handle all for robustness)
    seen_dates = set()
    for fpath in files:
        try:
            wb = openpyxl.load_workbook(fpath, read_only=True, data_only=True)
            ws = wb["XMII Weekly Data"]

            for row in ws.iter_rows(min_row=2, values_only=True):
                week_ending, index_value = row[0], row[1]
                if week_ending is None or index_value is None:
                    continue

                if isinstance(week_ending, datetime):
                    obs_date = week_ending.date()
                elif isinstance(week_ending, date):
                    obs_date = week_ending
                else:
                    continue

                if obs_date in seen_dates:
                    continue
                seen_dates.add(obs_date)

                if last and obs_date <= last:
                    continue

                all_new.append(Observation(
                    series_id=SERIES_ID,
                    name=SERIES_NAME,
                    date=obs_date,
                    value=float(index_value),
                ))

            wb.close()
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] SKIPPED Xactus file {fpath.name} — {e}")

    counts[SERIES_ID] = len(all_new)
    return {"new": all_new, "counts": counts}
