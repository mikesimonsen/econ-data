"""Tests for GDP growth math and the forecast parsers."""
import math
from datetime import date, datetime

from econ_data.calculations import cca_rate
from econ_data.fetch_gdp_forecasts import (parse_fred_vintages, parse_nyfed,
                                           parse_spf, parse_spf_release_dates)
from econ_data.gdp_render import latest_forecasts, to_cca


def test_cca_matches_fred_and_published_conversion():
    # GDPC1 2026 Q2 → Q3; FRED units=cca gives 4.19279.
    assert round(cca_rate(24408.01, 24665.2, 4), 2) == 4.19
    compounded = ((24665.2 / 24408.01) ** 4 - 1) * 100
    assert math.isclose(to_cca(compounded), cca_rate(24408.01, 24665.2, 4))


def test_fred_vintages_drop_reestimates_made_after_the_advance_release():
    payload = {"observations": [
        {"date": "2026-07-01", "realtime_start": "2026-10-23", "value": "3.1"},
        {"date": "2026-07-01", "realtime_start": "2026-11-27", "value": "3.9"},
        {"date": "2026-10-01", "realtime_start": "2026-11-25", "value": "."},
    ]}
    assert parse_fred_vintages(payload) == [(date(2026, 7, 1), date(2026, 10, 23), 3.1)]


def test_nyfed_by_quarter_sheet():
    rows = [(None, "NEW YORK FED"), ("Forecast Date", "2026Q3", "2026Q4", None),
            (datetime(2026, 11, 27), None, 2.99, None)]
    assert parse_nyfed(rows) == [(date(2026, 10, 1), date(2026, 11, 27), 2.99)]


def test_spf_release_dates_carry_the_year_and_skip_footnote_marks():
    text = ("Survey   True Deadline Date   News Release Date\n"
            "2026 Q1             3/2/26**            3/6/26** \n"
            "     Q2             5/12/26             5/15/26\n")
    assert parse_spf_release_dates(text) == {
        (2026, 1): date(2026, 3, 6), (2026, 2): date(2026, 5, 15)}


def test_spf_columns_map_to_survey_quarter_onward():
    rows = [("YEAR", "QUARTER", "DRGDP2", "DRGDP3", "DRGDP4", "DRGDP5", "DRGDP6"),
            (2026.0, 4.0, 2.3, 2.1, "#N/A", 2.2, 2.0),
            (2027.0, 1.0, 9.9, 9.9, 9.9, 9.9, 9.9)]  # no release date yet
    out = parse_spf(rows, {(2026, 4): date(2026, 11, 16)})
    assert [(t, v) for t, _, v in out] == [
        (date(2026, 10, 1), 2.3), (date(2027, 1, 1), 2.1),
        (date(2027, 7, 1), 2.2), (date(2027, 10, 1), 2.0)]


def test_latest_forecasts_keeps_newest_vintage_after_last_actual():
    q3, q4 = date(2026, 7, 1), date(2026, 10, 1)
    rows = [("atlanta", q3, date(2026, 10, 1), 3.0),
            ("atlanta", q4, date(2026, 11, 25), 2.9),
            ("atlanta", q4, date(2026, 11, 18), 3.2)]
    assert latest_forecasts(rows, q3) == {(q4, "atlanta"): (date(2026, 11, 25), 2.9)}
