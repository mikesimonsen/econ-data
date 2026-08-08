"""Tests for the multi-metro regional payroll aggregation in calc_regional.py.

The rule under test is that a region's total is only published for months
where every component MSA has reported. BLS does not always publish all five
Bay Area metros in the same batch, and summing a partial set would render as
a sharp fake decline in the regional total.
"""
from datetime import date

from econ_data.calc_regional import REGIONS, complete_month_totals

COMPONENTS = ["A", "B", "C"]


def test_sums_months_where_all_components_reported():
    by_date = {
        date(2026, 5, 1): {"A": 100.0, "B": 20.0, "C": 5.0},
        date(2026, 6, 1): {"A": 101.0, "B": 21.0, "C": 5.5},
    }
    assert complete_month_totals(by_date, COMPONENTS) == [
        (date(2026, 5, 1), 125.0),
        (date(2026, 6, 1), 127.5),
    ]


def test_skips_month_missing_a_component():
    by_date = {
        date(2026, 5, 1): {"A": 100.0, "B": 20.0, "C": 5.0},
        date(2026, 6, 1): {"A": 101.0, "B": 21.0},  # C hasn't reported yet
    }
    # June must be absent entirely, not published as 122.0.
    assert complete_month_totals(by_date, COMPONENTS) == [(date(2026, 5, 1), 125.0)]


def test_ignores_extra_series_not_in_the_region():
    by_date = {
        date(2026, 5, 1): {"A": 100.0, "B": 20.0, "C": 5.0, "ZZ": 999.0},
    }
    assert complete_month_totals(by_date, COMPONENTS) == [(date(2026, 5, 1), 125.0)]


def test_returns_months_in_chronological_order():
    by_date = {
        date(2026, 6, 1): {"A": 1.0, "B": 1.0, "C": 1.0},
        date(2026, 4, 1): {"A": 1.0, "B": 1.0, "C": 1.0},
        date(2026, 5, 1): {"A": 1.0, "B": 1.0, "C": 1.0},
    }
    dates = [d for d, _ in complete_month_totals(by_date, COMPONENTS)]
    assert dates == sorted(dates)


def test_empty_input_yields_nothing():
    assert complete_month_totals({}, COMPONENTS) == []


def test_bay_area_region_is_the_full_nine_county_definition():
    name, components = REGIONS["BAY_AREA_TOTAL"]
    assert "9-county" in name
    # San Jose is a separate MSA from San Francisco and is ~29% of regional
    # employment — omitting it was the specific mistake this guards against.
    assert "SANJ906NA" in components
    assert set(components) == {
        "SANF806NA", "SANJ906NA", "SANT206NA", "VALL706NA", "NAPA906NA",
    }
