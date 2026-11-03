"""Tests for FedWatch range → bps conversion in econ_data/fed_expectations.py.

The model reports post-meeting target ranges; the code converts them to bps
from the current range. Cases pin the failures that produced bad rows: a wrong
assumed current range (now irrelevant, since the code supplies it) and a
dropped "no change" outcome that still summed close enough to 1.
"""
from econ_data.fed_expectations import _normalize_ranges

JAN_2027 = ('{"ranges": {"3.75-4.00": 0.048, "4.00-4.25": 0.272, "4.25-4.50": 0.427, '
            '"4.50-4.75": 0.218, "4.75-5.00": 0.035}, "source": "CME FedWatch"}')


def test_ranges_convert_to_bps_from_current_lower_bound():
    assert _normalize_ranges(JAN_2027, 3.75) == {
        0: 0.048, 25: 0.272, 50: 0.427, 75: 0.218, 100: 0.035}


def test_cuts_are_negative():
    text = '{"ranges": {"3.25-3.50": 0.2, "3.50-3.75": 0.5, "3.75-4.00": 0.3}}'
    assert _normalize_ranges(text, 3.75) == {-50: 0.2, -25: 0.5, 0: 0.3}


def test_json_inside_prose_and_fences_with_percent_and_dash_variants():
    text = ("Here you go:\n```json\n"
            '{"ranges": {"3.75%–4.00%": 0.25, "4.00 - 4.25": 0.75}}\n```')
    assert _normalize_ranges(text, 3.75) == {0: 0.25, 25: 0.75}


def test_dropped_outcome_is_rejected():
    # The old extractor omitted January's 4.8% hold; the rest sums to 0.952.
    text = ('{"ranges": {"4.00-4.25": 0.272, "4.25-4.50": 0.427, '
            '"4.50-4.75": 0.218, "4.75-5.00": 0.035}}')
    assert _normalize_ranges(text, 3.75) is None


def test_off_grid_or_unparseable_ranges_are_rejected():
    assert _normalize_ranges('{"ranges": {"3.80-4.05": 1.0}}', 3.75) is None
    assert _normalize_ranges('{"ranges": {"+25bp": 1.0}}', 3.75) is None
    assert _normalize_ranges('{"ranges": null, "source": "not found"}', 3.75) is None
    assert _normalize_ranges("no data", 3.75) is None
