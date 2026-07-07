"""Tests for the Case-Shiller sanity guard in fetch.py.

Regression cover for the 2026-07 incident where FRED published a corrupted
vintage with dollar-scale values (e.g. 890422) for several Case-Shiller city
series, polluting the DB.
"""
from datetime import date

from econ_data.fetch import Observation, _drop_implausible, _case_shiller_ids


def _obs(series_id, value, d=date(2026, 4, 1)):
    return Observation(series_id=series_id, name=series_id, date=d, value=value)


def test_case_shiller_group_is_recognized():
    ids = _case_shiller_ids()
    # National + a couple of metros must be present for the guard to apply.
    assert "CSUSHPISA" in ids
    assert "ATXRSA" in ids


def test_drops_dollar_scale_garbage_for_case_shiller():
    obs = [
        _obs("ATXRSA", 249.5, date(2026, 3, 1)),  # sane index value
        _obs("ATXRSA", 355217.0),                  # corrupted dollar-scale value
    ]
    kept = _drop_implausible("ATXRSA", obs)
    assert [o.value for o in kept] == [249.5]


def test_keeps_valid_case_shiller_values():
    obs = [_obs("CSUSHPISA", 330.873)]
    assert _drop_implausible("CSUSHPISA", obs) == obs


def test_keeps_low_historical_values():
    # Early-history metro values are legitimately in the 40s (Portland bottomed
    # near 41 in 1987). The guard must not drop these.
    obs = [_obs("POXRSA", 41.27, d=date(1987, 1, 1))]
    assert _drop_implausible("POXRSA", obs) == obs


def test_rejects_below_lower_bound():
    obs = [_obs("LXXRSA", 5.0)]
    assert _drop_implausible("LXXRSA", obs) == []


def test_non_case_shiller_series_untouched():
    # A large value is legitimate for non-CS series (e.g. payrolls); the guard
    # must not touch series outside the Case-Shiller group.
    obs = [_obs("PAYEMS", 158798.0)]
    assert _drop_implausible("PAYEMS", obs) == obs
