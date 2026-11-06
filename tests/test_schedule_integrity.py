"""Scheduling safeguards behind the manual refresh work (plans/manual-refresh.md):
capture is only recorded after a save, the clock is ET, and only genuinely late
series count as overdue.
"""
import json
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from econ_data import export_sheets, fetch, pipeline
from econ_data.release_schedule import is_really_late


def _monthly(n=12, newest=date(2026, 10, 1)):
    out, d = [], newest
    for _ in range(n):
        out.append(d)
        d = (d.replace(day=1) - timedelta(days=1)).replace(day=1)
    return out


def test_monthly_series_on_daily_calendar_not_late_right_after_capture():
    # FEDFUNDS: captured Nov 2 on the daily H.15 calendar, next value ~Dec 1.
    assert not is_really_late(_monthly(), date(2026, 11, 2), 3, date(2026, 11, 20))


def test_monthly_series_late_once_past_normal_gap():
    # The real Redfin-style case: nothing new in months.
    assert is_really_late(_monthly(), date(2026, 6, 3), 3, date(2026, 11, 6))


def test_weekly_series_late_after_one_missed_cycle():
    weekly = [date(2026, 10, 30) - timedelta(days=7 * i) for i in range(12)]
    assert not is_really_late(weekly, date(2026, 10, 31), 3, date(2026, 11, 6))
    assert is_really_late(weekly, date(2026, 10, 31), 3, date(2026, 11, 12))


def test_series_with_no_history_counts_as_late():
    assert is_really_late([], None, 3, date(2026, 11, 6))


def test_process_runs_in_eastern_time():
    # 2026-11-06 is standard time, so the offset is -5 from UTC.
    probe = datetime(2026, 11, 6, 20, 25, tzinfo=timezone.utc).timestamp()
    assert time.localtime(probe).tm_hour == 15
    assert time.localtime(probe).tm_zone == "EST"


def test_fetch_all_leaves_the_schedule_to_the_caller(monkeypatch):
    """Regression: fetch_all used to mark rows CAPTURED before the save."""
    import econ_data.release_schedule as rs
    import econ_data.store as store

    class FakeFred:
        def __init__(self, api_key): pass
        def get_series(self, series_id, **_):
            return pd.Series([159293.0], index=[pd.Timestamp("2026-10-01")])
        def get_series_info(self, series_id):
            return {"last_updated": "2026-11-06 09:43:20-06"}

    def must_not_run(*a, **k):
        raise AssertionError("fetch_all must not mark captured")

    monkeypatch.setenv("FRED_API_KEY", "test")
    monkeypatch.setattr(fetch, "Fred", FakeFred)
    monkeypatch.setattr(fetch, "_detect_frequency", lambda sid: "monthly")
    monkeypatch.setattr(rs, "series_due_now", lambda sid: True)
    monkeypatch.setattr(rs, "mark_captured", must_not_run)
    monkeypatch.setattr(store, "clear_fetch_error", lambda sid: None)

    result = fetch.fetch_all([("PAYEMS", "Payrolls")],
                             last_dates={"PAYEMS": date(2026, 9, 1)})

    assert [o.date for o in result["new"]] == [date(2026, 10, 1)]
    assert result["counts"]["PAYEMS"] == 1
    assert result["source_updated"]["PAYEMS"] == datetime(
        2026, 11, 6, 15, 43, 20, tzinfo=timezone.utc)


def test_source_timestamp_failure_does_not_lose_the_capture(monkeypatch):
    class BrokenFred:
        def __init__(self, api_key): pass
        def get_series_info(self, series_id):
            raise RuntimeError("FRED hiccup")

    monkeypatch.setenv("FRED_API_KEY", "test")
    monkeypatch.setattr(fetch, "Fred", BrokenFred)
    assert fetch._source_last_updated("PAYEMS") is None


def test_mark_captured_passes_source_timestamp(monkeypatch):
    import econ_data.release_schedule as rs
    calls, advanced = [], []
    when = datetime(2026, 11, 6, 15, 43, tzinfo=timezone.utc)

    def fake(series_id, period_end, source_last_updated=None):
        calls.append((series_id, period_end, source_last_updated))
        return True

    monkeypatch.setattr(rs, "mark_captured", fake)
    obs = fetch.Observation("PAYEMS", "Payrolls", date(2026, 10, 1), 159293.0)
    pipeline._mark_captured([obs], advanced, {"PAYEMS": when})

    assert calls == [("PAYEMS", date(2026, 10, 1), when)]
    assert advanced == ["PAYEMS"]


def test_sheets_manifest_uses_explicit_utc(tmp_path):
    """Apps Script string-compares these against toISOString(), so ET would
    silently skip updates."""
    path = tmp_path / "last_updated.json"
    export_sheets.write_manifest(["jolts"], path)
    stamp = json.loads(path.read_text())["jolts"]
    assert stamp.endswith("Z")
    parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    assert abs((datetime.now(timezone.utc) - parsed).total_seconds()) < 60


def test_overdue_handles_rows_without_a_capture_time(monkeypatch):
    """Rows from before the Postgres migration have NULL captured_at."""
    import econ_data.release_schedule as rs

    class Result:
        def __init__(self, rows): self.rows = rows
        def fetchall(self): return self.rows

    class FakeConn:
        def execute(self, sql, params=None):
            if "release_schedule" in sql:
                return Result([("OLD", date(2026, 10, 1), 3, None, 2)])
            return Result([("OLD", date(2026, 3, 1), None),
                           ("OLD", date(2026, 2, 1), None)])

    monkeypatch.setattr(rs, "connect", lambda: FakeConn())
    overdue = rs.get_overdue(today=date(2026, 11, 6))
    assert [(r["series_id"], r["days_late"], r["missed"]) for r in overdue] == [("OLD", 36, 2)]
