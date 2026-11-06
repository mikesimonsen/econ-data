"""The refresh button's verdicts and service rules (plans/manual-refresh.md).
The verdict is the point of the feature: the page must say why data is missing.
"""
import importlib.util
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from econ_data.config import load
from econ_data.refresh import (Target, _Tee, classify, decide_verdict,
                               relevant_errors)
from econ_data.refresh_ui import refresh_widget

_spec = importlib.util.spec_from_file_location(
    "refresh_server", Path(__file__).parent.parent / "web" / "server.py")
server = importlib.util.module_from_spec(_spec)
sys.modules["refresh_server"] = server
_spec.loader.exec_module(server)

PAYEMS = Target("PAYEMS", "Total Nonfarm Payrolls", "fred", "labor-headlines")
SEP = date(2026, 9, 1)
OCT = date(2026, 10, 1)


@pytest.mark.parametrize("series_id,kind", [
    ("PAYEMS", "fred"),
    ("REDFIN_INVENTORY", "redfin"),
    ("CB_CCI", "confboard"),
    ("MND_30YR_FIXED", "mnd"),
    ("XACTUS_MII", "manual"),
    ("XACTUS_MII_4WK", "manual"),
    ("ALTOS_INVENTORY", "manual"),
    ("MBA_PURCHASE_4WK", "computed"),
    ("PRICE_TO_INCOME", "computed"),
])
def test_series_are_routed_to_their_real_source(series_id, kind):
    assert classify(load(), series_id).kind == kind


def test_unknown_series_has_no_target():
    assert classify(load(), "NOPE") is None


def test_captured_when_a_new_period_is_stored():
    v = decide_verdict(PAYEMS, before=SEP, after=(OCT, 159293.0), count=1)
    assert v["verdict"] == "CAPTURED"
    assert v["latest_date"] == "2026-10-01" and v["latest_value"] == 159293.0
    assert "159,293" in v["message"]


def test_repeat_rows_from_a_source_are_not_a_capture():
    """Some sources re-return their recent window; rows alone prove nothing."""
    v = decide_verdict(PAYEMS, before=OCT, after=(OCT, 159293.0), count=5,
                       next_release=date(2026, 12, 4))
    assert v["verdict"] == "NOTHING_DUE"


def test_not_published_when_due_but_source_has_nothing_newer():
    updated = datetime(2026, 10, 2, 13, 30, tzinfo=timezone.utc)
    v = decide_verdict(PAYEMS, before=SEP, after=(SEP, 159044.0),
                       due=date(2026, 11, 6), source_updated=updated)
    assert v["verdict"] == "NOT_PUBLISHED"
    assert "2026-11-06" in v["message"] and "Oct 02" in v["message"]


def test_nothing_due_names_next_release():
    v = decide_verdict(PAYEMS, before=OCT, after=(OCT, 159293.0),
                       next_release=date(2026, 12, 4))
    assert v["verdict"] == "NOTHING_DUE" and "2026-12-04" in v["message"]


def test_source_error_shows_the_actual_error():
    v = decide_verdict(PAYEMS, before=SEP, after=(SEP, 1.0), count=-1,
                       errors=["403 Client Error: Forbidden"], due=date(2026, 11, 6))
    assert v["verdict"] == "SOURCE_ERROR" and "403" in v["message"]


def test_sanity_rejection_is_reported_over_not_published():
    v = decide_verdict(PAYEMS, before=SEP, after=(SEP, 1.0), rejected=True,
                       due=date(2026, 11, 6))
    assert v["verdict"] == "REJECTED"


def test_capture_wins_over_another_series_error():
    v = decide_verdict(PAYEMS, before=SEP, after=(OCT, 1.0), errors=["x"])
    assert v["verdict"] == "CAPTURED"


def test_manual_and_computed_explain_themselves():
    manual = Target("XACTUS_MII", "Xactus", "manual")
    computed = Target("PRICE_TO_INCOME", "Price to Income", "computed")
    assert decide_verdict(manual, before=None, after=(SEP, 1.0))["verdict"] == "NEEDS_MANUAL"
    assert decide_verdict(computed, before=None, after=(SEP, 1.0))["verdict"] == "DERIVED"


def test_other_series_errors_are_ignored_but_source_wide_ones_count():
    errors = [("CB_CCI_PRESENT", "403"), ("Redfin download", "403 bucket")]
    known = {"CB_CCI", "CB_CCI_PRESENT"}
    assert relevant_errors(errors, "CB_CCI", known) == ["403 bucket"]
    assert relevant_errors(errors, "CB_CCI_PRESENT", known) == ["403", "403 bucket"]


def test_tee_collects_skipped_lines_and_still_logs_them():
    class Sink:
        text = ""
        def write(self, t):
            self.text += t
            return len(t)
        def flush(self): pass

    sink = Sink()
    tee = _Tee(sink)
    tee.write("[2026-11-06 13:58:58] SKIPPED CB_CCI — 403 Client Error: Forbidden\n")
    tee.write("[2026-11-06 13:58:59] Fetching something\n")
    assert tee.failures == [("CB_CCI", "403 Client Error: Forbidden")]
    assert "Fetching something" in sink.text


@pytest.mark.parametrize("header,key,ok", [
    ("Bearer right", "right", True),
    ("Bearer wrong", "right", False),
    (None, "right", False),
    ("Bearer ", "", False),
    ("Bearer right", None, False),
])
def test_key_check(header, key, ok):
    assert server.authorized(header, key) is ok


@pytest.mark.parametrize("stage,age,finished,expected", [
    ("fetching", 10, False, "fetching"),
    ("fetching", 200, False, "stalled"),
    ("starting", 200, False, "stalled"),
    ("pushed", 200, False, "pushed"),
    ("pushed", 1000, False, "publish_slow"),
    ("done", 5000, True, "done"),
])
def test_stage_reflects_a_silent_job(stage, age, finished, expected):
    assert server.public_stage(stage, age, finished) == expected


def test_one_off_machine_cannot_be_picked_by_cron():
    template = {"image": "img", "env": {"RUN_SCRIPT": "run.py", "KEEP": "1"},
                "metadata": {"fly_process_group": "app"}}
    config = server.machine_config(template, "abc")
    assert config["env"] == {"RUN_SCRIPT": "run_refresh.py", "KEEP": "1",
                             "REFRESH_REQUEST_ID": "abc"}
    assert config["auto_destroy"] is True
    assert config["restart"] == {"policy": "no"}
    assert "fly_process_group" not in config["metadata"]
    assert template["env"]["RUN_SCRIPT"] == "run.py"  # the original is untouched


def test_start_machine_copies_the_app_group_machine():
    calls = []

    def fly(method, url, body=None):
        calls.append((method, body))
        if method == "GET":
            return [
                {"id": "b", "region": "iad", "config": {"image": "new", "metadata": {"fly_process_group": "app"}}},
                {"id": "x", "region": "iad", "config": {"image": "old", "metadata": {}}},
            ]
        return {"id": "created"}

    assert server.start_machine("req", fly=fly) == "created"
    assert calls[1][1]["config"]["image"] == "new"


def test_widget_holds_no_secrets_and_stays_hidden_without_a_key():
    widget = refresh_widget()
    assert "econ-data-web.fly.dev" in widget
    assert "__SERVICE_URL__" not in widget
    assert "if (!read(KEY_NAME)) return;" in widget
    assert "innerHTML" not in widget
