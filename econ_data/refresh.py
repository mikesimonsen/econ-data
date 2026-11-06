"""Targeted refresh of one series, requested from the page button.

Runs on its own short-lived Fly machine, started by web/server.py. It fetches
only the source that series comes from, records a verdict the page can show,
and rebuilds the page only if something new arrived. See plans/manual-refresh.md.
"""
from __future__ import annotations

import contextlib
import io
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import date
from typing import Optional

from psycopg.types.json import Jsonb

from econ_data import pipeline, run_lock
from econ_data.calc_regional import ALL_IDS as REGIONAL_IDS
from econ_data.calc_rolling import ROLLING_SERIES
from econ_data.config import all_series, fred_series, load
from econ_data.db import connect, connect_checked
from econ_data.fetch import _source_last_updated, fetch_all
from econ_data.fetch_bls import build_series_map, fetch_bls
from econ_data.fetch_nar import SERIES_MAP as NAR_SERIES_MAP, fetch_nar
from econ_data.store import detect_and_save_revisions, get_last_dates, save, save_fetch_log

# Sources read from a local drop folder (import_files/ is gitignored), so a
# cloud machine can never fetch them.
MANUAL_SOURCES = {"altos", "xactus"}

SOURCE_FETCHERS = {
    "mnd": pipeline.fetch_mnd,
    "web": pipeline.fetch_web,
    "factset": pipeline.fetch_factset,
    "confboard": pipeline.fetch_confboard,
    "realtor": pipeline.fetch_realtor,
    "redfin": pipeline.fetch_redfin,
    "zillow": pipeline.fetch_zillow,
}

_SKIPPED = re.compile(r"SKIPPED (.+?) — (.+)")


@dataclass
class Target:
    series_id: str
    name: str
    kind: str  # fred | bls | manual | computed | <a SOURCE_FETCHERS key>
    group: str = ""


def classify(cfg: dict, series_id: str) -> Optional[Target]:
    """Where does this series come from? None if it isn't in config.yaml."""
    rolling = {derived for _, _, derived, _ in ROLLING_SERIES}
    for gid, group in cfg.get("groups", {}).items():
        for s in group["series"]:
            if s["id"] != series_id:
                continue
            source = group.get("source")
            if source in MANUAL_SOURCES:
                kind = "manual"
            elif (source == "calculated" or series_id in rolling
                  or series_id in REGIONAL_IDS):
                kind = "computed"
            elif source:
                kind = source
            elif s.get("bls_id"):
                kind = "bls"
            else:
                kind = "fred"
            return Target(series_id, s["name"], kind, gid)
    return None


def decide_verdict(target: Target, *, before: Optional[date],
                   after: Optional[tuple], count: int = 0,
                   rejected: bool = False, errors: Optional[list] = None,
                   due: Optional[date] = None, next_release: Optional[date] = None,
                   source_updated=None) -> dict:
    """Why the series is, or isn't, up to date. after is (date, value) of the
    latest stored observation. Compares stored periods rather than the fetch's
    row count, because some sources re-return their recent history every time.
    """
    latest_date = after[0] if after else None
    out = {"latest_date": latest_date.isoformat() if latest_date else None,
           "latest_value": after[1] if after else None}

    def verdict(code: str, message: str) -> dict:
        return {**out, "verdict": code, "message": message}

    if target.kind == "manual":
        return verdict("NEEDS_MANUAL",
                       f"{target.name} comes from a file you import locally, so "
                       f"the cloud can't fetch it. Latest period: {latest_date}.")
    if target.kind == "computed":
        return verdict("DERIVED",
                       f"{target.name} is calculated from other series. Refresh "
                       f"its inputs instead. Latest period: {latest_date}.")
    if latest_date and (before is None or latest_date > before):
        return verdict("CAPTURED",
                       f"New data: {latest_date} = {after[1]:,g} "
                       f"(previously {before}).")
    if rejected:
        return verdict("REJECTED",
                       "The source returned a value that failed the sanity check, "
                       f"so it wasn't stored. Latest good period: {latest_date}.")
    if errors or count == -1:
        detail = "; ".join(errors) if errors else "see the Fly logs"
        return verdict("SOURCE_ERROR", f"The fetch failed: {detail}")
    if due:
        updated = (f" The source last updated {source_updated.astimezone():%b %d at %H:%M %Z}."
                   if source_updated else "")
        return verdict("NOT_PUBLISHED",
                       f"A release was due {due}, but the source has nothing newer "
                       f"than {latest_date} yet.{updated}")
    next_note = f" Next release: {next_release}." if next_release else ""
    return verdict("NOTHING_DUE", f"You have the latest: {latest_date}.{next_note}")


def relevant_errors(errors: list, series_id: str, known_ids: set) -> list:
    """Keep errors for this series and source-wide ones ("Redfin download"),
    but not another series' failure from the same source."""
    return [message for label, message in errors
            if label == series_id or label not in known_ids]


class _Tee(io.TextIOBase):
    """Fetchers report failures by printing SKIPPED lines. Keep those lines
    going to the logs while also collecting them for the verdict."""

    def __init__(self, stream):
        self.stream, self.failures = stream, []

    def write(self, text):
        match = _SKIPPED.search(text)
        if match:
            self.failures.append((match.group(1), match.group(2).strip()))
        return self.stream.write(text)

    def flush(self):
        self.stream.flush()


def _store_fred(result: dict) -> list:
    """Same order as the scheduled runs: revisions, then save, then mark."""
    if result.get("checked"):
        save_fetch_log(result["checked"])
    fetched = result.get("all_fetched", [])
    revisions = detect_and_save_revisions(fetched)
    if fetched:
        save(fetched)
    advanced: list = []
    pipeline._mark_captured(result["new"], advanced, result["source_updated"])
    result["captured_advanced"] = advanced
    return revisions


def fetch_target(cfg: dict, target: Target, last_dates: dict) -> tuple[dict, list, list]:
    """Fetch only what this series needs. Returns (result, revisions, rejected)."""
    result = pipeline._empty_result()
    revisions: list = []
    rejected: list = []

    if target.kind == "fred":
        if target.series_id in {sid for sid, _ in NAR_SERIES_MAP.values()}:
            nar = fetch_nar(last_dates=last_dates)
            if nar["new"]:
                save(nar["new"])
                pipeline._mark_captured(nar["new"], result["captured_advanced"])
                last_dates = get_last_dates()
            pipeline._merge(result, nar)

        # The target alone first: if its release isn't out, there's no reason
        # to spend a minute checking the rest of the release.
        first = fetch_all([(target.series_id, target.name)],
                          last_dates=last_dates, force=True)
        revisions += _store_fred(first)
        rejected += first["rejected"]
        pipeline._merge(result, first)

        if first["new"]:
            rows = connect().execute(
                """
                SELECT series_id FROM series_release
                WHERE release_id = (SELECT release_id FROM series_release
                                    WHERE series_id = %s)
                """,
                (target.series_id,),
            ).fetchall()
            siblings = {r[0] for r in rows} - {target.series_id}
            pairs = [p for p in fred_series(cfg) if p[0] in siblings]
            if pairs:
                rest = fetch_all(pairs, last_dates=last_dates, force=True)
                revisions += _store_fred(rest)
                pipeline._merge(result, rest)
    else:
        if target.kind == "bls":
            fetched = fetch_bls(build_series_map(cfg), last_dates=last_dates)
        else:
            fetched = SOURCE_FETCHERS[target.kind](last_dates=last_dates)
        if fetched["new"]:
            save(fetched["new"])
            pipeline._mark_captured(fetched["new"], result["captured_advanced"])
        pipeline._merge(result, fetched)

    return result, revisions, rejected


def _latest(series_id: str) -> Optional[tuple]:
    return connect().execute(
        "SELECT date, value FROM observations WHERE series_id = %s "
        "ORDER BY date DESC LIMIT 1",
        (series_id,),
    ).fetchone()


def _schedule_context(series_id: str) -> tuple[Optional[date], Optional[date]]:
    """(oldest release due and uncaptured, next future release)."""
    row = connect().execute(
        """
        SELECT MIN(scheduled_release) FILTER (WHERE scheduled_release <= CURRENT_DATE),
               MIN(scheduled_release) FILTER (WHERE scheduled_release > CURRENT_DATE)
        FROM release_schedule
        WHERE series_id = %s AND status IN ('PENDING', 'OVERDUE')
        """,
        (series_id,),
    ).fetchone()
    return row[0], row[1]


def _update(request_id: str, **fields) -> None:
    """Field names only ever come from this module's own calls."""
    assignments = ", ".join(f"{name} = %s" for name in fields)
    prefix = f"{assignments}, " if assignments else ""
    connect_checked().execute(
        f"UPDATE refresh_requests SET {prefix}heartbeat_at = NOW() WHERE id = %s",
        (*fields.values(), request_id),
    )


def _finish(request_id: str, stage: str, **fields) -> None:
    connect_checked().execute(
        "UPDATE refresh_requests SET finished_at = NOW() WHERE id = %s", (request_id,))
    _update(request_id, stage=stage, **fields)


def _heartbeat_hook(request_id: str, every: int = 20):
    """Called on every pipeline log line, so the panel can tell a long render
    apart from a dead machine. A failed heartbeat must not stop the run."""
    last = [time.monotonic()]

    def hook():
        if time.monotonic() - last[0] >= every:
            last[0] = time.monotonic()
            try:
                _update(request_id)
            except Exception:
                pass
    return hook


def run_request(request_id: str) -> None:
    row = connect().execute(
        "SELECT series_id FROM refresh_requests WHERE id = %s", (request_id,)
    ).fetchone()
    if not row:
        raise SystemExit(f"Unknown refresh request {request_id}")
    series_id = row[0]
    _update(request_id, stage="started")

    cfg = load()
    target = classify(cfg, series_id)
    if target is None:
        _finish(request_id, "failed", error=f"{series_id} is not in config.yaml.")
        return

    if target.kind in ("manual", "computed"):
        verdict = decide_verdict(target, before=None, after=_latest(series_id))
        _finish(request_id, "done", verdict=Jsonb(verdict))
        return

    holder = f"refresh:{request_id}"
    _update(request_id, stage="waiting")
    if not run_lock.acquire(holder, on_wait=lambda: _update(request_id)):
        _finish(request_id, "failed",
                error="Another pipeline run held the lock for over 12 minutes.")
        return

    try:
        pipeline.set_log_hook(_heartbeat_hook(request_id))
        _update(request_id, stage="fetching")

        last_dates = get_last_dates()
        before = _latest(series_id)
        tee = _Tee(sys.stdout)
        with contextlib.redirect_stdout(tee):
            result, revisions, rejected = fetch_target(cfg, target, last_dates)

        after = _latest(series_id)
        due, next_release = _schedule_context(series_id)
        known = {sid for sid, _ in all_series(cfg)}
        verdict = decide_verdict(
            target,
            before=before[0] if before else None,
            after=after,
            count=result["counts"].get(series_id, 0),
            rejected=series_id in rejected,
            errors=relevant_errors(tee.failures, series_id, known),
            due=due,
            next_release=next_release,
            source_updated=_source_last_updated(series_id) if target.kind == "fred" else None,
        )
        _update(request_id, stage="saved", verdict=Jsonb(verdict))
        pipeline.log(f"Refresh verdict for {series_id}: {verdict['verdict']}")

        current = get_last_dates()
        advanced = any(d > last_dates[sid] if sid in last_dates else True
                       for sid, d in current.items())
        if not advanced and not revisions:
            _finish(request_id, "done")
            return

        _update(request_id, stage="rendering")
        pipeline.recompute_derived(result)
        epoch = int(time.time())
        outcome = pipeline.post_process(cfg, all_series(cfg), result, revisions,
                                        build_stamp={"id": request_id, "epoch": epoch})
        if outcome["pushed"]:
            # Not finished yet: web/server.py marks it live once Pages serves it.
            _update(request_id, stage="pushed", build_epoch=epoch)
        else:
            step = "push" if outcome["briefing_ok"] else "page rebuild"
            _finish(request_id, "failed",
                    error=f"The data is saved, but the {step} failed. See the Fly logs.")
    finally:
        pipeline.set_log_hook(None)
        run_lock.release(holder)


def main() -> None:
    request_id = os.environ["REFRESH_REQUEST_ID"]
    try:
        run_request(request_id)
    except Exception as e:
        traceback.print_exc()
        last_line = f"{type(e).__name__}: {e}".splitlines()[0][:300]
        _finish(request_id, "failed", error=f"The refresh job crashed: {last_line}")
        raise
