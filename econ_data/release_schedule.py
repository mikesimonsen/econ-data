"""Release-schedule-driven fetch & staleness detection.

Replaces the cooldown + last_checked heuristic in fetch.py with a deterministic
model: every series has a known publication schedule, expressed as either
(a) FRED's release calendar API for FRED series, or (b) a declared cadence
block in config.yaml for non-FRED sources (Altos, MND, BLS direct, web
scrapers, etc.). The release_schedule table holds one row per
(series_id, scheduled_release) with status PENDING|CAPTURED|OVERDUE|DELAYED.

Today's bug ("FRED hadn't published yet at 7 AM, marked checked, won't retry")
becomes structurally impossible: instead of "asked FRED today" the truth signal
is "FRED's last_updated > our last observation."

Public surface:
    refresh_fred_calendar(series_ids)        weekly job: pull schedule from FRED
    refresh_declared_calendar(cfg)           weekly job: compute schedule from cadence rules
    series_due_now(series_id)                fetch path: is there a pending release due?
    mark_captured(series_id, period_end, ts) fetch path: transition PENDING/OVERDUE → CAPTURED
    sweep_overdue()                          end-of-pipeline: PENDING past grace → OVERDUE
    get_overdue() / get_captured_today() / get_upcoming(days)   briefing inputs
"""
from __future__ import annotations

import calendar
import os
import time
from datetime import date, datetime, time as time_t, timedelta
from typing import Optional

import requests

from econ_data.db import connect

# How far ahead to populate the schedule table per series.
DEFAULT_HORIZON_DAYS = 90

FRED_API_BASE = "https://api.stlouisfed.org/fred"
FRED_RATE_LIMIT_DELAY = 0.6  # seconds between calls


# ─────────────────────────────────────────────────────────────────────────
# Cadence rule engine — for non-FRED series declared in config.yaml
# ─────────────────────────────────────────────────────────────────────────

WEEKDAY_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday",
                 "saturday", "sunday"]
WEEKDAY_INDEX = {n: i for i, n in enumerate(WEEKDAY_NAMES)}


def _next_weekday(d: date, weekday_idx: int) -> date:
    """Smallest d' >= d with d'.weekday() == weekday_idx."""
    delta = (weekday_idx - d.weekday()) % 7
    return d + timedelta(days=delta)


def _nth_weekday_of_month(year: int, month: int, n: int, weekday_idx: int) -> date:
    """E.g. n=1, weekday_idx=4 → first Friday. n=-1 → last."""
    if n > 0:
        first = date(year, month, 1)
        first_wd = _next_weekday(first, weekday_idx)
        return first_wd + timedelta(days=7 * (n - 1))
    # last
    last_day = calendar.monthrange(year, month)[1]
    last = date(year, month, last_day)
    delta = (last.weekday() - weekday_idx) % 7
    return last - timedelta(days=delta)


def _parse_monthly_rule(rule: str) -> tuple[str, int, int]:
    """Returns ('nth-weekday'|'day'|'around', n_or_day, weekday_idx_or_zero).

    Supported forms:
      'first-friday', 'second-thursday', 'last-tuesday', etc.
      'day-15'          → exact calendar day 15
      'around-25'       → calendar day 25 (grace_days handles slippage)
    """
    if rule.startswith("day-"):
        return ("day", int(rule.split("-", 1)[1]), 0)
    if rule.startswith("around-"):
        return ("around", int(rule.split("-", 1)[1]), 0)

    n_word_to_int = {"first": 1, "second": 2, "third": 3, "fourth": 4,
                     "fifth": 5, "last": -1}
    parts = rule.split("-", 1)
    if len(parts) != 2 or parts[0] not in n_word_to_int:
        raise ValueError(f"Unrecognized monthly rule: {rule}")
    n = n_word_to_int[parts[0]]
    if parts[1] not in WEEKDAY_INDEX:
        raise ValueError(f"Unrecognized weekday in rule: {rule}")
    return ("nth-weekday", n, WEEKDAY_INDEX[parts[1]])


def compute_next_release_dates(cadence: dict, after: date,
                               count: int = 12) -> list[date]:
    """Return the next `count` scheduled release dates strictly after `after`.

    Cadence schema (from config.yaml):
        frequency: daily | weekly | monthly | quarterly | derived
        rule:      see below per frequency
        release_time_et: "HH:MM" (informational; not used for date math)
        grace_days: int (informational; used downstream for OVERDUE)

    Rules:
        daily:    'weekday' (M-F)
        weekly:   weekday name ('monday', 'thursday', ...)
        monthly:  'first-friday' | 'last-tuesday' | 'day-15' | 'around-25'
        derived:  returns [] (no own schedule)
    """
    freq = cadence.get("frequency")
    if freq == "derived":
        return []
    rule = cadence.get("rule")
    out: list[date] = []
    cursor = after

    if freq == "daily":
        # 'weekday' = M-F. Skip weekends.
        if rule != "weekday":
            raise ValueError(f"Unsupported daily rule: {rule}")
        while len(out) < count:
            cursor += timedelta(days=1)
            if cursor.weekday() < 5:
                out.append(cursor)
        return out

    if freq == "weekly":
        if rule not in WEEKDAY_INDEX:
            raise ValueError(f"Weekly rule must be a weekday name; got: {rule}")
        wd = WEEKDAY_INDEX[rule]
        nxt = _next_weekday(cursor + timedelta(days=1), wd)
        while len(out) < count:
            out.append(nxt)
            nxt += timedelta(days=7)
        return out

    if freq == "monthly":
        kind, n_or_day, wd = _parse_monthly_rule(rule)
        # Walk forward month by month from `after` until we have enough
        y, m = cursor.year, cursor.month
        while len(out) < count:
            if kind == "nth-weekday":
                d = _nth_weekday_of_month(y, m, n_or_day, wd)
            elif kind in ("day", "around"):
                last_day = calendar.monthrange(y, m)[1]
                d = date(y, m, min(n_or_day, last_day))
            if d > cursor:
                out.append(d)
            # advance month
            m += 1
            if m > 12:
                m = 1
                y += 1
        return out

    if freq == "quarterly":
        # Simple model: every 3 months at day-15
        # (quarterly series are rare in our config; expand if needed)
        m = cursor.month + 3 - ((cursor.month - 1) % 3)
        y = cursor.year
        while len(out) < count:
            if m > 12:
                m -= 12
                y += 1
            out.append(date(y, m, 15))
            m += 3
        return out

    raise ValueError(f"Unsupported frequency: {freq}")


# ─────────────────────────────────────────────────────────────────────────
# Persistence helpers
# ─────────────────────────────────────────────────────────────────────────

_INSERT_SCHEDULE = """
INSERT INTO release_schedule
    (series_id, scheduled_release, expected_time_et, grace_days, status, updated_at)
VALUES (%s, %s, %s, %s, 'PENDING', %s)
ON CONFLICT (series_id, scheduled_release) DO UPDATE SET
    expected_time_et = EXCLUDED.expected_time_et,
    grace_days       = EXCLUDED.grace_days,
    updated_at       = EXCLUDED.updated_at
WHERE release_schedule.status = 'PENDING'
"""


def _upsert_pending(rows: list[tuple]) -> int:
    """Bulk-upsert PENDING schedule rows. Skips updates on already-CAPTURED rows."""
    if not rows:
        return 0
    con = connect()
    with con.transaction(), con.cursor() as cur:
        cur.executemany(_INSERT_SCHEDULE, rows)
        return cur.rowcount


def _parse_time_et(s: Optional[str]) -> Optional[time_t]:
    if not s:
        return None
    h, m = s.split(":")
    return time_t(int(h), int(m))


# ─────────────────────────────────────────────────────────────────────────
# Declared-cadence calendar refresher (config.yaml driven)
# ─────────────────────────────────────────────────────────────────────────

def refresh_declared_calendar(cfg: dict, horizon_days: int = DEFAULT_HORIZON_DAYS) -> int:
    """Populate release_schedule from cadence: blocks in config.yaml.

    For each group with a cadence:, compute the next N release dates and upsert
    PENDING rows for every series in the group whose source is non-FRED. FRED
    series in mixed groups (e.g. cpi-metro has both FRED and BLS direct) are
    skipped here — refresh_fred_calendar handles them.
    """
    today = date.today()
    until = today + timedelta(days=horizon_days)
    now = datetime.now()
    rows: list[tuple] = []

    for gid, group in cfg.get("groups", {}).items():
        cadence = group.get("cadence")
        if not cadence:
            continue
        if cadence.get("frequency") == "derived":
            continue  # calculated series have no own cadence

        try:
            dates = compute_next_release_dates(cadence, today - timedelta(days=1),
                                               count=horizon_days)
        except ValueError as e:
            print(f"  [{gid}] cadence error: {e}")
            continue
        dates = [d for d in dates if d <= until]
        if not dates:
            continue

        time_et = _parse_time_et(cadence.get("release_time_et"))
        grace = int(cadence.get("grace_days", 3))
        group_source = group.get("source")

        for s in group["series"]:
            sid = s["id"]
            # In a mixed group (no source: tag), only series with bls_id are
            # non-FRED; pure FRED ones get their schedule from FRED's API.
            if not group_source and not s.get("bls_id"):
                continue
            for d in dates:
                rows.append((sid, d, time_et, grace, now))

    return _upsert_pending(rows)


# ─────────────────────────────────────────────────────────────────────────
# FRED-API calendar refresher
# ─────────────────────────────────────────────────────────────────────────

def _fred_get(path: str, **params) -> dict:
    """GET a FRED endpoint with simple retry on 500s (FRED is intermittently flaky)."""
    params["api_key"] = os.environ["FRED_API_KEY"]
    params["file_type"] = "json"
    last_err = None
    for attempt in range(3):
        try:
            r = requests.get(f"{FRED_API_BASE}/{path}", params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            last_err = e
            if e.response is not None and e.response.status_code >= 500:
                time.sleep(1 + attempt)
                continue
            raise
        except requests.RequestException as e:
            last_err = e
            time.sleep(1 + attempt)
    raise last_err


def _ensure_series_release_mapping(series_ids: list[str]) -> dict[str, int]:
    """Return {series_id: release_id}. Populates series_release for unknown sids."""
    con = connect()
    rows = con.execute(
        "SELECT series_id, release_id FROM series_release "
        "WHERE series_id = ANY(%s)", (series_ids,)
    ).fetchall()
    known = dict(rows)
    missing = [sid for sid in series_ids if sid not in known]
    if not missing:
        return known

    print(f"  Looking up release_id for {len(missing)} series...")
    now = datetime.now()
    for i, sid in enumerate(missing):
        try:
            data = _fred_get(f"series/release", series_id=sid)
            rid = int(data["releases"][0]["id"])
            known[sid] = rid
            con.execute(
                "INSERT INTO series_release (series_id, release_id, fetched_at) "
                "VALUES (%s, %s, %s) ON CONFLICT (series_id) DO UPDATE SET "
                "release_id = EXCLUDED.release_id, fetched_at = EXCLUDED.fetched_at",
                (sid, rid, now),
            )
        except Exception as e:
            print(f"    {sid}: {e}")
        if i + 1 < len(missing):
            time.sleep(FRED_RATE_LIMIT_DELAY)
    return known


def synthesize_from_history(series_ids: list[str],
                            horizon_days: int = DEFAULT_HORIZON_DAYS) -> int:
    """Fallback for series where FRED has no forward calendar.

    Uses the observation history to estimate a typical inter-arrival gap and
    project the next N expected releases. Used for irregular FRED series
    (Cleveland Fed expectations, Atlanta Fed wage tracker, annual data).
    """
    if not series_ids:
        return 0
    today = date.today()
    until = today + timedelta(days=horizon_days)
    now = datetime.now()
    rows: list[tuple] = []
    con = connect()
    for sid in series_ids:
        history = con.execute(
            "SELECT date FROM observations WHERE series_id = %s "
            "ORDER BY date DESC LIMIT 12",
            (sid,),
        ).fetchall()
        if len(history) < 3:
            continue
        gaps = [(history[i][0] - history[i+1][0]).days for i in range(len(history)-1)]
        gaps.sort()
        median_gap = gaps[len(gaps) // 2]
        if median_gap < 1:
            continue
        last_obs = history[0][0]
        # Add the typical lag between period end and publication. We don't have
        # this precisely, but for most monthly series it's ~3-6 weeks.
        # Approximation: schedule the next release at last_obs + 1.5 * median_gap.
        nxt = last_obs + timedelta(days=int(median_gap * 1.5))
        while nxt <= until:
            if nxt > today:
                rows.append((sid, nxt, None, 7, now))
            nxt += timedelta(days=median_gap)
    return _upsert_pending(rows)


def refresh_fred_calendar(series_ids: list[str],
                          horizon_days: int = DEFAULT_HORIZON_DAYS) -> int:
    """Pull upcoming release dates from FRED and upsert PENDING schedule rows.

    For each FRED series, looks up its release_id (cached in series_release)
    then queries FRED's release/dates endpoint for upcoming dates. Schedules
    are upserted as PENDING; existing CAPTURED rows are not overwritten.
    """
    today = date.today()
    until = today + timedelta(days=horizon_days)
    mapping = _ensure_series_release_mapping(series_ids)

    # Group series by release_id so we make one API call per release, not
    # one per series. Several hundred FRED series share ~30 releases.
    by_release: dict[int, list[str]] = {}
    for sid, rid in mapping.items():
        by_release.setdefault(rid, []).append(sid)

    print(f"  Pulling release dates for {len(by_release)} FRED releases...")
    now = datetime.now()
    rows: list[tuple] = []
    for i, (rid, sids) in enumerate(by_release.items()):
        try:
            data = _fred_get(
                f"release/dates",
                release_id=rid,
                realtime_start=today.isoformat(),
                realtime_end=until.isoformat(),
                include_release_dates_with_no_data="true",
                sort_order="asc",
            )
            dates = [date.fromisoformat(d["date"]) for d in data.get("release_dates", [])]
            dates = [d for d in dates if today <= d <= until]
        except Exception as e:
            print(f"    release_id={rid}: {e}")
            dates = []
        if not dates:
            continue
        for sid in sids:
            for d in dates:
                # FRED publishes most economic releases at 8:30 AM ET; we don't
                # have a per-release time from the API, so leave time NULL and
                # rely on grace_days for OVERDUE logic.
                rows.append((sid, d, None, 3, now))
        if i + 1 < len(by_release):
            time.sleep(FRED_RATE_LIMIT_DELAY)

    return _upsert_pending(rows)


# ─────────────────────────────────────────────────────────────────────────
# Fetch-path helpers
# ─────────────────────────────────────────────────────────────────────────

def series_due_now(series_id: str, today: Optional[date] = None) -> bool:
    """Is there a PENDING/OVERDUE release for this series due on or before today?"""
    today = today or date.today()
    row = connect().execute(
        "SELECT 1 FROM release_schedule "
        "WHERE series_id = %s AND status IN ('PENDING','OVERDUE') "
        "AND scheduled_release <= %s LIMIT 1",
        (series_id, today),
    ).fetchone()
    return row is not None


def mark_captured(series_id: str, period_end: date,
                  source_last_updated: Optional[datetime] = None,
                  today: Optional[date] = None) -> bool:
    """Transition the earliest PENDING/OVERDUE row for this series to CAPTURED.

    Called from the fetch path when a new observation arrives. Returns True
    if a row was updated.
    """
    today = today or date.today()
    con = connect()
    cur = con.execute(
        """
        UPDATE release_schedule SET
            status = 'CAPTURED',
            captured_at = NOW(),
            captured_period_end = %s,
            source_last_updated = COALESCE(%s, source_last_updated),
            updated_at = NOW()
        WHERE (series_id, scheduled_release) = (
            SELECT series_id, scheduled_release FROM release_schedule
            WHERE series_id = %s AND status IN ('PENDING','OVERDUE')
              AND scheduled_release <= %s
            ORDER BY scheduled_release ASC LIMIT 1
        )
        """,
        (period_end, source_last_updated, series_id, today),
    )
    return cur.rowcount > 0


def sweep_overdue(today: Optional[date] = None) -> list[dict]:
    """Mark stale PENDING rows as OVERDUE. Returns the rows newly transitioned."""
    today = today or date.today()
    con = connect()
    rows = con.execute(
        """
        UPDATE release_schedule SET status = 'OVERDUE', updated_at = NOW()
        WHERE status = 'PENDING'
          AND (scheduled_release + grace_days * INTERVAL '1 day')::date < %s
        RETURNING series_id, scheduled_release::text, grace_days, notes
        """,
        (today,),
    ).fetchall()
    return [{"series_id": r[0], "scheduled_release": r[1],
             "grace_days": r[2], "notes": r[3]} for r in rows]


# ─────────────────────────────────────────────────────────────────────────
# Read helpers (briefing + summary)
# ─────────────────────────────────────────────────────────────────────────

def get_overdue() -> list[dict]:
    rows = connect().execute(
        """
        SELECT series_id, scheduled_release::text, grace_days,
               EXTRACT(DAY FROM NOW() - scheduled_release::timestamp)::int AS days_late,
               notes
        FROM release_schedule WHERE status = 'OVERDUE'
        ORDER BY scheduled_release ASC
        """
    ).fetchall()
    return [{"series_id": r[0], "scheduled_release": r[1],
             "grace_days": r[2], "days_late": r[3], "notes": r[4]}
            for r in rows]


def get_captured_today() -> list[dict]:
    rows = connect().execute(
        """
        SELECT series_id, scheduled_release::text,
               captured_period_end::text, captured_at
        FROM release_schedule
        WHERE captured_at::date = CURRENT_DATE
        ORDER BY captured_at ASC
        """
    ).fetchall()
    return [{"series_id": r[0], "scheduled_release": r[1],
             "captured_period_end": r[2], "captured_at": r[3]}
            for r in rows]


def get_upcoming(days: int = 7) -> list[dict]:
    rows = connect().execute(
        """
        SELECT series_id, scheduled_release::text, expected_time_et::text
        FROM release_schedule
        WHERE status = 'PENDING'
          AND scheduled_release > CURRENT_DATE
          AND scheduled_release <= CURRENT_DATE + (%s * INTERVAL '1 day')
        ORDER BY scheduled_release ASC, series_id ASC
        """,
        (days,),
    ).fetchall()
    return [{"series_id": r[0], "scheduled_release": r[1],
             "expected_time_et": r[2]} for r in rows]
