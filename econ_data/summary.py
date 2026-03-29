"""
Analyze latest data for trend direction, reversals, and unusual moves.
"""
import sqlite3
import statistics
from pathlib import Path

from econ_data.config import load as load_config, percent_series
from econ_data.store_sqlite import DB_PATH

# Cached set of series IDs with percent units
_PERCENT_IDS = None


def _get_percent_ids():
    global _PERCENT_IDS
    if _PERCENT_IDS is None:
        _PERCENT_IDS = percent_series(load_config())
    return _PERCENT_IDS

# Windows by frequency (observation counts approximating real-time spans)
MONTHLY_TREND = 6        # 6 months
MONTHLY_HISTORY = 24     # 2 years
DAILY_TREND = 126        # ~6 months of trading days
DAILY_HISTORY = 504      # ~2 years of trading days
UNUSUAL_THRESHOLD = 1.5  # std devs from mean to flag as unusual


def _detect_frequency(rows: list) -> str:
    """Detect whether a series is daily or monthly based on median gap between observations."""
    if len(rows) < 3:
        return "monthly"
    from datetime import date as date_type
    dates = [date_type.fromisoformat(r[0]) if isinstance(r[0], str) else r[0] for r in rows[-20:]]
    gaps = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates))]
    median_gap = sorted(gaps)[len(gaps) // 2]
    return "daily" if median_gap <= 5 else "monthly"


def _get_series_data(series_id: str, db_path: Path = DB_PATH) -> dict:
    """Get raw values and calculated changes for a series."""
    con = sqlite3.connect(db_path)
    is_pct = series_id in _get_percent_ids()
    period_type = "period_pp" if is_pct else "period_pct"
    yoy_type = "yoy_pp" if is_pct else "yoy_pct"

    rows = con.execute(
        "SELECT date, value FROM observations WHERE series_id = ? ORDER BY date",
        (series_id,),
    ).fetchall()

    period_pct = dict(con.execute(
        "SELECT date, value FROM calculated WHERE series_id = ? AND calc_type = ? ORDER BY date",
        (series_id, period_type),
    ).fetchall())

    yoy_pct = dict(con.execute(
        "SELECT date, value FROM calculated WHERE series_id = ? AND calc_type = ? ORDER BY date",
        (series_id, yoy_type),
    ).fetchall())

    con.close()

    return {"rows": rows, "period_pct": period_pct, "yoy_pct": yoy_pct, "is_percent": is_pct}


def analyze_series(series_id: str, name: str, db_path: Path = DB_PATH) -> dict:
    """
    Analyze a single series and return a summary dict:
      latest_date, latest_value, period_pct, yoy_pct,
      trend_dir, trend_periods, frequency, signals[]
    """
    data = _get_series_data(series_id, db_path)
    rows = data["rows"]

    if len(rows) < 2:
        return {
            "series_id": series_id, "name": name,
            "latest_date": rows[-1][0] if rows else None,
            "latest_value": rows[-1][1] if rows else None,
            "period_pct": None, "yoy_pct": None,
            "trend_dir": None, "trend_periods": 0,
            "frequency": "monthly",
            "signals": [],
        }

    freq = _detect_frequency(rows)
    trend_window = DAILY_TREND if freq == "daily" else MONTHLY_TREND
    history_window = DAILY_HISTORY if freq == "daily" else MONTHLY_HISTORY

    latest_date = rows[-1][0]
    latest_value = rows[-1][1]

    # Period and YoY change for latest
    period = data["period_pct"].get(latest_date)
    yoy = data["yoy_pct"].get(latest_date)

    # Recent period changes for trend analysis
    recent_dates = [r[0] for r in rows[-trend_window - 1:]]
    recent_changes = [data["period_pct"][d] for d in recent_dates if d in data["period_pct"]]

    # Longer history for "normal" baseline
    history_dates = [r[0] for r in rows[-history_window - 1:]]
    history_changes = [data["period_pct"][d] for d in history_dates if d in data["period_pct"]]

    # Recent YoY values for acceleration/deceleration
    recent_yoy = [data["yoy_pct"][d] for d in recent_dates if d in data["yoy_pct"]]

    signals = []

    # ── Trend direction ─────────────────────────────────────
    trend_dir = None
    trend_periods = 0

    if len(recent_changes) >= 2:
        # Count consecutive periods in same direction from most recent
        if recent_changes[-1] > 0:
            trend_dir = "rising"
        elif recent_changes[-1] < 0:
            trend_dir = "falling"
        else:
            trend_dir = "flat"

        for ch in reversed(recent_changes):
            if trend_dir == "rising" and ch > 0:
                trend_periods += 1
            elif trend_dir == "falling" and ch < 0:
                trend_periods += 1
            elif trend_dir == "flat" and ch == 0:
                trend_periods += 1
            else:
                break

    # ── Trend reversal ──────────────────────────────────────
    if len(recent_changes) >= 3 and period is not None:
        prior_changes = recent_changes[:-1]
        if len(prior_changes) >= 2:
            prior_positive = sum(1 for c in prior_changes[-3:] if c > 0)
            prior_negative = sum(1 for c in prior_changes[-3:] if c < 0)

            if prior_negative >= 2 and period > 0:
                signals.append("Reversal: uptick breaks recent decline")
            elif prior_positive >= 2 and period < 0:
                signals.append("Reversal: downtick breaks recent rise")

    # ── Unusual move ────────────────────────────────────────
    if len(history_changes) >= 6 and period is not None:
        mean_ch = statistics.mean(history_changes)
        stdev_ch = statistics.stdev(history_changes)
        if stdev_ch > 0:
            z_score = (period - mean_ch) / stdev_ch
            if abs(z_score) >= UNUSUAL_THRESHOLD:
                direction = "jump" if z_score > 0 else "drop"
                signals.append(f"Unusual {direction} ({z_score:+.1f} std devs)")

    # ── Sign changes (crosses zero) ────────────────────────
    if len(recent_yoy) >= 2:
        prev_yoy = recent_yoy[-2]
        curr_yoy = recent_yoy[-1]
        if prev_yoy > 0 and curr_yoy < 0:
            signals.append("YoY turned negative (was positive)")
        elif prev_yoy < 0 and curr_yoy > 0:
            signals.append("YoY turned positive (was negative)")

    if len(recent_changes) >= 2:
        prev_ch = recent_changes[-2]
        curr_ch = recent_changes[-1]
        if prev_ch > 0 and curr_ch < 0:
            signals.append("Period change turned negative (was positive)")
        elif prev_ch < 0 and curr_ch > 0:
            signals.append("Period change turned positive (was negative)")

    # ── YoY acceleration/deceleration ───────────────────────
    if len(recent_yoy) >= 3:
        yoy_diffs = [recent_yoy[i] - recent_yoy[i - 1] for i in range(1, len(recent_yoy))]
        recent_yoy_diffs = yoy_diffs[-3:]

        if all(d > 0 for d in recent_yoy_diffs):
            signals.append("YoY accelerating")
        elif all(d < 0 for d in recent_yoy_diffs):
            signals.append("YoY decelerating")
        # Softer check: if 4+ of last 5 diffs trend the same way, flag it
        elif len(yoy_diffs) >= 5:
            last5 = yoy_diffs[-5:]
            neg_count = sum(1 for d in last5 if d < 0)
            pos_count = sum(1 for d in last5 if d > 0)
            if neg_count >= 4 and "YoY decelerating" not in signals:
                signals.append("YoY trend worsening")
            elif pos_count >= 4 and "YoY accelerating" not in signals:
                signals.append("YoY trend improving")

    # ── Sustained negative/positive YoY ────────────────────
    # Flag when YoY has stayed negative (or positive in a normally-negative
    # context) for an extended period — the zero-crossing was the event,
    # but staying there is the story.
    # Only flag sustained streaks at 6+ months — short streaks are
    # already covered by the zero-crossing signal.
    sustained_min = 6 if freq != "daily" else 126
    if len(recent_yoy) >= sustained_min and yoy is not None:
        if yoy < 0:
            neg_streak = 0
            for y in reversed(recent_yoy):
                if y < 0:
                    neg_streak += 1
                else:
                    break
            if neg_streak >= sustained_min:
                unit = "d" if freq == "daily" else "mo"
                signals.append(f"YoY negative {neg_streak}{unit}")
        elif yoy > 0:
            pos_streak = 0
            for y in reversed(recent_yoy):
                if y > 0:
                    pos_streak += 1
                else:
                    break
            # Only flag sustained positive if it recently crossed from negative
            # (i.e., streak started within the recent window)
            if pos_streak >= sustained_min and pos_streak < len(recent_yoy):
                unit = "d" if freq == "daily" else "mo"
                signals.append(f"YoY positive {pos_streak}{unit}")

    # ── YoY at multi-year extreme ──────────────────────────
    # Flag when YoY is at its most extreme level in 2+ years.
    # "When was the last time it was this bad/good?" — if the answer
    # is 2+ years ago, that's a signal.
    all_dates = [r[0] for r in rows]
    yoy_dated = [(d, data["yoy_pct"][d]) for d in all_dates
                 if d in data["yoy_pct"]]
    if len(yoy_dated) >= history_window and yoy is not None:
        from datetime import datetime as _dt
        min_gap_days = 730  # 2 years
        try:
            current_dt = _dt.strptime(latest_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            current_dt = None

        if current_dt:
            if yoy < 0:
                # Find the most recent prior period with a YoY as low or lower
                last_as_bad = None
                for d, y in reversed(yoy_dated[:-1]):
                    if y <= yoy:
                        last_as_bad = d
                        break
                if last_as_bad is None:
                    signals.append("YoY at all-time low")
                else:
                    try:
                        gap = (current_dt - _dt.strptime(last_as_bad, "%Y-%m-%d")).days
                        if gap >= min_gap_days:
                            label = _dt.strptime(last_as_bad, "%Y-%m-%d").strftime("%b %Y")
                            signals.append(f"YoY at lowest since {label}")
                    except (ValueError, TypeError):
                        pass
            elif yoy > 0:
                last_as_good = None
                for d, y in reversed(yoy_dated[:-1]):
                    if y >= yoy:
                        last_as_good = d
                        break
                if last_as_good is None:
                    signals.append("YoY at all-time high")
                else:
                    try:
                        gap = (current_dt - _dt.strptime(last_as_good, "%Y-%m-%d")).days
                        if gap >= min_gap_days:
                            label = _dt.strptime(last_as_good, "%Y-%m-%d").strftime("%b %Y")
                            signals.append(f"YoY at highest since {label}")
                    except (ValueError, TypeError):
                        pass

    return {
        "series_id": series_id,
        "name": name,
        "latest_date": latest_date,
        "latest_value": latest_value,
        "period_pct": period,
        "yoy_pct": yoy,
        "trend_dir": trend_dir,
        "trend_periods": trend_periods,
        "frequency": freq,
        "signals": signals,
        "is_percent": data.get("is_percent", False),
    }


OUTLIER_MIN_GROUP = 4    # need at least this many series with data
OUTLIER_CONSENSUS = 0.75  # fraction that must agree for a "consensus"


def _detect_group_outliers(analyses: list):
    """Detect series that diverge from the group consensus. Mutates analyses in place.

    Checks both period change direction and YoY direction. If a supermajority
    of series in the group are moving one way, flags any going the other way.
    """
    # Filter to series with data
    with_data = [a for a in analyses if a.get("period_pct") is not None]
    if len(with_data) < OUTLIER_MIN_GROUP:
        return

    # --- Period direction outliers ---
    _check_direction_outliers(with_data, "period_pct", "period")

    # --- YoY direction outliers ---
    with_yoy = [a for a in analyses if a.get("yoy_pct") is not None]
    if len(with_yoy) >= OUTLIER_MIN_GROUP:
        _check_direction_outliers(with_yoy, "yoy_pct", "YoY")


def _check_direction_outliers(analyses: list, field: str, label: str):
    """Check if a few series diverge from the group's consensus direction."""
    positive = [a for a in analyses if a[field] > 0]
    negative = [a for a in analyses if a[field] < 0]
    total = len(positive) + len(negative)  # exclude zeros

    if total < OUTLIER_MIN_GROUP:
        return

    pos_frac = len(positive) / total
    neg_frac = len(negative) / total

    if pos_frac >= OUTLIER_CONSENSUS and negative:
        # Consensus is positive, flag the negatives
        majority_word = "rising" if label == "period" else "positive"
        for a in negative:
            val = a[field]
            is_pct = a.get("is_percent", False)
            unit = "pp" if is_pct else "%"
            a["signals"].append(
                f"Group outlier: {label} {val:+.2f}{unit} while most peers are {majority_word}"
            )
    elif neg_frac >= OUTLIER_CONSENSUS and positive:
        # Consensus is negative, flag the positives
        majority_word = "falling" if label == "period" else "negative"
        for a in positive:
            val = a[field]
            is_pct = a.get("is_percent", False)
            unit = "pp" if is_pct else "%"
            a["signals"].append(
                f"Group outlier: {label} {val:+.2f}{unit} while most peers are {majority_word}"
            )


def generate_summary(cfg: dict, db_path: Path = DB_PATH) -> dict:
    """
    Analyze all series in the config and return a structured summary.
    Returns {"standalone": [analysis...], "groups": {group_id: {"name":..., "series": [analysis...]}}}
    """
    from econ_data.config import all_series

    standalone = cfg.get("series", [])
    groups = cfg.get("groups", {})

    grouped_ids = set()
    for gdata in groups.values():
        for s in gdata["series"]:
            grouped_ids.add(s["id"])

    result_standalone = []
    for s in standalone:
        if s["id"] not in grouped_ids:
            result_standalone.append(analyze_series(s["id"], s["name"], db_path))

    result_groups = {}
    for gid, gdata in groups.items():
        analyses = []
        for s in gdata["series"]:
            analyses.append(analyze_series(s["id"], s["name"], db_path))
        _detect_group_outliers(analyses)
        result_groups[gid] = {"name": gdata["name"], "series": analyses}

    return {"standalone": result_standalone, "groups": result_groups}


def format_summary(summary: dict) -> str:
    """Format the summary as a readable text report."""
    lines = []

    for a in summary.get("standalone", []):
        lines.extend(_format_series(a))

    for gid, gdata in summary.get("groups", {}).items():
        lines.append("")
        lines.append(f"  {gdata['name']}")
        lines.append(f"  {'─' * len(gdata['name'])}")
        for a in gdata["series"]:
            lines.extend(_format_series(a))

    return "\n".join(lines)


def filter_signals(summary: dict) -> dict:
    """Return a copy of summary containing only series with signals."""
    filtered = {
        "standalone": [a for a in summary.get("standalone", []) if a["signals"]],
        "groups": {},
    }
    for gid, gdata in summary.get("groups", {}).items():
        flagged = [a for a in gdata["series"] if a["signals"]]
        if flagged:
            filtered["groups"][gid] = {"name": gdata["name"], "series": flagged}
    return filtered


def format_signals_by_recency(summary: dict, updated_series: set,
                              recent_days: int = 60) -> str:
    """Format signals split into 'Updated Today' and 'Updated This Week' sections.

    updated_series: set of series_ids that received new data in this run.
    recent_days: how far back to look for "this week" bucket. Default 60 days
        to capture recent monthly releases (e.g. Feb data released in March).
    """
    from datetime import date, timedelta

    cutoff = (date.today() - timedelta(days=recent_days)).isoformat()

    today_groups = {}
    week_groups = {}

    # Process standalone
    for a in summary.get("standalone", []):
        if not a["signals"]:
            continue
        if a["series_id"] in updated_series:
            today_groups.setdefault("_standalone", {"name": "", "series": []})
            today_groups["_standalone"]["series"].append(a)
        elif a.get("latest_date", "") >= cutoff:
            week_groups.setdefault("_standalone", {"name": "", "series": []})
            week_groups["_standalone"]["series"].append(a)

    # Process groups
    for gid, gdata in summary.get("groups", {}).items():
        for a in gdata["series"]:
            if not a["signals"]:
                continue
            if a["series_id"] in updated_series:
                if gid not in today_groups:
                    today_groups[gid] = {"name": gdata["name"], "series": []}
                today_groups[gid]["series"].append(a)
            elif a.get("latest_date", "") >= cutoff:
                if gid not in week_groups:
                    week_groups[gid] = {"name": gdata["name"], "series": []}
                week_groups[gid]["series"].append(a)

    lines = []

    # Today section
    if today_groups:
        lines.append("  ┌─────────────────────────────┐")
        lines.append("  │     UPDATED TODAY            │")
        lines.append("  └─────────────────────────────┘")
        lines.extend(_format_group_block(today_groups))

    # This week section
    if week_groups:
        if today_groups:
            lines.append("")
        lines.append("  ┌─────────────────────────────┐")
        lines.append("  │     UPDATED THIS WEEK        │")
        lines.append("  └─────────────────────────────┘")
        lines.extend(_format_group_block(week_groups))

    if not today_groups and not week_groups:
        lines.append("  No signals today or this week.")

    return "\n".join(lines)


def _format_group_block(groups: dict) -> list:
    """Format a dict of groups into output lines."""
    lines = []
    for gid, gdata in groups.items():
        if gid != "_standalone":
            lines.append("")
            lines.append(f"  {gdata['name']}")
            lines.append(f"  {'─' * len(gdata['name'])}")
        for a in gdata["series"]:
            lines.extend(_format_series(a))
    return lines


def _format_value(val):
    """Format a number concisely."""
    if val is None:
        return "—"
    if abs(val) >= 1000:
        return f"{val:,.0f}"
    if abs(val) >= 10:
        return f"{val:.1f}"
    return f"{val:.2f}"


def _format_pct(val):
    if val is None:
        return "—"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


def _format_pp(val):
    """Format a percentage-point change."""
    if val is None:
        return "—"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}pp"


def _format_series(a: dict) -> list:
    """Format a single series analysis into output lines."""
    lines = []

    if a["latest_date"] is None:
        lines.append(f"    {a['series_id']:<16} No data")
        return lines

    # Arrow for period direction
    if a["period_pct"] is not None:
        if a["period_pct"] > 0:
            arrow = "↑"
        elif a["period_pct"] < 0:
            arrow = "↓"
        else:
            arrow = "→"
    else:
        arrow = " "

    # Trend description
    trend = ""
    freq = a.get("frequency", "monthly")
    if a["trend_dir"] and a["trend_periods"] > 1:
        unit = "d" if freq == "daily" else "mo"
        trend = f"{a['trend_dir']} {a['trend_periods']}{unit}"

    # Build the line — full date for daily, YYYY-MM for monthly
    date_str = a["latest_date"] if freq == "daily" else a["latest_date"][:7]
    val = _format_value(a["latest_value"])
    is_pct = a.get("is_percent", False)
    pch = _format_pp(a["period_pct"]) if is_pct else _format_pct(a["period_pct"])
    yoy = _format_pp(a["yoy_pct"]) if is_pct else _format_pct(a["yoy_pct"])

    line = f"    {a['series_id']:<16} {date_str}  {val:>10}  {arrow} {pch:>8}   YoY {yoy:>8}"
    if trend:
        line += f"   {trend}"
    lines.append(line)

    # Signals on separate indented lines
    for sig in a["signals"]:
        lines.append(f"    {'':<16}                                              ⚑ {sig}")

    return lines
