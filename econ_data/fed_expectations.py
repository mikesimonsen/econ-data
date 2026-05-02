"""
Federal Reserve FOMC meeting calendar and market-implied rate expectations.

The FOMC meeting schedule is hardcoded (publicly known and changes annually).
Market expectations come from CME FedWatch (Fed Funds futures), captured via
Claude web search and stored in the fed_expectations table.

Storage:
  fed_expectations table — one row per (meeting_date, outcome_bps)
    meeting_date: ISO date of the FOMC decision day
    outcome_bps:  rate change in basis points (-50, -25, 0, 25, 50)
    probability:  market-implied probability (0..1)
    captured_at:  when this snapshot was taken
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from econ_data.db import connect
from econ_data.store import DB_PATH

load_dotenv()


# ── FOMC Meeting Calendar ────────────────────────────────────
# Two-day meetings; the rate decision is announced on the second day.
# Source: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
# Update annually as the Fed publishes the next year's schedule.
FOMC_MEETINGS = [
    # 2024
    "2024-01-31", "2024-03-20", "2024-05-01", "2024-06-12",
    "2024-07-31", "2024-09-18", "2024-11-07", "2024-12-18",
    # 2025
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10",
    # 2026
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
    # 2027
    "2027-01-27", "2027-03-17", "2027-04-28", "2027-06-16",
    "2027-07-28", "2027-09-22", "2027-11-03", "2027-12-15",
]


def _connect(db_path: Path = DB_PATH):
    """Singleton Postgres connection. db_path arg ignored (signature compat)."""
    return connect()


def get_past_meetings(today: date | None = None) -> list[date]:
    """Return all FOMC meetings on or before today."""
    if today is None:
        today = date.today()
    return [date.fromisoformat(d) for d in FOMC_MEETINGS
            if date.fromisoformat(d) <= today]


def get_future_meetings(today: date | None = None,
                        limit: int = 6) -> list[date]:
    """Return upcoming FOMC meetings (next N)."""
    if today is None:
        today = date.today()
    future = [date.fromisoformat(d) for d in FOMC_MEETINGS
              if date.fromisoformat(d) > today]
    return future[:limit]


def get_meeting_changes(db_path: Path = DB_PATH) -> dict[str, float]:
    """For each past FOMC meeting, return the rate change (in pp).

    Compares effective Fed Funds rate ~3 days before and ~3 days after
    each meeting. Returns {meeting_date_iso: change_pp}.
    """
    rows = connect().execute(
        "SELECT date::text, value FROM observations WHERE series_id = 'DFF' "
        "ORDER BY date"
    ).fetchall()

    if not rows:
        return {}

    by_date = {date.fromisoformat(d): v for d, v in rows}
    sorted_dates = sorted(by_date.keys())

    result = {}
    for meeting_str in FOMC_MEETINGS:
        m = date.fromisoformat(meeting_str)
        if m > date.today():
            continue

        # Find rate ~5 days before and ~5 days after
        before = _nearest_value(by_date, sorted_dates, m - timedelta(days=5),
                                direction="before")
        after = _nearest_value(by_date, sorted_dates, m + timedelta(days=5),
                               direction="after")
        if before is not None and after is not None:
            # Round to nearest 25bp to filter out IORB drift / weekend noise
            change = round((after - before) * 4) / 4
            result[meeting_str] = change

    return result


def _nearest_value(by_date: dict, sorted_dates: list,
                   target: date, direction: str) -> float | None:
    """Find the value at the nearest date in the given direction."""
    if direction == "before":
        # Latest date <= target
        candidates = [d for d in sorted_dates if d <= target]
        if not candidates:
            return None
        return by_date[candidates[-1]]
    else:
        # Earliest date >= target
        candidates = [d for d in sorted_dates if d >= target]
        if not candidates:
            return None
        return by_date[candidates[0]]


def get_current_target_range(db_path: Path = DB_PATH) -> tuple[float, float] | None:
    """Estimate current Fed Funds target range from recent DFF observations.

    The Fed sets a 25bp target range; the effective rate trades within it.
    We round the latest DFF reading down to the nearest 25bp lower bound.
    """
    row = connect().execute(
        "SELECT value FROM observations WHERE series_id = 'DFF' "
        "ORDER BY date DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    rate = row[0]
    # Round to nearest 25bp lower bound
    lower = (int(rate * 4)) / 4
    return (lower, lower + 0.25)


def store_expectations(meeting_date: str,
                       probabilities: dict[int, float],
                       db_path: Path = DB_PATH) -> None:
    """Store CME FedWatch probabilities for a meeting.

    probabilities: {outcome_bps: probability}, e.g. {-25: 0.40, 0: 0.60}
    """
    con = connect()
    now = datetime.now()
    # Snapshot replace: DELETE + INSERTs must be atomic so readers never
    # observe an empty meeting (autocommit would commit each statement).
    with con.transaction():
        con.execute("DELETE FROM fed_expectations WHERE meeting_date = %s",
                    (meeting_date,))
        for bps, prob in probabilities.items():
            con.execute(
                "INSERT INTO fed_expectations "
                "(meeting_date, outcome_bps, probability, captured_at) "
                "VALUES (%s, %s, %s, %s)",
                (meeting_date, int(bps), float(prob), now),
            )


def get_expectations(db_path: Path = DB_PATH) -> dict[str, dict]:
    """Return all stored expectations as {meeting_date_iso: {bps: prob, ...}}."""
    rows = connect().execute(
        "SELECT meeting_date::text, outcome_bps, probability, captured_at::text "
        "FROM fed_expectations ORDER BY meeting_date, outcome_bps"
    ).fetchall()

    result = {}
    for meeting, bps, prob, captured in rows:
        if meeting not in result:
            result[meeting] = {"probabilities": {}, "captured_at": captured}
        result[meeting]["probabilities"][bps] = prob
    return result


def fetch_fedwatch_probabilities(meeting_date: date) -> dict[int, float] | None:
    """Use Claude web search to fetch CME FedWatch probabilities.

    Returns {outcome_bps: probability} or None if not found.
    Outcomes are rate changes from current target: -50, -25, 0, +25, +50
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    meeting_label = meeting_date.strftime("%B %d, %Y")
    ask = (
        f"What does CME FedWatch currently show for the FOMC meeting on "
        f"{meeting_label}? I need the implied probabilities for each "
        f"possible rate decision (no change, -25bp cut, -50bp cut, "
        f"+25bp hike, etc.) based on Fed Funds futures pricing.\n\n"
        f"Return the probabilities as percentages."
    )

    client = anthropic.Anthropic(api_key=api_key, timeout=60.0)

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 3,
            }],
            system=(
                "You are extracting CME FedWatch implied probabilities for "
                "FOMC rate decisions. Search the web for the current "
                "(real-time) probabilities, then respond with ONLY a JSON "
                "object in this exact format:\n\n"
                '{"probabilities": {"-50": 0.05, "-25": 0.40, "0": 0.55, '
                '"+25": 0.00}, "source": "<source URL or name>"}\n\n'
                "Rules:\n"
                "- Keys are rate changes in basis points: -50, -25, 0, +25, +50\n"
                "- Values are decimal probabilities (0..1) — NOT percentages\n"
                "- Probabilities should sum to ~1.0\n"
                "- Only include outcomes with probability > 0\n"
                "- If you cannot find current data, respond with "
                '{"probabilities": null, "source": "not found"}'
            ),
            messages=[{"role": "user", "content": ask}],
        )
    except Exception as e:
        print(f"  FedWatch fetch failed for {meeting_date}: {e}")
        return None

    full_text = "\n".join(b.text for b in msg.content if hasattr(b, "text"))

    parsed = _normalize_probs(full_text)
    if parsed:
        return parsed

    # Web-search responses often come back as prose + markdown tables
    # rather than JSON. Fall back to a no-tools extraction pass that
    # converts the prose to the strict JSON shape we need.
    return _extract_probs_from_text(client, full_text)


def _normalize_probs(text: str) -> dict[int, float] | None:
    """Parse JSON (or JSON embedded in markdown / prose) and return
    {bps: prob} if valid, else None."""
    import re
    candidates = [text]
    for m in re.finditer(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL):
        candidates.append(m.group(1).strip())
    for sm in re.finditer(r'\{\s*"probabilities"', text):
        depth = 0
        for i in range(sm.start(), len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    candidates.append(text[sm.start():i + 1])
                    break

    result = None
    for cand in candidates:
        try:
            result = json.loads(cand)
            break
        except (json.JSONDecodeError, ValueError, TypeError):
            continue
    if result is None:
        return None
    probs = result.get("probabilities") if isinstance(result, dict) else None
    if not probs:
        return None
    normalized = {}
    for k, v in probs.items():
        try:
            key = int(str(k).lstrip("+"))
        except ValueError:
            continue
        if v and v > 0:
            normalized[key] = float(v)
    if not normalized:
        return None
    total = sum(normalized.values())
    if 0.9 <= total <= 1.1:
        return normalized
    return None


def _extract_probs_from_text(client: "anthropic.Anthropic",
                             prose: str) -> dict[int, float] | None:
    """Second-pass extractor: convert prose/table output to strict JSON."""
    if not prose.strip():
        return None
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            system=(
                "Extract CME FedWatch probabilities from the user's text. "
                "Output exactly one JSON object and nothing else. No prose, "
                "no markdown, no code fences. Begin your response with { "
                "and end with }. Format:\n"
                '{"probabilities": {"-25": 0.28, "0": 0.70, "+25": 0.02}}\n'
                "Keys are basis-point rate changes (-50,-25,0,+25,+50); "
                "values are decimal probabilities (0..1). Only include "
                "outcomes with probability > 0."
            ),
            messages=[{"role": "user", "content": prose}],
        )
    except Exception as e:
        print(f"  Extraction pass failed: {e}")
        return None
    text = "\n".join(b.text for b in msg.content if hasattr(b, "text")).strip()
    return _normalize_probs(text)


def render_fed_chart(db_path: Path = DB_PATH,
                     months_back: int = 24,
                     months_forward: int = 12) -> str:
    """Render the Fed Funds rate chart as standalone HTML+SVG.

    Shows:
    - Historical effective Fed Funds rate (step line)
    - Past FOMC meetings as dots (green = rate changed, hollow gray = held)
    - Forward implied rate path from CME FedWatch probabilities
    - Future meetings as vertical markers with probability bars
    """
    from datetime import datetime as dt

    today = date.today()
    start_date = date(today.year, today.month, 1)
    # Roll back N months
    for _ in range(months_back):
        start_date = (start_date - timedelta(days=1)).replace(day=1)
    end_date = date(today.year, today.month, 28)
    for _ in range(months_forward):
        m = end_date.month + 1
        y = end_date.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        end_date = date(y, m, 28)

    # ── Historical DFF data ─────────────────────────────────
    rows = connect().execute(
        "SELECT date::text, value FROM observations WHERE series_id = 'DFF' "
        "AND date >= %s ORDER BY date",
        (start_date,),
    ).fetchall()

    if not rows:
        return "<p class='muted'>No Fed Funds data available.</p>"

    # Downsample daily DFF to weekly (Fridays) and snap to nearest
    # 12.5bp to eliminate daily jitter within the target range.
    raw_dates = [date.fromisoformat(r[0]) for r in rows]
    raw_values = [r[1] for r in rows]
    hist_dates = []
    hist_values = []
    prev_snap = None
    for d, v in zip(raw_dates, raw_values):
        snap = round(v * 8) / 8  # nearest 12.5bp
        # Keep one point per week OR when the snapped value changes
        if (not hist_dates
                or snap != prev_snap
                or (d - hist_dates[-1]).days >= 5):
            hist_dates.append(d)
            hist_values.append(snap)
            prev_snap = snap

    # ── Compute implied forward path from probabilities ──────
    expectations = get_expectations(db_path)
    current_rate = hist_values[-1]
    forward_points = [(hist_dates[-1], current_rate)]  # start at today
    running_rate = current_rate
    for meeting_str in sorted(expectations.keys()):
        m_date = date.fromisoformat(meeting_str)
        if m_date <= today:
            continue
        probs = expectations[meeting_str]["probabilities"]
        # Expected change = sum(prob * bps/100)
        expected_change_pp = sum(p * (bps / 100) for bps, p in probs.items())
        running_rate += expected_change_pp
        forward_points.append((m_date, running_rate))

    # ── Chart geometry ──────────────────────────────────────
    width = 900
    height = 380
    pad_left = 50
    pad_right = 30
    pad_top = 30
    pad_bottom = 50
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom

    # Y range
    all_y = hist_values + [p[1] for p in forward_points]
    y_min = min(all_y) - 0.25
    y_max = max(all_y) + 0.5
    y_range = y_max - y_min

    # X range
    x_start = start_date
    x_end = end_date
    x_range_days = (x_end - x_start).days

    def x_pos(d: date) -> float:
        return pad_left + plot_w * (d - x_start).days / x_range_days

    def y_pos(v: float) -> float:
        return pad_top + plot_h * (1 - (v - y_min) / y_range)

    # ── Build SVG ───────────────────────────────────────────
    svg = []
    svg.append(f'<svg viewBox="0 0 {width} {height}" '
               f'xmlns="http://www.w3.org/2000/svg" '
               f'class="fed-chart" style="width:100%;height:auto">')

    # Background grid (horizontal lines every 0.25%)
    grid_step = 0.25
    g_val = round(y_min * 4) / 4
    while g_val <= y_max:
        y = y_pos(g_val)
        svg.append(f'<line x1="{pad_left}" y1="{y:.1f}" '
                   f'x2="{width - pad_right}" y2="{y:.1f}" '
                   f'stroke="#eef" stroke-width="1"/>')
        svg.append(f'<text x="{pad_left - 6}" y="{y + 4:.1f}" '
                   f'font-size="11" fill="#678" text-anchor="end">'
                   f'{g_val:.2f}%</text>')
        g_val += grid_step

    # Vertical "today" line
    today_x = x_pos(today)
    svg.append(f'<line x1="{today_x:.1f}" y1="{pad_top}" '
               f'x2="{today_x:.1f}" y2="{pad_top + plot_h}" '
               f'stroke="#999" stroke-width="1" stroke-dasharray="2,3"/>')
    svg.append(f'<text x="{today_x:.1f}" y="{pad_top - 8}" '
               f'font-size="10" fill="#666" text-anchor="middle">today</text>')

    # X-axis month labels (every 3 months, past only — future uses meeting labels)
    cur = x_start.replace(day=1)
    while cur <= today:
        if cur.month in (1, 4, 7, 10):
            x = x_pos(cur)
            svg.append(f'<text x="{x:.1f}" y="{height - pad_bottom + 16}" '
                       f'font-size="10" fill="#678" text-anchor="middle">'
                       f'{cur.strftime("%b %y")}</text>')
        m = cur.month + 1
        y = cur.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        cur = date(y, m, 1)

    # ── Historical step line ────────────────────────────────
    pts = []
    for i, (d, v) in enumerate(zip(hist_dates, hist_values)):
        x = x_pos(d)
        yv = y_pos(v)
        if i == 0:
            pts.append(f"M {x:.1f},{yv:.1f}")
        else:
            # Step: horizontal then vertical
            prev_y = y_pos(hist_values[i - 1])
            pts.append(f"L {x:.1f},{prev_y:.1f} L {x:.1f},{yv:.1f}")
    svg.append(f'<path d="{" ".join(pts)}" fill="none" '
               f'stroke="#2C5F8D" stroke-width="2"/>')

    # ── Forward implied path (dotted) ───────────────────────
    if len(forward_points) > 1:
        path_pts = []
        for i, (d, v) in enumerate(forward_points):
            x = x_pos(d)
            yv = y_pos(v)
            cmd = "M" if i == 0 else "L"
            path_pts.append(f"{cmd} {x:.1f},{yv:.1f}")
        svg.append(f'<path d="{" ".join(path_pts)}" fill="none" '
                   f'stroke="#2C5F8D" stroke-width="2" '
                   f'stroke-dasharray="5,4" opacity="0.7"/>')

    # ── Past FOMC meeting dots ──────────────────────────────
    changes = get_meeting_changes(db_path)
    for meeting_str in FOMC_MEETINGS:
        m = date.fromisoformat(meeting_str)
        if m > today or m < x_start:
            continue
        x = x_pos(m)
        # Find rate at meeting date
        before_idx = None
        for i, d in enumerate(hist_dates):
            if d <= m:
                before_idx = i
            else:
                break
        if before_idx is None:
            continue
        rate_at_meeting = hist_values[before_idx]
        y = y_pos(rate_at_meeting)
        change = changes.get(meeting_str, 0)
        if abs(change) > 0.01:
            # Rate changed — solid green dot
            color = "#2A8B3C"
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" '
                       f'fill="{color}" stroke="#fff" stroke-width="1.5">'
                       f'<title>{meeting_str}: {change:+.2f}pp</title>'
                       f'</circle>')
        else:
            # Held — hollow gray dot
            svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" '
                       f'fill="white" stroke="#888" stroke-width="2">'
                       f'<title>{meeting_str}: held</title>'
                       f'</circle>')

    # ── Future FOMC meetings + probability bars ─────────────
    future_meetings = [date.fromisoformat(m) for m in FOMC_MEETINGS
                       if today < date.fromisoformat(m) <= end_date]
    forward_dict = {d: r for d, r in forward_points}

    for m in future_meetings:
        x = x_pos(m)

        # Vertical marker line
        svg.append(f'<line x1="{x:.1f}" y1="{pad_top}" '
                   f'x2="{x:.1f}" y2="{pad_top + plot_h}" '
                   f'stroke="#ccc" stroke-width="1" stroke-dasharray="1,3"/>')

        # If we have probabilities, draw a stacked probability bar
        meeting_str = m.isoformat()
        exp = expectations.get(meeting_str)
        if exp:
            probs = exp["probabilities"]
            # Find center y for this meeting (use the implied rate path)
            implied_rate = forward_dict.get(m, current_rate)
            center_y = y_pos(implied_rate)

            # Stack outcome bars vertically — each represents 25bp from center
            # Height proportional to probability, color by direction
            total_h = 60
            current_y = center_y - total_h / 2
            sorted_outcomes = sorted(probs.keys())
            for bps in sorted_outcomes:
                prob = probs[bps]
                if prob < 0.01:
                    continue
                bar_h = prob * total_h
                if bps < 0:
                    color = "#2A8B3C"  # cuts = green
                elif bps > 0:
                    color = "#C13B3B"  # hikes = red
                else:
                    color = "#888"     # hold = gray
                svg.append(f'<rect x="{x - 4:.1f}" y="{current_y:.1f}" '
                           f'width="8" height="{bar_h:.1f}" '
                           f'fill="{color}" opacity="0.7">'
                           f'<title>{m}: {bps:+}bp = {prob*100:.0f}%</title>'
                           f'</rect>')
                current_y += bar_h

            # Most likely outcome label
            top_bps, top_prob = max(probs.items(), key=lambda kv: kv[1])
            if top_bps == 0:
                label = f"hold {top_prob*100:.0f}%"
            else:
                sign = "+" if top_bps > 0 else ""
                label = f"{sign}{top_bps}bp {top_prob*100:.0f}%"
            svg.append(f'<text x="{x:.1f}" y="{pad_top + plot_h + 32:.1f}" '
                       f'font-size="9" fill="#444" text-anchor="middle">'
                       f'{label}</text>')

        # Meeting date label below x-axis
        svg.append(f'<text x="{x:.1f}" y="{pad_top + plot_h + 16:.1f}" '
                   f'font-size="9" fill="#888" text-anchor="middle">'
                   f'{m.strftime("%b %d")}</text>')

    svg.append("</svg>")

    # ── Build CSV data for download ─────────────────────────
    import csv as csv_mod, io
    csv_buf = io.StringIO()
    csv_writer = csv_mod.writer(csv_buf)
    # Historical rows
    csv_writer.writerow(["date", "type", "rate", "change_pp",
                         "cut_prob", "hold_prob", "hike_prob"])
    for d, v in zip(hist_dates, hist_values):
        ch = changes.get(d.isoformat(), "")
        csv_writer.writerow([d.isoformat(), "actual", f"{v:.4f}", ch, "", "", ""])
    # Forward rows
    for m in future_meetings:
        ms = m.isoformat()
        exp = expectations.get(ms)
        implied = forward_dict.get(m, "")
        if exp:
            probs = exp["probabilities"]
            cut = sum(p for bps, p in probs.items() if bps < 0)
            hold = probs.get(0, 0)
            hike = sum(p for bps, p in probs.items() if bps > 0)
            csv_writer.writerow([ms, "forecast", f"{implied:.4f}" if implied else "",
                                 "", f"{cut:.3f}", f"{hold:.3f}", f"{hike:.3f}"])
        else:
            csv_writer.writerow([ms, "forecast", "", "", "", "", ""])
    fed_csv = csv_buf.getvalue()

    # ── Legend + meeting table ───────────────────────────────
    legend = (
        '<div class="fed-legend">'
        '<span><span class="dot solid"></span>Rate changed</span>'
        '<span><span class="dot hollow"></span>Rate held</span>'
        '<span><span class="line solid"></span>Effective rate</span>'
        '<span><span class="line dashed"></span>Implied path</span>'
        '<span><span class="bar green"></span>Cut probability</span>'
        '<span><span class="bar gray"></span>Hold probability</span>'
        '<span><span class="bar red"></span>Hike probability</span>'
        '</div>'
    )

    # Probability table for upcoming meetings
    rows_html = []
    for m in future_meetings[:6]:
        meeting_str = m.isoformat()
        exp = expectations.get(meeting_str)
        if exp:
            probs = exp["probabilities"]
            # Render each outcome as a colored chip
            chips = []
            for bps in sorted(probs.keys()):
                prob = probs[bps]
                if prob < 0.005:
                    continue
                if bps == 0:
                    label = "hold"
                else:
                    sign = "+" if bps > 0 else ""
                    label = f"{sign}{bps}bp"
                cls = "chip-cut" if bps < 0 else ("chip-hike" if bps > 0 else "chip-hold")
                chips.append(f'<span class="prob-chip {cls}">'
                             f'{label} {prob*100:.0f}%</span>')
            chips_html = " ".join(chips)
            captured = exp.get("captured_at", "")[:10]
            captured_html = f'<span class="muted">captured {captured}</span>' if captured else ""
        else:
            chips_html = '<span class="muted">— no data — run fetch_fed_expectations.py</span>'
            captured_html = ""
        rows_html.append(
            f'<tr><td><strong>{m.strftime("%a, %b %-d, %Y")}</strong></td>'
            f'<td>{chips_html}</td>'
            f'<td>{captured_html}</td></tr>'
        )

    table_html = (
        '<h3 style="margin-top:1.5em">Upcoming FOMC Meetings</h3>'
        '<table class="fed-table"><tbody>'
        + "\n".join(rows_html)
        + '</tbody></table>'
    )

    target = get_current_target_range(db_path)
    target_str = (f"<p class='fed-target'>Current target range: "
                  f"<strong>{target[0]:.2f}% – {target[1]:.2f}%</strong></p>"
                  if target else "")

    import json as json_mod
    csv_json = json_mod.dumps(fed_csv)

    export_buttons = (
        '<div style="margin:0.5em 0">'
        '<button class="btn-export" onclick="downloadFedCsv()">CSV</button> '
        '<button class="btn-export" onclick="downloadFedPng()">PNG</button>'
        '</div>'
    )

    chart_html = (
        f'<div id="fed-chart-container">'
        f'{"".join(svg)}'
        f'</div>'
    )

    fed_js = (
        f'<script>'
        f'var fedChartCsv = {csv_json};'
        f'function downloadFedCsv() {{'
        f'  var b = new Blob([fedChartCsv], {{type:"text/csv"}});'
        f'  var a = document.createElement("a");'
        f'  a.href = URL.createObjectURL(b);'
        f'  a.download = "fed_funds_rate.csv";'
        f'  a.click();'
        f'}}'
        f'function downloadFedPng() {{'
        f'  var svg = document.querySelector("#fed-chart-container svg");'
        f'  if (!svg) return;'
        f'  var svgData = new XMLSerializer().serializeToString(svg);'
        f'  var canvas = document.createElement("canvas");'
        f'  var rect = svg.getBoundingClientRect();'
        f'  var scale = 2;'
        f'  canvas.width = rect.width * scale;'
        f'  canvas.height = rect.height * scale;'
        f'  var ctx = canvas.getContext("2d");'
        f'  ctx.scale(scale, scale);'
        f'  var img = new Image();'
        f'  img.onload = function() {{'
        f'    ctx.fillStyle = "#fff";'
        f'    ctx.fillRect(0, 0, rect.width, rect.height);'
        f'    ctx.drawImage(img, 0, 0, rect.width, rect.height);'
        f'    canvas.toBlob(function(blob) {{'
        f'      var a = document.createElement("a");'
        f'      a.href = URL.createObjectURL(blob);'
        f'      a.download = "fed_funds_rate.png";'
        f'      a.click();'
        f'    }});'
        f'  }};'
        f'  img.src = "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent(svgData)));'
        f'}}'
        f'</script>'
    )

    return target_str + chart_html + export_buttons + legend + table_html + fed_js


def fetch_all_upcoming(limit: int = 6, db_path: Path = DB_PATH) -> int:
    """Fetch CME FedWatch probabilities for the next N FOMC meetings.

    Returns count of meetings successfully updated.
    """
    meetings = get_future_meetings(limit=limit)
    updated = 0
    for m in meetings:
        print(f"  Fetching probabilities for {m}...")
        probs = fetch_fedwatch_probabilities(m)
        if probs:
            store_expectations(m.isoformat(), probs, db_path)
            updated += 1
            top = max(probs.items(), key=lambda x: x[1])
            print(f"    → most likely: {top[0]:+}bp ({top[1]*100:.0f}%)")
        else:
            print(f"    → not found")
        time.sleep(1)
    return updated
