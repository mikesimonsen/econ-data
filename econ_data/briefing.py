"""Generate a daily editorial briefing as a self-contained HTML page.

Sections:
  1. What's Moving — signal triage with sparklines and color coding
  2. Deep Dive — expandable detail with comparison charts and data tables
  3. Export — one-click CSV downloads formatted for Flourish
"""

import csv
import io
import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from econ_data.store_sqlite import DB_PATH, get_recent_revisions
from econ_data.summary import generate_summary


def _sparkline_data(series_id: str, n: int = 24,
                    db_path: Path = DB_PATH) -> list:
    """Return last n observations as [(date, value), ...] for sparklines."""
    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT date, value FROM observations "
        "WHERE series_id = ? ORDER BY date DESC LIMIT ?",
        (series_id, n),
    ).fetchall()
    con.close()
    return list(reversed(rows))


def _sparkline_svg(points: list, width: int = 120, height: int = 28,
                   color: str = "#4A90D9") -> str:
    """Render a list of (date, value) as an inline SVG sparkline with hover tooltips."""
    if len(points) < 2:
        return ""
    values = [p[1] for p in points]
    vmin, vmax = min(values), max(values)
    vrange = vmax - vmin if vmax != vmin else 1
    pad = 2

    coords = []
    for i, v in enumerate(values):
        x = pad + (width - 2 * pad) * i / (len(values) - 1)
        y = pad + (height - 2 * pad) * (1 - (v - vmin) / vrange)
        coords.append((f"{x:.1f}", f"{y:.1f}"))

    polyline = " ".join(f"{x},{y}" for x, y in coords)

    # Hover circles with tooltips at each point
    circles = []
    for i, (x, y) in enumerate(coords):
        d, v = points[i]
        label = d[:7] if len(d) > 7 else d  # YYYY-MM for monthly
        if abs(v) >= 1000:
            fmt_v = f"{v:,.0f}"
        elif abs(v) >= 10:
            fmt_v = f"{v:.1f}"
        else:
            fmt_v = f"{v:.2f}"
        circles.append(
            f'<circle cx="{x}" cy="{y}" r="4" fill="transparent" '
            f'stroke="none" class="spark-hover">'
            f'<title>{label}: {fmt_v}</title></circle>'
        )

    # Visible dot on the last point
    lx, ly = coords[-1]
    return (
        f'<svg width="{width}" height="{height}" style="vertical-align:middle">'
        f'<polyline points="{polyline}" fill="none" stroke="{color}" '
        f'stroke-width="1.5" stroke-linejoin="round"/>'
        f'<circle cx="{lx}" cy="{ly}" r="2" fill="{color}"/>'
        + "".join(circles)
        + f'</svg>'
    )


def _format_val(val, is_pct=False):
    """Format a value for display."""
    if val is None:
        return "&mdash;"
    if is_pct:
        return f"{val:.1f}%"
    if abs(val) >= 1000:
        return f"{val:,.0f}"
    if abs(val) >= 10:
        return f"{val:.1f}"
    return f"{val:.2f}"


def _format_change(val, is_pct=False):
    """Format a period/YoY change with sign and unit."""
    if val is None:
        return "&mdash;"
    sign = "+" if val > 0 else ""
    unit = "pp" if is_pct else "%"
    return f"{sign}{val:.2f}{unit}"


def _signal_class(signal: str) -> str:
    """Map a signal string to a CSS class."""
    s = signal.lower()
    if "reversal" in s:
        return "signal-reversal"
    if "unusual" in s:
        return "signal-unusual"
    if "accelerat" in s or "decelerat" in s:
        return "signal-accel"
    if "turned negative" in s or "turned positive" in s:
        return "signal-turn"
    return "signal-info"


def _format_release(ts: str) -> str:
    """Format a captured_at timestamp like '2026-03-26T08:58:24' for display."""
    if not ts:
        return ""
    # Show as 'Mar 26 8:58a'
    try:
        dt = datetime.fromisoformat(ts)
        ampm = "a" if dt.hour < 12 else "p"
        hour = dt.hour % 12 or 12
        return dt.strftime(f"%b %-d") + f" {hour}:{dt.minute:02d}{ampm}"
    except (ValueError, TypeError):
        return ts[:10]


def _trend_arrow(a: dict) -> str:
    if a["period_pct"] is None:
        return ""
    if a["period_pct"] > 0:
        return '<span class="arrow up">&uarr;</span>'
    if a["period_pct"] < 0:
        return '<span class="arrow down">&darr;</span>'
    return '<span class="arrow flat">&rarr;</span>'


def _series_csv(series_id: str, name: str, db_path: Path = DB_PATH) -> str:
    """Build a Flourish-ready CSV string for a series."""
    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT date, value FROM observations "
        "WHERE series_id = ? ORDER BY date",
        (series_id,),
    ).fetchall()

    # Get the right calc types
    period_rows = con.execute(
        "SELECT date, value FROM calculated "
        "WHERE series_id = ? AND calc_type IN ('period_pct', 'period_pp') "
        "ORDER BY date",
        (series_id,),
    ).fetchall()
    yoy_rows = con.execute(
        "SELECT date, value FROM calculated "
        "WHERE series_id = ? AND calc_type IN ('yoy_pct', 'yoy_pp') "
        "ORDER BY date",
        (series_id,),
    ).fetchall()
    con.close()

    period_map = dict(period_rows)
    yoy_map = dict(yoy_rows)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["date", name, "Period Change", "YoY Change"])
    for d, v in rows:
        writer.writerow([d, v, period_map.get(d, ""), yoy_map.get(d, "")])
    return buf.getvalue()


def _group_csv(group_id: str, series_list: list,
               db_path: Path = DB_PATH) -> str:
    """Build a Flourish-ready CSV for a whole group (wide format)."""
    con = sqlite3.connect(db_path)
    all_dates = set()
    series_data = {}
    names = {}

    for a in series_list:
        sid = a["series_id"]
        names[sid] = a["name"]
        rows = con.execute(
            "SELECT date, value FROM observations "
            "WHERE series_id = ? ORDER BY date", (sid,),
        ).fetchall()
        series_data[sid] = dict(rows)
        all_dates.update(d for d, _ in rows)

    con.close()
    dates = sorted(all_dates)

    buf = io.StringIO()
    writer = csv.writer(buf)
    sids = [a["series_id"] for a in series_list]
    writer.writerow(["date"] + [names[s] for s in sids])
    for d in dates:
        writer.writerow([d] + [series_data[s].get(d, "") for s in sids])
    return buf.getvalue()


def _release_dates(db_path: Path = DB_PATH) -> dict:
    """Return {series_id: captured_at} for the latest observation of each series."""
    con = sqlite3.connect(db_path)
    rows = con.execute("""
        SELECT o.series_id, o.captured_at
        FROM observations o
        INNER JOIN (
            SELECT series_id, MAX(date) as max_date
            FROM observations GROUP BY series_id
        ) latest ON o.series_id = latest.series_id AND o.date = latest.max_date
        WHERE o.captured_at IS NOT NULL
    """).fetchall()
    con.close()
    return {sid: ts for sid, ts in rows}


def _load_analysis(today: str) -> str:
    """Load today's LLM daily analysis markdown, returning just the narrative."""
    analysis_path = Path(__file__).parent.parent / "summaries" / f"daily analysis {today}.md"
    if not analysis_path.exists():
        return ""
    text = analysis_path.read_text()
    # Strip the title line and the appended raw Signals/Summary sections
    parts = text.split("\n---\n", 1)
    narrative = parts[0]
    # Remove the leading "# Daily Analysis — date" line
    lines = narrative.split("\n")
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    return "\n".join(lines).strip()


def _md_to_html(md: str) -> str:
    """Minimal markdown→HTML for the analysis (headers, paragraphs, bold, italic)."""
    html_lines = []
    for line in md.split("\n"):
        stripped = line.strip()
        if not stripped:
            html_lines.append("")
            continue
        if stripped.startswith("## "):
            html_lines.append(f'<h3 class="analysis-h">{stripped[3:]}</h3>')
        elif stripped.startswith("# "):
            html_lines.append(f'<h2 class="analysis-h">{stripped[2:]}</h2>')
        else:
            # Bold and italic
            import re
            s = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', stripped)
            s = re.sub(r'\*(.+?)\*', r'<em>\1</em>', s)
            html_lines.append(f'<p class="analysis-p">{s}</p>')
    return "\n".join(html_lines)


def generate_briefing(cfg: dict, db_path: Path = DB_PATH,
                      updated_ids: set = None) -> str:
    """Generate the full HTML briefing. Returns HTML string."""
    summary = generate_summary(cfg, db_path)
    revisions = get_recent_revisions(days=30, db_path=db_path)
    today = date.today().isoformat()

    # Collect all analyses with group context
    all_series = []  # (group_id, group_name, analysis)
    for gid, gdata in summary.get("groups", {}).items():
        for a in gdata["series"]:
            all_series.append((gid, gdata["name"], a))

    # Split into signal vs quiet
    signal_series = [(g, gn, a) for g, gn, a in all_series if a["signals"]]
    quiet_series = [(g, gn, a) for g, gn, a in all_series if not a["signals"]]

    # Get sparkline data for all series
    sparklines = {}
    for _, _, a in all_series:
        sid = a["series_id"]
        pts = _sparkline_data(sid, n=24, db_path=db_path)
        sparklines[sid] = pts

    # Build CSV data for export (as JSON-encoded strings embedded in page)
    csv_data = {}
    for _, _, a in all_series:
        csv_data[a["series_id"]] = _series_csv(
            a["series_id"], a["name"], db_path)
    group_csvs = {}
    for gid, gdata in summary.get("groups", {}).items():
        group_csvs[gid] = _group_csv(gid, gdata["series"], db_path)

    release_ts = _release_dates(db_path)
    analysis_md = _load_analysis(today)

    html = _render_page(
        today=today,
        analysis_html=_md_to_html(analysis_md) if analysis_md else "",
        signal_series=signal_series,
        quiet_series=quiet_series,
        revisions=revisions,
        release_dates=release_ts,
        sparklines=sparklines,
        summary=summary,
        csv_data=csv_data,
        group_csvs=group_csvs,
        updated_ids=updated_ids or set(),
    )
    return html


def _render_page(**ctx) -> str:
    """Render the full HTML page."""
    today = ctx["today"]

    # Build sections
    analysis_html = ctx.get("analysis_html", "")
    signals_html = _render_signals(ctx)
    revisions_html = _render_revisions(ctx["revisions"])
    all_groups_html = _render_all_groups(ctx)
    csv_json = json.dumps(ctx["csv_data"])
    group_csv_json = json.dumps(ctx["group_csvs"])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Econ Data Briefing — {today}</title>
<style>
{_css()}
</style>
</head>
<body>

<header>
  <h1>Econ Data Briefing</h1>
  <div class="date">{today}</div>
</header>

<nav>
  <a href="#signals" class="active" onclick="showTab('signals', this)">What's Moving</a>
  <a href="#deepdive" onclick="showTab('deepdive', this)">All Data</a>
  <a href="#revisions" onclick="showTab('revisions', this)">Revisions{_badge(len(ctx['revisions']))}</a>
</nav>

<main>
  <section id="signals" class="tab-content active">
    {f'<div class="analysis-block">{analysis_html}</div>' if analysis_html else ''}
    {signals_html}
  </section>

  <section id="deepdive" class="tab-content">
    {all_groups_html}
  </section>

  <section id="revisions" class="tab-content">
    {revisions_html}
  </section>
</main>

<script>
var csvData = {csv_json};
var groupCsvData = {group_csv_json};
{_js()}
</script>
</body>
</html>"""


def _badge(n: int) -> str:
    if n == 0:
        return ""
    return f' <span class="badge">{n}</span>'


def _render_signals(ctx) -> str:
    """Render the What's Moving section."""
    signal_series = ctx["signal_series"]
    sparklines = ctx["sparklines"]
    updated_ids = ctx["updated_ids"]
    release_dates = ctx.get("release_dates", {})

    if not signal_series:
        return "<p class='muted'>No signals today.</p>"

    # Group by group_id
    by_group = {}
    for gid, gname, a in signal_series:
        by_group.setdefault(gid, {"name": gname, "series": []})
        by_group[gid]["series"].append(a)

    parts = []
    for gid, gdata in by_group.items():
        rows = []
        for a in gdata["series"]:
            sid = a["series_id"]
            is_pct = a.get("is_percent", False)
            fresh = "fresh" if sid in updated_ids else ""
            spark = _sparkline_svg(sparklines.get(sid, []))
            released = _format_release(release_dates.get(sid, ""))

            signal_tags = " ".join(
                f'<span class="signal-tag {_signal_class(s)}">{s}</span>'
                for s in a["signals"]
            )

            freq = a.get("frequency", "monthly")
            date_str = a["latest_date"] if freq == "daily" else a["latest_date"][:7]

            rows.append(f"""
            <tr class="series-row {fresh}" onclick="toggleDetail('{sid}')">
              <td class="name-col">
                <div class="series-name">{a['name']}</div>
                <div class="series-id">{sid}</div>
              </td>
              <td class="spark-col">{spark}</td>
              <td class="date-col">{date_str}</td>
              <td class="val-col">{_format_val(a['latest_value'], is_pct)}</td>
              <td class="chg-col">{_trend_arrow(a)} {_format_change(a['period_pct'], is_pct)}</td>
              <td class="chg-col">{_format_change(a['yoy_pct'], is_pct)}</td>
              <td class="release-col">{released}</td>
              <td class="signal-col">{signal_tags}</td>
              <td class="export-col"><button class="btn-export" onclick="event.stopPropagation(); downloadCsv('{sid}')">CSV</button></td>
            </tr>
            <tr class="detail-row" id="detail-{sid}" style="display:none">
              <td colspan="9">
                <div class="detail-content" id="detail-content-{sid}">Loading...</div>
              </td>
            </tr>""")

        parts.append(f"""
        <div class="group-block">
          <div class="group-header">
            <h3>{gdata['name']}</h3>
            <button class="btn-export-group" onclick="downloadGroupCsv('{gid}')">Export Group CSV</button>
          </div>
          <table class="data-table">
            <thead>
              <tr>
                <th class="name-col">Series</th>
                <th class="spark-col"></th>
                <th class="date-col">Date</th>
                <th class="val-col">Value</th>
                <th class="chg-col">Period</th>
                <th class="chg-col">YoY</th>
                <th class="release-col">Released</th>
                <th class="signal-col">Signals</th>
                <th class="export-col"></th>
              </tr>
            </thead>
            <tbody>{"".join(rows)}</tbody>
          </table>
        </div>""")

    return "\n".join(parts)


def _render_all_groups(ctx) -> str:
    """Render the All Data tab with every group."""
    summary = ctx["summary"]
    sparklines = ctx["sparklines"]
    updated_ids = ctx["updated_ids"]
    release_dates = ctx.get("release_dates", {})

    parts = []
    for gid, gdata in summary.get("groups", {}).items():
        rows = []
        for a in gdata["series"]:
            sid = a["series_id"]
            is_pct = a.get("is_percent", False)
            fresh = "fresh" if sid in updated_ids else ""
            spark = _sparkline_svg(sparklines.get(sid, []))
            released = _format_release(release_dates.get(sid, ""))

            signal_tags = ""
            if a["signals"]:
                signal_tags = " ".join(
                    f'<span class="signal-tag {_signal_class(s)}">{s}</span>'
                    for s in a["signals"]
                )

            freq = a.get("frequency", "monthly")
            date_str = a["latest_date"] if freq == "daily" else a["latest_date"][:7] if a["latest_date"] else ""

            trend = ""
            if a["trend_dir"] and a["trend_periods"] > 1:
                unit = "d" if freq == "daily" else "mo"
                trend = f'<span class="trend">{a["trend_dir"]} {a["trend_periods"]}{unit}</span>'

            rows.append(f"""
            <tr class="series-row {fresh}" onclick="toggleDetail('{sid}')">
              <td class="name-col">
                <div class="series-name">{a['name']}</div>
                <div class="series-id">{sid}</div>
              </td>
              <td class="spark-col">{spark}</td>
              <td class="date-col">{date_str}</td>
              <td class="val-col">{_format_val(a['latest_value'], is_pct)}</td>
              <td class="chg-col">{_trend_arrow(a)} {_format_change(a['period_pct'], is_pct)}</td>
              <td class="chg-col">{_format_change(a['yoy_pct'], is_pct)}</td>
              <td class="release-col">{released}</td>
              <td class="signal-col">{signal_tags} {trend}</td>
              <td class="export-col"><button class="btn-export" onclick="event.stopPropagation(); downloadCsv('{sid}')">CSV</button></td>
            </tr>
            <tr class="detail-row" id="detail-{sid}" style="display:none">
              <td colspan="9">
                <div class="detail-content" id="detail-content-{sid}">Loading...</div>
              </td>
            </tr>""")

        parts.append(f"""
        <div class="group-block">
          <div class="group-header">
            <h3>{gdata['name']}</h3>
            <button class="btn-export-group" onclick="downloadGroupCsv('{gid}')">Export Group CSV</button>
          </div>
          <table class="data-table">
            <thead>
              <tr>
                <th class="name-col">Series</th>
                <th class="spark-col"></th>
                <th class="date-col">Date</th>
                <th class="val-col">Value</th>
                <th class="chg-col">Period</th>
                <th class="chg-col">YoY</th>
                <th class="release-col">Released</th>
                <th class="signal-col">Signals</th>
                <th class="export-col"></th>
              </tr>
            </thead>
            <tbody>{"".join(rows)}</tbody>
          </table>
        </div>""")

    return "\n".join(parts)


def _render_revisions(revisions: list) -> str:
    if not revisions:
        return "<p class='muted'>No revisions in the past 30 days.</p>"

    # Group by series, then sort dates within each group
    by_series = {}
    for r in revisions:
        sid = r["series_id"]
        if sid not in by_series:
            by_series[sid] = {
                "name": r.get("name", sid),
                "series_id": sid,
                "revisions": [],
            }
        by_series[sid]["revisions"].append(r)

    # Sort each group's revisions by date
    for group in by_series.values():
        group["revisions"].sort(key=lambda r: r["date"])

    # Sort groups by most recent detected_at (newest first)
    sorted_groups = sorted(
        by_series.values(),
        key=lambda g: max(r.get("detected_at", "") for r in g["revisions"]),
        reverse=True,
    )

    parts = []
    for group in sorted_groups:
        rows = []
        for r in group["revisions"]:
            direction = "up" if r["new_value"] > r["old_value"] else "down"
            arrow = "&uarr;" if direction == "up" else "&darr;"
            detected = r.get("detected_at", "")[:10]
            rows.append(f"""
            <tr>
              <td>{r['date']}</td>
              <td class="val-col">{r['old_value']:,.1f}</td>
              <td class="val-col">{r['new_value']:,.1f}</td>
              <td class="chg-col"><span class="arrow {direction}">{arrow}</span> {r['pct_change']:+.2f}%</td>
              <td class="date-col">{detected}</td>
            </tr>""")

        parts.append(f"""
        <div class="group-block">
          <div class="group-header">
            <h3>{group['name']}</h3>
            <span class="series-id">{group['series_id']}</span>
          </div>
          <table class="data-table revision-table">
            <thead>
              <tr>
                <th>Observation Date</th>
                <th class="val-col">Old Value</th>
                <th class="val-col">New Value</th>
                <th class="chg-col">Change</th>
                <th class="date-col">Detected</th>
              </tr>
            </thead>
            <tbody>{"".join(rows)}</tbody>
          </table>
        </div>""")

    return "\n".join(parts)


def _css() -> str:
    return """
:root {
  --bg: #0d1117;
  --surface: #161b22;
  --border: #30363d;
  --text: #e6edf3;
  --text-muted: #8b949e;
  --accent: #58a6ff;
  --green: #3fb950;
  --red: #f85149;
  --amber: #d29922;
  --blue: #58a6ff;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.5;
  padding: 0;
}

header {
  padding: 24px 32px 16px;
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: baseline;
  gap: 16px;
}
header h1 { font-size: 20px; font-weight: 600; }
header .date { color: var(--text-muted); font-size: 14px; }

nav {
  display: flex;
  gap: 0;
  border-bottom: 1px solid var(--border);
  padding: 0 32px;
  background: var(--surface);
}
nav a {
  padding: 12px 20px;
  color: var(--text-muted);
  text-decoration: none;
  font-size: 14px;
  font-weight: 500;
  border-bottom: 2px solid transparent;
  transition: all 0.15s;
}
nav a:hover { color: var(--text); }
nav a.active {
  color: var(--text);
  border-bottom-color: var(--accent);
}

.badge {
  background: var(--amber);
  color: #000;
  font-size: 11px;
  font-weight: 600;
  padding: 1px 6px;
  border-radius: 10px;
  vertical-align: middle;
}

main { padding: 24px 32px; max-width: 1400px; }

.tab-content { display: none; }
.tab-content.active { display: block; }

.analysis-block {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 20px 24px;
  margin-bottom: 32px;
  line-height: 1.7;
}
.analysis-h {
  font-size: 14px;
  font-weight: 600;
  color: var(--accent);
  margin: 20px 0 8px;
}
.analysis-h:first-child { margin-top: 0; }
.analysis-p {
  font-size: 14px;
  color: var(--text);
  margin: 0 0 10px;
}
.analysis-p em {
  color: var(--text-muted);
  font-size: 12px;
}

.group-block {
  margin-bottom: 32px;
  border: 1px solid var(--border);
  border-radius: 8px;
  overflow: hidden;
}
.group-header {
  background: var(--surface);
  padding: 12px 16px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border);
}
.group-header h3 {
  font-size: 14px;
  font-weight: 600;
  color: var(--text);
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}
.data-table thead th {
  text-align: left;
  padding: 8px 12px;
  color: var(--text-muted);
  font-weight: 500;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 1px solid var(--border);
  background: var(--surface);
}

.series-row {
  cursor: pointer;
  transition: background 0.1s;
}
.series-row:hover { background: rgba(88, 166, 255, 0.06); }
.series-row td { padding: 8px 12px; border-bottom: 1px solid var(--border); }

.series-row.fresh td { background: rgba(63, 185, 80, 0.05); }

.name-col { min-width: 200px; }
.series-name { font-weight: 500; }
.series-id { font-size: 11px; color: var(--text-muted); font-family: monospace; }
.spark-col { width: 130px; }
.spark-hover:hover { fill: var(--accent); stroke: var(--accent); stroke-width: 1; r: 3; }
.date-col { width: 90px; color: var(--text-muted); font-family: monospace; font-size: 12px; }
.val-col { text-align: right; font-family: monospace; width: 100px; }
.chg-col { text-align: right; font-family: monospace; width: 100px; }
.release-col { width: 100px; color: var(--text-muted); font-size: 11px; white-space: nowrap; }
.signal-col { min-width: 160px; }
.export-col { width: 50px; text-align: center; }

.arrow { font-weight: 700; }
.arrow.up { color: var(--green); }
.arrow.down { color: var(--red); }
.arrow.flat { color: var(--text-muted); }

.trend {
  font-size: 11px;
  color: var(--text-muted);
  font-style: italic;
}

.signal-tag {
  display: inline-block;
  font-size: 11px;
  padding: 1px 8px;
  border-radius: 10px;
  margin: 1px 2px;
  white-space: nowrap;
}
.signal-reversal { background: rgba(210, 153, 34, 0.2); color: var(--amber); }
.signal-unusual { background: rgba(248, 81, 73, 0.2); color: var(--red); }
.signal-accel { background: rgba(88, 166, 255, 0.2); color: var(--blue); }
.signal-turn { background: rgba(63, 185, 80, 0.15); color: var(--green); }
.signal-info { background: rgba(139, 148, 158, 0.15); color: var(--text-muted); }

.detail-row td {
  padding: 0 !important;
  border-bottom: 1px solid var(--border);
  background: var(--surface);
}
.detail-content {
  padding: 16px 24px;
  font-size: 13px;
}
.detail-content table {
  width: auto;
  margin-top: 8px;
  font-size: 12px;
  font-family: monospace;
}
.detail-content table th,
.detail-content table td {
  padding: 3px 12px;
  text-align: right;
  border-bottom: 1px solid var(--border);
}
.detail-content table th:first-child,
.detail-content table td:first-child { text-align: left; }

.btn-export, .btn-export-group {
  background: transparent;
  border: 1px solid var(--border);
  color: var(--text-muted);
  padding: 3px 10px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 11px;
  transition: all 0.15s;
}
.btn-export:hover, .btn-export-group:hover {
  border-color: var(--accent);
  color: var(--accent);
}

.muted { color: var(--text-muted); padding: 32px 0; }

.revision-table .series-id { font-size: 12px; }
"""


def _js() -> str:
    return """
function showTab(id, el) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('nav a').forEach(a => a.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  el.classList.add('active');
}

function toggleDetail(sid) {
  var row = document.getElementById('detail-' + sid);
  if (!row) return;
  var visible = row.style.display !== 'none';
  row.style.display = visible ? 'none' : 'table-row';

  if (!visible) {
    var content = document.getElementById('detail-content-' + sid);
    if (content.innerText === 'Loading...') {
      loadDetail(sid, content);
    }
  }
}

function loadDetail(sid, el) {
  var csv = csvData[sid];
  if (!csv) { el.innerHTML = '<em>No data</em>'; return; }

  var lines = csv.trim().split('\\n');
  var headers = lines[0].split(',');
  var rows = lines.slice(1).map(function(l) { return l.split(','); });

  // Show last 24 rows
  var recent = rows.slice(-24);
  var html = '<table><thead><tr>';
  headers.forEach(function(h) { html += '<th>' + h + '</th>'; });
  html += '</tr></thead><tbody>';
  recent.forEach(function(r) {
    html += '<tr>';
    r.forEach(function(v, i) {
      html += '<td>' + (v || '&mdash;') + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  el.innerHTML = html;
}

function downloadCsv(sid) {
  var csv = csvData[sid];
  if (!csv) return;
  var blob = new Blob([csv], {type: 'text/csv'});
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = sid + '.csv';
  a.click();
}

function downloadGroupCsv(gid) {
  var csv = groupCsvData[gid];
  if (!csv) return;
  var blob = new Blob([csv], {type: 'text/csv'});
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = gid + '.csv';
  a.click();
}
"""
