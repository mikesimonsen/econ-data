"""Generate a daily editorial briefing as a self-contained HTML page.

Sections:
  1. Today — series released today with inline revision notes
  2. Recent Data Releases — last 7 days (excluding today) with inline revisions
  3. All Data — expandable detail with sparklines and data tables
"""

import base64
import csv
import io
import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from econ_data.store_sqlite import DB_PATH, get_recent_revisions
from econ_data.summary import generate_summary

# Extra search keywords per group so users can find series by concept,
# not just by name.  Keys = group_id, values = space-separated terms.
_GROUP_SEARCH_TAGS = {
    "cpi": "inflation prices consumer",
    "cpi-metro": "inflation prices consumer metro city",
    "pce": "inflation prices consumer",
    "ppi": "inflation prices producer wholesale",
    "case-shiller": "home price prices housing",
    "existing-home-sales": "home sales housing prices",
    "existing-home-sales-nsa": "home sales housing prices",
    "new-home-sales": "home sales housing prices",
    "housing-starts": "housing construction residential",
    "building-permits": "housing construction residential",
    "housing-under-construction": "housing construction residential",
    "housing-completions": "housing construction residential",
    "construction-spending": "housing construction residential spending",
    "construction-employment": "construction jobs employment",
    "jolts": "jobs labor openings hires quits",
    "unemployment": "jobs labor unemployment",
    "labor-force": "jobs labor employment participation",
    "labor-quality": "jobs labor",
    "jobless-claims": "jobs labor unemployment claims",
    "treasury-yields": "rates bonds interest",
    "mortgage-rates": "rates mortgage interest housing",
    "households": "housing homeownership vacancy",
    "mortgage-spread": "spread mortgage treasury rates housing",
    "altos-inventory": "housing inventory listings supply altos",
    "altos-new-listings": "housing new listings supply altos",
    "altos-new-pending": "housing pending sales contracts altos",
}

_FAVICON_CACHE = None
_FAVICON_PATH = Path(__file__).parent.parent / "MS-MikeSimonsen-logo1.png"


def _favicon_b64() -> str:
    global _FAVICON_CACHE
    if _FAVICON_CACHE is None:
        if _FAVICON_PATH.exists():
            _FAVICON_CACHE = base64.b64encode(_FAVICON_PATH.read_bytes()).decode()
        else:
            _FAVICON_CACHE = ""
    return _FAVICON_CACHE


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
    if "outlier" in s:
        return "signal-outlier"
    if "reversal" in s:
        return "signal-reversal"
    if "unusual" in s:
        return "signal-unusual"
    if "accelerat" in s or "decelerat" in s:
        return "signal-accel"
    if "worsening" in s or "improving" in s:
        return "signal-accel"
    if "turned negative" in s or "turned positive" in s:
        return "signal-turn"
    if "lowest since" in s or "highest since" in s or "all-time" in s:
        return "signal-extreme"
    if "yoy negative" in s or "yoy positive" in s:
        return "signal-sustained"
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


def _series_csv(series_id: str, name: str, db_path: Path = DB_PATH,
                revisions: list = None) -> str:
    """Build a Flourish-ready CSV string for a series, with revision columns."""
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

    # Build revision lookup: date -> (old_value, diff)
    rev_map = {}
    if revisions:
        for r in revisions:
            rev_map[r["date"]] = (r["old_value"], r["new_value"] - r["old_value"])

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["date", name, "Period Change", "YoY Change",
                     "Revised From", "Revision Diff"])
    for d, v in rows:
        old_val, diff = rev_map.get(d, ("", ""))
        writer.writerow([d, v, period_map.get(d, ""), yoy_map.get(d, ""),
                        old_val, diff])
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
    """Load today's LLM daily analysis markdown, returning just the narrative.

    Falls back to the most recent analysis file if today's doesn't exist.
    """
    import re
    summaries_dir = Path(__file__).parent.parent / "summaries"
    analysis_path = summaries_dir / f"daily analysis {today}.md"
    if not analysis_path.exists():
        # Find the most recent analysis file
        candidates = sorted(summaries_dir.glob("daily analysis *.md"), reverse=True)
        if not candidates:
            return ""
        analysis_path = candidates[0]
    text = analysis_path.read_text()
    # Strip the title line and the appended raw Signals/Summary sections
    parts = text.split("\n---\n", 1)
    narrative = parts[0]
    # Remove the leading "# Daily Analysis — date" line
    lines = narrative.split("\n")
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    narrative = "\n".join(lines).strip()
    # Strip any "## Revisions" section (revisions now shown inline with data)
    narrative = re.sub(
        r'## Revisions\s*\n.*?(?=\n## |\Z)', '', narrative, flags=re.DOTALL
    ).strip()
    return narrative


def _md_to_html(md: str, name_to_sid: dict = None) -> str:
    """Minimal markdown→HTML for the analysis. Links series names to their rows."""
    import re

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
            s = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', stripped)
            s = re.sub(r'\*(.+?)\*', r'<em>\1</em>', s)
            # Link series names/keywords to their rows
            if name_to_sid:
                s = _linkify_series(s, name_to_sid)
            html_lines.append(f'<p class="analysis-p">{s}</p>')
    return "\n".join(html_lines)


def _linkify_series(text: str, name_to_sid: dict) -> str:
    """Replace series name mentions with anchor links to their rows."""
    import re
    # Sort by length descending so longer names match first
    for name, sid in sorted(name_to_sid.items(), key=lambda x: -len(x[0])):
        # Only match text that isn't already inside an <a> tag
        # Split on existing links, only search in non-link parts
        parts = re.split(r'(<a [^>]*>.*?</a>)', text)
        new_parts = []
        replaced = False
        for part in parts:
            if part.startswith('<a ') or replaced:
                new_parts.append(part)
            else:
                pattern = re.compile(re.escape(name), re.IGNORECASE)
                if pattern.search(part):
                    link = f'<a href="#row-{sid}" class="series-link" onclick="scrollToSeries(\'{sid}\')">{name}</a>'
                    part = pattern.sub(link, part, count=1)
                    replaced = True
                new_parts.append(part)
        text = "".join(new_parts)
    return text


def _build_name_map(summary: dict) -> dict:
    """Build a map of recognizable names/keywords → series_id for linking."""
    name_to_sid = {}
    for gdata in summary.get("groups", {}).values():
        for a in gdata["series"]:
            sid = a["series_id"]
            name = a["name"]
            # Full name
            name_to_sid[name] = sid
            # Short names — but not bare city names (ambiguous with Case-Shiller)
            # Keep "CPI" in the alias so "Dallas CPI" doesn't match Case-Shiller Dallas
            for prefix in ["CPI All Items (", "CPI Less Food & Energy ("]:
                if name.startswith(prefix):
                    short = name[len(prefix):].rstrip(")")
                    if len(short) > 4:
                        name_to_sid[short] = sid
    # Add well-known aliases
    aliases = {
        "unemployment rate": "UNRATE", "jobless rate": "UNRATE",
        "nonfarm payrolls": "PAYEMS", "payrolls": "PAYEMS",
        "initial claims": "ICSA", "continuing claims": "CCSA",
        "core CPI": "CPILFESL", "headline CPI": "CPIAUCSL",
        "core PCE": "PCEPILFE",
        "housing starts": "HOUST", "building permits": "PERMIT",
        "existing home sales": "EXHOSLUSM495S",
        "new home sales": "HSN1F", "new single-family homes": "HSN1F",
        "median sales price": "HOSMEDUSM052N",
        "months supply": "MSACSR", "months' supply": "MSACSR",
        "homeownership rate": "RHORUSQ156N",
        "Case-Shiller": "CSUSHPISA", "Case Shiller": "CSUSHPISA",
        "10-year Treasury": "DGS10", "10-year yield": "DGS10",
        "labor force participation": "CIVPART",
        "job openings": "JTSJOL", "quits": "JTSQUL",
        "construction spending": "TLRESCONS",
        "Energy CPI": "CPIENGSL", "CPI energy": "CPIENGSL",
        "shelter CPI": "CUSR0000SAH1", "CPI shelter": "CUSR0000SAH1",
        "food CPI": "CPIFABSL",
        "apparel": "CPIAPPSL",
        "Chicago CPI": "CUURA207SA0",
        "Detroit CPI": "CUURA208SA0",
        "Dallas CPI": "CUURA316SA0",
        "Houston CPI": "CUURA318SA0",
        "Atlanta CPI": "CUURA319SA0",
        "Miami CPI": "CUURA320SA0",
        "San Francisco CPI": "CUURA422SA0",
        "Seattle CPI": "CUURA423SA0",
        "New York CPI": "CUURA101SA0",
        "Philadelphia CPI": "CUURA102SA0",
        "Boston CPI": "CUURA103SA0",
        "Los Angeles CPI": "BLS_CPI_LA", "LA CPI": "BLS_CPI_LA",
        "Los Angeles-area": "BLS_CPI_LA",
        "Denver CPI": "BLS_CPI_DENVER",
        "Minneapolis CPI": "BLS_CPI_MINNEAPOLIS",
        "Phoenix CPI": "BLS_CPI_PHOENIX",
        "San Diego CPI": "BLS_CPI_SANDIEGO",
        "Portland CPI": "BLS_CPI_PORTLAND",
        "Baltimore CPI": "BLS_CPI_BALTIMORE",
        "Tampa CPI": "BLS_CPI_TAMPA",
        "Washington CPI": "BLS_CPI_DC", "DC CPI": "BLS_CPI_DC",
        "St. Louis CPI": "BLS_CPI_STLOUIS",
        "Milwaukee CPI": "BLS_CPI_MILWAUKEE",
        "Cincinnati CPI": "BLS_CPI_CINCINNATI",
        "Riverside CPI": "BLS_CPI_RIVERSIDE",
        "jumbo mortgage": "MND_30YR_JUMBO",
        "30-year fixed": "MND_30YR_FIXED", "30-year mortgage": "MND_30YR_FIXED",
        "median duration": "UEMPMED",
        "long-term joblessness": "UEMP27OV", "27+ weeks": "UEMP27OV",
        "mortgage spread": "SPREAD_30Y_10Y", "rate spread": "SPREAD_30Y_10Y",
        "Altos inventory": "ALTOS_INVENTORY", "total inventory": "ALTOS_INVENTORY",
    }
    name_to_sid.update(aliases)
    return name_to_sid


def _group_max_date(series_list: list) -> str:
    """Return the most recent latest_date across a list of series analyses."""
    dates = [a["latest_date"] for a in series_list if a.get("latest_date")]
    return max(dates) if dates else ""


def generate_briefing(cfg: dict, db_path: Path = DB_PATH,
                      updated_ids: set = None) -> str:
    """Generate the full HTML briefing. Returns HTML string."""
    summary = generate_summary(cfg, db_path)
    revisions = get_recent_revisions(days=30, db_path=db_path)
    today = date.today().isoformat()
    recent_cutoff = (date.today() - timedelta(days=7)).isoformat()

    # Collect all analyses with group context
    all_series = []  # (group_id, group_name, analysis)
    for gid, gdata in summary.get("groups", {}).items():
        for a in gdata["series"]:
            all_series.append((gid, gdata["name"], a))

    # Get sparkline data for all series
    sparklines = {}
    for _, _, a in all_series:
        sid = a["series_id"]
        pts = _sparkline_data(sid, n=24, db_path=db_path)
        sparklines[sid] = pts

    release_ts = _release_dates(db_path)
    analysis_md = _load_analysis(today)
    name_map = _build_name_map(summary)

    # Build revision lookup by series_id
    revisions_by_series = {}
    for r in revisions:
        revisions_by_series.setdefault(r["series_id"], []).append(r)

    # Build CSV data for export (with revision columns)
    csv_data = {}
    for _, _, a in all_series:
        sid = a["series_id"]
        csv_data[sid] = _series_csv(
            sid, a["name"], db_path, revisions_by_series.get(sid))
    group_csvs = {}
    for gid, gdata in summary.get("groups", {}).items():
        group_csvs[gid] = _group_csv(gid, gdata["series"], db_path)

    # Classify series by release recency
    today_series = []
    recent_series = []
    for gid, gname, a in all_series:
        sid = a["series_id"]
        captured = release_ts.get(sid, "")[:10]
        if captured == today:
            today_series.append((gid, gname, a))
        elif captured >= recent_cutoff:
            recent_series.append((gid, gname, a))

    html = _render_page(
        today=today,
        analysis_html=_md_to_html(analysis_md, name_map) if analysis_md else "",
        today_series=today_series,
        recent_series=recent_series,
        revisions_by_series=revisions_by_series,
        release_dates=release_ts,
        sparklines=sparklines,
        summary=summary,
        csv_data=csv_data,
        group_csvs=group_csvs,
        updated_ids=updated_ids or set(),
        cfg=cfg,
        db_path=db_path,
    )
    return html


def _render_page(**ctx) -> str:
    """Render the full HTML page."""
    today = ctx["today"]

    # Build sections — today_html must render first (it populates chart_csv_data)
    analysis_html = ctx.get("analysis_html", "")
    today_html = _render_today(ctx)
    recent_html = _render_recent(ctx)
    all_groups_html = _render_all_groups(ctx)
    csv_json = json.dumps(ctx["csv_data"])
    group_csv_json = json.dumps(ctx["group_csvs"])
    chart_csv_json = json.dumps(ctx.get("chart_csv_data", {}))
    # Build release date map for CSV filenames (series_id -> YYYY-MM-DD)
    release_date_map = {
        sid: ts[:10] for sid, ts in ctx.get("release_dates", {}).items() if ts
    }
    release_dates_json = json.dumps(release_date_map)

    recent_count = len(ctx["recent_series"])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/png" href="data:image/png;base64,{_favicon_b64()}">
<title>Simonsen Daily Briefing — {today}</title>
<style>
{_css()}
</style>
</head>
<body>

<header>
  <h1>Simonsen Daily Briefing</h1>
  <div class="date">{today}</div>
</header>

<nav>
  <a href="#today" class="active" onclick="showTab('today', this)">Today</a>
  <a href="#recent" onclick="showTab('recent', this)">Recent Data Releases{_badge(recent_count)}</a>
  <a href="#deepdive" onclick="showTab('deepdive', this)">All Data</a>
  <div class="search-box">
    <input type="text" id="search-input" placeholder="Search series..." oninput="filterSeries(this.value)">
  </div>
</nav>

<main>
  <section id="today" class="tab-content active">
    {f'<div class="analysis-block">{analysis_html}</div>' if analysis_html else ''}
    {today_html}
  </section>

  <section id="recent" class="tab-content">
    {recent_html}
  </section>

  <section id="deepdive" class="tab-content">
    {all_groups_html}
  </section>
</main>

<script>
var csvData = {csv_json};
var groupCsvData = {group_csv_json};
var chartCsvData = {chart_csv_json};
var releaseDates = {release_dates_json};
{_js()}
</script>
<script data-goatcounter="https://mikesimonsen.goatcounter.com/count" async src="//gc.zgo.at/count.js"></script>
</body>
</html>"""


def _badge(n: int) -> str:
    if n == 0:
        return ""
    return f' <span class="badge">{n}</span>'


def _render_revision_notes(series_id: str, revisions_by_series: dict) -> str:
    """Render a mini revision table for a series."""
    revs = revisions_by_series.get(series_id, [])
    if not revs:
        return ""
    # Sort by observation date, most recent first
    revs = sorted(revs, key=lambda r: r["date"], reverse=True)
    rows = []
    for r in revs:
        direction = "up" if r["new_value"] > r["old_value"] else "down"
        arrow = "&uarr;" if direction == "up" else "&darr;"
        # Always show full date — monthly data is always the 1st so it's
        # still clear, and weekly/daily data keeps its precision
        try:
            d = datetime.strptime(r["date"], "%Y-%m-%d")
            label = d.strftime("%b %-d, %Y")
        except (ValueError, TypeError):
            label = r["date"]
        diff = r["new_value"] - r["old_value"]
        rows.append(
            f'<tr>'
            f'<td>{label}</td>'
            f'<td class="val-col">{r["old_value"]:,.0f}</td>'
            f'<td class="val-col">{r["new_value"]:,.0f}</td>'
            f'<td class="chg-col"><span class="arrow {direction}">{arrow}</span> {diff:+,.0f}</td>'
            f'<td class="chg-col">{r["pct_change"]:+.1f}%</td>'
            f'</tr>'
        )
    return (
        '<div class="revision-note">'
        '<div class="revision-label">Revisions</div>'
        '<table class="revision-mini-table">'
        '<thead><tr><th>Period</th><th class="val-col">Was</th>'
        '<th class="val-col">Now</th><th class="chg-col">Diff</th>'
        '<th class="chg-col">%</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table></div>'
    )


def _revision_direction_tag(series_id: str, revisions_by_series: dict) -> str:
    """Return a 'revised higher' or 'revised lower' signal tag if revisions exist."""
    revs = revisions_by_series.get(series_id, [])
    if not revs:
        return ""
    # Use the most recent revision to determine direction
    latest = max(revs, key=lambda r: r["date"])
    if latest["new_value"] > latest["old_value"]:
        return '<span class="signal-tag revised-higher">revised higher</span> '
    elif latest["new_value"] < latest["old_value"]:
        return '<span class="signal-tag revised-lower">revised lower</span> '
    return ""


def _search_terms(series_id: str, name: str, group_id: str,
                   group_name: str) -> str:
    """Build a lowercase search string for a series row."""
    extra = _GROUP_SEARCH_TAGS.get(group_id, "")
    return f"{name} {series_id} {group_name} {extra}".lower()


def _render_series_table(series_list, ctx, show_signals=True) -> str:
    """Render a grouped table of series with optional inline revisions."""
    sparklines = ctx["sparklines"]
    updated_ids = ctx["updated_ids"]
    release_dates = ctx.get("release_dates", {})
    revisions_by_series = ctx.get("revisions_by_series", {})

    # Group by group_id
    by_group = {}
    for gid, gname, a in series_list:
        by_group.setdefault(gid, {"name": gname, "series": []})
        by_group[gid]["series"].append(a)

    # Sort groups by most recent data (newest first)
    sorted_groups = sorted(
        by_group.items(),
        key=lambda item: _group_max_date(item[1]["series"]),
        reverse=True,
    )

    parts = []
    for gid, gdata in sorted_groups:
        rows = []
        for a in gdata["series"]:
            sid = a["series_id"]
            is_pct = a.get("is_percent", False)
            fresh = "fresh" if sid in updated_ids else ""
            spark = _sparkline_svg(sparklines.get(sid, []))
            released = _format_release(release_dates.get(sid, ""))
            search = _search_terms(sid, a["name"], gid, gdata["name"])

            signal_tags = ""
            if show_signals and a["signals"]:
                signal_tags = " ".join(
                    f'<span class="signal-tag {_signal_class(s)}">{s}</span>'
                    for s in a["signals"]
                )

            freq = a.get("frequency", "monthly")
            date_str = a["latest_date"] if freq in ("daily", "weekly") else a["latest_date"][:7]

            revision_html = _render_revision_notes(sid, revisions_by_series)
            has_rev = ' has-revision' if revision_html else ''
            rev_tag = _revision_direction_tag(sid, revisions_by_series)

            rows.append(f"""
            <tr class="series-row {fresh}{has_rev}" id="row-{sid}" data-search="{search}" data-sid="{sid}" onclick="toggleDetail('{sid}')">
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
              <td class="signal-col">{rev_tag}{signal_tags}</td>
              <td class="export-col"><button class="btn-export" onclick="event.stopPropagation(); downloadCsv('{sid}')">CSV</button></td>
            </tr>""")
            if revision_html:
                rows.append(f"""
            <tr class="revision-row" data-rev-for="{sid}">
              <td colspan="9">{revision_html}</td>
            </tr>""")
            rows.append(f"""
            <tr class="detail-row" id="detail-{sid}" style="display:none">
              <td colspan="9">
                <div class="detail-content" id="detail-content-{sid}">Loading...</div>
              </td>
            </tr>""")

        parts.append(f"""
        <div class="group-block" data-group="{gid}">
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


def _signal_score(signal: str) -> int:
    """Weight a signal string for hero chart ranking."""
    s = signal.lower()
    if "lowest since" in s or "highest since" in s or "all-time" in s:
        return 10
    if "unusual" in s:
        return 8
    if "reversal" in s:
        return 6
    if "yoy negative" in s or "yoy positive" in s:
        return 5
    if "accelerat" in s or "decelerat" in s or "worsening" in s or "improving" in s:
        return 4
    if "turned negative" in s or "turned positive" in s:
        return 3
    if "outlier" in s:
        return 2
    return 1


def _pick_hero_series(today_series: list, cfg: dict) -> list:
    """Score today's series and return top 1-2 for hero charts.

    Returns list of (group_id, group_name, analysis, chart_type, n_points)
    where chart_type is 'yoy' or 'raw'.
    """
    if not today_series:
        return []

    # Build headline set (first series in each group)
    headlines = set()
    for gid, g in cfg.get("groups", {}).items():
        series = g.get("series", [])
        if series:
            headlines.add(series[0]["id"])

    scored = []
    for gid, gname, a in today_series:
        sid = a["series_id"]
        freq = a.get("frequency", "monthly")

        # Sum signal weights
        score = sum(_signal_score(s) for s in a["signals"])
        if score == 0:
            score = 1  # baseline so headline bonus still works

        # Frequency penalty
        if freq == "daily":
            score *= 0.5
        elif freq == "weekly":
            score *= 0.8

        # Headline bonus — headline/national series get strong preference
        if sid in headlines:
            score *= 2.0

        # Decide what to chart
        signals_lower = " ".join(a["signals"]).lower()
        is_yoy_signal = any(kw in signals_lower for kw in [
            "yoy", "lowest since", "highest since", "all-time",
            "accelerat", "decelerat", "worsening", "improving",
        ])
        chart_type = "yoy" if is_yoy_signal else "raw"

        # Decide how many points of history
        n_points = _chart_history_length(a, freq)

        scored.append((score, gid, gname, a, chart_type, n_points))

    scored.sort(key=lambda x: x[0], reverse=True)
    if not scored:
        return []

    result = [(scored[0][1], scored[0][2], scored[0][3],
               scored[0][4], scored[0][5])]
    # Add second chart if it scores within 70% of the top
    if len(scored) > 1 and scored[1][0] >= scored[0][0] * 0.7:
        result.append((scored[1][1], scored[1][2], scored[1][3],
                       scored[1][4], scored[1][5]))
    return result


def _chart_history_length(a: dict, freq: str) -> int:
    """Determine how many observations to show in the hero chart."""
    signals_lower = " ".join(a["signals"]).lower()

    # If signal mentions "since YYYY", go back to that date + buffer
    import re
    since_match = re.search(r'since (\w+ \d{4})', signals_lower)
    if since_match:
        try:
            ref = datetime.strptime(since_match.group(1).title(), "%b %Y")
            latest = datetime.strptime(a["latest_date"], "%Y-%m-%d")
            months = (latest.year - ref.year) * 12 + (latest.month - ref.month)
            n = months + 6  # buffer
        except (ValueError, TypeError):
            n = 36
    elif re.search(r'negative \d+mo|positive \d+mo', signals_lower):
        # Sustained streak — show 2x the streak
        streak_match = re.search(r'(\d+)mo', signals_lower)
        if streak_match:
            n = int(streak_match.group(1)) * 2 + 6
        else:
            n = 36
    else:
        n = 36

    # Clamp
    if freq == "daily":
        return min(max(n * 21, 252), 504)  # trading days, 1-2 years
    return min(max(n, 18), 60)


def _chart_data(series_id: str, chart_type: str, n: int,
                db_path: Path = DB_PATH) -> list:
    """Fetch data for a hero chart. Returns [(date, value), ...]."""
    con = sqlite3.connect(db_path)
    if chart_type == "yoy":
        rows = con.execute(
            "SELECT date, value FROM calculated "
            "WHERE series_id = ? AND calc_type IN ('yoy_pct', 'yoy_pp') "
            "ORDER BY date DESC LIMIT ?",
            (series_id, n),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT date, value FROM observations "
            "WHERE series_id = ? ORDER BY date DESC LIMIT ?",
            (series_id, n),
        ).fetchall()
    con.close()
    return list(reversed(rows))


def _hero_chart_svg(points: list, title: str, is_yoy: bool = False,
                    width: int = 800, height: int = 280) -> str:
    """Render a full chart SVG with axes, gridlines, labels, and zero line."""
    if len(points) < 2:
        return ""

    values = [p[1] for p in points]
    vmin, vmax = min(values), max(values)
    vrange = vmax - vmin if vmax != vmin else 1
    # 10% padding
    pad_v = vrange * 0.1
    vmin_p = vmin - pad_v
    vmax_p = vmax + pad_v
    vrange_p = vmax_p - vmin_p

    # Layout
    left_margin = 65
    right_margin = 20
    top_margin = 15
    bottom_margin = 35
    plot_w = width - left_margin - right_margin
    plot_h = height - top_margin - bottom_margin

    color = "#d29922" if is_yoy else "#4A90D9"
    unit = "%" if is_yoy else ""

    def x_pos(i):
        return left_margin + plot_w * i / (len(points) - 1)

    def y_pos(v):
        return top_margin + plot_h * (1 - (v - vmin_p) / vrange_p)

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {width} {height}" '
        f'style="width:100%;max-width:{width}px;height:auto;'
        f'background:var(--surface);border-radius:8px">'
    )

    # Gridlines + Y-axis labels
    n_grid = 5
    for i in range(n_grid + 1):
        gv = vmin_p + vrange_p * i / n_grid
        gy = y_pos(gv)
        parts.append(
            f'<line x1="{left_margin}" y1="{gy:.1f}" '
            f'x2="{width - right_margin}" y2="{gy:.1f}" '
            f'stroke="#30363d" stroke-width="0.5"/>'
        )
        if is_yoy:
            label = f"{gv:+.1f}{unit}"
        elif abs(gv) >= 1000:
            label = f"{gv:,.0f}"
        elif abs(gv) >= 10:
            label = f"{gv:.1f}"
        else:
            label = f"{gv:.2f}"
        parts.append(
            f'<text x="{left_margin - 8}" y="{gy + 4:.1f}" '
            f'text-anchor="end" fill="#8b949e" font-size="11" '
            f'font-family="-apple-system,sans-serif">{label}</text>'
        )

    # Zero line for YoY charts
    if is_yoy and vmin_p < 0 < vmax_p:
        zy = y_pos(0)
        parts.append(
            f'<line x1="{left_margin}" y1="{zy:.1f}" '
            f'x2="{width - right_margin}" y2="{zy:.1f}" '
            f'stroke="#8b949e" stroke-width="1" stroke-dasharray="4,3"/>'
        )

    # X-axis labels — pick ~6 evenly spaced dates
    n_labels = min(6, len(points))
    step = max(1, (len(points) - 1) // (n_labels - 1)) if n_labels > 1 else 1
    for i in range(0, len(points), step):
        d = points[i][0]
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            label = dt.strftime("%b '%y")
        except (ValueError, TypeError):
            label = d[:7]
        xp = x_pos(i)
        parts.append(
            f'<text x="{xp:.1f}" y="{height - 8}" '
            f'text-anchor="middle" fill="#8b949e" font-size="11" '
            f'font-family="-apple-system,sans-serif">{label}</text>'
        )

    # Data polyline
    coords = []
    for i, (d, v) in enumerate(points):
        coords.append(f"{x_pos(i):.1f},{y_pos(v):.1f}")
    polyline = " ".join(coords)
    parts.append(
        f'<polyline points="{polyline}" fill="none" stroke="{color}" '
        f'stroke-width="2" stroke-linejoin="round"/>'
    )

    # Hover circles with tooltips
    for i, (d, v) in enumerate(points):
        xp = x_pos(i)
        yp = y_pos(v)
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            dlabel = dt.strftime("%b %Y")
        except (ValueError, TypeError):
            dlabel = d
        if is_yoy:
            vlabel = f"{v:+.1f}%"
        elif abs(v) >= 1000:
            vlabel = f"{v:,.0f}"
        elif abs(v) >= 10:
            vlabel = f"{v:.1f}"
        else:
            vlabel = f"{v:.2f}"
        parts.append(
            f'<circle cx="{xp:.1f}" cy="{yp:.1f}" r="6" '
            f'fill="transparent" stroke="none" class="spark-hover">'
            f'<title>{dlabel}: {vlabel}</title></circle>'
        )

    # Visible dot on last point
    lx = x_pos(len(points) - 1)
    ly = y_pos(values[-1])
    parts.append(
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="4" fill="{color}"/>'
    )

    parts.append('</svg>')
    return "\n".join(parts)


def _chart_csv_data(series_id: str, name: str, points: list,
                    chart_type: str) -> str:
    """Build CSV string for chart data download."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    col_name = f"{name} ({'YoY %' if chart_type == 'yoy' else 'Value'})"
    writer.writerow(["date", col_name])
    for d, v in points:
        writer.writerow([d, v])
    return buf.getvalue()


def _render_hero_charts(ctx) -> str:
    """Pick hero series and render charts for the Today tab."""
    cfg = ctx.get("cfg", {})
    today_series = ctx["today_series"]
    db_path = ctx.get("db_path", DB_PATH)
    release_dates = ctx.get("release_dates", {})

    heroes = _pick_hero_series(today_series, cfg)
    if not heroes:
        return ""

    chart_csvs = {}
    parts = []
    for i, (gid, gname, a, chart_type, n_points) in enumerate(heroes):
        sid = a["series_id"]
        points = _chart_data(sid, chart_type, n_points, db_path)
        if len(points) < 2:
            continue

        chart_id = f"hero-chart-{i}"
        is_yoy = chart_type == "yoy"
        chart_label = "Year-over-Year Change" if is_yoy else a["name"]
        svg = _hero_chart_svg(points, chart_label, is_yoy)

        # Caption
        is_pct = a.get("is_percent", False)
        latest_str = _format_val(a["latest_value"], is_pct)
        yoy_str = f" ({a['yoy_pct']:+.1f}% YoY)" if a.get("yoy_pct") is not None else ""
        signal_str = ". ".join(a["signals"]) if a["signals"] else ""
        caption = f"<strong>{a['name']}</strong>"
        if signal_str:
            caption += f" &mdash; {signal_str}."
        caption += f" Latest: {latest_str}{yoy_str}"

        # Release date for filename
        rd = release_dates.get(sid, "")[:10]
        suffix = f"_yoy" if is_yoy else ""
        fname_base = f"{sid}{suffix}_{rd}" if rd else f"{sid}{suffix}"

        # Store chart CSV
        chart_csvs[chart_id] = _chart_csv_data(sid, a["name"], points, chart_type)

        parts.append(f"""
        <div class="hero-chart" id="{chart_id}-container">
          <div class="chart-title">{a['name']}{' — YoY %' if is_yoy else ''}</div>
          {svg}
          <div class="chart-caption">{caption}</div>
          <div class="chart-actions">
            <button class="btn-export" onclick="downloadChartCsv('{chart_id}', '{fname_base}')">CSV</button>
            <button class="btn-export" onclick="downloadChartPng('{chart_id}', '{fname_base}')">PNG</button>
          </div>
        </div>""")

    # Store chart CSVs in context for JS embedding
    ctx["chart_csv_data"] = chart_csvs
    return "\n".join(parts)


def _render_today(ctx) -> str:
    """Render the Today section — hero charts + data table."""
    today_series = ctx["today_series"]
    hero_html = _render_hero_charts(ctx)
    if not today_series:
        return hero_html + "<p class='muted'>No new data released today.</p>"
    table_html = _render_series_table(today_series, ctx)
    return hero_html + table_html


def _render_recent(ctx) -> str:
    """Render the Recent Data Releases section — last 7 days, excluding today."""
    recent_series = ctx["recent_series"]
    if not recent_series:
        return "<p class='muted'>No data releases in the past 7 days.</p>"
    return _render_series_table(recent_series, ctx)


def _render_all_groups(ctx) -> str:
    """Render the All Data tab with every group."""
    summary = ctx["summary"]
    sparklines = ctx["sparklines"]
    updated_ids = ctx["updated_ids"]
    release_dates = ctx.get("release_dates", {})

    # Sort groups by most recent data (newest first)
    sorted_groups = sorted(
        summary.get("groups", {}).items(),
        key=lambda item: _group_max_date(item[1]["series"]),
        reverse=True,
    )

    parts = []
    for gid, gdata in sorted_groups:
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
            date_str = a["latest_date"] if freq in ("daily", "weekly") else a["latest_date"][:7] if a["latest_date"] else ""

            trend = ""
            if a["trend_dir"] and a["trend_periods"] > 1:
                unit = "d" if freq == "daily" else "mo"
                trend = f'<span class="trend">{a["trend_dir"]} {a["trend_periods"]}{unit}</span>'

            search = _search_terms(sid, a["name"], gid, gdata["name"])
            rows.append(f"""
            <tr class="series-row {fresh}" id="row-{sid}" data-search="{search}" data-sid="{sid}" onclick="toggleDetail('{sid}')">
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
        <div class="group-block" data-group="{gid}">
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

.search-box {
  margin-left: auto;
  display: flex;
  align-items: center;
}
.search-box input {
  background: var(--bg);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 13px;
  width: 200px;
  outline: none;
}
.search-box input:focus {
  border-color: var(--accent);
}
.search-box input::placeholder { color: var(--text-muted); }

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
.series-link {
  color: var(--accent);
  text-decoration: none;
  border-bottom: 1px dotted var(--accent);
  cursor: pointer;
}
.series-link:hover { border-bottom-style: solid; }

@keyframes highlight-row {
  0% { background: rgba(88, 166, 255, 0.25); }
  100% { background: transparent; }
}
.series-row.highlighted {
  animation: highlight-row 2s ease-out;
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
.signal-outlier { background: rgba(188, 76, 255, 0.2); color: #bc4cff; }
.signal-extreme { background: rgba(248, 81, 73, 0.25); color: var(--red); font-weight: 600; }
.signal-sustained { background: rgba(210, 153, 34, 0.15); color: var(--amber); }
.signal-info { background: rgba(139, 148, 158, 0.15); color: var(--text-muted); }
.revised-higher { background: rgba(63, 185, 80, 0.15); color: var(--green); }
.revised-lower { background: rgba(248, 81, 73, 0.2); color: var(--red); }

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

.hero-chart {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 20px 24px;
  margin-bottom: 24px;
}
.chart-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--text);
  margin-bottom: 12px;
}
.chart-caption {
  font-size: 13px;
  color: var(--text-muted);
  margin-top: 12px;
  line-height: 1.6;
}
.chart-caption strong { color: var(--text); }
.chart-actions {
  margin-top: 10px;
  display: flex;
  gap: 8px;
}

.muted { color: var(--text-muted); padding: 32px 0; }
.search-active .analysis-block { display: none; }
.search-active .muted { display: none; }

.revision-row td {
  padding: 4px 12px !important;
  border-bottom: 1px solid var(--border);
  background: rgba(210, 153, 34, 0.04);
}
.revision-note {
  padding: 6px 12px;
}
.revision-label {
  font-size: 11px;
  font-weight: 600;
  color: var(--amber);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 4px;
  cursor: pointer;
}
.revision-label::before {
  content: "▸ ";
  font-size: 10px;
}
.revision-row.rev-open .revision-label::before {
  content: "▾ ";
}
.revision-mini-table {
  display: none;
  width: auto;
  font-size: 12px;
  font-family: monospace;
  border-collapse: collapse;
}
.revision-row.rev-open .revision-mini-table {
  display: table;
}
.rev-badge {
  font-size: 10px;
  color: var(--amber);
  margin-left: 4px;
  vertical-align: middle;
}
.revision-mini-table th {
  color: var(--text-muted);
  font-weight: 500;
  font-size: 11px;
  padding: 2px 10px;
  border-bottom: 1px solid var(--border);
  text-align: right;
}
.revision-mini-table th:first-child { text-align: left; }
.revision-mini-table td {
  padding: 2px 10px;
  border-bottom: none;
}
.revision-mini-table .val-col { text-align: right; }
.revision-mini-table .chg-col { text-align: right; }
"""


def _js() -> str:
    return """
function scrollToSeries(sid) {
  // Find the row and switch to its tab if needed
  var row = document.getElementById('row-' + sid);
  if (!row) return;

  // If the row is in a hidden tab, switch to the right tab
  var section = row.closest('.tab-content');
  if (section && !section.classList.contains('active')) {
    // Find which tab to activate
    var tabId = section.id;
    var tabLink = document.querySelector('nav a[href=\"#' + tabId + '\"]');
    if (tabLink) showTab(tabId, tabLink);
  }

  row.scrollIntoView({behavior: 'smooth', block: 'center'});
  row.classList.remove('highlighted');
  void row.offsetWidth; // trigger reflow
  row.classList.add('highlighted');
}

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
  var rd = releaseDates[sid] || '';
  a.download = rd ? sid + '_' + rd + '.csv' : sid + '.csv';
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

function downloadChartCsv(chartId, fnameBase) {
  var csv = chartCsvData[chartId];
  if (!csv) return;
  var blob = new Blob([csv], {type: 'text/csv'});
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = fnameBase + '.csv';
  a.click();
}

function downloadChartPng(chartId, fnameBase) {
  var container = document.getElementById(chartId + '-container');
  if (!container) return;
  var svg = container.querySelector('svg');
  if (!svg) return;
  var svgData = new XMLSerializer().serializeToString(svg);
  // Inline CSS variables for export
  svgData = svgData.replace(/var\(--surface\)/g, '#161b22');
  svgData = svgData.replace(/var\(--border\)/g, '#30363d');
  var canvas = document.createElement('canvas');
  var rect = svg.getBoundingClientRect();
  var scale = 2; // retina
  canvas.width = rect.width * scale;
  canvas.height = rect.height * scale;
  var ctx = canvas.getContext('2d');
  ctx.scale(scale, scale);
  var img = new Image();
  img.onload = function() {
    ctx.fillStyle = '#161b22';
    ctx.fillRect(0, 0, rect.width, rect.height);
    ctx.drawImage(img, 0, 0, rect.width, rect.height);
    canvas.toBlob(function(blob) {
      var a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = fnameBase + '.png';
      a.click();
    });
  };
  img.src = 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(svgData)));
}

// --- Revision toggle ---
document.addEventListener('click', function(e) {
  var label = e.target.closest('.revision-label');
  if (!label) return;
  e.stopPropagation();
  var revRow = label.closest('.revision-row');
  if (revRow) revRow.classList.toggle('rev-open');
});

// --- Search ---
var _searchActiveTab = null;

function filterSeries(query) {
  var q = query.toLowerCase().trim();
  var main = document.querySelector('main');
  var allRows = document.querySelectorAll('.series-row');
  var allRevRows = document.querySelectorAll('.revision-row');
  var allDetailRows = document.querySelectorAll('.detail-row');
  var allGroups = document.querySelectorAll('.group-block');
  var tabs = document.querySelectorAll('.tab-content');

  if (!q) {
    // Restore normal tab view
    main.classList.remove('search-active');
    allRows.forEach(function(r) { r.style.display = ''; });
    allRevRows.forEach(function(r) { r.style.display = ''; });
    allGroups.forEach(function(g) { g.style.display = ''; });
    // Restore tab visibility
    if (_searchActiveTab) {
      tabs.forEach(function(t) { t.classList.remove('active'); });
      document.getElementById(_searchActiveTab).classList.add('active');
      _searchActiveTab = null;
    }
    return;
  }

  // Remember which tab was active before search, show all tabs
  if (!_searchActiveTab) {
    var active = document.querySelector('.tab-content.active');
    if (active) _searchActiveTab = active.id;
  }
  main.classList.add('search-active');
  tabs.forEach(function(t) { t.classList.add('active'); });

  var terms = q.split(/\s+/);
  var visibleSids = {};

  allRows.forEach(function(row) {
    var search = row.getAttribute('data-search') || '';
    var match = terms.every(function(t) { return search.indexOf(t) >= 0; });
    row.style.display = match ? '' : 'none';
    if (match) visibleSids[row.getAttribute('data-sid')] = true;
  });

  // Hide/show associated revision and detail rows
  allRevRows.forEach(function(r) {
    var sid = r.getAttribute('data-rev-for');
    r.style.display = visibleSids[sid] ? '' : 'none';
  });
  allDetailRows.forEach(function(r) {
    r.style.display = 'none';
  });

  // Hide groups with no visible rows
  allGroups.forEach(function(g) {
    var rows = g.querySelectorAll('.series-row');
    var anyVisible = false;
    rows.forEach(function(r) { if (r.style.display !== 'none') anyVisible = true; });
    g.style.display = anyVisible ? '' : 'none';
  });

  // Hide tab sections with no visible groups
  tabs.forEach(function(t) {
    var groups = t.querySelectorAll('.group-block');
    if (groups.length === 0) return; // e.g. today with "no data" message
    var anyGroupVisible = false;
    groups.forEach(function(g) { if (g.style.display !== 'none') anyGroupVisible = true; });
    t.style.display = anyGroupVisible ? '' : 'none';
  });
}

// Keyboard shortcut: / to focus search
document.addEventListener('keydown', function(e) {
  if (e.key === '/' && !e.ctrlKey && !e.metaKey) {
    var el = document.activeElement;
    if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) return;
    e.preventDefault();
    document.getElementById('search-input').focus();
  }
  if (e.key === 'Escape') {
    var input = document.getElementById('search-input');
    if (document.activeElement === input) {
      input.value = '';
      filterSeries('');
      input.blur();
    }
  }
});
"""
