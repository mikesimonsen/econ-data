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
import math
from datetime import date, datetime, timedelta
from pathlib import Path

from econ_data.config import fred_series
from econ_data.db import connect
from econ_data.store import DB_PATH, get_recent_revisions
from econ_data.summary import generate_summary


def _build_source_urls(cfg: dict) -> dict[str, str]:
    """Map {series_id: external source URL}.

    FRED series → fred.stlouisfed.org. Series with a `bls_id` override →
    data.bls.gov (preferred over FRED when both apply). Computed/scraped
    series (Altos, MND, Xactus, etc.) get no entry.
    """
    urls = {sid: f"https://fred.stlouisfed.org/series/{sid}"
            for sid, _ in fred_series(cfg)}
    for s in cfg.get("series", []):
        if s.get("bls_id"):
            urls[s["id"]] = f"https://data.bls.gov/timeseries/{s['bls_id']}"
    for group in cfg.get("groups", {}).values():
        for s in group["series"]:
            if s.get("bls_id"):
                urls[s["id"]] = f"https://data.bls.gov/timeseries/{s['bls_id']}"
    return urls


def _sid_html(sid: str, source_urls: dict) -> str:
    """Render a series ID as a clickable link if its source page is known,
    otherwise as plain text. The link stops click propagation so it doesn't
    also toggle the row's detail panel."""
    url = source_urls.get(sid)
    if not url:
        return sid
    return (f'<a href="{url}" target="_blank" rel="noopener" '
            f'class="sid-link" onclick="event.stopPropagation()">{sid}</a>')

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
    "sp500": "stocks equity market sp500 index",
    "labor-headlines": "jobs labor unemployment payrolls openings hires claims",
    "labor-detail": "jobs labor unemployment quits separations layoffs participation",
    "treasury-yields": "rates bonds interest",
    "mortgage-rates": "rates mortgage interest housing",
    "households": "housing homeownership vacancy",
    "mortgage-spread": "spread mortgage treasury rates housing",
    "altos-inventory": "housing inventory listings supply altos",
    "altos-new-listings": "housing new listings supply altos",
    "altos-new-pending": "housing pending sales contracts altos",
    "realtor-dot-com": "housing inventory listings pending price realtor",
    "realtor-dot-com-extra": "housing pending ratio price reduced realtor",
    "redfin": "housing pending sold listings inventory price redfin",
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


def _nice_y_axis(vmin: float, vmax: float, *, include_zero: bool = False,
                 n_ticks: int = 5) -> tuple[float, float, list[float]]:
    """Return (nice_min, nice_max, ticks) with round-number tick values.

    ``include_zero`` forces zero to be within the range (use for YoY %
    charts that cross zero). The step size is chosen from {1, 2, 5} × 10^k
    so labels fall on intuitive values.
    """
    if vmax == vmin:
        vmax = vmin + 1
    if include_zero:
        vmin = min(vmin, 0.0)
        vmax = max(vmax, 0.0)

    raw_step = (vmax - vmin) / max(n_ticks - 1, 1)
    if raw_step <= 0:
        raw_step = 1.0
    exp = math.floor(math.log10(raw_step))
    base = 10 ** exp
    frac = raw_step / base
    if frac < 1.5:
        step = 1 * base
    elif frac < 3:
        step = 2 * base
    elif frac < 7:
        step = 5 * base
    else:
        step = 10 * base

    nice_min = math.floor(vmin / step) * step
    nice_max = math.ceil(vmax / step) * step

    ticks: list[float] = []
    digits = max(0, -exp + (1 if step / base < 2 else 0))
    t = nice_min
    # Use a guarded loop; avoid fp drift by snapping each tick to step precision.
    for _ in range(64):
        ticks.append(round(t, digits))
        if t >= nice_max - step * 1e-6:
            break
        t += step

    return float(nice_min), float(nice_max), ticks


def _median_gap_days(points: list) -> int:
    """Median gap between consecutive observations (in days). Used to size
    the tooltip 'same period' tolerance so a weekly series shows ~3-4d
    matches and a monthly series shows ~15d matches."""
    if not points or len(points) < 2:
        return 30
    dates = []
    for d, _ in points:
        try:
            dates.append(date.fromisoformat(d) if isinstance(d, str) else d)
        except (ValueError, TypeError):
            continue
    if len(dates) < 2:
        return 30
    dates.sort()
    gaps = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
    gaps = [g for g in gaps if g > 0]
    if not gaps:
        return 30
    gaps.sort()
    return gaps[len(gaps) // 2]


def _format_tick(v: float, *, is_yoy: bool) -> str:
    if is_yoy:
        return f"{v:+.1f}%" if v != 0 else "0%"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    if abs(v) >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}"


def _sparkline_data(series_id: str, n: int = 24,
                    db_path: Path = DB_PATH) -> list:
    """Return last n observations as [(date, value), ...] for sparklines."""
    rows = connect().execute(
        "SELECT date::text, value FROM observations "
        "WHERE series_id = %s ORDER BY date DESC LIMIT %s",
        (series_id, n),
    ).fetchall()
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
    if "beat" in s or "hotter" in s:
        return "signal-beat"
    if "missed" in s or "cooler" in s:
        return "signal-miss"
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
    con = connect()
    rows = con.execute(
        "SELECT date::text, value FROM observations "
        "WHERE series_id = %s ORDER BY date",
        (series_id,),
    ).fetchall()

    # Get the right calc types
    period_rows = con.execute(
        "SELECT date::text, value FROM calculated "
        "WHERE series_id = %s AND calc_type IN ('period_pct', 'period_pp') "
        "ORDER BY date",
        (series_id,),
    ).fetchall()
    yoy_rows = con.execute(
        "SELECT date::text, value FROM calculated "
        "WHERE series_id = %s AND calc_type IN ('yoy_pct', 'yoy_pp') "
        "ORDER BY date",
        (series_id,),
    ).fetchall()

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
    con = connect()
    all_dates = set()
    series_data = {}
    names = {}

    for a in series_list:
        sid = a["series_id"]
        names[sid] = a["name"]
        rows = con.execute(
            "SELECT date::text, value FROM observations "
            "WHERE series_id = %s ORDER BY date", (sid,),
        ).fetchall()
        series_data[sid] = dict(rows)
        all_dates.update(d for d, _ in rows)

    dates = sorted(all_dates)

    buf = io.StringIO()
    writer = csv.writer(buf)
    sids = [a["series_id"] for a in series_list]
    writer.writerow(["date"] + [names[s] for s in sids])
    for d in dates:
        writer.writerow([d] + [series_data[s].get(d, "") for s in sids])
    return buf.getvalue()


def _release_dates(db_path: Path = DB_PATH) -> dict:
    """Return {series_id: captured_at_iso} for the latest observation of each series."""
    # to_char yields "YYYY-MM-DDTHH:MM:SS" matching SQLite's stored format,
    # which Python 3.9's datetime.fromisoformat() can parse (the default
    # TIMESTAMPTZ::text rendering "+00" trailing offset breaks it on 3.9).
    rows = connect().execute("""
        SELECT o.series_id,
               to_char(o.captured_at, 'YYYY-MM-DD"T"HH24:MI:SS')
        FROM observations o
        INNER JOIN (
            SELECT series_id, MAX(date) as max_date
            FROM observations GROUP BY series_id
        ) latest ON o.series_id = latest.series_id AND o.date = latest.max_date
        WHERE o.captured_at IS NOT NULL
    """).fetchall()
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
            html_lines.append(
                f'<p class="analysis-p">{s}'
                f'<button class="btn-flag-inline" onclick="flagParagraph(this)" '
                f'title="Flag for commentary">&#9873;</button></p>'
            )
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
    from econ_data.housing_analysis import load_housing_analysis
    housing_md = load_housing_analysis(today)
    name_map = _build_name_map(summary)

    from econ_data.expectations import get_upcoming_releases
    upcoming_releases = get_upcoming_releases(days_ahead=7, db_path=db_path)

    from econ_data.fed_expectations import render_fed_chart
    fed_chart_html = render_fed_chart(db_path=db_path)

    source_urls = _build_source_urls(cfg)

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

    housing_html = _md_to_html(housing_md, name_map) if housing_md else ""

    html = _render_page(
        today=today,
        analysis_html=_md_to_html(analysis_md, name_map) if analysis_md else "",
        housing_html=housing_html,
        upcoming_releases=upcoming_releases,
        fed_chart_html=fed_chart_html,
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
        source_urls=source_urls,
    )
    return html


def _render_page(**ctx) -> str:
    """Render the full HTML page."""
    today = ctx["today"]

    # Build sections — today_html must render first (it populates chart_csv_data)
    analysis_html = ctx.get("analysis_html", "")
    housing_html = ctx.get("housing_html", "")
    housing_charts_html = _render_housing_charts(ctx)
    employment_charts_html = _render_employment_charts(ctx)
    fed_chart_html = ctx.get("fed_chart_html", "")
    today_html = _render_today(ctx)
    upcoming_html = _render_upcoming(ctx)
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
  <img src="data:image/png;base64,{_favicon_b64()}" alt="Logo" class="header-logo">
  <h1>Simonsen Daily Briefing</h1>
  <div class="date">{today}</div>
</header>

<nav>
  <a href="#today" class="active" onclick="showTab('today', this)">Today</a>
  <a href="#housing" onclick="showTab('housing', this)">Housing</a>
  <a href="#employment" onclick="showTab('employment', this)">Employment</a>
  <a href="#fed" onclick="showTab('fed', this)">Fed</a>
  <a href="#upcoming" onclick="showTab('upcoming', this)">Upcoming</a>
  <a href="#recent" onclick="showTab('recent', this)">Recent Data Releases{_badge(recent_count)}</a>
  <a href="#flagged" onclick="showTab('flagged', this)">Flagged <span id="flagged-badge" class="badge" style="display:none">0</span></a>
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

  <section id="housing" class="tab-content">
    {housing_charts_html}
    {f'<div class="analysis-block">{housing_html}</div>' if housing_html else '<p class="muted">Housing analysis not yet generated. Run the pipeline to generate.</p>'}
  </section>

  <section id="employment" class="tab-content">
    {employment_charts_html if employment_charts_html else '<p class="muted">Employment charts unavailable — underlying series not yet populated.</p>'}
  </section>

  <section id="fed" class="tab-content">
    <h2 style="margin-top:0">Federal Funds Rate &amp; Market Expectations</h2>
    {fed_chart_html}
  </section>

  <section id="upcoming" class="tab-content">
    {upcoming_html}
  </section>

  <section id="recent" class="tab-content">
    {recent_html}
  </section>

  <section id="flagged" class="tab-content">
    <h2 style="margin-top:0">Flagged for Commentary</h2>
    <div id="flagged-list"><p class="muted">No items flagged yet. Click the flag icon next to any data row or analysis paragraph to save it here.</p></div>
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
    source_urls = ctx.get("source_urls", {})

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
                <div class="series-id">{_sid_html(sid, source_urls)}</div>
              </td>
              <td class="spark-col">{spark}</td>
              <td class="date-col">{date_str}</td>
              <td class="val-col">{_format_val(a['latest_value'], is_pct)}</td>
              <td class="chg-col">{_trend_arrow(a)} {_format_change(a['period_pct'], is_pct)}</td>
              <td class="chg-col">{_format_change(a['yoy_pct'], is_pct)}</td>
              <td class="release-col">{released}</td>
              <td class="signal-col">{rev_tag}{signal_tags}</td>
              <td class="export-col"><button class="btn-flag" onclick="event.stopPropagation(); flagSeriesFromRow(this)" title="Flag for commentary">&#9873;</button><button class="btn-export" onclick="event.stopPropagation(); downloadCsv('{sid}')">CSV</button></td>
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
    con = connect()
    if chart_type == "yoy":
        rows = con.execute(
            "SELECT date::text, value FROM calculated "
            "WHERE series_id = %s AND calc_type IN ('yoy_pct', 'yoy_pp') "
            "ORDER BY date DESC LIMIT %s",
            (series_id, n),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT date::text, value FROM observations "
            "WHERE series_id = %s ORDER BY date DESC LIMIT %s",
            (series_id, n),
        ).fetchall()
    return list(reversed(rows))


def _hero_chart_svg(points: list, title: str, is_yoy: bool = False,
                    width: int = 800, height: int = 280,
                    chart_id: str = "") -> str:
    """Render a full chart SVG with axes, gridlines, labels, and zero line.

    When ``chart_id`` is provided, an invisible overlay rect is added that
    drives the same crosshair + floating tooltip used by multi-series
    charts (handled by the chartHover() JS in the page)."""
    if len(points) < 2:
        return ""

    values = [p[1] for p in points]
    vmin, vmax = min(values), max(values)

    include_zero = is_yoy and (vmin < 0 < vmax or vmin >= 0 or vmax <= 0)
    vmin_p, vmax_p, ticks = _nice_y_axis(vmin, vmax, include_zero=include_zero)
    vrange_p = vmax_p - vmin_p if vmax_p != vmin_p else 1

    # Layout
    left_margin = 65
    right_margin = 20
    top_margin = 15
    bottom_margin = 35
    plot_w = width - left_margin - right_margin
    plot_h = height - top_margin - bottom_margin

    color = "#d29922" if is_yoy else "#4A90D9"

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

    # Gridlines + Y-axis labels at nice tick values
    for gv in ticks:
        gy = y_pos(gv)
        parts.append(
            f'<line x1="{left_margin}" y1="{gy:.1f}" '
            f'x2="{width - right_margin}" y2="{gy:.1f}" '
            f'stroke="#30363d" stroke-width="0.5"/>'
        )
        label = _format_tick(gv, is_yoy=is_yoy)
        parts.append(
            f'<text x="{left_margin - 8}" y="{gy + 4:.1f}" '
            f'text-anchor="end" fill="#8b949e" font-size="11" '
            f'font-family="-apple-system,sans-serif">{label}</text>'
        )

    # Zero line for YoY charts (emphasized vs. regular gridline)
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

    # Visible dot on last point
    lx = x_pos(len(points) - 1)
    ly = y_pos(values[-1])
    parts.append(
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="4" fill="{color}"/>'
    )

    # Crosshair + floating tooltip infrastructure (driven by chartHover() JS).
    # Falls back to native <title> tooltips when no chart_id is supplied.
    if chart_id:
        parts.append(
            f'<line class="xhair xhair-{chart_id}" x1="0" y1="{top_margin}" '
            f'x2="0" y2="{top_margin + plot_h}" stroke="#8b949e" '
            f'stroke-width="1" stroke-dasharray="3,3" '
            f'style="display:none;pointer-events:none"/>'
        )
        parts.append(
            f'<g class="xhair-dots xhair-dots-{chart_id}" '
            f'style="display:none;pointer-events:none"></g>'
        )
        parts.append(
            f'<rect class="chart-overlay" x="{left_margin}" y="{top_margin}" '
            f'width="{plot_w}" height="{plot_h}" fill="transparent" '
            f'data-chart-id="{chart_id}" '
            f'onmousemove="chartHover(event, \'{chart_id}\')" '
            f'onmouseleave="chartHoverLeave(\'{chart_id}\')"/>'
        )
    else:
        # Per-point native tooltips for charts without a chart_id
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

    parts.append('</svg>')

    if chart_id:
        all_dates = [d for d, _ in points]
        gap = _median_gap_days(points)
        tolerance = max(gap // 2 + 1, 2)
        payload = {
            "dates": all_dates,
            "series": [{
                "label": title,
                "color": color,
                "points": [[d, v] for d, v in points],
                "tolerance": tolerance,
            }],
            "left": left_margin,
            "top": top_margin,
            "plotW": plot_w,
            "plotH": plot_h,
            "vmin": vmin_p,
            "vmax": vmax_p,
            "width": width,
            "height": height,
            "isYoy": bool(is_yoy),
        }
        parts.append(
            f'<script>(window._chartData=window._chartData||{{}})'
            f'["{chart_id}"]={json.dumps(payload)};</script>'
        )

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


def _slug(text: str) -> str:
    """Filesystem-safe slug for chart filenames."""
    out = []
    for ch in text.lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "-":
            out.append("-")
    return "".join(out).strip("-")


def _multi_series_csv(series: list) -> str:
    """Build CSV for an overlay chart: date + one column per series, aligned.

    ``series`` is a list of (label, [(date, value), ...]) tuples.
    """
    all_dates = sorted({d for _, pts in series for d, _ in pts})
    value_maps = [(label, dict(pts)) for label, pts in series]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["date"] + [label for label, _ in value_maps])
    for d in all_dates:
        row = [d]
        for _, vmap in value_maps:
            v = vmap.get(d)
            row.append("" if v is None else v)
        writer.writerow(row)
    return buf.getvalue()


# ── Housing comparison charts ──────────────────────────────────────

_HOUSING_CHART_COLORS = [
    "#58a6ff",  # blue
    "#3fb950",  # green
    "#d29922",  # amber
    "#f85149",  # red
    "#bc8cff",  # purple
    "#f0883e",  # orange
]

# Comparison chart definitions: each is a group of series to overlay
_HOUSING_COMPARISONS = [
    {
        "title": "Pending Sales — YoY % Change",
        "chart_type": "yoy",
        "series": [
            ("ALTOS_NEW_PENDING_13WK", "Altos (13wk avg)"),
            ("PENLISCOUUS", "Realtor.com"),
            ("REDFIN_PENDING", "Redfin"),
            ("EXHOSLUSM495S", "NAR (SA)"),
        ],
    },
    {
        "title": "Active Inventory — YoY % Change",
        "chart_type": "yoy",
        "series": [
            ("ALTOS_INVENTORY", "Altos"),
            ("ACTLISCOUUS", "Realtor.com"),
            ("REDFIN_INVENTORY", "Redfin"),
            ("HOSINVUSM495N", "NAR"),
        ],
    },
    {
        "title": "New Listings — YoY % Change",
        "chart_type": "yoy",
        "series": [
            ("ALTOS_NEW_LISTINGS_13WK", "Altos (13wk avg)"),
            ("NEWLISCOUUS", "Realtor.com"),
            ("REDFIN_NEW_LISTINGS", "Redfin"),
        ],
    },
    {
        "title": "Days on Market",
        "chart_type": "raw",
        "series": [
            ("ALTOS_PENDING_DOM", "Altos (pending)"),
            ("MEDDAYONMARUS", "Realtor.com"),
            ("REDFIN_MEDIAN_DOM", "Redfin"),
        ],
    },
    {
        "title": "Homes Sold / Existing Home Sales — YoY % Change",
        "chart_type": "yoy",
        "series": [
            ("REDFIN_SOLD", "Redfin"),
            ("EXHOSLUSM495S", "NAR (SA)"),
        ],
    },
]


def _multi_series_chart_svg(
    all_points: list,  # [(label, color, [(date, value), ...]), ...]
    title: str,
    is_yoy: bool = False,
    width: int = 800,
    height: int = 320,
    chart_id: str = "",
) -> str:
    """Render a multi-series overlay chart as SVG with legend + crosshair hover.

    When ``chart_id`` is provided, the SVG includes an invisible overlay
    rect wired up to a crosshair tooltip that shows all series values for
    the nearest date. A <script> block embeds the per-chart data used by
    chartHover() in the page's JS.
    """
    all_values = [v for _, _, pts in all_points for _, v in pts]
    if len(all_values) < 2:
        return ""

    vmin, vmax = min(all_values), max(all_values)
    include_zero = is_yoy and (vmin < 0 < vmax or vmin >= 0 or vmax <= 0)
    vmin_p, vmax_p, ticks = _nice_y_axis(vmin, vmax, include_zero=include_zero)
    vrange_p = vmax_p - vmin_p if vmax_p != vmin_p else 1

    # Layout — extra space at top for legend
    left_margin = 65
    right_margin = 20
    top_margin = 15
    legend_height = 30
    bottom_margin = 35
    plot_top = top_margin + legend_height
    plot_h = height - plot_top - bottom_margin
    plot_w = width - left_margin - right_margin

    # Collect all unique dates for shared x-axis
    date_set = set()
    for _, _, pts in all_points:
        for d, _ in pts:
            date_set.add(d)
    all_dates = sorted(date_set)
    if len(all_dates) < 2:
        return ""
    date_idx = {d: i for i, d in enumerate(all_dates)}
    n_dates = len(all_dates)

    def x_pos_i(i):
        return left_margin + plot_w * i / (n_dates - 1)

    def x_pos(d):
        return x_pos_i(date_idx[d])

    def y_pos(v):
        return plot_top + plot_h * (1 - (v - vmin_p) / vrange_p)

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {width} {height}" '
        f'style="width:100%;max-width:{width}px;height:auto;'
        f'background:var(--surface);border-radius:8px">'
    )

    # Legend
    lx = left_margin
    for label, color, _ in all_points:
        parts.append(
            f'<line x1="{lx}" y1="{top_margin + 10}" '
            f'x2="{lx + 20}" y2="{top_margin + 10}" '
            f'stroke="{color}" stroke-width="2.5"/>'
        )
        parts.append(
            f'<text x="{lx + 24}" y="{top_margin + 14}" '
            f'fill="#e6edf3" font-size="11" '
            f'font-family="-apple-system,sans-serif">{label}</text>'
        )
        lx += len(label) * 7 + 40

    # Gridlines + Y-axis labels at nice tick values
    for gv in ticks:
        gy = y_pos(gv)
        parts.append(
            f'<line x1="{left_margin}" y1="{gy:.1f}" '
            f'x2="{width - right_margin}" y2="{gy:.1f}" '
            f'stroke="#30363d" stroke-width="0.5"/>'
        )
        parts.append(
            f'<text x="{left_margin - 8}" y="{gy + 4:.1f}" '
            f'text-anchor="end" fill="#8b949e" font-size="11" '
            f'font-family="-apple-system,sans-serif">{_format_tick(gv, is_yoy=is_yoy)}</text>'
        )

    # Emphasized zero line for YoY charts that cross zero
    if is_yoy and vmin_p < 0 < vmax_p:
        zy = y_pos(0)
        parts.append(
            f'<line x1="{left_margin}" y1="{zy:.1f}" '
            f'x2="{width - right_margin}" y2="{zy:.1f}" '
            f'stroke="#8b949e" stroke-width="1" stroke-dasharray="4,3"/>'
        )

    # X-axis labels
    n_labels = min(6, n_dates)
    step = max(1, (n_dates - 1) // (n_labels - 1)) if n_labels > 1 else 1
    for i in range(0, n_dates, step):
        d = all_dates[i]
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            label = dt.strftime("%b '%y")
        except (ValueError, TypeError):
            label = d[:7]
        xp = x_pos(d)
        parts.append(
            f'<text x="{xp:.1f}" y="{height - 8}" '
            f'text-anchor="middle" fill="#8b949e" font-size="11" '
            f'font-family="-apple-system,sans-serif">{label}</text>'
        )

    # Data lines
    for label, color, pts in all_points:
        if len(pts) < 2:
            continue
        coords = []
        for d, v in pts:
            coords.append(f"{x_pos(d):.1f},{y_pos(v):.1f}")
        polyline = " ".join(coords)
        parts.append(
            f'<polyline points="{polyline}" fill="none" stroke="{color}" '
            f'stroke-width="2" stroke-linejoin="round" stroke-opacity="0.85"/>'
        )
        # Dot on last point
        last_d, last_v = pts[-1]
        parts.append(
            f'<circle cx="{x_pos(last_d):.1f}" cy="{y_pos(last_v):.1f}" '
            f'r="3.5" fill="{color}"/>'
        )

    # Crosshair infrastructure (hidden by default; JS toggles visibility)
    if chart_id:
        parts.append(
            f'<line class="xhair xhair-{chart_id}" x1="0" y1="{plot_top}" '
            f'x2="0" y2="{plot_top + plot_h}" stroke="#8b949e" '
            f'stroke-width="1" stroke-dasharray="3,3" '
            f'style="display:none;pointer-events:none"/>'
        )
        parts.append(f'<g class="xhair-dots xhair-dots-{chart_id}" style="display:none;pointer-events:none"></g>')
        parts.append(
            f'<rect class="chart-overlay" x="{left_margin}" y="{plot_top}" '
            f'width="{plot_w}" height="{plot_h}" fill="transparent" '
            f'data-chart-id="{chart_id}" '
            f'onmousemove="chartHover(event, \'{chart_id}\')" '
            f'onmouseleave="chartHoverLeave(\'{chart_id}\')"/>'
        )

    parts.append('</svg>')

    # Embed chart metadata + per-series points for the JS hover handler.
    # Each series carries its own observations + a "tolerance" (days) — the
    # JS picks the nearest data point per series within that tolerance, so
    # weekly series whose dates don't line up exactly still both appear in
    # the tooltip when hovering on the same week.
    if chart_id:
        series_payload = []
        for label, color, pts in all_points:
            gap = _median_gap_days(pts)
            tolerance = max(gap // 2 + 1, 2)
            series_payload.append({
                "label": label,
                "color": color,
                "points": [[d, v] for d, v in pts],
                "tolerance": tolerance,
            })
        payload = {
            "dates": all_dates,
            "series": series_payload,
            "left": left_margin,
            "top": plot_top,
            "plotW": plot_w,
            "plotH": plot_h,
            "vmin": vmin_p,
            "vmax": vmax_p,
            "width": width,
            "height": height,
            "isYoy": bool(is_yoy),
        }
        parts.append(
            f'<script>(window._chartData=window._chartData||{{}})'
            f'["{chart_id}"]={json.dumps(payload)};</script>'
        )

    return "\n".join(parts)


def _housing_chart_data(series_id: str, chart_type: str, since: str,
                        db_path: Path = DB_PATH) -> list:
    """Fetch data for housing charts by date range (not point count).

    This avoids the problem where LIMIT N returns 8 months of weekly Altos
    data but 3 years of monthly Redfin data.
    """
    con = connect()
    if chart_type == "yoy":
        rows = con.execute(
            "SELECT date::text, value FROM calculated "
            "WHERE series_id = %s AND calc_type IN ('yoy_pct', 'yoy_pp') "
            "AND date >= %s ORDER BY date",
            (series_id, since),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT date::text, value FROM observations "
            "WHERE series_id = %s AND date >= %s ORDER BY date",
            (series_id, since),
        ).fetchall()
    return rows


def _render_housing_charts(ctx: dict) -> str:
    """Build HTML for housing comparison overlay charts."""
    db_path = ctx.get("db_path", DB_PATH)
    chart_csvs = ctx.setdefault("chart_csv_data", {})
    since = (date.today() - timedelta(days=3 * 365)).isoformat()
    chart_parts = []

    for idx, comp in enumerate(_HOUSING_COMPARISONS):
        series_data = []
        for i, (sid, label) in enumerate(comp["series"]):
            pts = _housing_chart_data(sid, comp["chart_type"], since, db_path)
            if pts:
                color = _HOUSING_CHART_COLORS[i % len(_HOUSING_CHART_COLORS)]
                series_data.append((label, color, pts))

        if not series_data:
            continue

        is_yoy = comp["chart_type"] == "yoy"
        chart_id = f"cmp-chart-{idx}"
        svg = _multi_series_chart_svg(
            series_data, comp["title"], is_yoy, chart_id=chart_id,
        )
        if not svg:
            continue

        chart_csvs[chart_id] = _multi_series_csv(
            [(label, pts) for label, _, pts in series_data]
        )
        fname_base = f"housing_{_slug(comp['title'])}"

        chart_parts.append(f"""
        <div class="hero-chart chart-wrap" id="{chart_id}-container">
          <div class="chart-title">{comp['title']}</div>
          {svg}
          <div class="chart-tooltip" id="tip-{chart_id}"></div>
          <div class="chart-actions">
            <button class="btn-export" onclick="downloadChartCsv('{chart_id}', '{fname_base}')">CSV</button>
          </div>
        </div>""")

    if not chart_parts:
        return ""

    return (
        '<h2 style="margin-top:0">Housing Data — Cross-Source Comparison</h2>\n'
        '<p class="muted">Same metrics from different sources overlaid to spot '
        'convergence and divergence. YoY % normalizes different scales.</p>\n'
        + "\n".join(chart_parts)
    )


# ── Employment comparison charts ───────────────────────────────────

_EMPLOYMENT_COMPARISONS = [
    {
        "title": "Unemployment Rates — U-1 through U-6",
        "chart_type": "raw",
        "series": [
            ("U1RATE", "U-1 (15+ wks)"),
            ("U2RATE", "U-2 (job losers)"),
            ("UNRATE", "U-3 (headline)"),
            ("U4RATE", "U-4 (+ discouraged)"),
            ("U5RATE", "U-5 (+ marginally attached)"),
            ("U6RATE", "U-6 (+ part-time econ)"),
        ],
    },
    {
        "title": "Initial Jobless Claims",
        "chart_type": "raw",
        "series": [
            ("ICSA", "Initial Claims (weekly)"),
            ("IC4WSA", "Initial Claims (4-wk avg)"),
        ],
    },
    {
        "title": "Continuing Claims",
        "chart_type": "raw",
        "series": [
            ("CCSA", "Continuing Claims (weekly)"),
            ("CC4WSA", "Continuing Claims (4-wk avg)"),
        ],
    },
    {
        "title": "JOLTS Rates — Openings, Hires, Quits, Layoffs, Separations",
        "chart_type": "raw",
        "series": [
            ("JTSJOR", "Openings rate"),
            ("JTSHIR", "Hires rate"),
            ("JTSQUR", "Quits rate"),
            ("JTSLDR", "Layoffs rate"),
            ("JTSTSR", "Total separations rate"),
        ],
    },
    {
        "title": "Employment Growth — YoY % Change",
        "chart_type": "yoy",
        "series": [
            ("PAYEMS", "Nonfarm Payrolls"),
            ("CE16OV", "CPS Employment"),
        ],
    },
    {
        "title": "Wage Growth — YoY %",
        "chart_type": "mixed",
        "series": [
            ("FRBATLWGT3MMAUMHWGO", "Atlanta Fed 3M MA", "raw"),
            ("FRBATLWGT12MMUMHGO", "Atlanta Fed 12M MA", "raw"),
            ("CES0500000003", "Avg Hourly Earnings (YoY)", "yoy"),
        ],
    },
    {
        "title": "Labor Force Participation & Employment-Population Ratio",
        "chart_type": "raw",
        "series": [
            ("CIVPART", "Labor Force Participation Rate"),
            ("EMRATIO", "Employment-Population Ratio"),
        ],
    },
    {
        "title": "Duration of Unemployment (Weeks)",
        "chart_type": "raw",
        "series": [
            ("UEMPMED", "Median Duration"),
            ("UEMPMEAN", "Average Duration"),
        ],
    },
]


def _render_employment_charts(ctx: dict) -> str:
    """Build HTML for employment comparison overlay charts."""
    db_path = ctx.get("db_path", DB_PATH)
    chart_csvs = ctx.setdefault("chart_csv_data", {})
    since = (date.today() - timedelta(days=3 * 365)).isoformat()
    chart_parts = []

    for idx, comp in enumerate(_EMPLOYMENT_COMPARISONS):
        series_data = []
        chart_type = comp["chart_type"]
        for i, entry in enumerate(comp["series"]):
            if chart_type == "mixed":
                sid, label, per_series_type = entry
            else:
                sid, label = entry
                per_series_type = chart_type
            pts = _housing_chart_data(sid, per_series_type, since, db_path)
            if pts:
                color = _HOUSING_CHART_COLORS[i % len(_HOUSING_CHART_COLORS)]
                series_data.append((label, color, pts))

        if not series_data:
            continue

        is_yoy = chart_type == "yoy" or chart_type == "mixed"
        chart_id = f"emp-chart-{idx}"
        svg = _multi_series_chart_svg(
            series_data, comp["title"], is_yoy, chart_id=chart_id,
        )
        if not svg:
            continue

        chart_csvs[chart_id] = _multi_series_csv(
            [(label, pts) for label, _, pts in series_data]
        )
        fname_base = f"employment_{_slug(comp['title'])}"

        chart_parts.append(f"""
        <div class="hero-chart chart-wrap" id="{chart_id}-container">
          <div class="chart-title">{comp['title']}</div>
          {svg}
          <div class="chart-tooltip" id="tip-{chart_id}"></div>
          <div class="chart-actions">
            <button class="btn-export" onclick="downloadChartCsv('{chart_id}', '{fname_base}')">CSV</button>
          </div>
        </div>""")

    if not chart_parts:
        return ""

    return (
        '<h2 style="margin-top:0">Employment Data — Overlay Comparisons</h2>\n'
        '<p class="muted">Related employment series plotted together to spot '
        'convergence, divergence, and turning points.</p>\n'
        + "\n".join(chart_parts)
    )


def _render_hero_charts(ctx) -> str:
    """Pick hero series and render charts for the Today tab."""
    cfg = ctx.get("cfg", {})
    today_series = ctx["today_series"]
    db_path = ctx.get("db_path", DB_PATH)
    release_dates = ctx.get("release_dates", {})

    heroes = _pick_hero_series(today_series, cfg)
    if not heroes:
        return ""

    chart_csvs = ctx.setdefault("chart_csv_data", {})
    parts = []
    for i, (gid, gname, a, chart_type, n_points) in enumerate(heroes):
        sid = a["series_id"]
        points = _chart_data(sid, chart_type, n_points, db_path)
        if len(points) < 2:
            continue

        chart_id = f"hero-chart-{i}"
        is_yoy = chart_type == "yoy"
        chart_label = "Year-over-Year Change" if is_yoy else a["name"]
        svg = _hero_chart_svg(points, chart_label, is_yoy, chart_id=chart_id)

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
        <div class="hero-chart chart-wrap" id="{chart_id}-container">
          <div class="chart-title">{a['name']}{' — YoY %' if is_yoy else ''}</div>
          {svg}
          <div class="chart-tooltip" id="tip-{chart_id}"></div>
          <div class="chart-caption">{caption}</div>
          <div class="chart-actions">
            <button class="btn-export" onclick="downloadChartCsv('{chart_id}', '{fname_base}')">CSV</button>
            <button class="btn-export" onclick="downloadChartPng('{chart_id}', '{fname_base}')">PNG</button>
          </div>
        </div>""")

    return "\n".join(parts)


def _render_upcoming(ctx) -> str:
    """Render the Upcoming tab — releases expected in the next 7 days."""
    from datetime import datetime
    upcoming = ctx.get("upcoming_releases", [])
    source_urls = ctx.get("source_urls", {})
    if not upcoming:
        return "<p class='muted'>No tracked releases expected in the next 7 days.</p>"

    # Group by release date
    by_date = {}
    for rel in upcoming:
        by_date.setdefault(rel["release_date"], []).append(rel)

    parts = []
    for release_date in sorted(by_date.keys()):
        try:
            dt = datetime.strptime(release_date, "%Y-%m-%d")
            label = dt.strftime("%A, %B %-d")
        except (ValueError, TypeError):
            label = release_date

        rows = []
        for rel in by_date[release_date]:
            prior = ""
            if rel["prior_value"] is not None:
                v = rel["prior_value"]
                if abs(v) >= 1000:
                    prior = f"{v:,.0f}"
                elif abs(v) >= 10:
                    prior = f"{v:.1f}"
                else:
                    prior = f"{v:.2f}"
                if rel["prior_date"]:
                    pdate = rel["prior_date"][:7]  # YYYY-MM
                    prior = f"{prior}  ({pdate})"

            if rel["expected"] is not None:
                exp_val = rel["expected"]
                ctype = rel["compare_type"]
                if ctype == "change":
                    expected = f"+{exp_val:,.0f}K"
                elif ctype == "mom_pct":
                    expected = f"+{exp_val:.1f}%"
                elif exp_val >= 1000:
                    expected = f"{exp_val:,.0f}"
                else:
                    expected = f"{exp_val:.2f}"
                source = rel.get("source_text", "")[:60]
                expected_cell = (
                    f'<span class="exp-value">{expected}</span>'
                    f'<div class="exp-source">{source}</div>'
                )
            else:
                expected_cell = '<span class="muted">— not captured —</span>'

            rows.append(f"""
              <tr>
                <td class="name-col">
                  <div class="series-name">{rel['name']}</div>
                  <div class="series-id">{_sid_html(rel['series_id'], source_urls)} · {rel['period']}</div>
                </td>
                <td class="val-col">{prior}</td>
                <td class="exp-col">{expected_cell}</td>
              </tr>""")

        parts.append(f"""
        <div class="upcoming-day">
          <h3>{label}</h3>
          <table class="data-table upcoming-table">
            <thead>
              <tr>
                <th class="name-col">Release</th>
                <th class="val-col">Prior</th>
                <th class="exp-col">Consensus</th>
              </tr>
            </thead>
            <tbody>{"".join(rows)}</tbody>
          </table>
        </div>""")

    note = (
        '<p class="muted" style="margin-top:1em">'
        'Consensus expectations are captured separately via '
        '<code>fetch_expectations.py</code>. Run that to populate '
        'missing forecasts.</p>'
    )
    return "\n".join(parts) + note


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
    source_urls = ctx.get("source_urls", {})

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
                <div class="series-id">{_sid_html(sid, source_urls)}</div>
              </td>
              <td class="spark-col">{spark}</td>
              <td class="date-col">{date_str}</td>
              <td class="val-col">{_format_val(a['latest_value'], is_pct)}</td>
              <td class="chg-col">{_trend_arrow(a)} {_format_change(a['period_pct'], is_pct)}</td>
              <td class="chg-col">{_format_change(a['yoy_pct'], is_pct)}</td>
              <td class="release-col">{released}</td>
              <td class="signal-col">{signal_tags} {trend}</td>
              <td class="export-col"><button class="btn-flag" onclick="event.stopPropagation(); flagSeriesFromRow(this)" title="Flag for commentary">&#9873;</button><button class="btn-export" onclick="event.stopPropagation(); downloadCsv('{sid}')">CSV</button></td>
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
.btn-flag { background: none; border: none; cursor: pointer; font-size: 14px; padding: 2px 6px; opacity: 0.3; color: var(--text-muted); }
.btn-flag:hover { opacity: 1; color: var(--amber); }
.btn-flag-inline { background: none; border: none; cursor: pointer; font-size: 12px; padding: 0 4px; opacity: 0; color: var(--text-muted); margin-left: 4px; vertical-align: middle; }
.analysis-p:hover .btn-flag-inline { opacity: 0.4; }
.btn-flag-inline:hover { opacity: 1 !important; color: var(--amber) !important; }
.flagged-item { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 12px; position: relative; }
.flagged-item .flagged-meta { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }
.flagged-item .flagged-text { font-size: 14px; color: var(--text); line-height: 1.5; }
.flagged-item .flagged-data { font-family: monospace; font-size: 13px; color: var(--accent); }
.flagged-item .flagged-note { margin-top: 8px; }
.flagged-item .flagged-note textarea { width: 100%; background: var(--bg); color: var(--text); border: 1px solid var(--border); border-radius: 4px; padding: 6px; font-size: 13px; resize: vertical; min-height: 40px; }
.btn-unflag { position: absolute; top: 8px; right: 8px; background: none; border: none; cursor: pointer; color: var(--text-muted); font-size: 16px; padding: 4px 8px; }
.btn-unflag:hover { color: var(--red); }
.flagged-stale { opacity: 0.5; border-color: var(--border); }
.flagged-age { font-size: 11px; padding: 2px 6px; border-radius: 8px; margin-left: 6px; }
.age-fresh { background: rgba(63, 185, 80, 0.15); color: var(--green); }
.age-aging { background: rgba(210, 153, 34, 0.15); color: var(--amber); }
.age-stale { background: rgba(248, 81, 73, 0.15); color: var(--red); }
.fed-chart { background: #fff; border: 1px solid var(--border); border-radius: 6px; padding: 8px; }
.fed-target { font-size: 14px; color: var(--text-muted); margin: 0 0 1em; }
.fed-target strong { color: var(--text); }
.fed-legend { display: flex; flex-wrap: wrap; gap: 1em; margin: 1em 0; font-size: 12px; color: var(--text-muted); }
.fed-legend span { display: inline-flex; align-items: center; gap: 0.4em; }
.fed-legend .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; }
.fed-legend .dot.solid { background: #2A8B3C; }
.fed-legend .dot.hollow { background: #fff; border: 2px solid #888; }
.fed-legend .line { display: inline-block; width: 24px; height: 2px; background: #2C5F8D; }
.fed-legend .line.dashed { background: linear-gradient(to right, #2C5F8D 50%, transparent 50%); background-size: 6px 2px; }
.fed-legend .bar { display: inline-block; width: 8px; height: 12px; opacity: 0.7; }
.fed-legend .bar.green { background: #2A8B3C; }
.fed-legend .bar.gray { background: #888; }
.fed-legend .bar.red { background: #C13B3B; }
.fed-table { border-collapse: collapse; width: 100%; margin-top: 0.5em; }
.fed-table td { padding: 8px 12px; border-bottom: 1px solid var(--border); vertical-align: middle; }
.fed-table td:first-child { width: 200px; color: var(--text); }
.prob-chip { display: inline-block; padding: 3px 8px; border-radius: 12px; font-size: 12px; margin-right: 4px; font-weight: 500; }
.prob-chip.chip-cut { background: rgba(42, 139, 60, 0.18); color: #3fb950; border: 1px solid rgba(42, 139, 60, 0.4); }
.prob-chip.chip-hold { background: rgba(136, 136, 136, 0.18); color: #aaa; border: 1px solid rgba(136, 136, 136, 0.4); }
.prob-chip.chip-hike { background: rgba(193, 59, 59, 0.18); color: #f85149; border: 1px solid rgba(193, 59, 59, 0.4); }

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
  align-items: center;
  gap: 16px;
}
header .header-logo { height: 32px; width: auto; }
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
.series-id .sid-link { color: inherit; text-decoration: none; border-bottom: 1px dotted currentColor; }
.series-id .sid-link:hover { color: var(--accent); border-bottom-style: solid; }
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
.chart-wrap { position: relative; }
.chart-overlay { cursor: crosshair; }
.chart-tooltip {
  position: absolute;
  display: none;
  background: rgba(22, 27, 34, 0.96);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 10px;
  font-size: 12px;
  line-height: 1.5;
  pointer-events: none;
  box-shadow: 0 4px 12px rgba(0,0,0,0.4);
  z-index: 10;
  white-space: nowrap;
}
.chart-tooltip .tip-date {
  font-weight: 600;
  color: var(--text);
  margin-bottom: 4px;
  border-bottom: 1px solid var(--border);
  padding-bottom: 4px;
}
.chart-tooltip .tip-row { display: flex; align-items: center; gap: 6px; color: var(--text-muted); }
.chart-tooltip .tip-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; }
.chart-tooltip .tip-val { color: var(--text); font-family: monospace; margin-left: auto; padding-left: 12px; }
.chart-tooltip .tip-rdate { color: var(--text-muted); font-style: normal; font-size: 11px; opacity: 0.75; margin-left: 4px; }
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

  // Hide tab sections with no visible content
  tabs.forEach(function(t) {
    var groups = t.querySelectorAll('.group-block');
    if (groups.length === 0) {
      // Tabs without data tables (Fed, Housing, Flagged, Upcoming)
      // are hidden during search — they don't have searchable series
      t.style.display = 'none';
      return;
    }
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

// --- Flag for commentary ---
// TEMPORARY: Flagged items are stored in browser localStorage.
// This works while the app runs locally and the user accesses the page
// from a single browser. If/when this system moves to cloud hosting
// with public access, replace localStorage with proper database storage
// (e.g. a flags table in SQLite served via an API endpoint).
var FLAG_KEY = 'econ_flagged_items';

function getFlags() {
  try { return JSON.parse(localStorage.getItem(FLAG_KEY) || '[]'); }
  catch(e) { return []; }
}
function saveFlags(flags) {
  localStorage.setItem(FLAG_KEY, JSON.stringify(flags));
  renderFlagged();
}

function flagSeriesFromRow(btn) {
  var row = btn.closest('.series-row');
  if (!row) return;
  var sid = row.dataset.sid;
  var name = row.querySelector('.series-name').textContent;
  var cells = row.querySelectorAll('td');
  var dateStr = cells[2] ? cells[2].textContent.trim() : '';
  var value = cells[3] ? cells[3].textContent.trim() : '';
  var period = cells[4] ? cells[4].textContent.trim() : '';
  var yoy = cells[5] ? cells[5].textContent.trim() : '';
  var flags = getFlags();
  flags.unshift({
    id: 'series-' + sid + '-' + Date.now(),
    type: 'series',
    series_id: sid,
    text: name + ' (' + sid + ')',
    data: dateStr + '  ' + value + '  period ' + period + '  YoY ' + yoy,
    flagged_at: new Date().toISOString(),
    note: ''
  });
  saveFlags(flags);
  btn.style.color = 'var(--amber)';
  btn.style.opacity = '1';
}

function flagParagraph(btn) {
  var p = btn.closest('p');
  if (!p) return;
  var text = p.textContent.replace(/\s*⚑\s*$/, '').trim();
  // Truncate if too long
  if (text.length > 300) text = text.substring(0, 300) + '...';
  var section = p.closest('.tab-content');
  var context = section ? section.id : 'analysis';
  var flags = getFlags();
  flags.unshift({
    id: 'para-' + Date.now(),
    type: 'paragraph',
    text: text,
    context: context,
    flagged_at: new Date().toISOString(),
    note: ''
  });
  saveFlags(flags);
  btn.style.color = 'var(--amber)';
  btn.style.opacity = '1';
}

function unflag(id) {
  var flags = getFlags().filter(function(f) { return f.id !== id; });
  saveFlags(flags);
}

function updateNote(id, note) {
  var flags = getFlags();
  flags.forEach(function(f) { if (f.id === id) f.note = note; });
  // Save without re-rendering (avoid destroying the active textarea)
  localStorage.setItem(FLAG_KEY, JSON.stringify(flags));
}

function renderFlagged() {
  var flags = getFlags();
  var container = document.getElementById('flagged-list');
  var badge = document.getElementById('flagged-badge');

  if (badge) {
    if (flags.length > 0) {
      badge.textContent = flags.length;
      badge.style.display = 'inline';
    } else {
      badge.style.display = 'none';
    }
  }

  if (!container) return;
  if (flags.length === 0) {
    container.innerHTML = '<p class="muted">No items flagged yet. Click the flag icon next to any data row or analysis paragraph to save it here.</p>';
    return;
  }

  var now = Date.now();
  var html = '';
  flags.forEach(function(f) {
    var flagDate = new Date(f.flagged_at);
    var ageDays = Math.floor((now - flagDate.getTime()) / 86400000);
    var ageLabel, ageClass;
    if (ageDays <= 2) { ageLabel = 'today'; ageClass = 'age-fresh'; }
    else if (ageDays <= 7) { ageLabel = ageDays + 'd ago'; ageClass = 'age-fresh'; }
    else if (ageDays <= 14) { ageLabel = ageDays + 'd ago'; ageClass = 'age-aging'; }
    else { ageLabel = ageDays + 'd ago'; ageClass = 'age-stale'; }
    var staleClass = ageDays > 14 ? ' flagged-stale' : '';

    var dataLine = f.data ? '<div class="flagged-data">' + f.data + '</div>' : '';
    html += '<div class="flagged-item' + staleClass + '" data-flag-id="' + f.id + '">';
    html += '<button class="btn-unflag" onclick="unflag(\\'' + f.id + '\\')" title="Remove">&times;</button>';
    html += '<div class="flagged-meta">';
    html += flagDate.toLocaleDateString('en-US', {weekday:'short', month:'short', day:'numeric'});
    html += ' <span class="flagged-age ' + ageClass + '">' + ageLabel + '</span>';
    if (f.type === 'series') html += ' &middot; data series';
    if (f.type === 'paragraph') html += ' &middot; ' + (f.context || 'analysis');
    html += '</div>';
    html += '<div class="flagged-text">' + f.text + '</div>';
    html += dataLine;
    html += '<div class="flagged-note"><textarea placeholder="Add a note..." oninput="updateNote(\\'' + f.id + '\\', this.value)">' + (f.note || '') + '</textarea></div>';
    html += '</div>';
  });
  container.innerHTML = html;
}

// Render on page load
document.addEventListener('DOMContentLoaded', renderFlagged);

// ── Chart crosshair hover (single- and multi-series) ─────────────────
// Parse a YYYY-MM-DD ISO date as a *local* date so toLocaleDateString
// doesn't shift by a day in negative-offset timezones.
function _isoToLocal(s) {
  var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (!m) return new Date(s);
  return new Date(+m[1], +m[2] - 1, +m[3]);
}

function _fmtChartDate(s, opts) {
  var d = _isoToLocal(s);
  if (isNaN(d)) return s;
  return d.toLocaleDateString(undefined, opts || {year:'numeric', month:'short', day:'numeric'});
}

function _fmtChartVal(v, isYoy) {
  if (isYoy) return (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
  if (Math.abs(v) >= 1000) return v.toLocaleString(undefined, {maximumFractionDigits: 0});
  if (Math.abs(v) >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

function chartHover(evt, id) {
  var cd = (window._chartData || {})[id];
  if (!cd) return;

  // Cache a date->index map for positioning per-series dots.
  if (!cd._dateMap) {
    cd._dateMap = {};
    for (var i = 0; i < cd.dates.length; i++) cd._dateMap[cd.dates[i]] = i;
  }

  var overlay = evt.currentTarget;
  var svg = overlay.ownerSVGElement;
  var orect = overlay.getBoundingClientRect();
  var frac = Math.max(0, Math.min(1, (evt.clientX - orect.left) / orect.width));
  var n = cd.dates.length;
  var idx = Math.max(0, Math.min(n - 1, Math.round(frac * (n - 1))));
  var hoverDate = cd.dates[idx];
  var hoverMs = _isoToLocal(hoverDate).getTime();
  var xSvg = cd.left + cd.plotW * (n > 1 ? idx / (n - 1) : 0);

  var xhair = svg.querySelector('.xhair-' + id);
  if (xhair) {
    xhair.setAttribute('x1', xSvg);
    xhair.setAttribute('x2', xSvg);
    xhair.style.display = 'block';
  }

  // For each series, find the observation closest to the hover date,
  // within that series' tolerance window (median gap / 2). This handles
  // weekly series whose dates don't align (Altos Friday vs Realtor Sunday)
  // and mixed weekly/monthly charts.
  var vrange = (cd.vmax - cd.vmin) || 1;
  var hits = [];
  var anyDifferent = false;
  cd.series.forEach(function(s) {
    var pts = s.points;
    if (!pts || !pts.length) return;
    var bestI = -1, bestDiff = Infinity;
    for (var i = 0; i < pts.length; i++) {
      var diff = Math.abs(_isoToLocal(pts[i][0]).getTime() - hoverMs);
      if (diff < bestDiff) { bestDiff = diff; bestI = i; }
    }
    if (bestI === -1) return;
    var v = pts[bestI][1];
    if (v === null || v === undefined) return;
    var maxMs = (s.tolerance || 30) * 86400000;
    if (bestDiff > maxMs) return;
    var actualDate = pts[bestI][0];
    if (actualDate !== hoverDate) anyDifferent = true;
    var actualIdx = cd._dateMap[actualDate];
    var xActual = (actualIdx === undefined)
      ? xSvg
      : cd.left + cd.plotW * (n > 1 ? actualIdx / (n - 1) : 0);
    var yActual = cd.top + cd.plotH * (1 - (v - cd.vmin) / vrange);
    hits.push({s: s, v: v, actualDate: actualDate, x: xActual, y: yActual});
  });

  var dotsHtml = '';
  var rowsHtml = '';
  hits.forEach(function(h) {
    dotsHtml += '<circle cx="' + h.x + '" cy="' + h.y + '" r="4" fill="' + h.s.color +
                '" stroke="#0d1117" stroke-width="1.5"/>';
    var dateSuffix = '';
    if (anyDifferent) {
      dateSuffix = ' <em class="tip-rdate">' +
        _fmtChartDate(h.actualDate, {month:'short', day:'numeric'}) + '</em>';
    }
    rowsHtml += '<div class="tip-row">' +
                '<span class="tip-dot" style="background:' + h.s.color + '"></span>' +
                '<span>' + h.s.label + dateSuffix + '</span>' +
                '<span class="tip-val">' + _fmtChartVal(h.v, cd.isYoy) + '</span></div>';
  });

  var dotsG = svg.querySelector('.xhair-dots-' + id);
  if (dotsG) {
    dotsG.innerHTML = dotsHtml;
    dotsG.style.display = 'block';
  }

  var tip = document.getElementById('tip-' + id);
  if (tip) {
    if (rowsHtml === '') { tip.style.display = 'none'; return; }
    tip.innerHTML = '<div class="tip-date">' + _fmtChartDate(hoverDate) + '</div>' + rowsHtml;
    tip.style.display = 'block';
    var wrap = tip.parentElement;
    var wrapRect = wrap.getBoundingClientRect();
    var svgRect = svg.getBoundingClientRect();
    var px = svgRect.left + (xSvg / cd.width) * svgRect.width - wrapRect.left;
    var tipW = tip.offsetWidth;
    if (px + tipW + 16 > wrapRect.width) {
      tip.style.left = (px - tipW - 12) + 'px';
    } else {
      tip.style.left = (px + 12) + 'px';
    }
    tip.style.top = (svgRect.top - wrapRect.top + 10) + 'px';
  }
}

function chartHoverLeave(id) {
  var tip = document.getElementById('tip-' + id);
  if (tip) tip.style.display = 'none';
  var xhair = document.querySelector('.xhair-' + id);
  if (xhair) xhair.style.display = 'none';
  var dotsG = document.querySelector('.xhair-dots-' + id);
  if (dotsG) dotsG.style.display = 'none';
}
"""
