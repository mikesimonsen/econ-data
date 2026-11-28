"""GDP tab: quarterly real GDP growth bar chart (actual + forecasts), forecast
table, and Flourish-ready CSVs.

Growth in the chart is the continuously compounded annual rate of change:
400 × ln(level / prior level) for actuals (the period_cca calc on GDPC1).
Forecasters publish compounded annual rates, so those are converted with
100 × ln(1 + rate/100) to sit on the same basis.
"""
from __future__ import annotations

import html
import math
from datetime import date

from econ_data.db import connect
from econ_data.fomc_render import _DOWNLOAD_JS, _exports, _n, csv_button

LEVEL_ID = "GDPC1"
HEADLINE_ID = "A191RL1Q225SBEA"
ACTUAL_QUARTERS = 12

# (key, label, color, source page)
SOURCES = [
    ("atlanta", "Atlanta Fed GDPNow", "#3fb950",
     "https://www.atlantafed.org/cqer/research/gdpnow"),
    ("newyork", "NY Fed Nowcast", "#d29922",
     "https://www.newyorkfed.org/research/policy/nowcast"),
    ("stlouis", "St. Louis Fed Nowcast", "#bc8cff",
     "https://fred.stlouisfed.org/series/STLENI"),
    ("philly", "Philly Fed SPF (median)", "#f0883e",
     "https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/"
     "survey-of-professional-forecasters"),
]
ACTUAL_COLOR = "#58a6ff"


def to_cca(compounded_rate: float) -> float:
    """Compounded annual rate (%) → continuously compounded annual rate (%)."""
    return 100 * math.log1p(compounded_rate / 100)


def quarter_label(d: date) -> str:
    return f"{d.year} Q{(d.month - 1) // 3 + 1}"


def latest_forecasts(rows: list, after: date) -> dict:
    """{(target_quarter, source): (forecast_date, value)} keeping the newest
    vintage, for quarters after the last published actual.
    rows are (source, target_quarter, forecast_date, value)."""
    out = {}
    for source, target, made, value in rows:
        if target <= after:
            continue
        key = (target, source)
        if key not in out or made > out[key][0]:
            out[key] = (made, value)
    return out


def _bar_chart(slots: list) -> str:
    """slots: [(quarter, [(label, color, value, hover), ...])]."""
    values = [b[2] for _, bars in slots for b in bars]
    lo, hi = min(0.0, min(values)), max(0.0, max(values))
    step = 1 if hi - lo <= 8 else 2 if hi - lo <= 16 else 5
    lo = math.floor((lo - 0.4) / step) * step if lo < 0 else 0
    hi = math.ceil((hi + 0.4) / step) * step

    width, height = 960, 380
    left, right, top, bottom = 48, 12, 16, 34
    plot_w, plot_h = width - left - right, height - top - bottom
    weights = [max(1.0, 0.75 * len(bars)) for _, bars in slots]
    unit = plot_w / sum(weights)

    def y(v):
        return top + plot_h * (hi - v) / (hi - lo)

    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
           f'style="width:100%;max-width:{width}px;height:auto;background:var(--surface);'
           f'border-radius:8px" role="img" aria-label="Real GDP growth by quarter">']
    tick = lo
    while tick <= hi:
        svg.append(
            f'<line x1="{left}" y1="{y(tick):.1f}" x2="{width - right}" y2="{y(tick):.1f}" '
            f'stroke="var(--border)" stroke-width="{1.5 if tick == 0 else 0.5}"/>'
            f'<text x="{left - 6}" y="{y(tick) + 4:.1f}" text-anchor="end" font-size="11" '
            f'fill="var(--text-muted)">{tick:g}%</text>')
        tick += step

    x = left
    for (quarter, bars), weight in zip(slots, weights):
        slot_w = unit * weight
        bar_w = slot_w * 0.84 / len(bars)
        for i, (label, color, value, hover) in enumerate(bars):
            bx = x + slot_w * 0.08 + bar_w * i
            y0, y1 = sorted((y(0), y(value)))
            opacity = 1 if label == "Actual" else 0.75
            label_y = y(value) - 4 if value >= 0 else y(value) + 11
            svg.append(
                f'<rect x="{bx + 0.5:.1f}" y="{y0:.1f}" width="{bar_w - 1:.1f}" '
                f'height="{max(y1 - y0, 0.5):.1f}" fill="{color}" fill-opacity="{opacity}">'
                f'<title>{html.escape(hover)}</title></rect>'
                f'<text x="{bx + bar_w / 2:.1f}" y="{label_y:.1f}" text-anchor="middle" '
                f'font-size="{10 if bar_w >= 22 else 8}" fill="var(--text)">{value:.1f}</text>')
        q = (quarter.month - 1) // 3 + 1
        svg.append(
            f'<text x="{x + slot_w / 2:.1f}" y="{height - 18}" text-anchor="middle" '
            f'font-size="11" fill="var(--text-muted)">Q{q}</text>'
            f'<text x="{x + slot_w / 2:.1f}" y="{height - 5}" text-anchor="middle" '
            f'font-size="10" fill="var(--text-muted)">{quarter.year}</text>')
        x += slot_w
    svg.append("</svg>")
    return "".join(svg)


def _legend(used: set) -> str:
    items = [("Actual (BEA)", ACTUAL_COLOR)]
    items += [(label, color) for key, label, color, _ in SOURCES if key in used]
    return '<div class="fed-legend">' + "".join(
        f'<span><span class="dot" style="background:{color};border-radius:2px"></span>'
        f'{html.escape(label)}</span>' for label, color in items) + "</div>"


def _forecast_table(fc: dict, quarters: list) -> str:
    head = "".join(f"<th>{quarter_label(q)}</th>" for q in quarters)
    body = []
    for key, label, _, url in SOURCES:
        made = [fc[(q, key)][0] for q in quarters if (q, key) in fc]
        if not made:
            continue
        cells = "".join(
            f"<td>{fc[(q, key)][1]:.1f}%</td>" if (q, key) in fc else "<td>–</td>"
            for q in quarters)
        body.append(
            f'<tr><td><a href="{url}" target="_blank" rel="noopener">{html.escape(label)}</a>'
            f"</td><td>{max(made).isoformat()}</td>{cells}</tr>")
    return (
        '<h3 class="fw-h3">Latest forecasts, as published</h3>'
        '<div class="fw-scroll"><table class="fw-table"><thead><tr><th>Source</th>'
        f'<th>Updated</th>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'
        '<p class="muted fw-note">Real GDP growth, compounded annual rate, matching each '
        'source’s own page. The chart restates these as continuously compounded, so '
        'they read slightly lower there.</p>')


def _sep_table(con) -> str:
    rows = con.execute(
        "SELECT meeting_date, horizon, median, ct_lo, ct_hi FROM sep_projections "
        "WHERE variable = 'gdp' AND meeting_date = "
        "(SELECT MAX(meeting_date) FROM sep_projections WHERE variable = 'gdp')"
    ).fetchall()
    if not rows:
        return ""
    rows.sort(key=lambda r: (r[1] == "LR", r[1]))
    meeting = rows[0][0]
    names = ["Longer run" if r[1] == "LR" else r[1] for r in rows]
    head = "".join(f"<th>{n}</th>" for n in names)
    cells = "".join(f"<td>{'–' if r[2] is None else f'{r[2]:.1f}%'}</td>" for r in rows)
    button = csv_button(
        "gdpsep", f"fomc_gdp_projections_{meeting.isoformat()}.csv",
        ["Horizon", "Median (%)", "Central tendency low (%)", "Central tendency high (%)"],
        [[n, _n(r[2], 1), _n(r[3], 1), _n(r[4], 1)] for n, r in zip(names, rows)],
        label="CSV: FOMC projections")
    return (
        f'<h3 class="fw-h3">FOMC projections, {meeting.strftime("%b %Y")} (median)</h3>'
        f'<div class="fw-scroll"><table class="fw-table"><thead><tr>{head}</tr></thead>'
        f'<tbody><tr>{cells}</tr></tbody></table></div>'
        '<p class="muted fw-note">Annual, Q4 over Q4, so not comparable bar-for-bar with '
        'the quarterly rates above.</p>' + _exports(button))


def _render(con) -> str:
    actual = con.execute(
        "SELECT o.date, o.value, c.value, h.value FROM observations o "
        "JOIN calculated c ON c.series_id = o.series_id AND c.date = o.date "
        "AND c.calc_type = 'period_cca' "
        "LEFT JOIN observations h ON h.series_id = %s AND h.date = o.date "
        "WHERE o.series_id = %s ORDER BY o.date", (HEADLINE_ID, LEVEL_ID)).fetchall()
    if not actual:
        return ""
    last_q = actual[-1][0]
    rows = con.execute(
        "SELECT source, target_quarter, forecast_date, value FROM gdp_forecasts "
        "ORDER BY source, target_quarter, forecast_date").fetchall()
    fc = latest_forecasts(rows, last_q)
    quarters = sorted({q for q, _ in fc})
    shown = actual[-ACTUAL_QUARTERS:]

    slots, wide = [], []
    for d, _, cca, _ in shown:
        slots.append((d, [("Actual", ACTUAL_COLOR, cca,
                           f"{quarter_label(d)} actual: {cca:.2f}%")]))
        wide.append([quarter_label(d), d.isoformat(), _n(cca)] + [""] * len(SOURCES))
    for q in quarters:
        bars, cells = [], []
        for key, label, color, _ in SOURCES:
            if (q, key) not in fc:
                cells.append("")
                continue
            made, value = fc[(q, key)]
            cca = to_cca(value)
            bars.append((label, color, cca,
                         f"{quarter_label(q)} {label}: {cca:.2f}% (as of {made.isoformat()})"))
            cells.append(_n(cca))
        slots.append((q, bars))
        wide.append([quarter_label(q), q.isoformat(), ""] + cells)

    exports = _exports(
        csv_button(
            "gdpchart", f"real_gdp_growth_chart_{last_q.isoformat()}.csv",
            ["Quarter", "Quarter start", "Actual (%, cc annual rate)"]
            + [f"{label} (%, cc annual rate)" for _, label, _, _ in SOURCES],
            wide, label="CSV: chart as shown (bar chart)"),
        csv_button(
            "gdphist", f"real_gdp_history_{last_q.isoformat()}.csv",
            ["Quarter", "Quarter start", "Real GDP (billions of chained 2017 $, annual rate)",
             "Growth (%, cc annual rate)", "Growth (%, compounded annual rate, BEA)"],
            [[quarter_label(d), d.isoformat(), _n(level), _n(cca), _n(headline, 1)]
             for d, level, cca, headline in actual],
            label="CSV: full history"),
        csv_button(
            "gdpfc", f"real_gdp_forecast_history_{last_q.isoformat()}.csv",
            ["Source", "Target quarter", "Target quarter start", "Forecast date",
             "Growth (%, compounded annual rate)", "Growth (%, cc annual rate)"],
            [[dict((k, l) for k, l, _, _ in SOURCES).get(s, s), quarter_label(t),
              t.isoformat(), m.isoformat(), _n(v), _n(to_cca(v))] for s, t, m, v in rows],
            label="CSV: every forecast vintage (long)"))

    headline = actual[-1][3]
    note = (f"Latest actual: {quarter_label(last_q)}, {actual[-1][2]:.1f}% continuously "
            "compounded"
            + (f" ({headline:.1f}% on BEA’s headline basis)." if headline is not None else "."))
    return (
        _DOWNLOAD_JS
        + '<h2 style="margin-top:0">Real GDP Growth by Quarter</h2>'
        f'<p class="muted">Continuously compounded annual rate of change, seasonally '
        f'adjusted. {note}</p>'
        + _legend({s for _, s in fc})
        + f'<div class="hero-chart">{_bar_chart(slots)}</div>'
        + exports
        + (_forecast_table(fc, quarters) if quarters else "")
        + _sep_table(con))


def render_gdp() -> str:
    try:
        return _render(connect())
    except Exception as e:
        return f'<p class="muted">GDP data unavailable: {html.escape(str(e))}</p>'
