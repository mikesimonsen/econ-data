"""
Fed tab sections built from the fomc_* / sep_* tables (see econ_data/fomc.py).

render_fomc_top()    — next meeting + blackout status, latest decision card,
                       statement redline vs the prior statement
render_fomc_bottom() — dot plot + SEP medians (vs prior SEP and FedWatch),
                       committee roster with 2026/2027 voting status,
                       vote record grid

Every visualization carries CSV download buttons shaped for Flourish: one
header row with units in the column names, plain numbers (percentages as
0–100, no symbols), ISO dates, wide layout for line charts and long layout
for scatter/heatmap/filterable charts.
"""
from __future__ import annotations

import csv
import difflib
import html
import io
import json
import re
from datetime import date, timedelta

from econ_data.db import connect
from econ_data.fomc import blackout_window

BASE = "https://www.federalreserve.gov"

SEP_LABELS = [("fed_funds", "Fed funds rate"), ("core_pce", "Core PCE inflation"),
              ("pce", "PCE inflation"), ("unemployment", "Unemployment rate"),
              ("gdp", "Real GDP growth")]


_DOWNLOAD_JS = (
    "<script>function fwDownloadCsv(k, f) {"
    " var c = (window.fwCsv || {})[k]; if (!c) return;"
    " var a = document.createElement('a');"
    " a.href = URL.createObjectURL(new Blob([c], {type: 'text/csv'}));"
    " a.download = f; a.click(); }</script>")


def _n(x, nd: int = 2):
    """Plain CSV number: rounded, blank for missing."""
    return "" if x is None else round(float(x), nd)


def csv_button(key: str, filename: str, header: list, rows: list, label: str = "CSV") -> str:
    """Download button carrying its CSV inline (no server; the briefing is static)."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    data = json.dumps(buf.getvalue()).replace("</", "<\\/")
    return (f'<script>(window.fwCsv = window.fwCsv || {{}})["{key}"] = {data};</script>'
            f'<button class="btn-export" onclick="fwDownloadCsv(\'{key}\', \'{filename}\')"'
            f' title="Download {html.escape(filename)}">{html.escape(label)}</button>')


def _exports(*buttons: str) -> str:
    return '<div class="fw-exports">' + " ".join(buttons) + "</div>"


def _url(u: str | None) -> str | None:
    return (u if u.startswith("http") else BASE + u) if u else None


def _fmt_range(lo: float, hi: float) -> str:
    def f(x):
        whole, frac = int(x), round((x - int(x)) * 4)
        tail = {0: "", 1: "¼", 2: "½", 3: "¾"}[frac]
        return f"{whole}{tail}" if whole or not tail else tail
    return f"{f(lo)}–{f(hi)}%"


def _fmt_bps(bps: int | None) -> str:
    if bps is None:
        return "?"
    return "Hold" if bps == 0 else f"{bps:+d}bp"


def _md(d: date) -> str:
    return d.strftime("%b %-d")


def _span(start: date, end: date) -> str:
    """'Oct 27–28' or 'Apr 30–May 1'."""
    tail = end.strftime("%-d") if start.month == end.month else _md(end)
    return f"{_md(start)}–{tail}"


def _short(d: date) -> str:
    return d.strftime("%b ’%y")


# ── Top: meeting status + latest decision ───────────────────

def _status_banner(con, today: date) -> str:
    nxt = con.execute(
        "SELECT meeting_date, start_date, has_sep FROM fomc_meetings "
        "WHERE NOT unscheduled AND (meeting_date > %s OR "
        "(meeting_date = %s AND statement_parsed_at IS NULL)) ORDER BY meeting_date LIMIT 1",
        (today, today)).fetchone()
    last = con.execute(
        "SELECT meeting_date, start_date, minutes_release, minutes_url FROM fomc_meetings "
        "WHERE meeting_date <= %s AND statement_parsed_at IS NOT NULL "
        "ORDER BY meeting_date DESC LIMIT 1", (today,)).fetchone()
    items = []
    # A meeting's blackout extends one day past the decision, so the most
    # recent meeting can still be in blackout after it's "past".
    for m in [last, nxt]:
        if not m:
            continue
        b0, b1 = blackout_window(m[1], m[0])
        if b0 <= today <= b1:
            items.append(f'<span class="fw-pill fw-pill-on">Blackout in effect</span> '
                         f'through {_md(b1)} (meeting {_span(m[1], m[0])})')
            break
    if nxt:
        b0, _ = blackout_window(nxt[1], nxt[0])
        sep = " · with SEP / dot plot" if nxt[2] else ""
        days = (nxt[0] - today).days
        when = "today" if days == 0 else f"in {days} days"
        items.append(f'<strong>Next FOMC:</strong> {_span(nxt[1], nxt[0])} ({when}){sep}')
        if today < b0:
            items.append(f'Blackout begins Sat {_md(b0)}')
    if last:
        if last[2]:
            if last[2] >= today - timedelta(days=7):
                items.append(f'Minutes of {_md(last[0])} meeting released {_md(last[2])}')
        else:
            due = last[0] + timedelta(days=21)
            items.append(f'Minutes of {_md(last[0])} meeting due ~{due.strftime("%a %b %-d")}')
    return '<div class="fw-banner">' + " &nbsp;·&nbsp; ".join(items) + '</div>'


def _redline(old: str, new: str) -> str:
    """Word-level diff of two statements as HTML with <del>/<ins>."""
    def tokens(t):
        return re.findall(r"\n\n|\S+", t)
    a, b = tokens(old), tokens(new)
    out = []

    def emit(ws, tag=None):
        text = " ".join(ws).replace(" \n\n ", "</p><p>").replace("\n\n", "</p><p>")
        text = html.escape(text).replace("&lt;/p&gt;&lt;p&gt;", "</p><p>")
        out.append(f"<{tag}>{text}</{tag}>" if tag else text)

    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(a=a, b=b, autojunk=False).get_opcodes():
        if op == "equal":
            emit(a[i1:i2])
        if op in ("delete", "replace"):
            emit(a[i1:i2], "del")
        if op in ("insert", "replace"):
            emit(b[j1:j2], "ins")
    return '<div class="fw-redline"><p>' + " ".join(out) + "</p></div>"


def _decision_card(con) -> str:
    rows = con.execute(
        "SELECT meeting_date, start_date, decision_bps, target_low, target_high, votes_for, "
        "votes_against, statement_text, statement_url, presser_url, sep_url "
        "FROM fomc_meetings WHERE statement_parsed_at IS NOT NULL "
        "ORDER BY meeting_date DESC LIMIT 2").fetchall()
    if not rows:
        return ""
    (d, start, bps, lo, hi, n_for, n_against, text, st_url, pc_url, sep_url) = rows[0]
    dissents = con.execute(
        "SELECT p.name, p.institution, v.preferred_bps, v.dissent_text FROM fomc_votes v "
        "JOIN fomc_participants p ON p.id = v.participant_id "
        "WHERE v.meeting_date = %s AND v.vote = 'against' ORDER BY p.name", (d,)).fetchall()

    cls = "hold" if bps == 0 else ("hike" if (bps or 0) > 0 else "cut")
    decision = {"hold": "Held", "hike": f"Raised {bps}bp", "cut": f"Cut {-bps}bp"}[cls] \
        if bps is not None else "Decision"
    links = " · ".join(f'<a href="{_url(u)}" target="_blank">{label}</a>'
                       for label, u in [("Statement", st_url), ("Press conference", pc_url),
                                        ("Projections", sep_url)] if u)
    dis_html = ""
    if dissents:
        items = []
        for name, inst, pref, why in dissents:
            if pref is None:
                lean = "dissent (non-rate)"
            else:
                lean = f"wanted {_fmt_bps(pref)}"
            seat = f" ({html.escape(inst)})" if inst and inst != "Board of Governors" else ""
            items.append(f'<li><strong>{html.escape(name)}</strong>{seat}: {lean} '
                         f'<span class="muted">— {html.escape(why or "")}</span></li>')
        dis_html = '<ul class="fw-dissents">' + "".join(items) + "</ul>"

    redline = ""
    if len(rows) > 1 and rows[1][7] and text:
        prev_d = rows[1][0]
        redline = (f'<details class="fw-details"><summary>Statement redline vs '
                   f'{_md(prev_d)}, {prev_d.year} <span class="muted">'
                   f'(<del>removed</del> / <ins>added</ins>)</span></summary>'
                   f'{_redline(rows[1][7], text)}</details>')

    return (
        f'<div class="fw-card">'
        f'<div class="fw-card-head"><span class="prob-chip chip-{cls}">{decision}</span> '
        f'<strong>FOMC {_span(start, d)}, {d.year}</strong> '
        f'&nbsp;Target range <strong>{_fmt_range(lo, hi) if hi is not None else "?"}</strong> '
        f'&nbsp;Vote <strong>{n_for}–{n_against}</strong></div>'
        f'{dis_html}<div class="fw-links">{links}</div>{redline}'
        f'{_exports(_decisions_csv(con))}</div>'
    )


def _decisions_csv(con) -> str:
    rows = con.execute(
        "SELECT m.meeting_date, m.decision_bps, m.target_low, m.target_high, m.votes_for, "
        "m.votes_against, m.has_sep, "
        "STRING_AGG(p.name || COALESCE(' (' || CASE WHEN v.preferred_bps > 0 THEN '+' "
        "ELSE '' END || v.preferred_bps || 'bp)', ' (non-rate)'), '; ' ORDER BY p.name) "
        "FROM fomc_meetings m LEFT JOIN fomc_votes v ON v.meeting_date = m.meeting_date "
        "AND v.vote = 'against' LEFT JOIN fomc_participants p ON p.id = v.participant_id "
        "WHERE m.statement_parsed_at IS NOT NULL GROUP BY m.meeting_date ORDER BY 1").fetchall()
    first, last = rows[0][0].year, rows[-1][0].year
    return csv_button(
        "decisions", f"fomc_decisions_{first}-{last}.csv",
        ["Meeting date", "Decision (bp)", "Target low (%)", "Target high (%)",
         "Target midpoint (%)", "Votes for", "Votes against", "Dissenters (preferred change)",
         "Projections released"],
        [[d.isoformat(), bps, _n(lo), _n(hi), _n((lo + hi) / 2 if hi is not None else None, 3),
          nf, na, dis or "", "Yes" if sep else "No"]
         for d, bps, lo, hi, nf, na, sep, dis in rows],
        label=f"CSV: all decisions since {first}")


def render_fomc_top(today: date | None = None) -> str:
    today = today or date.today()
    try:
        con = connect()
        return _DOWNLOAD_JS + _status_banner(con, today) + _decision_card(con)
    except Exception as e:
        return f'<p class="muted">FOMC data unavailable: {html.escape(str(e))}</p>'


# ── Bottom: dot plot, SEP, committee, votes ─────────────────

def _market_year_end(con, today: date) -> dict[str, float]:
    """FedWatch-implied rate after each year's final meeting, where covered.

    outcome_bps is cumulative from today (see render_fed_chart), so implied =
    current-range midpoint + expected change."""
    rng = con.execute("SELECT target_low, target_high FROM fomc_meetings "
                      "WHERE target_high IS NOT NULL ORDER BY meeting_date DESC LIMIT 1"
                      ).fetchone()
    if not rng:
        return {}
    mid = (rng[0] + rng[1]) / 2
    year_last = dict(con.execute(
        "SELECT EXTRACT(YEAR FROM meeting_date)::int, MAX(meeting_date) FROM fomc_meetings "
        "WHERE NOT unscheduled GROUP BY 1").fetchall())
    out = {}
    for y, last in year_last.items():
        if last <= today:
            continue
        probs = con.execute("SELECT outcome_bps, probability FROM fed_expectations "
                            "WHERE meeting_date = %s", (last,)).fetchall()
        if probs:
            out[str(y)] = mid + sum(b * p for b, p in probs) / 100
    return out


def _dot_plot(dots, med_now, med_prev, market, prev_label) -> str:
    horizons = sorted({h for h, _, _ in dots}, key=lambda h: (h == "LR", h))
    rates = [r for _, r, _ in dots] + list(med_now.values()) + list(med_prev.values()) \
        + list(market.values())
    y_min = (int(min(rates) * 4) - 1) / 4
    y_max = (int(max(rates) * 4) + 2) / 4
    width, height = 900, 380
    pl, pr, pt, pb = 50, 20, 20, 40
    pw, ph = width - pl - pr, height - pt - pb
    col_w = pw / len(horizons)

    def y(v):
        return pt + ph * (1 - (v - y_min) / (y_max - y_min))

    def cx(i):
        return pl + col_w * (i + 0.5)

    svg = [f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
           f'font-family="-apple-system, Helvetica, Arial, sans-serif" font-size="12">']
    v = y_min
    while v <= y_max + 1e-9:
        major = abs(v * 2 - round(v * 2)) < 1e-9
        svg.append(f'<line x1="{pl}" x2="{width - pr}" y1="{y(v):.1f}" y2="{y(v):.1f}" '
                   f'stroke="#e5e5e5" stroke-width="{1 if major else 0.5}"/>')
        if major:
            svg.append(f'<text x="{pl - 8}" y="{y(v) + 4:.1f}" text-anchor="end" '
                       f'fill="#666">{v:.1f}%</text>')
        v += 0.125
    for i, h in enumerate(horizons):
        label = "Longer run" if h == "LR" else h
        svg.append(f'<text x="{cx(i):.1f}" y="{height - 12}" text-anchor="middle" '
                   f'fill="#333" font-weight="600">{label}</text>')
        x0 = cx(i)
        if h in med_prev:
            svg.append(f'<line x1="{x0 - col_w * 0.42:.1f}" x2="{x0 + col_w * 0.42:.1f}" '
                       f'y1="{y(med_prev[h]):.1f}" y2="{y(med_prev[h]):.1f}" stroke="#999" '
                       f'stroke-width="2" stroke-dasharray="5 4"><title>{prev_label} median '
                       f'{label}: {med_prev[h]:.2f}%</title></line>')
        if h in med_now:
            svg.append(f'<line x1="{x0 - col_w * 0.42:.1f}" x2="{x0 + col_w * 0.42:.1f}" '
                       f'y1="{y(med_now[h]):.1f}" y2="{y(med_now[h]):.1f}" stroke="#2C5F8D" '
                       f'stroke-width="2"><title>Median {label}: {med_now[h]:.2f}%</title></line>')
        for _, r, n in [d for d in dots if d[0] == h]:
            step = min(10, col_w * 0.8 / max(n, 1))
            svg.append(f'<g><title>{label}: {n} participant{"s" if n != 1 else ""} at '
                       f'{r:.3f}%</title>')
            for k in range(n):
                svg.append(f'<circle cx="{x0 + (k - (n - 1) / 2) * step:.1f}" cy="{y(r):.1f}" '
                           f'r="4" fill="#2C5F8D" stroke="#fff" stroke-width="1.5"/>')
            svg.append('</g>')
        if h in market:
            mx, my = x0 + col_w * 0.46, y(market[h])
            svg.append(f'<path d="M{mx:.1f} {my - 6:.1f} L{mx + 6:.1f} {my:.1f} L{mx:.1f} '
                       f'{my + 6:.1f} L{mx - 6:.1f} {my:.1f} Z" fill="#fff" stroke="#C26A1B" '
                       f'stroke-width="2"><title>Market (FedWatch) year-end {h}: '
                       f'{market[h]:.2f}%</title></path>')
    svg.append('</svg>')
    return "".join(svg)


def _sep_section(con, today: date) -> str:
    seps = [r[0] for r in con.execute(
        "SELECT DISTINCT meeting_date FROM sep_dots ORDER BY meeting_date DESC LIMIT 2"
    ).fetchall()]
    if not seps:
        return ""
    cur = seps[0]
    prev = seps[1] if len(seps) > 1 else None
    prev_label = prev.strftime("%b %Y") if prev else ""
    dots = con.execute("SELECT horizon, rate, n FROM sep_dots WHERE meeting_date = %s",
                       (cur,)).fetchall()

    def medians(d):
        return {h: m for v, h, m in con.execute(
            "SELECT variable, horizon, median FROM sep_projections WHERE meeting_date = %s",
            (d,)).fetchall() if v == "fed_funds" and m is not None} if d else {}

    med_now, med_prev = medians(cur), medians(prev)
    horizons = sorted({h for h, _, _ in dots}, key=lambda h: (h == "LR", h))
    market = {h: v for h, v in _market_year_end(con, today).items() if h in horizons}
    chart = _dot_plot(dots, med_now, med_prev, market, prev_label)
    n_part = max(sum(n for h, _, n in dots if h == hz) for hz in horizons)

    legend = (
        '<div class="fed-legend">'
        f'<span><span class="dot solid" style="background:#2C5F8D"></span>One participant’s '
        f'year-end rate ({n_part} participants, anonymous)</span>'
        f'<span><span class="line"></span>Median, {cur.strftime("%b %Y")}</span>'
        + (f'<span><span class="line dashed" style="background:linear-gradient(to right,#999 50%,'
           f'transparent 50%);background-size:6px 2px"></span>Median, {prev_label}</span>'
           if prev else "")
        + ('<span><span class="dot hollow" style="border-color:#C26A1B;border-radius:2px;'
           'transform:rotate(45deg)"></span>Market-implied (FedWatch), after year’s last meeting'
           '</span>' if market else "")
        + '</div>')

    # Median table: current vs prior SEP
    cur_rows = {(v, h): m for v, h, m in con.execute(
        "SELECT variable, horizon, median FROM sep_projections WHERE meeting_date = %s",
        (cur,)).fetchall()}
    prev_rows = {(v, h): m for v, h, m in con.execute(
        "SELECT variable, horizon, median FROM sep_projections WHERE meeting_date = %s",
        (prev,)).fetchall()} if prev else {}
    head = "".join(f"<th>{'Longer run' if h == 'LR' else h}</th>" for h in horizons)
    body = []
    for var, label in SEP_LABELS:
        cells = []
        for h in horizons:
            m = cur_rows.get((var, h))
            if m is None:
                cells.append("<td class='muted'>—</td>")
                continue
            p = prev_rows.get((var, h))
            chg = ""
            if p is not None and abs(m - p) > 1e-9:
                chg = (f' <span class="fw-chg {"up" if m > p else "down"}">'
                       f'{"▲" if m > p else "▼"}{abs(m - p):.1f}</span>')
            cells.append(f"<td>{m:.1f}{chg}</td>")
        body.append(f"<tr><td>{label}</td>{''.join(cells)}</tr>")
    if market:
        body.append("<tr><td>Market (FedWatch) fed funds</td>"
                    + "".join(f"<td>{market[h]:.2f}</td>" if h in market
                              else "<td class='muted'>—</td>" for h in horizons) + "</tr>")
    note = (f'▲/▼ = change vs the {prev_label} SEP. ' if prev else "") + \
        "Dots are anonymous: the Fed does not say whose dot is whose."
    table = (f'<table class="fw-table"><thead><tr><th>Median projection</th>{head}</tr></thead>'
             f'<tbody>{"".join(body)}</tbody></table><p class="muted fw-note">{note}</p>')
    tag = cur.strftime("%b%Y").lower()
    x_index = {h: i for i, h in enumerate(horizons)}
    dot_rows = []
    for h in horizons:
        for _, r, n in sorted(d for d in dots if d[0] == h):
            for k in range(n):
                # X position stacks dots side by side around each horizon so a
                # Flourish scatter reproduces the Fed's layout directly.
                dot_rows.append([h if h != "LR" else "Longer run", x_index[h] + 1,
                                 round(x_index[h] + 1 + (k - (n - 1) / 2) * 0.04, 3),
                                 r, k + 1, _n(med_now.get(h)), _n(med_prev.get(h)),
                                 _n(market.get(h))])
    rates_desc = sorted({r for _, r, _ in dots}, reverse=True)
    counts = {(h, r): n for h, r, n in dots}
    hz_names = ["Longer run" if h == "LR" else h for h in horizons]
    dot_exports = _exports(
        csv_button("dots", f"fomc_dot_plot_{tag}.csv",
                   ["Horizon", "Horizon order", "X position", "Rate (%)", "Dot number",
                    f"Median {cur.strftime('%b %Y')} (%)",
                    f"Median {prev_label} (%)" if prev else "Prior median (%)",
                    "Market-implied (%)"],
                   dot_rows, label="CSV: one row per dot (scatter)"),
        csv_button("dotgrid", f"fomc_dot_counts_{tag}.csv",
                   ["Rate (%)"] + hz_names,
                   [[r] + [counts.get((h, r), 0) for h in horizons] for r in rates_desc],
                   label="CSV: count grid (table/heatmap)"))

    sep_rows = []
    for var, label in SEP_LABELS:
        rng = {(h): (lo, hi, rl, rh) for v, h, lo, hi, rl, rh in con.execute(
            "SELECT variable, horizon, ct_lo, ct_hi, range_lo, range_hi FROM sep_projections "
            "WHERE meeting_date = %s AND variable = %s", (cur, var)).fetchall()}
        for h in horizons:
            m = cur_rows.get((var, h))
            if m is None:
                continue
            p = prev_rows.get((var, h))
            ct_lo, ct_hi, rl, rh = rng.get(h, (None,) * 4)
            sep_rows.append([label, "Longer run" if h == "LR" else h, _n(m, 1), _n(p, 1),
                             _n(m - p, 1) if p is not None else "", _n(ct_lo, 1), _n(ct_hi, 1),
                             _n(rl, 1), _n(rh, 1)])
    hist = con.execute(
        "SELECT meeting_date, variable, horizon, median, ct_lo, ct_hi, range_lo, range_hi "
        "FROM sep_projections ORDER BY meeting_date, variable, horizon").fetchall()
    var_label = dict(SEP_LABELS)
    sep_exports = _exports(
        csv_button("sep", f"fomc_sep_medians_{tag}.csv",
                   ["Variable", "Horizon", f"Median {cur.strftime('%b %Y')}",
                    f"Median {prev_label}" if prev else "Prior median", "Change",
                    "Central tendency low", "Central tendency high", "Range low", "Range high"],
                   sep_rows, label="CSV: this table"),
        csv_button("sephist", f"fomc_sep_history_{hist[0][0].year}-{cur.year}.csv",
                   ["SEP date", "Variable", "Horizon", "Median", "Central tendency low",
                    "Central tendency high", "Range low", "Range high"],
                   [[d.isoformat(), var_label.get(v, v), "Longer run" if h == "LR" else h,
                     _n(m, 1), _n(a, 1), _n(b, 1), _n(c, 1), _n(e, 1)]
                    for d, v, h, m, a, b, c, e in hist],
                   label=f"CSV: every SEP since {hist[0][0].year} (long)"),
        csv_button("seppath", f"fomc_fed_funds_median_path_{hist[0][0].year}-{cur.year}.csv",
                   *_median_path(hist), label="CSV: fed funds median by SEP (line chart)"))
    return (f'<h3 class="fw-h3">Dot plot: {cur.strftime("%B %Y")} Summary of Economic '
            f'Projections</h3><div class="fed-chart">{chart}</div>{legend}{dot_exports}'
            f'{table}{sep_exports}')


def _median_path(hist) -> tuple[list, list]:
    """Wide: one row per SEP, one column per horizon — how the committee's
    expected year-end fed funds rate evolved, ready for a Flourish line chart."""
    ff = [(d, h, m) for d, v, h, m, *_ in hist if v == "fed_funds"]
    horizons = sorted({h for _, h, _ in ff}, key=lambda h: (h == "LR", h))
    by_sep: dict = {}
    for d, h, m in ff:
        by_sep.setdefault(d, {})[h] = m
    header = ["SEP date"] + [f"End-{h} median (%)" if h != "LR" else "Longer-run median (%)"
                             for h in horizons]
    rows = [[d.isoformat()] + [_n(vals.get(h), 3) for h in horizons]
            for d, vals in sorted(by_sep.items())]
    return header, rows


def _committee(con, today: date) -> str:
    year = today.year
    people = con.execute(
        "SELECT id, name, institution, role, appointed_by, board_term_end, chair_term_end, "
        "verified, bio_url FROM fomc_participants WHERE active "
        "AND COALESCE(role, '') <> 'First Vice President'").fetchall()
    if not people:
        return ""
    voters = dict(con.execute("SELECT participant_id, status FROM fomc_voters WHERE year = %s",
                              (year,)).fetchall())
    rotation = dict(con.execute("SELECT institution, status FROM fomc_rotation WHERE year = %s",
                                (year + 1,)).fetchall())
    dissents = dict(con.execute(
        "SELECT participant_id, COUNT(*) FROM fomc_votes WHERE vote = 'against' "
        "AND meeting_date >= %s GROUP BY 1", (date(year - 1, 1, 1),)).fetchall())

    order = {"Chairman": 0, "Chair": 0, "Vice Chair": 1, "Vice Chair for Supervision": 2}

    def key(p):
        board = p[2] == "Board of Governors"
        return (0 if board else (1 if p[2] == "New York" else 2),
                order.get(p[3] or "", 9) if board else 0, p[2] or "", p[1])

    def status(s):
        return {"member": '<span class="fw-pill fw-pill-on">Voter</span>',
                "alternate": '<span class="fw-pill">Alternate</span>'}.get(
            s, '<span class="muted">—</span>')

    rows, csv_rows = [], []
    label_of = {"member": "Voter", "alternate": "Alternate"}
    for pid, name, inst, role, appt, term, chair_term, verified, bio in sorted(people, key=key):
        board = inst == "Board of Governors"
        now = voters.get(pid) or ("member" if board else None)
        nxt = "member" if board else rotation.get(inst)
        nm = html.escape(name)
        if bio:
            nm = f'<a href="{_url(bio)}" target="_blank">{nm}</a>'
        if not verified:
            nm += ' <span class="fw-unverified" title="Not confirmed at a primary source — check fed_roster.yaml">unverified</span>'
        title = html.escape(role or ("Governor" if board else "President"))
        if chair_term:
            title += f' <span class="muted">(to {chair_term.strftime("%b %Y")})</span>'
        csv_rows.append([name, inst or "", role or ("Governor" if board else "President"),
                         label_of.get(now, "Non-voter"), label_of.get(nxt, "Non-voter"),
                         appt or "", term.isoformat() if term else "",
                         chair_term.isoformat() if chair_term else "",
                         dissents.get(pid, 0), "Yes" if verified else "No"])
        rows.append(
            f"<tr><td>{nm}</td><td>{html.escape(inst or '')}</td><td>{title}</td>"
            f"<td>{status(now)}</td><td>{status(nxt)}</td>"
            f"<td class='muted fw-wrap'>{html.escape(appt or '')}</td>"
            f"<td>{term.year if term else ''}</td>"
            f"<td>{dissents.get(pid, '') or ''}</td></tr>")
    return (f'<h3 class="fw-h3">The Committee</h3>'
            f'<div class="fw-scroll"><table class="fw-table"><thead><tr><th>Participant</th>'
            f'<th>Seat</th><th>Title</th><th>{year}</th><th>{year + 1}</th>'
            f'<th>Appointed by</th><th>Board term ends</th><th>Dissents since {year - 1}</th>'
            f'</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'
            f'<p class="muted fw-note">All 19 participants debate and submit dots; 12 vote. '
            f'Governors and New York vote every year; four other banks rotate annually.</p>'
            + _exports(csv_button(
                "committee", f"fomc_committee_{today.isoformat()}.csv",
                ["Participant", "Seat", "Title", f"{year} status", f"{year + 1} status",
                 "Appointed by", "Board term ends", "Leadership term ends",
                 f"Dissents since {year - 1}", "Verified at source"], csv_rows)))


def _vote_grid(con, n_meetings: int = 12) -> str:
    meetings = con.execute(
        "SELECT meeting_date, decision_bps FROM fomc_meetings WHERE statement_parsed_at IS NOT NULL "
        "ORDER BY meeting_date DESC LIMIT %s", (n_meetings,)).fetchall()[::-1]
    if not meetings:
        return ""
    dates = [m[0] for m in meetings]
    decision = dict(meetings)
    votes = con.execute(
        "SELECT v.meeting_date, v.participant_id, p.name, p.institution, v.vote, "
        "v.preferred_bps, v.dissent_text, v.inferred, p.active FROM fomc_votes v "
        "JOIN fomc_participants p ON p.id = v.participant_id WHERE v.meeting_date = ANY(%s)",
        (dates,)).fetchall()
    grid, people = {}, {}
    for d, pid, name, inst, vote, pref, why, inferred, active in votes:
        grid[(pid, d)] = (vote, pref, why, inferred)
        people[pid] = (name, inst, active)

    def sort_key(pid):
        name, inst, active = people[pid]
        return (not active, inst != "Board of Governors", inst or "~", name)

    head = "".join(f"<th>{_short(d)}<br><span class='muted'>{_fmt_bps(decision[d])}</span></th>"
                   for d in dates)

    def vote_label(vote, pref, dec):
        if vote == "for":
            return "For", 0
        if pref is None:
            return "Dissent: other grounds", 0.5
        return ("Dissent: tighter", 1) if pref > (dec or 0) else ("Dissent: easier", -1)

    grid_csv = []
    rows = []
    for pid in sorted(people, key=sort_key):
        name, inst, active = people[pid]
        cells = []
        for d in dates:
            v = grid.get((pid, d))
            if not v:
                cells.append("<td></td>")
                continue
            vote, pref, why, inferred = v
            if vote == "for":
                tip = "Voted for" + (" (inferred: statement lists dissenters only)" if inferred else "")
                cells.append(f'<td class="fw-for" title="{tip}">✓</td>')
            else:
                dec = decision[d] or 0
                if pref is None:
                    sym, cls = "◆", "fw-other"
                elif pref > dec:
                    sym, cls = "▲", "fw-hawk"
                else:
                    sym, cls = "▼", "fw-dove"
                cells.append(f'<td class="{cls}" title="{html.escape(why or "")}">{sym}</td>')
        grid_csv.append([name, inst or ""] + [
            vote_label(grid[(pid, d)][0], grid[(pid, d)][1], decision[d])[0]
            if (pid, d) in grid else "" for d in dates])
        seat = "" if inst in (None, "Board of Governors") else f" <span class='muted'>{html.escape(inst)}</span>"
        rows.append(f"<tr{'' if active else ' class=fw-former'}><td>{html.escape(name)}{seat}</td>"
                    f"{''.join(cells)}</tr>")
    return (f'<h3 class="fw-h3">Vote record: last {len(dates)} meetings</h3>'
            f'<div class="fw-scroll"><table class="fw-table fw-grid"><thead><tr><th></th>{head}'
            f'</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'
            f'<p class="muted fw-note">✓ voted with the majority &nbsp; <span class="fw-hawk">▲</span> '
            f'dissented for tighter policy &nbsp; <span class="fw-dove">▼</span> dissented for '
            f'easier policy &nbsp; <span class="fw-other">◆</span> dissented on other grounds. '
            f'Hover a mark for the stated reason. Blank = not a voter that meeting.</p>'
            + _exports(
                csv_button("votegrid", f"fomc_vote_grid_{dates[0].isoformat()}_{dates[-1].isoformat()}.csv",
                           ["Participant", "Seat"] + [d.isoformat() for d in dates], grid_csv,
                           label="CSV: grid as shown (table)"),
                _all_votes_csv(con, vote_label)))


def _all_votes_csv(con, vote_label) -> str:
    rows = con.execute(
        "SELECT v.meeting_date, p.name, p.institution, v.vote, v.preferred_bps, m.decision_bps, "
        "v.dissent_text, v.inferred FROM fomc_votes v JOIN fomc_participants p "
        "ON p.id = v.participant_id JOIN fomc_meetings m ON m.meeting_date = v.meeting_date "
        "ORDER BY 1, 2").fetchall()
    out = []
    for d, name, inst, vote, pref, dec, why, inferred in rows:
        label, code = vote_label(vote, pref, dec)
        out.append([d.isoformat(), name, inst or "", label, code, dec,
                    "" if pref is None else pref, why or "", "Yes" if inferred else "No"])
    first, last = rows[0][0].year, rows[-1][0].year
    return csv_button(
        "votes", f"fomc_votes_{first}-{last}.csv",
        ["Meeting date", "Participant", "Seat", "Vote", "Vote code", "Decision (bp)",
         "Preferred change (bp)", "Stated reason", "Inferred from roster"], out,
        label=f"CSV: every vote since {first} (long, heatmap)")


def render_fomc_bottom(today: date | None = None) -> str:
    today = today or date.today()
    try:
        con = connect()
        return (_DOWNLOAD_JS + _sep_section(con, today) + _committee(con, today)
                + _vote_grid(con))
    except Exception as e:
        return f'<p class="muted">FOMC data unavailable: {html.escape(str(e))}</p>'
