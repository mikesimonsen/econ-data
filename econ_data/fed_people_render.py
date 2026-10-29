"""
Fed tab sections built from fed_communications / comm_analysis / member_profiles
(see econ_data/fed_speeches.py).

render_movers()      — "Who's moving": recent remarks well off the speaker's own
                       recent average
render_spectrum()    — hawk–dove spectrum of every participant, 90-day stance
                       with the move from the prior 90 days
render_remarks()     — recent remarks + upcoming Board appearances
render_profiles()    — one expandable profile per member: framework write-up,
                       stance trajectory, quotes, remarks

Stance scores run -2 (dovish) … +2 (hawkish) and are model reads of each
speech (prompts/fed_speech_analysis.txt); the page says so wherever they appear.
Every section has Flourish-ready CSV downloads (see fomc_render.csv_button).
"""
from __future__ import annotations

import html
import re
from datetime import date, timedelta

from econ_data.db import connect
from econ_data.fomc_render import _DOWNLOAD_JS, _exports, _n, csv_button

WINDOW = 90  # days per stance window

LEAD_ROLES = ("Chairman", "Chair", "Vice Chair")  # FOMC chair + vice chair (NY)


def _fmt_score(s: float | None) -> str:
    return "—" if s is None else f"{s:+.1f}"


def _stance_chip(s: float | None) -> str:
    if s is None:
        return '<span class="fw-chip fw-chip-none" title="No monetary-policy content">n/a</span>'
    cls = "hawk" if s >= 0.5 else ("dove" if s <= -0.5 else "mid")
    return f'<span class="fw-chip fw-chip-{cls}" title="Stance score, -2 dovish … +2 hawkish">{s:+.1f}</span>'


def _md(md: str) -> str:
    """Small escaping Markdown → HTML for model-written profiles (headings,
    bold/italic, bullets, quotes), with the briefing's flag-for-commentary button."""
    out, in_list = [], False
    flag = ('<button class="btn-flag-inline" onclick="flagParagraph(this)" '
            'title="Flag for commentary">&#9873;</button>')
    for raw in md.split("\n"):
        line = raw.strip()
        is_item = line.startswith(("- ", "* "))
        if in_list and not is_item:
            out.append("</ul>")
            in_list = False
        if not line or re.fullmatch(r"-{3,}", line):
            continue
        # Strip exactly the block marker; a character-set strip would also eat
        # the "**" of a bolded lead-in ("- **Mandate:** …").
        if line.startswith("#"):
            content = line.lstrip("#").strip()
        elif is_item:
            content = line[2:].strip()
        elif line.startswith(">"):
            content = line[1:].strip()
        else:
            content = line
        s = html.escape(content)
        s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
        s = re.sub(r"(?<!\w)\*(.+?)\*(?!\w)", r"<em>\1</em>", s)
        if line.startswith("#"):
            out.append(f'<h4 class="fw-prof-h">{s}</h4>')
        elif is_item:
            if not in_list:
                out.append('<ul class="fw-prof-ul">')
                in_list = True
            out.append(f'<li class="analysis-p">{s}{flag}</li>')
        elif line.startswith(">"):
            out.append(f'<blockquote class="fw-quote">{s}</blockquote>')
        else:
            out.append(f'<p class="analysis-p">{s}{flag}</p>')
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


def _scored(con, since: date | None = None):
    """All scored remarks (policy content present), oldest first."""
    q = ("SELECT c.id, c.participant_id, c.comm_date, c.kind, c.title, c.url, a.policy_content, "
         "a.hawk_dove, a.confidence, a.rate_view, a.rate_horizon, a.headline, a.summary, "
         "a.framework_tags, a.topics, a.key_quotes FROM fed_communications c "
         "JOIN comm_analysis a ON a.comm_id = c.id")
    args = ()
    if since:
        q += " WHERE c.comm_date >= %s"
        args = (since,)
    return con.execute(q + " ORDER BY c.comm_date, c.id", args).fetchall()


def _weighted_mean(rows) -> float | None:
    """Mean stance; 'light' policy content counts half."""
    pts = [(r[7], 0.5 if r[6] == "light" else 1.0) for r in rows if r[7] is not None]
    w = sum(x for _, x in pts)
    return sum(s * x for s, x in pts) / w if w else None


def _people(con, today: date) -> dict:
    year = today.year
    voters = dict(con.execute("SELECT participant_id, status FROM fomc_voters WHERE year = %s",
                              (year,)).fetchall())
    rotation = dict(con.execute("SELECT institution, status FROM fomc_rotation WHERE year = %s",
                                (year + 1,)).fetchall())
    out = {}
    for pid, name, inst, role in con.execute(
            "SELECT id, name, institution, role FROM fomc_participants WHERE active "
            "AND COALESCE(role, '') <> 'First Vice President'").fetchall():
        board = inst == "Board of Governors"
        out[pid] = {"name": name, "inst": inst or "", "role": role or "",
                    "now": voters.get(pid) or ("member" if board else None),
                    "next": "member" if board else rotation.get(inst)}
    return out


def stances(con, today: date) -> dict:
    """{pid: (current, prior, n_current, latest_date)} over WINDOW-day windows.
    With nothing in the current window, current falls back to the latest
    remark (flagged stale by its date)."""
    rows = _scored(con, today - timedelta(days=2 * WINDOW))
    all_rows = _scored(con)
    by = {}
    for r in rows:
        by.setdefault(r[1], []).append(r)
    latest = {}
    for r in all_rows:
        if r[7] is not None:
            latest[r[1]] = r
    cut = today - timedelta(days=WINDOW)
    out = {}
    for pid in set(by) | set(latest):
        cur_rows = [r for r in by.get(pid, []) if r[2] >= cut]
        prior_rows = [r for r in by.get(pid, []) if r[2] < cut]
        cur = _weighted_mean(cur_rows)
        n_cur = sum(1 for r in cur_rows if r[7] is not None)
        if cur is None and pid in latest:
            cur = latest[pid][7]
        out[pid] = (cur, _weighted_mean(prior_rows), n_cur,
                    latest[pid][2] if pid in latest else None)
    return out


# ── Who's moving ────────────────────────────────────────────

def movers(con, today: date, days: int = 10) -> list[dict]:
    """Remarks in the last `days` whose stance is far from the speaker's own
    average over the prior WINDOW days. Leadership moves at a lower bar."""
    rows = _scored(con, today - timedelta(days=days + WINDOW))
    roles = dict(con.execute("SELECT id, role FROM fomc_participants").fetchall())
    names = dict(con.execute("SELECT id, name FROM fomc_participants").fetchall())
    out = []
    for r in rows:
        if r[2] < today - timedelta(days=days) or r[7] is None:
            continue
        base_rows = [b for b in rows if b[1] == r[1] and r[2] - timedelta(days=WINDOW) <= b[2] < r[2]]
        base = _weighted_mean(base_rows)
        if base is None:
            continue
        bar = 0.5 if (roles.get(r[1]) or "") in LEAD_ROLES else 0.75
        if abs(r[7] - base) >= bar:
            out.append({"pid": r[1], "name": names.get(r[1], r[1]), "date": r[2], "score": r[7],
                        "base": base, "headline": r[11], "url": r[5], "title": r[4]})
    return sorted(out, key=lambda m: m["date"], reverse=True)


def render_movers(today: date | None = None) -> str:
    today = today or date.today()
    try:
        con = connect()
        ms = movers(con, today)
    except Exception as e:
        return f'<p class="muted">Movers unavailable: {html.escape(str(e))}</p>'
    if not ms:
        return ""
    items = []
    for m in ms:
        direction = "hawkish" if m["score"] > m["base"] else "dovish"
        items.append(
            f'<li><strong><a href="#member-{m["pid"]}">{html.escape(m["name"])}</a></strong> '
            f'turned {direction}: {_stance_chip(m["score"])} on {m["date"].strftime("%b %-d")} vs '
            f'{_fmt_score(m["base"])} average over the prior {WINDOW} days. '
            f'<a href="{html.escape(m["url"])}" target="_blank">{html.escape(m["headline"] or m["title"])}</a></li>')
    return (f'<div class="fw-card fw-movers"><strong>Who’s moving</strong> '
            f'<span class="muted">remarks in the last 10 days that break from the speaker’s own '
            f'recent stance</span><ul>{"".join(items)}</ul></div>')


# ── Spectrum ────────────────────────────────────────────────

def render_spectrum(today: date | None = None) -> str:
    today = today or date.today()
    try:
        con = connect()
        st = stances(con, today)
        people = _people(con, today)
    except Exception as e:
        return f'<p class="muted">Spectrum unavailable: {html.escape(str(e))}</p>'
    rows = [(pid, p, st[pid]) for pid, p in people.items() if pid in st and st[pid][0] is not None]
    if not rows:
        return ""
    rows.sort(key=lambda x: (-x[2][0], x[1]["name"]))
    stale_cut = today - timedelta(days=WINDOW)

    row_h, width, label_w, pad_r, pad_t = 26, 900, 230, 30, 34
    height = pad_t + row_h * len(rows) + 30
    plot_w = width - label_w - pad_r

    def x(v):
        return label_w + plot_w * (v + 2) / 4

    svg = [f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
           f'font-family="-apple-system, Helvetica, Arial, sans-serif" font-size="12">']
    for v in (-2, -1, 0, 1, 2):
        svg.append(f'<line x1="{x(v):.1f}" x2="{x(v):.1f}" y1="{pad_t - 6}" y2="{height - 26}" '
                   f'stroke="{"#bbb" if v == 0 else "#e5e5e5"}" stroke-width="1"/>')
        svg.append(f'<text x="{x(v):.1f}" y="{height - 10}" text-anchor="middle" fill="#666">'
                   f'{v:+d}</text>')
    svg.append(f'<text x="{x(-2):.1f}" y="18" fill="#2C5F8D" font-weight="600">← Dovish</text>')
    svg.append(f'<text x="{x(2):.1f}" y="18" text-anchor="end" fill="#B23A3A" '
               f'font-weight="600">Hawkish →</text>')
    for i, (pid, p, (cur, prior, n_cur, latest)) in enumerate(rows):
        cy = pad_t + row_h * i + row_h / 2
        voter = p["now"] == "member"
        stale = latest is not None and latest < stale_cut
        label = html.escape(p["name"]) + ("" if p["inst"] == "Board of Governors"
                                          else f' · {html.escape(p["inst"])}')
        svg.append(f'<a href="#member-{pid}"><text x="{label_w - 12}" y="{cy + 4:.1f}" '
                   f'text-anchor="end" fill="{"#222" if voter else "#666"}" '
                   f'font-weight="{600 if voter else 400}">{label}</text></a>')
        if prior is not None and abs(cur - prior) >= 0.05:
            x0, x1 = x(prior), x(cur)
            head = -6 if x1 > x0 else 6
            svg.append(f'<line x1="{x0:.1f}" x2="{x1 + head * 0.6:.1f}" y1="{cy:.1f}" y2="{cy:.1f}" '
                       f'stroke="#999" stroke-width="2"/>')
            svg.append(f'<path d="M{x1 + head:.1f} {cy - 4:.1f} L{x1 + head * 0.2:.1f} {cy:.1f} '
                       f'L{x1 + head:.1f} {cy + 4:.1f}" fill="none" stroke="#999" stroke-width="2"/>')
        tip = (f'{p["name"]}: {cur:+.2f} ({n_cur} remark{"s" if n_cur != 1 else ""} in last {WINDOW} days'
               + (f'; prior {WINDOW} days {prior:+.2f}' if prior is not None else '')
               + (f'; latest remark {latest:%b %-d, %Y} — stale' if stale else '') + ')')
        fill = "#2C5F8D" if voter else "#fff"
        svg.append(f'<circle cx="{x(cur):.1f}" cy="{cy:.1f}" r="6" fill="{fill}" stroke="#2C5F8D" '
                   f'stroke-width="2" opacity="{0.45 if stale else 1}"><title>{html.escape(tip)}</title></circle>')
    svg.append("</svg>")

    year = today.year
    label = {"member": "Voter", "alternate": "Alternate"}
    csv_rows = [[p["name"], p["inst"], label.get(p["now"], "Non-voter"),
                 label.get(p["next"], "Non-voter"), _n(cur), _n(prior),
                 _n(cur - prior) if prior is not None else "", n_cur,
                 latest.isoformat() if latest else ""]
                for pid, p, (cur, prior, n_cur, latest) in rows]
    legend = (
        '<div class="fed-legend">'
        f'<span><span class="dot solid" style="background:#2C5F8D"></span>{year} voter</span>'
        '<span><span class="dot hollow" style="border-color:#2C5F8D"></span>Non-voter</span>'
        f'<span><span class="line" style="background:#999"></span>Move from prior {WINDOW} days</span>'
        '<span>Faded = no remarks in the window (latest shown)</span></div>')
    return (
        f'<h3 class="fw-h3">Hawk–dove spectrum</h3>'
        f'<p class="muted fw-note">Each participant’s average stance in their remarks over the last '
        f'{WINDOW} days, read by Claude from the speech text (−2 strongly dovish … +2 strongly '
        f'hawkish; remarks with only passing policy content count half). These are reads of '
        f'what was said, not official positions. Click a name for the profile.</p>'
        f'<div class="fed-chart">{"".join(svg)}</div>{legend}'
        + _exports(csv_button(
            "spectrum", f"fed_hawk_dove_spectrum_{today.isoformat()}.csv",
            ["Participant", "Seat", f"{year} status", f"{year + 1} status",
             f"Stance, last {WINDOW} days", f"Stance, prior {WINDOW} days", "Change",
             f"Remarks scored, last {WINDOW} days", "Latest scored remark"],
            csv_rows, label="CSV: spectrum (dot/arrow chart)")))


# ── Recent remarks + upcoming ───────────────────────────────

def render_remarks(today: date | None = None, days: int = 21) -> str:
    today = today or date.today()
    try:
        con = connect()
        recent = con.execute(
            "SELECT c.comm_date, p.id, p.name, p.institution, c.kind, c.title, c.url, "
            "a.hawk_dove, a.headline FROM fed_communications c "
            "JOIN fomc_participants p ON p.id = c.participant_id "
            "LEFT JOIN comm_analysis a ON a.comm_id = c.id WHERE c.comm_date >= %s "
            "ORDER BY c.comm_date DESC, p.name", (today - timedelta(days=days),)).fetchall()
        upcoming = con.execute(
            "SELECT event_date, event_time, participant_id, speaker, format, topic, location "
            "FROM fed_appearances WHERE event_date >= %s ORDER BY event_date, event_time",
            (today,)).fetchall()
        all_rows = con.execute(
            "SELECT c.comm_date, p.name, p.institution, c.kind, c.title, c.url, c.source, "
            "a.policy_content, a.hawk_dove, a.confidence, a.rate_view, a.rate_horizon, a.headline, "
            "a.summary, a.framework_tags, a.topics FROM fed_communications c "
            "JOIN fomc_participants p ON p.id = c.participant_id "
            "LEFT JOIN comm_analysis a ON a.comm_id = c.id ORDER BY c.comm_date, p.name").fetchall()
        quotes = con.execute(
            "SELECT c.comm_date, p.name, q->>'quote', q->>'signal', q->>'why', "
            "COALESCE((q->>'verified')::boolean, false), c.title, c.url FROM fed_communications c "
            "JOIN fomc_participants p ON p.id = c.participant_id "
            "JOIN comm_analysis a ON a.comm_id = c.id, jsonb_array_elements(a.key_quotes) q "
            "ORDER BY c.comm_date, p.name").fetchall()
    except Exception as e:
        return f'<p class="muted">Remarks unavailable: {html.escape(str(e))}</p>'

    rows = []
    for d, pid, name, inst, kind, title, url, hd, headline in recent:
        seat = "" if inst == "Board of Governors" else f' <span class="muted">{html.escape(inst or "")}</span>'
        rows.append(
            f'<tr><td>{d.strftime("%b %-d")}</td><td><a href="#member-{pid}">{html.escape(name)}</a>{seat}</td>'
            f'<td>{_stance_chip(hd)}</td><td class="fw-wrap"><a href="{html.escape(url)}" target="_blank">'
            f'{html.escape(title)}</a><br><span class="muted">{html.escape(headline or "")}</span></td></tr>')
    recent_html = (
        f'<div class="fw-scroll"><table class="fw-table"><thead><tr><th>Date</th><th>Speaker</th>'
        f'<th>Stance</th><th>Remarks</th></tr></thead><tbody>{"".join(rows)}</tbody></table></div>'
        if rows else f'<p class="muted">No remarks in the last {days} days.</p>')

    up = []
    for d, t, pid, speaker, fmt, topic, loc in upcoming:
        who = (f'<a href="#member-{pid}">{html.escape(speaker)}</a>' if pid else html.escape(speaker))
        up.append(f'<li>{d.strftime("%a %b %-d")}{" " + html.escape(t) if t else ""}: {who}, '
                  f'{html.escape((fmt or "").lower() or "appearance")} on '
                  f'<strong>{html.escape(topic or "unspecified")}</strong> '
                  f'<span class="muted">— {html.escape(loc or "")}</span></li>')
    upcoming_html = (f'<ul class="fw-upcoming">{"".join(up)}</ul>' if up else
                     '<p class="muted">No Board appearances posted yet (the Fed posts its '
                     'calendar about a week ahead; none during the pre-meeting blackout).</p>')

    first = all_rows[0][0].year if all_rows else today.year
    exports = _exports(
        csv_button("remarks", f"fed_remarks_scored_{first}-{today.year}.csv",
                   ["Date", "Participant", "Seat", "Type", "Title", "URL", "Source",
                    "Policy content", "Stance (-2 dovish to +2 hawkish)", "Confidence",
                    "Rate view", "Horizon", "Headline", "Summary", "Framework", "Topics"],
                   [[d.isoformat(), n, i or "", k, t, u, s, pc or "", _n(hd, 1), conf or "",
                     rv or "", rh or "", hl or "", sm or "", "; ".join(fw or []),
                     "; ".join(tp or [])]
                    for d, n, i, k, t, u, s, pc, hd, conf, rv, rh, hl, sm, fw, tp in all_rows],
                   label=f"CSV: every remark since {first} (long)"),
        csv_button("quotes", f"fed_quotes_{first}-{today.year}.csv",
                   ["Date", "Participant", "Quote", "Signal", "Why it matters",
                    "Verified verbatim", "Remarks", "URL"],
                   [[d.isoformat(), n, q, sig, why, "Yes" if v else "No", t, u]
                    for d, n, q, sig, why, v, t, u in quotes],
                   label="CSV: quote bank"))
    return (f'<h3 class="fw-h3">Fed speakers</h3>'
            f'<h4 class="fw-sub">Coming up</h4>{upcoming_html}'
            f'<h4 class="fw-sub">Last {days} days</h4>{recent_html}{exports}')


# ── Member profiles ─────────────────────────────────────────

def _trajectory(points, dissents, start: date, end: date) -> str:
    """Stance over time: dots per remark, 90-day running mean, dissent ticks."""
    width, height, pl, pr, pt, pb = 640, 150, 34, 10, 10, 22
    pw, ph = width - pl - pr, height - pt - pb
    span = max((end - start).days, 1)

    def x(d):
        return pl + pw * (d - start).days / span

    def y(v):
        return pt + ph * (1 - (v + 2) / 4)

    svg = [f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
           f'font-family="-apple-system, Helvetica, Arial, sans-serif" font-size="10">']
    for v in (-2, 0, 2):
        svg.append(f'<line x1="{pl}" x2="{width - pr}" y1="{y(v):.1f}" y2="{y(v):.1f}" '
                   f'stroke="{"#bbb" if v == 0 else "#eee"}"/>')
        svg.append(f'<text x="{pl - 4}" y="{y(v) + 3:.1f}" text-anchor="end" fill="#888">{v:+d}</text>')
    yr = start.year + 1
    while date(yr, 1, 1) <= end:
        xx = x(date(yr, 1, 1))
        svg.append(f'<line x1="{xx:.1f}" x2="{xx:.1f}" y1="{pt}" y2="{height - pb}" stroke="#f0f0f0"/>'
                   f'<text x="{xx:.1f}" y="{height - 6}" text-anchor="middle" fill="#888">{yr}</text>')
        yr += 1
    for d, sym, color, why in dissents:
        xx = x(d)
        svg.append(f'<g><title>Dissent {d:%b %-d, %Y}: {html.escape(why or "")}</title>'
                   f'<line x1="{xx:.1f}" x2="{xx:.1f}" y1="{pt}" y2="{height - pb}" stroke="{color}" '
                   f'stroke-dasharray="3 3"/><text x="{xx:.1f}" y="{pt + 9}" text-anchor="middle" '
                   f'fill="{color}" font-size="11">{sym}</text></g>')
    if len(points) > 1:
        mean_pts = []
        for d, _, _ in points:
            win = [s for dd, s, _ in points if d - timedelta(days=WINDOW) < dd <= d]
            mean_pts.append(f"{x(d):.1f},{y(sum(win) / len(win)):.1f}")
        svg.append(f'<polyline points="{" ".join(mean_pts)}" fill="none" stroke="#2C5F8D" '
                   f'stroke-width="2" opacity="0.8"/>')
    for d, s, title in points:
        svg.append(f'<circle cx="{x(d):.1f}" cy="{y(s):.1f}" r="3.5" fill="#2C5F8D" stroke="#fff" '
                   f'stroke-width="1"><title>{d:%b %-d, %Y} {s:+.1f}: {html.escape(title)}</title></circle>')
    svg.append("</svg>")
    return "".join(svg)


def render_profiles(today: date | None = None) -> str:
    today = today or date.today()
    try:
        con = connect()
        people = _people(con, today)
        st = stances(con, today)
        profiles = {r[0]: r[1:] for r in con.execute(
            "SELECT participant_id, profile_md, generated_at FROM member_profiles").fetchall()}
        facts = {r[0]: r[1:] for r in con.execute(
            "SELECT id, appointed_by, board_term_end, chair_term_end, bio_url FROM fomc_participants"
        ).fetchall()}
        scored = _scored(con)
        votes = con.execute(
            "SELECT v.participant_id, v.meeting_date, v.preferred_bps, v.dissent_text, m.decision_bps "
            "FROM fomc_votes v JOIN fomc_meetings m USING (meeting_date) WHERE v.vote = 'against'"
        ).fetchall()
    except Exception as e:
        return f'<p class="muted">Profiles unavailable: {html.escape(str(e))}</p>'

    by_pid = {}
    for r in scored:
        by_pid.setdefault(r[1], []).append(r)
    start = min((r[2] for r in scored), default=today - timedelta(days=365))
    order = {"Chairman": 0, "Chair": 0, "Vice Chair": 1, "Vice Chair for Supervision": 2}

    def key(item):
        pid, p = item
        board = p["inst"] == "Board of Governors"
        return (0 if board else (1 if p["inst"] == "New York" else 2),
                order.get(p["role"], 9) if board else 0, p["inst"], p["name"])

    blocks = []
    for pid, p in sorted(people.items(), key=key):
        rows = by_pid.get(pid, [])
        appt, term, chair_term, bio = facts.get(pid, (None,) * 4)
        cur = st.get(pid, (None,))[0]
        status = {"member": "voter", "alternate": "alternate"}
        now, nxt = status.get(p["now"], "non-voter"), status.get(p["next"], "non-voter")
        head = (f'<summary><strong>{html.escape(p["name"])}</strong> '
                f'<span class="muted">{html.escape(p["role"] or ("Governor" if p["inst"] == "Board of Governors" else "President"))}'
                f', {html.escape(p["inst"])} · {today.year} {now} · {today.year + 1} {nxt}</span> '
                f'{_stance_chip(cur) if cur is not None else ""}</summary>')
        fact_bits = [b for b in [
            f"Appointed by {html.escape(appt)}" if appt else "",
            f"Board term ends {term:%b %Y}" if term else "",
            f"Leadership term ends {chair_term:%b %Y}" if chair_term else "",
            f'<a href="{html.escape(bio)}" target="_blank">Fed bio</a>' if bio else ""] if b]
        body = [f'<p class="muted fw-note">{" · ".join(fact_bits)}</p>'] if fact_bits else []
        prof = profiles.get(pid)
        if prof:
            body.append(f'<div class="fw-profile">{_md(prof[0])}</div>'
                        f'<p class="muted fw-note">Profile written by Claude from the remarks and votes '
                        f'below, {prof[1]:%b %-d, %Y}.</p>')
        else:
            body.append('<p class="muted">Not enough scored remarks yet for a profile.</p>')
        pts = [(r[2], r[7], r[4]) for r in rows if r[7] is not None]
        ds = []
        for vpid, d, pref, why, dec in votes:
            if vpid != pid:
                continue
            if pref is None:
                ds.append((d, "◆", "#B07D1A", why))
            elif pref > (dec or 0):
                ds.append((d, "▲", "#B23A3A", why))
            else:
                ds.append((d, "▼", "#2C5F8D", why))
        if pts or ds:
            body.append(f'<h4 class="fw-prof-h">Stance in remarks over time</h4>'
                        f'<div class="fed-chart fw-traj">{_trajectory(pts, [x for x in ds if x[0] >= start], start, today)}</div>'
                        f'<p class="muted fw-note">Dots: each scored remark. Line: {WINDOW}-day average. '
                        f'Dashed: FOMC dissents (▲ for tighter, ▼ for easier, ◆ other).</p>')
        quotes = []
        for r in reversed(rows):
            for q in (r[15] or []):
                if q.get("verified") and len(quotes) < 4:
                    quotes.append(f'<blockquote class="fw-quote">“{html.escape(q["quote"])}” '
                                  f'<span class="muted">— {r[2]:%b %-d, %Y}, '
                                  f'<a href="{html.escape(r[5])}" target="_blank">{html.escape(r[4])}</a></span>'
                                  f'</blockquote>')
        if quotes:
            body.append('<h4 class="fw-prof-h">Recent quotes</h4>' + "".join(quotes))
        lst = [f'<li>{r[2]:%b %-d, %Y} {_stance_chip(r[7])} <a href="{html.escape(r[5])}" '
               f'target="_blank">{html.escape(r[4])}</a> <span class="muted">{html.escape(r[11] or "")}</span></li>'
               for r in reversed(rows[-12:])]
        if lst:
            body.append(f'<h4 class="fw-prof-h">Remarks ({len(rows)} analyzed)</h4>'
                        f'<ul class="fw-remarks">{"".join(lst)}</ul>')
        blocks.append(f'<details class="fw-member" id="member-{pid}">{head}{"".join(body)}</details>')

    # Opening a #member-… link expands that profile.
    js = ("<script>function fwOpenMember(){var h=location.hash;if(h.indexOf('#member-')!==0)return;"
          "var el=document.getElementById(h.slice(1));if(el){el.open=true;el.scrollIntoView();}}"
          "window.addEventListener('hashchange',fwOpenMember);"
          "document.addEventListener('click',function(e){var a=e.target.closest&&e.target.closest('a[href^=\"#member-\"]');"
          "if(a){setTimeout(fwOpenMember,0);}});</script>")
    return (f'<h3 class="fw-h3">Member profiles</h3>'
            f'<p class="muted fw-note">Profiles are regenerated weekly from each member’s scored '
            f'remarks and FOMC votes. Expand a name; links elsewhere on the tab jump here.</p>'
            f'{"".join(blocks)}{js}')


def render_people_top(today: date | None = None) -> str:
    return _DOWNLOAD_JS + render_movers(today) + render_spectrum(today)


def render_people_bottom(today: date | None = None) -> str:
    return _DOWNLOAD_JS + render_remarks(today) + render_profiles(today)
