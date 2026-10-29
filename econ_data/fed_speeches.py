"""
Fed communications: ingest, LLM stance analysis, and member profiles.

Sources
  Governors   federalreserve.gov/json/ne-speeches.json and ne-testimony.json
              (complete archive), text from each speech page.
  Presidents  No common feed exists. Each bank's XML sitemap or speeches
              listing page is scanned for speech URLs (BANK_SOURCES); a page
              counts when the president's name is in its title/opening. A
              weekly fallback uses Claude's web_search on the bank's domain for
              any president with nothing found in 45 days. When a page refuses
              a plain fetch, analysis reads it via Claude's web_fetch tool
              (full_text stays NULL).
  Upcoming    federalreserve.gov/json/calendar.json (Board appearances only).

Analysis
  prompts/fed_speech_analysis.txt + a JSON schema → comm_analysis. The prompt
  version is a hash of the prompt file, so editing the prompt is traceable.
  Quotes are checked verbatim against full_text and dropped if they don't match.
  Backfills go through the Batch API (half price); new remarks are scored live
  with server-side refusal fallbacks.

Profiles
  prompts/fed_member_profile.txt → member_profiles, regenerated weekly for
  members with newly analyzed remarks.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup

from econ_data.db import connect, connect_checked
from econ_data.fomc import BASE, UA, participant_id

MODEL = "claude-opus-5-5"
PROMPTS = Path(__file__).resolve().parent.parent / "prompts"
FALLBACK_BETA = "server-side-fallback-2026-07-01"

BANK_DOMAINS = {
    "Boston": "bostonfed.org", "New York": "newyorkfed.org",
    "Philadelphia": "philadelphiafed.org", "Cleveland": "clevelandfed.org",
    "Richmond": "richmondfed.org", "Atlanta": "atlantafed.org",
    "Chicago": "chicagofed.org", "St. Louis": "stlouisfed.org",
    "Minneapolis": "minneapolisfed.org", "Kansas City": "kansascityfed.org",
    "Dallas": "dallasfed.org", "San Francisco": "frbsf.org",
}

TOPICS = ["inflation", "labor market", "growth and outlook", "rate path", "balance sheet",
          "neutral rate", "inflation expectations", "tariffs and trade", "AI and productivity",
          "financial stability", "housing", "fiscal policy", "Fed independence",
          "bank supervision and regulation", "payments and digital assets", "international"]
FRAMEWORK_TAGS = ["data-dependent", "forecast-driven", "risk-management",
                  "inflation-expectations-focus", "labor-mandate-emphasis", "rstar-matters",
                  "rules-based", "balance-sheet-focus", "supply-side-view",
                  "financial-conditions-focus", "independence-emphasis"]

ANALYSIS_SCHEMA = {
    "type": "object",
    "properties": {
        "policy_content": {"type": "string", "enum": ["none", "light", "substantial"]},
        "hawk_dove": {"anyOf": [{"type": "number"}, {"type": "null"}]},
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        "rate_view": {"type": "string", "enum": ["cut", "hold", "hike", "unclear"]},
        "rate_horizon": {"type": "string",
                         "enum": ["next meeting", "coming months", "longer term", "unspecified"]},
        "headline": {"type": "string"},
        "summary": {"type": "string"},
        "topics": {"type": "array", "items": {"type": "string", "enum": TOPICS}},
        "framework_tags": {"type": "array", "items": {"type": "string", "enum": FRAMEWORK_TAGS}},
        "key_quotes": {"type": "array", "items": {
            "type": "object",
            "properties": {"quote": {"type": "string"},
                           "signal": {"type": "string", "enum": ["hawkish", "dovish", "neutral"]},
                           "why": {"type": "string"}},
            "required": ["quote", "signal", "why"], "additionalProperties": False}},
    },
    "required": ["policy_content", "hawk_dove", "confidence", "rate_view", "rate_horizon",
                 "headline", "summary", "topics", "framework_tags", "key_quotes"],
    "additionalProperties": False,
}

_TITLE = re.compile(r"^(Chairman|Chair|Vice Chair for Supervision|Vice Chair|Governor|President)\s+")


def comm_id(url: str) -> str:
    """Stable id; also valid as a Batch API custom_id."""
    return "c" + hashlib.sha1(url.encode()).hexdigest()[:16]


def _prompt(name: str) -> tuple[str, str]:
    text = (PROMPTS / name).read_text()
    return text, hashlib.sha1(text.encode()).hexdigest()[:8]


def _client() -> anthropic.Anthropic:
    return anthropic.Anthropic(timeout=300.0)


# ── Text extraction ─────────────────────────────────────────

def extract_text(html: str) -> str:
    """Main body text of a speech page: paragraphs from <article>/<main>/#article."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer", "aside", "form", "noscript"]):
        tag.decompose()
    root = (soup.find(id="article") or soup.find("article") or soup.find("main")
            or soup.body or soup)
    paras = []
    for p in root.find_all(["p", "li", "h2", "h3", "blockquote"]):
        t = re.sub(r"\s+", " ", p.get_text(" ", strip=True)).strip()
        if len(t) > 1 and not t.startswith(("Share", "Return to text", "Back to Top")):
            paras.append(t)
    return "\n\n".join(paras)


def fetch_page_text(url: str) -> str | None:
    """Plain, honestly identified fetch. None when refused or too thin to be
    the speech itself (the analysis call then uses web_fetch)."""
    try:
        r = requests.get(url, headers=UA, timeout=30)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    if "pdf" in r.headers.get("content-type", "") or url.lower().endswith(".pdf"):
        from pypdf import PdfReader
        try:
            text = "\n\n".join(p.extract_text() or "" for p in PdfReader(io.BytesIO(r.content)).pages)
        except Exception:
            return None
    else:
        r.encoding = r.encoding if r.encoding and r.encoding.lower() != "iso-8859-1" else "utf-8"
        text = extract_text(r.text)
    return text if len(text) >= 1500 else None


# ── Ingest: Board ───────────────────────────────────────────

def _board_items(kind: str) -> list[dict]:
    path = {"speech": "/json/ne-speeches.json", "testimony": "/json/ne-testimony.json"}[kind]
    r = requests.get(BASE + path, headers=UA, timeout=60)
    r.raise_for_status()
    out = []
    for x in json.loads(r.content.decode("utf-8-sig")):
        if "d" not in x or not x.get("l"):
            continue
        speaker = _TITLE.sub("", x.get("s", "").strip())
        out.append({"date": datetime.strptime(x["d"].split(" ")[0], "%m/%d/%Y").date(),
                    "title": x.get("t", "").strip(), "speaker": speaker,
                    "venue": (x.get("lo") or "").strip() or None,
                    "url": BASE + x["l"], "kind": kind})
    return out


def _insert_comm(con, pid: str, d: date, kind: str, title: str, venue, url: str,
                 source: str, text: str | None) -> bool:
    cur = con.execute(
        "INSERT INTO fed_communications (id, participant_id, comm_date, kind, title, venue, url, "
        "source, full_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (url) DO NOTHING",
        (comm_id(url), pid, d, kind, title, venue, url, source, text))
    return cur.rowcount > 0


def ingest_board(since: date, log=print) -> int:
    """New governor speeches/testimony since `since` (fetches each page once)."""
    con = connect()
    known = {r[0] for r in con.execute("SELECT url FROM fed_communications "
                                       "WHERE source LIKE 'frb_%%'").fetchall()}
    n = 0
    for kind in ("speech", "testimony"):
        for it in _board_items(kind):
            if it["date"] < since or it["url"] in known or not it["speaker"]:
                continue
            # Joint statements ("Chair Powell and Vice Chair …") have no single speaker.
            if " and " in it["speaker"]:
                continue
            pid = participant_id(it["speaker"])
            try:
                html = requests.get(it["url"], headers=UA, timeout=30)
                html.encoding = "utf-8"
                text = extract_text(html.text) if html.status_code == 200 else None
            except requests.RequestException:
                text = None
            connect_checked().execute(
                "INSERT INTO fomc_participants (id, name, institution) "
                "VALUES (%s, %s, 'Board of Governors') ON CONFLICT (id) DO NOTHING",
                (pid, it["speaker"]))
            if _insert_comm(connect_checked(), pid, it["date"], kind, it["title"], it["venue"], it["url"],
                            f"frb_{'speeches' if kind == 'speech' else 'testimony'}", text):
                n += 1
            time.sleep(0.2)
    log(f"  Board: {n} new speeches/testimony")
    return n


# ── Ingest: presidents (bank sitemaps / listing pages) ──────
#
# Each bank: (kind, url, link pattern). "sitemap" walks the XML sitemap (and
# its index); "listing" scans an HTML page's links. Patterns may capture y/m/d
# (or an 8-digit `ymd`, or a 6-digit `yymmdd`); otherwise the date is read from
# the page. A page only counts if the president's surname appears in its title
# or opening, which filters out other officials' remarks on shared paths.
BANK_SOURCES = {
    "Boston": ("sitemap", "https://www.bostonfed.org/sitemap.xml",
               r"/news-and-events/speeches/(?P<y>\d{4})/[^/?#]+\.aspx$"),
    "New York": ("listing", "https://www.newyorkfed.org/newsevents/speeches",
                 r"/newsevents/speeches/(?P<y>\d{4})/[a-z]+(?P<yymmdd>\d{6})[a-z]?$"),
    "Philadelphia": ("sitemap", "https://www.philadelphiafed.org/sitemap.xml",
                     r"/the-economy/[\w-]+/(?P<yymmdd>\d{6})-[\w-]+$"),
    "Cleveland": ("sitemap", "https://www.clevelandfed.org/sitemap.xml",
                  r"/collections/speeches/\d{4}/sp-(?P<ymd>\d{8})[\w-]*$"),
    "Richmond": ("sitemap", "https://www.richmondfed.org/sitemap.xml",
                 r"/press_room/speeches/[a-z_]+/(?P<y>\d{4})/[a-z_]+_(?P<ymd>\d{8})$"),
    "Atlanta": ("sitemap", "https://www.atlantafed.org/~/media/Project/Atlanta/FRBA/Documents/sitemap",
                r"/news-and-events/speeches/(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/[\w-]+$"),
    "Chicago": ("listing", "https://www.chicagofed.org/utilities/about-us/office-of-the-president/"
                           "office-of-the-president-speaking",
                r"/publications/speeches/(?P<y>\d{4})/(?P<mon>[a-z]{3,9})-(?P<d>\d{1,2})-[\w-]+$"),
    "St. Louis": ("sitemap", "https://www.stlouisfed.org/sitemap.xml",
                  r"/from-the-president/(?:remarks|speeches|commentary)/(?P<y>\d{4})/[\w-]+$"),
    "Minneapolis": ("sitemap", "https://www.minneapolisfed.org/sitemap.xml",
                    r"/speeches/(?P<y>\d{4})/[\w-]+$"),
    # KC posts a landing page plus a "-speech" URL serving the full-text PDF;
    # only the PDF is taken, so each speech is counted once with its full text.
    "Kansas City": ("sitemap", "https://www.kansascityfed.org/sitemap.xml",
                    r"/speeches/[\w-]*-speech(?:-[a-z]+-\d{4})?/?$"),
    "Dallas": ("listing", "https://www.dallasfed.org/news/speeches/logan",
               r"/news/speeches/logan/(?P<y>\d{4})/[a-z]+(?P<yymmdd>\d{6})$"),
    "San Francisco": ("sitemap", "https://www.frbsf.org/sitemap_index.xml",
                      r"/news-and-media/speeches/[a-z-]+/(?P<y>\d{4})/(?P<m>\d{2})/[\w-]+/?$"),
}

_DATE_TEXT = re.compile(r"(January|February|March|April|May|June|July|August|September|"
                        r"October|November|December)\s+(\d{1,2}),\s+(\d{4})")


def _get_text(url: str) -> str | None:
    try:
        r = requests.get(url, headers=UA, timeout=30)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    return r.content.decode("utf-8", "ignore")


def _pdf_text(data: bytes) -> str:
    from pypdf import PdfReader
    pages = PdfReader(io.BytesIO(data)).pages
    return re.sub(r"[ \t]+", " ", "\n\n".join(p.extract_text() or "" for p in pages)).strip()


def fetch_remarks(url: str) -> tuple[str, str, date | None] | None:
    """(title, text, date-from-page) for an HTML or PDF speech URL."""
    try:
        r = requests.get(url, headers=UA, timeout=30)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    if r.content[:5] == b"%PDF-" or "pdf" in r.headers.get("content-type", ""):
        try:
            text = _pdf_text(r.content)
        except Exception:
            return None
        m = _DATE_TEXT.search(text[:3000])
        d = datetime.strptime(" ".join(m.groups()), "%B %d %Y").date() if m else None
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        title = re.sub(r"-speech(-[a-z]+-\d{4})?$", "", slug).replace("-", " ").capitalize()
        return title, text, d
    html = r.content.decode("utf-8", "ignore")
    text = extract_text(html)
    title, d = _page_meta(html, text)
    return title, text, d


def _sitemap_locs(url: str, depth: int = 0) -> list[tuple[str, date | None]]:
    """(url, lastmod) pairs, following sitemap indexes."""
    x = _get_text(url)
    if not x or "<loc>" not in x:
        return []
    if "<sitemapindex" in x and depth < 2:
        out = []
        for child in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", x):
            out += _sitemap_locs(child, depth + 1)
        return out
    out = []
    for block in re.findall(r"<url>(.*?)</url>", x, re.S):
        loc = re.search(r"<loc>\s*([^<\s]+)\s*</loc>", block)
        mod = re.search(r"<lastmod>\s*(\d{4}-\d{2}-\d{2})", block)
        if loc:
            out.append((loc.group(1), date.fromisoformat(mod.group(1)) if mod else None))
    return out


def _listing_links(url: str) -> list[tuple[str, None]]:
    from urllib.parse import urljoin
    x = _get_text(url)
    if not x:
        return []
    return [(urljoin(url, h), None) for h in re.findall(r'href="([^"#?]+)"', x)]


def _url_date(m: re.Match) -> date | None:
    g = m.groupdict()
    try:
        if g.get("ymd"):
            return datetime.strptime(g["ymd"], "%Y%m%d").date()
        if g.get("yymmdd"):
            return datetime.strptime(g["yymmdd"], "%y%m%d").date()
        if g.get("mon") and g.get("d"):
            mon = g["mon"][:3].title()
            return datetime.strptime(f"{g['y']} {mon} {g['d']}", "%Y %b %d").date()
        if g.get("d"):
            return date(int(g["y"]), int(g["m"]), int(g["d"]))
    except ValueError:
        return None
    return None


def bank_candidates(inst: str, since: date) -> list[tuple[str, date | None]]:
    """Speech-page URLs for a bank that may fall on/after `since`. Exact dates
    come from the URL where it carries one; otherwise only the year filters."""
    kind, src, pattern = BANK_SOURCES[inst]
    links = _sitemap_locs(src) if kind == "sitemap" else _listing_links(src)
    out, seen = [], set()
    for link, lastmod in links:
        link = link.strip().replace("http://", "https://")
        m = re.search(pattern, link)
        if not m or link in seen:
            continue
        seen.add(link)
        d = _url_date(m)
        if d and d < since:
            continue
        y = m.groupdict().get("y")
        if not d and y and int(y) < since.year:
            continue
        # A page not modified since the cutoff was published before it.
        if not d and lastmod and lastmod < since:
            continue
        out.append((link, d))
    return out


def _page_meta(html: str, text: str) -> tuple[str, date | None]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = (h1.get_text(" ", strip=True) if h1 else
             (soup.title.get_text(" ", strip=True) if soup.title else ""))
    m = _DATE_TEXT.search(text[:3000]) or _DATE_TEXT.search(
        re.sub(r"\s+", " ", soup.get_text(" "))[:6000])
    d = datetime.strptime(" ".join(m.groups()), "%B %d %Y").date() if m else None
    return re.sub(r"\s+", " ", title).strip()[:300], d


def speaker_match(surname: str, url: str, title: str, text: str) -> bool:
    """The page is this president's: surname in the URL (Dallas /logan/,
    Richmond /thomas_i_barkin/), in the title/opening, or New York's id prefix
    (wil260112 = Williams)."""
    s = surname.lower()
    return (s in url.lower() or s in (title + " " + text[:800]).lower()
            or bool(re.search(rf"/{re.escape(s[:3])}\d{{6}}[a-z]?$", url.lower())))


def _presidents(con) -> list[tuple]:
    return con.execute(
        "SELECT id, name, institution FROM fomc_participants WHERE active "
        "AND institution = ANY(%s) AND COALESCE(role, '') <> 'First Vice President' "
        "ORDER BY institution", (list(BANK_DOMAINS),)).fetchall()


def _discovery_request(name: str, inst: str, since: date, until: date) -> dict:
    return dict(
        model=MODEL, max_tokens=16000,
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": {
            "type": "object",
            "properties": {"items": {"type": "array", "items": {
                "type": "object",
                "properties": {"date": {"type": "string"}, "title": {"type": "string"},
                               "url": {"type": "string"},
                               "kind": {"type": "string",
                                        "enum": ["speech", "testimony", "essay", "remarks"]},
                               "venue": {"type": "string"}},
                "required": ["date", "title", "url", "kind", "venue"],
                "additionalProperties": False}}},
            "required": ["items"], "additionalProperties": False}}},
        tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": 4,
                "allowed_domains": [BANK_DOMAINS[inst]]}],
        messages=[{"role": "user", "content": (
            f"List the speeches, prepared remarks, essays, and congressional testimony by "
            f"{name}, President of the Federal Reserve Bank of {inst}, published on "
            f"{BANK_DOMAINS[inst]} between {since.isoformat()} and {until.isoformat()}. "
            f"Use the page for the remarks themselves (the full text, or its PDF), not a "
            f"news release about them or an index page. Skip podcasts, videos, and media "
            f"interviews. date is YYYY-MM-DD; venue is the event or audience ('' if unknown). "
            f"Return an empty list if there are none in that window.")}],
    )


def _final_text(msg) -> str:
    texts = [b.text for b in msg.content if getattr(b, "type", "") == "text"]
    return texts[-1] if texts else ""


def _run_with_tools(client, params: dict, max_pauses: int = 3):
    """messages.create with server tools, resuming pause_turn."""
    msgs = list(params["messages"])
    for _ in range(max_pauses + 1):
        msg = client.beta.messages.create(**{**params, "messages": msgs},
                                          betas=[FALLBACK_BETA],
                                          extra_body={"fallbacks": "default"})
        if msg.stop_reason != "pause_turn":
            return msg
        msgs = list(params["messages"]) + [{"role": "assistant", "content": msg.content}]
    return msg


def ingest_presidents(since: date, log=print) -> int:
    """Presidents' remarks since `since` from each bank's sitemap / listing page.
    Free and deterministic; the primary discovery path."""
    con = connect()
    known = {r[0] for r in con.execute("SELECT url FROM fed_communications").fetchall()}
    n_new = 0
    for pid, name, inst in _presidents(con):
        surname = name.split()[-1]
        try:
            cands = [(u, d) for u, d in bank_candidates(inst, since) if u not in known]
        except Exception as e:
            log(f"  {inst}: discovery FAILED: {e}")
            continue
        for url, url_d in cands:
            got = fetch_remarks(url)
            time.sleep(0.3)
            if not got:
                continue
            title, text, page_d = got
            d = url_d or page_d
            # Must be this president's remarks, dated inside the window, and
            # long enough to be the remarks rather than an event stub.
            if (not d or not since <= d <= date.today()
                    or not speaker_match(surname, url, title, text) or len(text) < 800):
                continue
            kind = "testimony" if "testimony" in title.lower() else "speech"
            if _insert_comm(connect_checked(), pid, d, kind, title or url.rsplit("/", 1)[-1],
                            None, url, "bank_site", text):
                n_new += 1
                log(f"  + {name} {d} {title[:70]}")
    log(f"  Presidents: {n_new} new remarks from bank sites")
    return n_new


def search_president_remarks(since: date, until: date | None = None, only: set | None = None,
                             log=print) -> int:
    """Fallback discovery with Claude web_search on each bank's domain — for a
    bank whose sitemap/listing pattern has stopped matching. Costs API calls."""
    until = until or date.today()
    con, client = connect(), _client()
    n_new, usage_in, usage_out = 0, 0, 0
    for pid, name, inst in _presidents(con):
        if only is not None and pid not in only:
            continue
        try:
            msg = _run_with_tools(client, _discovery_request(name, inst, since, until))
            usage_in += msg.usage.input_tokens
            usage_out += msg.usage.output_tokens
            if msg.stop_reason == "refusal":
                log(f"  {name}: discovery declined")
                continue
            items = json.loads(_final_text(msg) or '{"items": []}')["items"]
        except Exception as e:
            log(f"  {name}: discovery FAILED: {e}")
            continue
        domain = BANK_DOMAINS[inst]
        for it in items:
            url = it["url"].strip()
            try:
                d = date.fromisoformat(it["date"][:10])
            except ValueError:
                continue
            # Only the bank's own pages, inside the window.
            if domain not in url or not (since <= d <= until):
                continue
            if connect_checked().execute("SELECT 1 FROM fed_communications WHERE url = %s",
                                         (url,)).fetchone():
                continue
            text = fetch_page_text(url)
            if _insert_comm(connect_checked(), pid, d, it["kind"], it["title"].strip(),
                            it["venue"] or None, url, "web_search", text):
                n_new += 1
                log(f"  + {name} {d} {it['title'][:70]}{'' if text else '  [web_fetch]'}")
    log(f"  Presidents: {n_new} new remarks (discovery tokens in={usage_in:,} out={usage_out:,})")
    return n_new


# ── Upcoming appearances ────────────────────────────────────

def refresh_appearances(log=print) -> int:
    r = requests.get(BASE + "/json/calendar.json", headers=UA, timeout=60)
    r.raise_for_status()
    events = json.loads(r.content.decode("utf-8-sig"))["events"]
    today = date.today()
    rows = []
    for e in events:
        if e.get("type") not in ("Speeches", "Testimony") or not e.get("month"):
            continue
        try:
            d = date.fromisoformat(f"{e['month']}-{int(str(e.get('days', '1')).split('-')[0]):02d}")
        except ValueError:
            continue
        if d < today - timedelta(days=1):
            continue
        fmt, _, who = (e.get("title") or "").partition(" - ")
        speaker = _TITLE.sub("", who.strip())
        rows.append((d, e.get("time"), participant_id(speaker) if speaker else None,
                     speaker or fmt, fmt.strip() if who else None,
                     BeautifulSoup(e.get("description") or "", "html.parser").get_text().strip(),
                     BeautifulSoup(e.get("location") or "", "html.parser").get_text().strip()))
    con = connect()
    with con.transaction():
        con.execute("DELETE FROM fed_appearances WHERE event_date >= %s", (today - timedelta(days=1),))
        con.cursor().executemany(
            "INSERT INTO fed_appearances VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT DO NOTHING", rows)
    log(f"  Upcoming Board appearances: {len(rows)}")
    return len(rows)


# ── Analysis ────────────────────────────────────────────────

def _policy_context(con, d: date) -> str:
    last = con.execute(
        "SELECT meeting_date, target_low, target_high, decision_bps FROM fomc_meetings "
        "WHERE meeting_date <= %s AND target_high IS NOT NULL ORDER BY meeting_date DESC LIMIT 1",
        (d,)).fetchone()
    nxt = con.execute("SELECT meeting_date FROM fomc_meetings WHERE meeting_date > %s "
                      "AND NOT unscheduled ORDER BY meeting_date LIMIT 1", (d,)).fetchone()
    parts = []
    if last:
        act = ("held" if last[3] == 0 else
               f"{'raised' if last[3] > 0 else 'cut'} by {abs(last[3])}bp") if last[3] is not None else "set"
        parts.append(f"Target range {last[1]:g}–{last[2]:g}% (FOMC {act} it on "
                     f"{last[0].isoformat()}).")
    if nxt:
        parts.append(f"Next FOMC decision: {nxt[0].isoformat()}.")
    return " ".join(parts) or "Policy setting unavailable."


def _analysis_params(con, row) -> dict:
    cid, pid, d, kind, title, venue, url, text, name, inst, role = row
    system, _ = _prompt("fed_speech_analysis.txt")
    seat = inst if inst == "Board of Governors" else f"Federal Reserve Bank of {inst}"
    header = (f"Speaker: {name} ({role or ('Governor' if inst == 'Board of Governors' else 'President')}, "
              f"{seat})\nDate: {d.isoformat()}\nType: {kind}\nTitle: {title}\n"
              f"Venue: {venue or 'n/a'}\nURL: {url}\n"
              f"Policy setting at the time: {_policy_context(con, d)}\n")
    params = dict(
        model=MODEL, max_tokens=16000,
        system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        output_config={"effort": "medium",
                       "format": {"type": "json_schema", "schema": ANALYSIS_SCHEMA}},
    )
    if text:
        params["messages"] = [{"role": "user", "content": f"{header}\n--- Remarks ---\n\n{text}"}]
    else:
        params["tools"] = [{"type": "web_fetch_20260209", "name": "web_fetch", "max_uses": 2}]
        params["messages"] = [{"role": "user", "content": (
            f"{header}\nThe text could not be retrieved directly. Fetch the URL above with "
            f"web_fetch and analyze the remarks found there. If the page is not the remarks "
            f"themselves, set policy_content to none.")}]
    return params


_PENDING_SQL = (
    "SELECT c.id, c.participant_id, c.comm_date, c.kind, c.title, c.venue, c.url, c.full_text, "
    "p.name, p.institution, p.role FROM fed_communications c "
    "JOIN fomc_participants p ON p.id = c.participant_id "
    "LEFT JOIN comm_analysis a ON a.comm_id = c.id WHERE a.comm_id IS NULL")


def _norm(s: str) -> str:
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = s.replace("—", "-").replace("–", "-").replace(" ", " ")
    return re.sub(r"\s+", " ", s).strip().lower()


def verify_quotes(quotes: list[dict], text: str | None) -> list[dict]:
    """Keep only quotes that appear verbatim (modulo whitespace/quote marks).
    Without source text (web_fetch case) quotes are kept but flagged."""
    if not text:
        return [{**q, "verified": False} for q in quotes]
    body = _norm(text)
    return [{**q, "verified": True} for q in quotes
            if q.get("quote") and _norm(q["quote"]).rstrip(".") in body]


def store_analysis(con, cid: str, data: dict, text: str | None, model: str) -> None:
    _, version = _prompt("fed_speech_analysis.txt")
    hd = data.get("hawk_dove")
    if data.get("policy_content") == "none" or hd is None:
        hd = None
    else:
        hd = max(-2.0, min(2.0, round(float(hd) * 2) / 2))
    quotes = verify_quotes(data.get("key_quotes") or [], text)
    con.execute(
        "INSERT INTO comm_analysis (comm_id, policy_content, hawk_dove, confidence, rate_view, "
        "rate_horizon, headline, summary, topics, framework_tags, key_quotes, model, "
        "prompt_version) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (comm_id) DO UPDATE SET policy_content = EXCLUDED.policy_content, "
        "hawk_dove = EXCLUDED.hawk_dove, confidence = EXCLUDED.confidence, "
        "rate_view = EXCLUDED.rate_view, rate_horizon = EXCLUDED.rate_horizon, "
        "headline = EXCLUDED.headline, summary = EXCLUDED.summary, topics = EXCLUDED.topics, "
        "framework_tags = EXCLUDED.framework_tags, key_quotes = EXCLUDED.key_quotes, "
        "model = EXCLUDED.model, prompt_version = EXCLUDED.prompt_version, analyzed_at = NOW()",
        (cid, data["policy_content"], hd, data.get("confidence"), data.get("rate_view"),
         data.get("rate_horizon"), data.get("headline"), data.get("summary"),
         json.dumps(data.get("topics") or []), json.dumps(data.get("framework_tags") or []),
         json.dumps(quotes), model, version))


def analyze_pending(limit: int = 25, log=print) -> int:
    """Score unanalyzed remarks one by one (live). For new remarks each run."""
    con, client = connect(), _client()
    rows = con.execute(_PENDING_SQL + " ORDER BY c.comm_date DESC LIMIT %s", (limit,)).fetchall()
    n = 0
    for row in rows:
        try:
            params = _analysis_params(connect_checked(), row)
            msg = (_run_with_tools(client, params) if "tools" in params else
                   client.beta.messages.create(**params, betas=[FALLBACK_BETA],
                                               extra_body={"fallbacks": "default"}))
            if msg.stop_reason == "refusal":
                log(f"  {row[8]} {row[2]}: analysis declined")
                continue
            data = json.loads(_final_text(msg))
            store_analysis(connect_checked(), row[0], data, row[7], msg.model)
            n += 1
            hd = data.get("hawk_dove")
            score = "" if hd is None else f" {hd:+.1f}"
            log(f"  scored {row[8]} {row[2]}: {data['policy_content']}{score}"
                f" — {data.get('headline', '')[:80]}")
        except Exception as e:
            log(f"  {row[8]} {row[2]}: analysis FAILED: {e}")
    return n


def submit_batch(log=print) -> str | None:
    """Batch-score every pending remark that has full text (half price)."""
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request
    con = connect()
    rows = con.execute(_PENDING_SQL + " AND c.full_text IS NOT NULL ORDER BY c.comm_date").fetchall()
    if not rows:
        return None
    reqs = [Request(custom_id=row[0],
                    params=MessageCreateParamsNonStreaming(**_analysis_params(con, row)))
            for row in rows]
    batch = _client().messages.batches.create(requests=reqs)
    log(f"  Submitted batch {batch.id} with {len(reqs)} remarks")
    return batch.id


def collect_batch(batch_id: str, poll_seconds: int = 60, log=print) -> int:
    client, con = _client(), connect()
    while True:
        b = client.messages.batches.retrieve(batch_id)
        if b.processing_status == "ended":
            break
        c = b.request_counts
        log(f"  batch {batch_id}: {c.succeeded} done, {c.processing} processing")
        time.sleep(poll_seconds)
    con = connect_checked()
    texts = dict(con.execute("SELECT id, full_text FROM fed_communications").fetchall())
    n = bad = 0
    for res in client.messages.batches.results(batch_id):
        if res.result.type != "succeeded":
            bad += 1
            continue
        msg = res.result.message
        if msg.stop_reason == "refusal":
            bad += 1
            continue
        try:
            store_analysis(con, res.custom_id, json.loads(_final_text(msg)),
                           texts.get(res.custom_id), msg.model)
            n += 1
        except Exception as e:
            log(f"  {res.custom_id}: could not store: {e}")
            bad += 1
    log(f"  batch {batch_id}: stored {n}, failed/declined {bad}")
    return n


# ── Member profiles ─────────────────────────────────────────

def _profile_input(con, pid: str) -> tuple[str, int, date | None]:
    p = con.execute("SELECT name, institution, role, appointed_by, board_term_end, "
                    "chair_term_end, bio_text FROM fomc_participants WHERE id = %s",
                    (pid,)).fetchone()
    name, inst, role, appt, term, chair_term, bio = p
    votes = con.execute(
        "SELECT v.meeting_date, v.vote, v.preferred_bps, v.dissent_text, m.decision_bps "
        "FROM fomc_votes v JOIN fomc_meetings m USING (meeting_date) "
        "WHERE v.participant_id = %s ORDER BY 1", (pid,)).fetchall()
    rows = con.execute(
        "SELECT c.comm_date, c.kind, c.title, a.hawk_dove, a.confidence, a.rate_view, "
        "a.headline, a.summary, a.framework_tags, a.key_quotes FROM fed_communications c "
        "JOIN comm_analysis a ON a.comm_id = c.id WHERE c.participant_id = %s "
        "AND a.policy_content <> 'none' ORDER BY c.comm_date DESC LIMIT 20", (pid,)).fetchall()
    lines = [f"# {name}", f"Seat: {inst}; title: {role or 'n/a'}; appointed by: {appt or 'n/a'}; "
             f"Board term ends: {term or 'n/a'}; leadership term ends: {chair_term or 'n/a'}"]
    if bio:
        lines += ["", "## Bio (Federal Reserve)", bio[:2500]]
    lines += ["", "## FOMC votes"]
    if votes:
        # Every meeting they voted at, with the committee's decision — the
        # profile needs the policy path, not just the dissents.
        for d, vote, pref, why, dec in votes:
            decision = "hold" if dec == 0 else f"{dec:+d}bp"
            if vote == "against":
                lines.append(f"- {d}: committee {decision}; DISSENTED — {why}")
            else:
                lines.append(f"- {d}: committee {decision}; voted with the majority")
    else:
        lines.append("Not a voter in the period covered.")
    lines += ["", "## Recent remarks (newest first)"]
    for d, kind, title, hd, conf, rv, head, summ, tags, quotes in rows:
        score = "n/a" if hd is None else f"{hd:+.1f}"
        lines.append(f"### {d} — {title} ({kind}); stance {score} ({conf}); rate view: {rv}")
        lines.append(f"{head}. {summ}")
        if tags:
            lines.append(f"Framework: {', '.join(tags)}")
        for q in (quotes or [])[:2]:
            lines.append(f'> "{q["quote"]}" ({q["signal"]})')
    return "\n".join(lines), len(rows), (rows[0][0] if rows else None)


def refresh_profiles(force: bool = False, log=print) -> int:
    con, client = connect(), _client()
    system, version = _prompt("fed_member_profile.txt")
    cands = con.execute(
        "SELECT p.id, MAX(c.comm_date) FROM fomc_participants p "
        "JOIN fed_communications c ON c.participant_id = p.id "
        "JOIN comm_analysis a ON a.comm_id = c.id AND a.policy_content <> 'none' "
        "WHERE p.active GROUP BY p.id HAVING COUNT(*) >= 2").fetchall()
    existing = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT participant_id, latest_comm, prompt_version FROM member_profiles").fetchall()}
    n = 0
    for pid, latest in cands:
        prev = existing.get(pid)
        if not force and prev and prev[0] == latest and prev[1] == version:
            continue
        text, n_src, latest_comm = _profile_input(connect_checked(), pid)
        try:
            msg = client.beta.messages.create(
                model=MODEL, max_tokens=16000, system=system,
                output_config={"effort": "high"},
                messages=[{"role": "user", "content": text}],
                betas=[FALLBACK_BETA], extra_body={"fallbacks": "default"})
            if msg.stop_reason == "refusal":
                continue
            connect_checked().execute(
                "INSERT INTO member_profiles VALUES (%s, %s, %s, %s, %s, %s, NOW()) "
                "ON CONFLICT (participant_id) DO UPDATE SET profile_md = EXCLUDED.profile_md, "
                "n_sources = EXCLUDED.n_sources, latest_comm = EXCLUDED.latest_comm, "
                "model = EXCLUDED.model, prompt_version = EXCLUDED.prompt_version, "
                "generated_at = NOW()",
                (pid, _final_text(msg), n_src, latest_comm, msg.model, version))
            n += 1
            log(f"  profile: {pid} ({n_src} sources)")
        except Exception as e:
            log(f"  profile {pid} FAILED: {e}")
    return n


# ── Cron entry points ───────────────────────────────────────

def _ran_today(con, job: str) -> bool:
    row = con.execute("SELECT last_run FROM job_runs WHERE job = %s", (job,)).fetchone()
    return bool(row and row[0].date() >= date.today())


def _mark_run(con, job: str, detail: str = "") -> None:
    con.execute("INSERT INTO job_runs VALUES (%s, NOW(), %s) ON CONFLICT (job) "
                "DO UPDATE SET last_run = NOW(), detail = EXCLUDED.detail", (job, detail))


def refresh(log=print) -> int:
    """Every cohort: new Board remarks + upcoming appearances; bank-site
    discovery once a day. Scores whatever is new. Returns remarks scored."""
    con = connect()
    ingest_board(since=date.today() - timedelta(days=14), log=log)
    refresh_appearances(log=log)
    if not _ran_today(con, "president_discovery"):
        n = ingest_presidents(since=date.today() - timedelta(days=21), log=log)
        _mark_run(con, "president_discovery", f"{n} new")
    return analyze_pending(log=log)


def weekly(log=print) -> None:
    """Sunday cohort: web-search fallback for presidents with nothing found on
    their bank site in 45 days (their adapter may have broken), then profiles."""
    con = connect()
    quiet = {r[0] for r in con.execute(
        "SELECT p.id FROM fomc_participants p WHERE p.active AND p.institution = ANY(%s) "
        "AND COALESCE(p.role, '') <> 'First Vice President' AND NOT EXISTS ("
        "SELECT 1 FROM fed_communications c WHERE c.participant_id = p.id "
        "AND c.comm_date >= CURRENT_DATE - 45)", (list(BANK_DOMAINS),)).fetchall()}
    if quiet:
        log(f"  No bank-site remarks in 45 days for {sorted(quiet)}; trying web search")
        search_president_remarks(since=date.today() - timedelta(days=45), only=quiet, log=log)
        analyze_pending(log=log)
    refresh_profiles(log=log)
