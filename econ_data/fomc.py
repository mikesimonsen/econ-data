"""
FOMC meetings, votes, Summary of Economic Projections, and the participant
roster — scraped from federalreserve.gov.

Sources (all public, no API key):
  monetarypolicy/fomccalendars.htm   meeting dates (2021+ and next year),
                                     statement / presser / SEP / minutes links
  newsevents/pressreleases/monetary{YYYYMMDD}a.htm   the statement
  monetarypolicy/fomcprojtabl{YYYYMMDD}.htm          SEP Table 1 + dot plot
  monetarypolicy/fomc.htm            current-year voters, alternates, and the
                                     forward bank rotation
  aboutthefed/bios/board/*.htm       governor bios (term-end dates)

Vote parsing has two statement formats:
  - through April 2026: "Voting for the monetary policy action were A, Chair;
    B, Vice Chair; …" plus "Voting against … was/were X, who preferred …"
  - from June 2026: "approved … by a 10 – 2 vote" and only dissenters named.
    'for' votes are inferred from that year's roster (fomc_voters) minus the
    dissenters, checked against the stated tally, and flagged inferred=TRUE.

refresh() is cheap (two page fetches, plus one per newly published statement
or SEP) and runs in every cron cohort; refresh_roster() also pulls the Board
bios and runs weekly.
"""
from __future__ import annotations

import re
import time
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
import yaml
from bs4 import BeautifulSoup

from econ_data.db import connect

BASE = "https://www.federalreserve.gov"
UA = {"User-Agent": "Mozilla/5.0 (econ-data; Cognita Research)"}
ROSTER_SEED = Path(__file__).resolve().parent.parent / "fed_roster.yaml"

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}

BANKS = ["Boston", "New York", "Philadelphia", "Cleveland", "Richmond",
         "Atlanta", "Chicago", "St. Louis", "Minneapolis", "Kansas City",
         "Dallas", "San Francisco"]


def _get(path: str) -> str:
    url = path if path.startswith("http") else BASE + path
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    # federalreserve.gov sends no charset, so requests would fall back to
    # Latin-1 and mangle the non-breaking hyphens in "3‑1/2".
    r.encoding = "utf-8"
    return r.text


# ── Small parsers ───────────────────────────────────────────

def participant_id(name: str) -> str:
    """'Jerome H. Powell, Chair' → 'powell'; "Kathleen O'Neill Paese" → 'paese'."""
    name = name.split(",")[0].strip()
    parts = [p for p in name.split() if p.rstrip(".").lower() not in ("jr", "sr", "iii")]
    last = unicodedata.normalize("NFKD", parts[-1]).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", last.lower())


_FRAC = r"\d+[-‑‐]\d+/\d+|\d+/\d+|\d+(?:\.\d+)?"


def parse_rate(s: str) -> float:
    """'3-3/4' → 3.75, '1/4' → 0.25, '0.5' → 0.5 (handles non-breaking hyphens)."""
    s = s.strip().replace("‑", "-").replace("‐", "-")
    if "/" in s:
        whole, _, frac = s.rpartition("-") if "-" in s else ("0", "", s)
        num, den = frac.split("/")
        return int(whole) + int(num) / int(den)
    return float(s)


def blackout_window(start_date: date, meeting_date: date) -> tuple[date, date]:
    """FOMC communications blackout: from the second Saturday before the
    meeting's first day through the day after the decision (inclusive)."""
    d = start_date - timedelta(days=1)
    while d.weekday() != 5:  # Saturday
        d -= timedelta(days=1)
    return d - timedelta(days=7), meeting_date + timedelta(days=1)


# ── Calendar ────────────────────────────────────────────────

def parse_calendar(html: str) -> list[dict]:
    """Parse fomccalendars.htm into one dict per meeting."""
    soup = BeautifulSoup(html, "html.parser")
    meetings = []
    year = None
    for el in soup.find_all(["h4", "div"]):
        if el.name == "h4":
            m = re.search(r"(\d{4}) FOMC Meetings", el.get_text())
            if m:
                year = int(m.group(1))
            continue
        if "fomc-meeting" not in (el.get("class") or []) or year is None:
            continue
        month_el = el.find(class_="fomc-meeting__month")
        date_el = el.find(class_="fomc-meeting__date")
        if not month_el or not date_el:
            continue
        month_txt = month_el.get_text(" ", strip=True)
        date_txt = date_el.get_text(" ", strip=True)
        if "notation" in date_txt.lower():
            continue  # notation votes aren't meetings
        months = [MONTHS[m[:3].lower()] for m in re.findall(r"[A-Za-z]+", month_txt)
                  if m[:3].lower() in MONTHS]
        days = [int(d) for d in re.findall(r"\d+", date_txt)]
        if not months or not days:
            continue
        end_month = months[-1]
        start_month = months[0]
        start = date(year, start_month, days[0])
        end = date(year, end_month, days[-1])

        hrefs = [a["href"] for a in el.find_all("a", href=True)]

        def find(pat):
            return next((h for h in hrefs if re.search(pat, h)), None)

        statement = find(r"pressreleases/monetary\d{8}a\.htm$")
        if statement:  # the link date is authoritative (matters for unscheduled)
            end = datetime.strptime(re.search(r"(\d{8})a\.htm", statement).group(1),
                                    "%Y%m%d").date()
        minutes_release = None
        mr = re.search(r"Released (\w+ \d{1,2}, \d{4})", el.get_text(" ", strip=True))
        if mr:
            minutes_release = datetime.strptime(mr.group(1), "%B %d, %Y").date()

        meetings.append({
            "meeting_date": end,
            "start_date": start,
            "has_sep": "*" in date_txt or bool(find(r"fomcprojtabl\d{8}\.htm$")),
            "unscheduled": "unscheduled" in date_txt.lower(),
            "statement_url": statement,
            "presser_url": find(r"fomcpres+conf\d{8}\.htm$"),
            "sep_url": find(r"fomcprojtabl\d{8}\.htm$"),
            "minutes_url": find(r"fomcminutes\d{8}\.htm$"),
            "minutes_release": minutes_release,
        })
    return meetings


# ── Statement ───────────────────────────────────────────────

def statement_text(html: str) -> str:
    """Body paragraphs of a statement press release, joined by blank lines."""
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find(id="article") or soup
    paras = []
    for p in article.find_all("p"):
        t = re.sub(r"\s+", " ", p.get_text(" ", strip=True)).strip()
        if not t or t.startswith(("For release at", "For media inquiries",
                                  "Implementation Note", "Share",
                                  "Federal Reserve issues FOMC statement")):
            continue
        if re.fullmatch(r"[A-Z][a-z]+ \d{1,2}, \d{4}", t):  # dateline
            continue
        paras.append(t)
    return "\n\n".join(paras)


_PREF = re.compile(r"preferred[^.;]*?\b(raise|lower|increase|reduce)\b[^.;]*?by\s+("
                   + _FRAC + r")\s+percentage point", re.I)


def _preferred_bps(text: str) -> int | None:
    m = _PREF.search(text)
    if m:
        sign = 1 if m.group(1).lower() in ("raise", "increase") else -1
        return sign * round(parse_rate(m.group(2)) * 100)
    if re.search(r"preferred (?:no change|to (?:maintain|leave))", text, re.I):
        return 0
    return None


def _split_names(s: str) -> list[str]:
    s = re.sub(r"\band\b", ",", s)
    return [n.strip() for n in s.split(",")
            if n.strip() and n.strip() not in ("Chair", "Vice Chair", "Chairman")]


def parse_statement(text: str) -> dict:
    """Extract decision, target range, and votes from statement text."""
    flat = re.sub(r"\s+", " ", text)
    vote_idx = flat.find("Voting")
    head = flat[:vote_idx] if vote_idx > 0 else flat

    out = {"target_low": None, "target_high": None, "action_bps": None,
           "votes_for_names": None, "dissents": [], "tally": None}

    rng = re.search(r"(?:at|to) (" + _FRAC + r") to (" + _FRAC + r") percent", head)
    if rng:
        out["target_low"] = parse_rate(rng.group(1))
        out["target_high"] = parse_rate(rng.group(2))

    act = re.search(r"decided to (raise|lower|maintain|keep)\b([^.]*)", head)
    if act:
        verb = act.group(1)
        by = re.search(r"\bby (" + _FRAC + r") percentage point", act.group(2))
        if verb in ("maintain", "keep"):
            out["action_bps"] = 0
        elif by:
            out["action_bps"] = (1 if verb == "raise" else -1) * round(parse_rate(by.group(1)) * 100)

    tally = re.search(r"by an? (\d+)\s*[–—-]\s*(\d+) vote", flat)
    if tally:
        out["tally"] = (int(tally.group(1)), int(tally.group(2)))
    elif re.search(r"unanimous", flat, re.I):
        out["tally"] = None

    # Vote sentences end at the next "Voting …", an "X voted as an alternate
    # member" note, or end of text — not at the first ". ", which would cut
    # inside a middle initial ("Jerome H. Powell").
    end = r"(?=\. Voting|\. (?:(?:[A-Z][a-z'-]+|[A-Z]\.) ){1,4}voted as|\.?\s*$)"
    vf = re.search(r"Voting for (?:the|this) (?:monetary policy )?action were (.*?)" + end,
                   flat)
    if vf:
        names = [seg.split(",")[0].strip() for seg in vf.group(1).split(";")]
        out["votes_for_names"] = [re.sub(r"^and ", "", n).strip() for n in names if n.strip()]

    va = re.search(r"Voting against (?:the|this) (?:monetary policy )?action (?:was|were) "
                   r"(.*?)" + end, flat)
    if va:
        for seg in re.split(r";\s*(?:and\s+)?", va.group(1)):
            names_part, _, why = seg.partition(", who ")
            why = why.split(". ")[0]
            for n in _split_names(names_part):
                out["dissents"].append({
                    "name": n,
                    "preferred_bps": _preferred_bps("who " + why),
                    "text": ("who " + why).strip().rstrip("."),
                })
    return out


# ── SEP ─────────────────────────────────────────────────────

_SEP_VARS = [("core pce", "core_pce"), ("pce inflation", "pce"),
             ("real gdp", "gdp"), ("unemployment", "unemployment"),
             ("federal funds", "fed_funds")]


def _horizon(label: str) -> str:
    label = label.strip()
    return "LR" if label.lower().startswith("longer") else label[:4]


def _range(cell: str) -> tuple[float | None, float | None]:
    nums = re.findall(r"-?\d+(?:\.\d+)?", cell.replace("–", " ").replace("—", " "))
    if not nums:
        return None, None
    return float(nums[0]), float(nums[-1])


def parse_sep(html: str) -> dict:
    """Parse SEP Table 1 (medians/CT/range) and Figure 2 (dot counts)."""
    soup = BeautifulSoup(html, "html.parser")
    projections, dots = [], []
    for table in soup.find_all("table"):
        head_text = table.get_text(" ", strip=True)[:300]
        thead_rows = table.find_all("tr")
        if "Midpoint of target range" in head_text:
            header = [_horizon(th.get_text()) for th in thead_rows[0].find_all("th")[1:]]
            for tr in thead_rows[1:]:
                stub = tr.find("th")
                if not stub:
                    continue
                try:
                    rate = float(stub.get_text(strip=True))
                except ValueError:
                    continue
                for h, td in zip(header, tr.find_all("td")):
                    t = td.get_text(strip=True)
                    if t.isdigit():
                        dots.append({"horizon": h, "rate": rate, "n": int(t)})
        elif "Median" in head_text and "Central" in head_text and not projections:
            years = None
            for tr in thead_rows:
                ths = [th.get_text(" ", strip=True) for th in tr.find_all("th")]
                cand = [x for x in ths if re.match(r"^(\d{4}|Longer run)", x)]
                if len(cand) >= 6:
                    years = [_horizon(x) for x in cand[:len(cand) // 3]]
                    break
            if not years:
                continue
            n = len(years)
            for tr in thead_rows:
                stub = tr.find("th")
                tds = tr.find_all("td")
                if not stub or len(tds) < 3 * n:
                    continue
                label = re.sub(r"\d+$", "", stub.get_text(" ", strip=True)).strip().lower()
                if "projection" in label:
                    continue
                var = next((v for k, v in _SEP_VARS if k in label), None)
                if not var:
                    continue
                cells = [td.get_text(" ", strip=True) for td in tds]
                for i, h in enumerate(years):
                    med = _range(cells[i])[0]
                    ct = _range(cells[n + i])
                    rg = _range(cells[2 * n + i])
                    if med is None and ct[0] is None:
                        continue
                    projections.append({"variable": var, "horizon": h, "median": med,
                                        "ct_lo": ct[0], "ct_hi": ct[1],
                                        "range_lo": rg[0], "range_hi": rg[1]})
    return {"projections": projections, "dots": dots}


# ── Roster ──────────────────────────────────────────────────

def parse_fomc_page(html: str) -> dict:
    """Current-year members/alternates and forward rotation from fomc.htm."""
    soup = BeautifulSoup(html, "html.parser")
    out = {"year": None, "people": [], "rotation": []}
    for h4 in soup.find_all("h4"):
        title = h4.get_text(" ", strip=True)
        m = re.match(r"(\d{4}) Committee Members", title)
        if m:
            out["year"] = int(m.group(1))
            status = "member"
        elif title == "Alternate Members":
            status = "alternate"
        else:
            continue
        ul = h4.find_next_sibling("ul")
        for li in ul.find_all("li") if ul else []:
            a = li.find("a")
            name = a.get_text(" ", strip=True) if a else li.get_text().split(",")[0]
            rest = [s.strip() for s in li.get_text(" ", strip=True)[len(name):].split(",")
                    if s.strip()]
            # rest: [institution, title?] or [title, institution] for alternates
            inst = next((r for r in rest if r in BANKS or r == "Board of Governors"), None)
            role = next((r for r in rest if r != inst), None)
            out["people"].append({"name": name, "institution": inst, "role": role,
                                  "status": status,
                                  "bio_url": a["href"] if a else None})
    rot = soup.find(string=re.compile("Rotation on the FOMC"))
    table = rot.find_next("table") if rot else None
    if table:
        rows = table.find_all("tr")
        years = [int(th.get_text(strip=True)) for th in rows[0].find_all("th")
                 if th.get_text(strip=True).isdigit()]
        for tr in rows[1:]:
            status = "alternate" if "Alternate" in tr.find("th").get_text() else "member"
            for y, td in zip(years, tr.find_all("td")):
                for bank in td.stripped_strings:
                    bank = bank.strip("† ")
                    if bank in BANKS:
                        out["rotation"].append({"year": y, "institution": bank,
                                                "status": status})
    return out


def parse_board_bio(html: str) -> dict:
    """Term-end dates, Board role, and lead bio text from a governor's bio page."""
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find(id="article") or soup
    text = re.sub(r"\s+", " ", article.get_text(" ", strip=True))
    terms = [int(y) for y in re.findall(r"January 31, (\d{4})", text)]
    role = None
    mr = re.search(r"took office as (?:the )?(Vice Chair for Supervision|Vice Chair|chairman|Chair)\b",
                   text)
    if mr:
        role = mr.group(1).title() if mr.group(1).islower() else mr.group(1)
    chair_end = None
    mc = re.search(r"four-year term ending on (\w+ \d{1,2}, \d{4})", text)
    if mc:
        chair_end = datetime.strptime(mc.group(1), "%B %d, %Y").date()
    mn = re.search(r"([A-Z][\w.]*(?: [A-Z][\w.]*){1,3}) (?:first )?took office", text)
    name = None
    if mn:  # the regex also swallows preceding nav words ("More", "Chairman")
        tokens = mn.group(1).split()
        while len(tokens) > 2 and tokens[0] in ("More", "Chairman", "Chair", "Vice",
                                                "Governor", "View"):
            tokens.pop(0)
        name = " ".join(tokens)
    return {"name": name,
            "bio_text": text[text.find(name, mn.start()):][:3000] if mn else None,
            "board_term_end": date(max(terms), 1, 31) if terms else None,
            "role": role, "chair_term_end": chair_end}


# ── Storage ─────────────────────────────────────────────────

def _upsert_participant(con, pid: str, name: str, **fields) -> None:
    fields = {k: v for k, v in fields.items() if v is not None}
    cols = ["id", "name"] + list(fields)
    vals = [pid, name] + list(fields.values())
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[1:])
    con.execute(
        f"INSERT INTO fomc_participants ({', '.join(cols)}, updated_at) "
        f"VALUES ({', '.join(['%s'] * len(cols))}, NOW()) "
        f"ON CONFLICT (id) DO UPDATE SET {updates}, updated_at = NOW()",
        vals)


def store_calendar(meetings: list[dict]) -> None:
    con = connect()
    with con.transaction():
        con.cursor().executemany(
                "INSERT INTO fomc_meetings (meeting_date, start_date, has_sep, unscheduled, "
                "statement_url, presser_url, sep_url, minutes_url, minutes_release) "
                "VALUES (%(meeting_date)s, %(start_date)s, %(has_sep)s, %(unscheduled)s, "
                "%(statement_url)s, %(presser_url)s, %(sep_url)s, %(minutes_url)s, "
                "%(minutes_release)s) "
                "ON CONFLICT (meeting_date) DO UPDATE SET start_date = EXCLUDED.start_date, "
                "has_sep = EXCLUDED.has_sep, unscheduled = EXCLUDED.unscheduled, "
                "statement_url = COALESCE(EXCLUDED.statement_url, fomc_meetings.statement_url), "
                "presser_url = COALESCE(EXCLUDED.presser_url, fomc_meetings.presser_url), "
                "sep_url = COALESCE(EXCLUDED.sep_url, fomc_meetings.sep_url), "
                "minutes_url = COALESCE(EXCLUDED.minutes_url, fomc_meetings.minutes_url), "
                "minutes_release = COALESCE(EXCLUDED.minutes_release, fomc_meetings.minutes_release)",
                meetings)
        # Meetings the Fed has dropped from the calendar (rescheduled) and
        # that never produced a statement are stale — remove them.
        dates = [m["meeting_date"] for m in meetings]
        if dates:
            con.execute("DELETE FROM fomc_meetings WHERE statement_url IS NULL "
                        "AND meeting_date >= %s AND NOT (meeting_date = ANY(%s))",
                        (min(dates), dates))


def store_roster(page: dict) -> None:
    con = connect()
    year = page["year"]
    with con.transaction():
        if year:
            con.execute("DELETE FROM fomc_voters WHERE year = %s", (year,))
        for p in page["people"]:
            pid = participant_id(p["name"])
            _upsert_participant(con, pid, p["name"], institution=p["institution"],
                                role=p["role"], bio_url=p["bio_url"], verified=True,
                                active=True)
            if year:
                con.execute("INSERT INTO fomc_voters VALUES (%s, %s, %s) "
                            "ON CONFLICT DO NOTHING", (year, pid, p["status"]))
        for r in page["rotation"]:
            con.execute("INSERT INTO fomc_rotation VALUES (%(year)s, %(institution)s, "
                        "%(status)s) ON CONFLICT (year, institution) "
                        "DO UPDATE SET status = EXCLUDED.status", r)


def apply_seed(seed_path: Path = ROSTER_SEED) -> None:
    seed = yaml.safe_load(seed_path.read_text())
    con = connect()
    with con.transaction():
        for pid, p in (seed.get("presidents") or {}).items():
            # Don't let a seed entry override a name confirmed at the source.
            row = con.execute("SELECT verified FROM fomc_participants WHERE id = %s",
                              (pid,)).fetchone()
            if row and row[0] and not p.get("verified", True):
                continue
            _upsert_participant(con, pid, p["name"], institution=p["institution"],
                                verified=bool(p.get("verified", True)), active=True)
        for pid, who in (seed.get("appointed_by") or {}).items():
            con.execute("UPDATE fomc_participants SET appointed_by = %s WHERE id = %s",
                        (who, pid))
        con.execute("UPDATE fomc_participants SET appointed_by = %s "
                    "WHERE institution IS NOT NULL AND institution <> 'Board of Governors'",
                    (seed.get("president_appointed_by"),))


def store_statement(meeting_date: date, text: str) -> int:
    """Parse + store one statement and its votes. Returns number of vote rows."""
    con = connect()
    p = parse_statement(text)
    prev = con.execute("SELECT target_high FROM fomc_meetings WHERE meeting_date < %s "
                       "AND target_high IS NOT NULL ORDER BY meeting_date DESC LIMIT 1",
                       (meeting_date,)).fetchone()
    decision = p["action_bps"]
    if decision is None and prev and p["target_high"] is not None:
        decision = round((p["target_high"] - prev[0]) * 100)

    votes = []
    dissent_ids = set()
    for d in p["dissents"]:
        pid = participant_id(d["name"])
        dissent_ids.add(pid)
        votes.append((pid, d["name"], "against", d["preferred_bps"], d["text"], False))
    if p["votes_for_names"]:
        for n in p["votes_for_names"]:
            votes.append((participant_id(n), n, "for", None, None, False))
    else:
        roster = con.execute(
            "SELECT v.participant_id, pt.name FROM fomc_voters v "
            "JOIN fomc_participants pt ON pt.id = v.participant_id "
            "WHERE v.year = %s AND v.status = 'member'", (meeting_date.year,)).fetchall()
        for pid, name in roster:
            if pid not in dissent_ids:
                votes.append((pid, name, "for", None, None, True))
        n_for = sum(1 for v in votes if v[2] == "for")
        if p["tally"] and p["tally"][0] != n_for:
            print(f"  WARNING {meeting_date}: stated tally {p['tally']} but roster "
                  f"implies {n_for} for — check fomc_voters for {meeting_date.year}")

    n_for = p["tally"][0] if p["tally"] else sum(1 for v in votes if v[2] == "for")
    n_against = p["tally"][1] if p["tally"] else len(p["dissents"])
    with con.transaction():
        con.execute(
            "UPDATE fomc_meetings SET statement_text = %s, target_low = %s, target_high = %s, "
            "decision_bps = %s, votes_for = %s, votes_against = %s, statement_parsed_at = NOW() "
            "WHERE meeting_date = %s",
            (text, p["target_low"], p["target_high"], decision, n_for, n_against,
             meeting_date))
        con.execute("DELETE FROM fomc_votes WHERE meeting_date = %s", (meeting_date,))
        cur = con.cursor()
        cur.executemany("INSERT INTO fomc_participants (id, name) VALUES (%s, %s) "
                        "ON CONFLICT (id) DO NOTHING", [(v[0], v[1]) for v in votes])
        cur.executemany("INSERT INTO fomc_votes VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT DO NOTHING",
                        [(meeting_date, pid, vote, pref, why, inferred)
                         for pid, _name, vote, pref, why, inferred in votes])
    return len(votes)


def store_sep(meeting_date: date, sep: dict) -> None:
    con = connect()
    with con.transaction():
        con.execute("DELETE FROM sep_projections WHERE meeting_date = %s", (meeting_date,))
        con.execute("DELETE FROM sep_dots WHERE meeting_date = %s", (meeting_date,))
        cur = con.cursor()
        cur.executemany("INSERT INTO sep_projections VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        [(meeting_date, r["variable"], r["horizon"], r["median"], r["ct_lo"],
                          r["ct_hi"], r["range_lo"], r["range_hi"])
                         for r in sep["projections"]])
        cur.executemany("INSERT INTO sep_dots VALUES (%s, %s, %s, %s)",
                        [(meeting_date, r["horizon"], r["rate"], r["n"]) for r in sep["dots"]])
        con.execute("UPDATE fomc_meetings SET sep_parsed_at = NOW() WHERE meeting_date = %s",
                    (meeting_date,))


# ── Entry points ────────────────────────────────────────────

def refresh(log=print) -> int:
    """Pull the calendar + current roster, then parse any statement or SEP that
    has been published but not yet stored. Returns the number of newly parsed
    documents (statements + SEPs) — nonzero means the briefing should regen."""
    con = connect()
    store_calendar(parse_calendar(_get("/monetarypolicy/fomccalendars.htm")))
    store_roster(parse_fomc_page(_get("/monetarypolicy/fomc.htm")))
    apply_seed()

    new = 0
    todo = con.execute(
        "SELECT meeting_date, statement_url FROM fomc_meetings "
        "WHERE statement_url IS NOT NULL AND statement_parsed_at IS NULL "
        "ORDER BY meeting_date").fetchall()
    for d, url in todo:
        try:
            n = store_statement(d, statement_text(_get(url)))
            log(f"  FOMC statement {d}: {n} votes")
            new += 1
        except Exception as e:
            log(f"  FOMC statement {d} FAILED: {e}")
        time.sleep(0.3)

    todo = con.execute(
        "SELECT meeting_date, sep_url FROM fomc_meetings "
        "WHERE sep_url IS NOT NULL AND sep_parsed_at IS NULL ORDER BY meeting_date").fetchall()
    for d, url in todo:
        try:
            sep = parse_sep(_get(url))
            if not sep["dots"] or not sep["projections"]:
                log(f"  SEP {d}: parse incomplete ({len(sep['projections'])} rows, "
                    f"{len(sep['dots'])} dot cells) — skipped")
                continue
            store_sep(d, sep)
            log(f"  SEP {d}: {len(sep['projections'])} projections, "
                f"{sum(x['n'] for x in sep['dots'])} dots")
            new += 1
        except Exception as e:
            log(f"  SEP {d} FAILED: {e}")
        time.sleep(0.3)
    return new


def refresh_roster(log=print) -> int:
    """Weekly: governor bios (term ends, Board role) + roster change check."""
    con = connect()
    page = parse_fomc_page(_get("/monetarypolicy/fomc.htm"))
    before = {r[0] for r in con.execute(
        "SELECT participant_id FROM fomc_voters WHERE year = %s", (page["year"],)).fetchall()}
    store_roster(page)
    apply_seed()
    after = {participant_id(p["name"]) for p in page["people"]}
    if before and before != after:
        log(f"  ROSTER CHANGE {page['year']}: +{sorted(after - before)} "
            f"-{sorted(before - after)}")

    board_html = _get("/aboutthefed/bios/board/default.htm")
    board_text = re.sub(r"\s+", " ", BeautifulSoup(board_html, "html.parser").get_text(" "))
    titles = {participant_id(n): t for n, t in re.findall(
        r"([A-Z][\w.]*(?: [A-Z][\w.]*){1,3}), (Chairman|Vice Chair for Supervision|Vice Chair)\b",
        board_text)}
    slugs = sorted(set(re.findall(r"/aboutthefed/bios/board/([a-z]+)\.htm", board_html))
                   - {"default", "boardmembership"})
    with con.transaction():
        con.execute("UPDATE fomc_participants SET active = FALSE "
                    "WHERE institution = 'Board of Governors'")
    n = 0
    for slug in slugs:
        try:
            html = _get(f"/aboutthefed/bios/board/{slug}.htm")
            bio = parse_board_bio(html)
            # Prefer the full name already captured from fomc.htm / statements;
            # bio phrasing sometimes uses the surname alone.
            row = con.execute("SELECT name FROM fomc_participants WHERE id = %s",
                              (slug,)).fetchone()
            name = row[0] if row and " " in row[0] else (bio["name"] or slug.title())
            _upsert_participant(con, participant_id(name), name,
                                institution="Board of Governors",
                                role=titles.get(slug) or bio["role"] or "Governor",
                                board_term_end=bio["board_term_end"],
                                chair_term_end=bio["chair_term_end"],
                                bio_url=f"{BASE}/aboutthefed/bios/board/{slug}.htm",
                                bio_text=bio["bio_text"], verified=True, active=True)
            n += 1
        except Exception as e:
            log(f"  bio {slug} FAILED: {e}")
        time.sleep(0.3)
    apply_seed()  # appointed_by for any governor row just (re)created
    return n


# ── Queries for the briefing / FedWatch ─────────────────────

def meeting_dates(scheduled_only: bool = True) -> list[date]:
    q = "SELECT meeting_date FROM fomc_meetings"
    if scheduled_only:
        q += " WHERE NOT unscheduled"
    return [r[0] for r in connect().execute(q + " ORDER BY meeting_date").fetchall()]


def latest_target_range() -> tuple[float, float] | None:
    row = connect().execute(
        "SELECT target_low, target_high FROM fomc_meetings WHERE target_high IS NOT NULL "
        "ORDER BY meeting_date DESC LIMIT 1").fetchone()
    return (row[0], row[1]) if row else None
