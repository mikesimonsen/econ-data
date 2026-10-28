"""Tests for the FOMC scrapers in econ_data/fomc.py.

Fixtures are trimmed copies of real federalreserve.gov markup/text. Each case
pins a format quirk that broke an earlier version of the parser: middle
initials ending a vote sentence, the "X voted as an alternate member" tail,
non-breaking hyphens in "3‑3/4", the June-2026 switch to tally-only vote
lines, and non-rate dissents.
"""
from datetime import date

from econ_data.fomc import (blackout_window, parse_calendar, parse_fomc_page, parse_rate,
                            parse_sep, parse_statement, participant_id)
from econ_data.fomc_render import _redline


def test_parse_rate_fractions_and_nonbreaking_hyphen():
    assert parse_rate("3-3/4") == 3.75
    assert parse_rate("3‑1/2") == 3.5
    assert parse_rate("1/4") == 0.25
    assert parse_rate("0.5") == 0.5
    assert parse_rate("4") == 4.0


def test_participant_id():
    assert participant_id("Jerome H. Powell, Chair") == "powell"
    assert participant_id("Kathleen O'Neill Paese") == "paese"
    assert participant_id("Kevin Warsh") == "warsh"


def test_blackout_starts_second_saturday_before_first_day():
    # Oct 27–28, 2026 meeting: Saturdays before Tue Oct 27 are Oct 24, Oct 17.
    assert blackout_window(date(2026, 10, 27), date(2026, 10, 28)) == (
        date(2026, 10, 17), date(2026, 10, 29))


OLD_FORMAT = (
    "In support of its goals and in light of the shift in the balance of risks, the Committee "
    "decided to lower the target range for the federal funds rate by 1/4 percentage point to "
    "3-1/2 to 3‑3/4 percent. "
    "Voting for the monetary policy action were Jerome H. Powell, Chair; John C. Williams, "
    "Vice Chair; Michael S. Barr; and Christopher J. Waller. Austan D. Goolsbee voted as an "
    "alternate member at this meeting. Voting against this action were Stephen I. Miran, who "
    "preferred to lower the target range for the federal funds rate by 1/2 percentage point at "
    "this meeting; and Austan D. Goolsbee and Jeffrey R. Schmid, who preferred no change to the "
    "target range for the federal funds rate at this meeting."
)


def test_old_format_votes_and_split_dissents():
    p = parse_statement(OLD_FORMAT)
    assert (p["target_low"], p["target_high"], p["action_bps"]) == (3.5, 3.75, -25)
    # The vote list must run through "Waller", not stop at a middle initial
    # or swallow the "voted as an alternate" sentence.
    assert p["votes_for_names"] == ["Jerome H. Powell", "John C. Williams",
                                    "Michael S. Barr", "Christopher J. Waller"]
    assert [(d["name"], d["preferred_bps"]) for d in p["dissents"]] == [
        ("Stephen I. Miran", -50), ("Austan D. Goolsbee", 0), ("Jeffrey R. Schmid", 0)]


def test_new_format_tally_and_dissenters_only():
    text = ("The Federal Open Market Committee approved the following statement for release "
            "by a 10 – 2 vote:\n\nThe Committee decided to maintain the target range for "
            "the federal funds rate at 3-3/4 to 4 percent, in support of the Federal Reserve's "
            "dual mandate.\n\nVoting against the monetary policy action were Lisa D. Cook and "
            "Beth M. Hammack, who preferred to raise the target range for the federal funds "
            "rate by 1/4 percentage point at this meeting.")
    p = parse_statement(text)
    assert p["tally"] == (10, 2)
    assert p["votes_for_names"] is None
    assert (p["target_low"], p["target_high"], p["action_bps"]) == (3.75, 4.0, 0)
    assert [(d["name"], d["preferred_bps"]) for d in p["dissents"]] == [
        ("Lisa D. Cook", 25), ("Beth M. Hammack", 25)]


def test_non_rate_dissent_has_no_preferred_bps():
    text = ("The Committee decided to maintain the target range for the federal funds rate at "
            "3-1/2 to 3-3/4 percent. Voting for the monetary policy action were Jerome H. "
            "Powell, Chair; and Michael S. Barr. Voting against this action was Beth M. "
            "Hammack, who supported maintaining the target range for the federal funds rate "
            "but preferred not to include an easing bias in the statement.")
    d = parse_statement(text)["dissents"]
    assert len(d) == 1 and d[0]["name"] == "Beth M. Hammack"
    assert d[0]["preferred_bps"] is None


CALENDAR = """
<h4><a>2027 FOMC Meetings</a></h4>
<div class="row fomc-meeting">
 <div class="fomc-meeting__month"><strong>June</strong></div>
 <div class="fomc-meeting__date">8-9*</div>
</div>
<h4><a>2026 FOMC Meetings</a></h4>
<div class="row fomc-meeting">
 <div class="fomc-meeting__month"><strong>October</strong></div>
 <div class="fomc-meeting__date">27-28</div>
 <div><a href="/newsevents/pressreleases/monetary20261028a.htm">HTML</a>
 <a href="/monetarypolicy/fomcpressconf20261028.htm">Press Conference</a></div>
</div>
<div class="row fomc-meeting">
 <div class="fomc-meeting__month"><strong>Apr/May</strong></div>
 <div class="fomc-meeting__date">30-1</div>
 <div><a href="/monetarypolicy/fomcminutes20260501.htm">HTML</a> (Released May 22, 2026)</div>
</div>
"""


def test_parse_calendar():
    m = {x["meeting_date"]: x for x in parse_calendar(CALENDAR)}
    assert m[date(2027, 6, 9)]["has_sep"] and m[date(2027, 6, 9)]["start_date"] == date(2027, 6, 8)
    oct_ = m[date(2026, 10, 28)]
    assert oct_["statement_url"].endswith("monetary20261028a.htm")
    assert oct_["presser_url"].endswith("fomcpressconf20261028.htm")
    assert not oct_["has_sep"]
    cross = m[date(2026, 5, 1)]  # meeting spanning a month boundary
    assert cross["start_date"] == date(2026, 4, 30)
    assert cross["minutes_release"] == date(2026, 5, 22)


SEP = """
<table><thead>
 <tr><th>Variable</th><th colspan="3">Median</th><th colspan="3">Central Tendency</th>
     <th colspan="3">Range</th></tr>
 <tr><th></th><th>2026</th><th>2027</th><th>Longer run</th><th>2026</th><th>2027</th>
     <th>Longer run</th><th>2026</th><th>2027</th><th>Longer run</th></tr></thead>
<tbody>
 <tr><th>Federal funds rate</th><td>4.1</td><td>3.9</td><td>3.2</td><td>4.1–4.4</td>
     <td>3.6–4.4</td><td>3.0–3.6</td><td>3.9–4.4</td><td>3.1–4.4</td><td>2.9–3.9</td></tr>
 <tr><th>June projection</th><td>3.8</td><td>3.6</td><td>3.1</td><td>3.6–4.1</td>
     <td>3.1–3.9</td><td>3.0–3.5</td><td>3.4–4.4</td><td>2.9–4.4</td><td>2.9–3.9</td></tr>
</tbody></table>
<table><thead><tr><th>Midpoint of target range or target level (Percent)</th><th>2026</th>
 <th>2027</th><th>Longer run</th></tr></thead>
<tbody>
 <tr><th>4.375</th><td>4</td><td>&nbsp;</td><td>&nbsp;</td></tr>
 <tr><th>4.125</th><td>12</td><td>6</td><td>&nbsp;</td></tr>
</tbody></table>
"""


def test_parse_sep_skips_prior_projection_rows():
    sep = parse_sep(SEP)
    ff = {r["horizon"]: r for r in sep["projections"]}
    assert set(ff) == {"2026", "2027", "LR"}
    assert ff["2026"]["median"] == 4.1  # not June's 3.8
    assert (ff["LR"]["ct_lo"], ff["LR"]["range_hi"]) == (3.0, 3.9)
    assert sorted((d["horizon"], d["rate"], d["n"]) for d in sep["dots"]) == [
        ("2026", 4.125, 12), ("2026", 4.375, 4), ("2027", 4.125, 6)]


FOMC_PAGE = """
<h4>2026 Committee Members</h4>
<ul><li><a href="/bios/warsh.htm">Kevin Warsh</a>, Board of Governors, Chairman</li>
<li><a href="/x">Beth M. Hammack</a>, Cleveland</li></ul>
<h4>Alternate Members</h4>
<ul><li><a href="/y">Cheryl Venable</a>, Interim President, Atlanta</li></ul>
<h4>Federal Reserve Bank Rotation on the FOMC</h4>
<table><tr><th></th><th>2027</th></tr>
<tr><th>Members</th><td>New York<br/>Chicago</td></tr>
<tr><th>Alternate<br/>Members</th><td>New York<sup>†</sup><br/>Cleveland</td></tr></table>
"""


def test_parse_fomc_page():
    page = parse_fomc_page(FOMC_PAGE)
    assert page["year"] == 2026
    people = {p["name"]: p for p in page["people"]}
    assert people["Kevin Warsh"]["role"] == "Chairman"
    assert people["Beth M. Hammack"]["institution"] == "Cleveland"
    assert people["Cheryl Venable"] == {**people["Cheryl Venable"], "institution": "Atlanta",
                                        "role": "Interim President", "status": "alternate"}
    assert {(r["institution"], r["status"]) for r in page["rotation"]} == {
        ("New York", "member"), ("Chicago", "member"),
        ("New York", "alternate"), ("Cleveland", "alternate")}


def test_redline_marks_changed_words():
    out = _redline("The Committee decided to raise the range.",
                   "The Committee decided to maintain the range.")
    assert "<del>raise</del>" in out and "<ins>maintain</ins>" in out
