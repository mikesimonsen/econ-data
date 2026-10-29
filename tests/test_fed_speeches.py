"""Tests for Fed remarks discovery and analysis plumbing (econ_data/fed_speeches.py)
and the stance math behind the Fed tab (econ_data/fed_people_render.py)."""
import re
from datetime import date

from econ_data.fed_people_render import _md, _weighted_mean
from econ_data.fed_speeches import (BANK_SOURCES, _url_date, comm_id, speaker_match,
                                    verify_quotes)


def _date_for(inst, url):
    m = re.search(BANK_SOURCES[inst][2], url)
    return _url_date(m) if m else "no match"


def test_bank_url_patterns_and_dates():
    assert _date_for("Cleveland", "https://www.clevelandfed.org/collections/speeches/2026/"
                     "sp-20261012-trimming-the-sails") == date(2026, 10, 12)
    assert _date_for("New York", "https://www.newyorkfed.org/newsevents/speeches/2026/wil260112") \
        == date(2026, 1, 12)
    assert _date_for("Dallas", "https://www.dallasfed.org/news/speeches/logan/2026/lkl261002") \
        == date(2026, 10, 2)
    assert _date_for("Chicago", "https://www.chicagofed.org/publications/speeches/2026/"
                     "feb-19-joint-conference-financial-crises") == date(2026, 2, 19)
    assert _date_for("Philadelphia", "https://www.philadelphiafed.org/the-economy/monetary-policy/"
                     "260924-10th-annual-fintech-conference-speech") == date(2026, 9, 24)
    # Richmond's archive has an impossible date; it must not crash, just go undated.
    assert _date_for("Richmond", "https://www.richmondfed.org/press_room/speeches/jeffrey_m_lacker/"
                     "2013/lacker_speech_20131503") is None
    # Kansas City: only the full-text "-speech" PDF URL, not the landing page.
    assert _date_for("Kansas City", "https://www.kansascityfed.org/speeches/"
                     "the-local-and-the-global-in-monetary-policy-speech/") is None
    assert _date_for("Kansas City", "https://www.kansascityfed.org/speeches/"
                     "the-local-and-the-global-in-monetary-policy/") == "no match"


def test_speaker_match():
    assert speaker_match("Logan", "https://www.dallasfed.org/news/speeches/logan/2026/lkl261002",
                         "Welcome remarks", "Thank you for the introduction")
    assert speaker_match("Williams", "https://www.newyorkfed.org/newsevents/speeches/2026/wil260112",
                         "A Few Words for the New Year", "Good evening")
    assert not speaker_match("Williams", "https://www.newyorkfed.org/newsevents/speeches/2026/ath261007",
                             "Opening remarks", "Good morning")
    assert speaker_match("Hammack", "https://x.org/a", "Trimming the Sails",
                         "Beth M. Hammack, President, Federal Reserve Bank of Cleveland")


def test_verify_quotes_drops_paraphrase_keeps_verbatim():
    text = "I see upside risks to inflation.\n\nPolicy  is  “modestly restrictive.”"
    quotes = [{"quote": "I see upside risks to inflation.", "signal": "hawkish", "why": ""},
              {"quote": 'Policy is "modestly restrictive."', "signal": "neutral", "why": ""},
              {"quote": "Inflation risks are to the upside.", "signal": "hawkish", "why": ""}]
    kept = verify_quotes(quotes, text)
    assert [q["quote"] for q in kept] == [quotes[0]["quote"], quotes[1]["quote"]]
    assert all(q["verified"] for q in kept)
    # Without source text nothing can be verified, so quotes are kept but flagged.
    assert [q["verified"] for q in verify_quotes(quotes, None)] == [False, False, False]


def test_comm_id_is_stable_batch_safe():
    cid = comm_id("https://www.federalreserve.gov/newsevents/speech/x.htm")
    assert cid == comm_id("https://www.federalreserve.gov/newsevents/speech/x.htm")
    assert re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", cid)


def test_weighted_mean_counts_light_content_half():
    # rows shaped like _scored(): index 6 = policy_content, 7 = hawk_dove
    row = lambda pc, s: (None,) * 6 + (pc, s)
    assert _weighted_mean([row("substantial", 1.0), row("light", -1.0)]) == (1.0 - 0.5) / 1.5
    assert _weighted_mean([row("none", None)]) is None


def test_profile_markdown_is_escaped():
    out = _md("## How they think\n- Watches <script>alert(1)</script> **wages**")
    assert "<script>" not in out and "&lt;script&gt;" in out
    assert "<strong>wages</strong>" in out and "<h4" in out and "<ul" in out


def test_profile_markdown_bold_lead_in_bullets():
    out = _md("- **Mandate:** Prices first.\n- **Neutral rate:** No remarks on r* yet.")
    assert "<strong>Mandate:</strong> Prices first." in out
    assert "r* yet" in out and "<em>" not in out
