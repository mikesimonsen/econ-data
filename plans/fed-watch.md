# Fed Watch: FOMC Members, Communications, Votes & Projections

## Goal

Turn the Fed tab from "rate chart + FedWatch odds" into a working Fed-watcher's
desk. The aim is to build durable expertise for speaking about policy in 2027+:
know each participant's framework, track who is moving, and anticipate the
committee's next step before the market fully prices it.

Today the tab shows: effective FFR history, past meeting dots, FedWatch
probabilities (Claude web search, Sunday cohort). Everything below builds on
that and on the existing `release_schedule` + cron cohorts.

## Verified sources (checked 2026-10-28)

| Source | What it gives | Access |
|---|---|---|
| `federalreserve.gov/feeds/speeches.xml` | Governor speeches (full text linked) | RSS, 200 OK |
| `federalreserve.gov/feeds/testimony.xml` | Congressional testimony | RSS |
| `federalreserve.gov/feeds/press_monetary.xml` | FOMC statements, minutes, SEP, implementation notes | RSS |
| `federalreserve.gov/json/calendar.json` | **Forward** calendar: upcoming speeches, meetings, releases | JSON (BOM-prefixed) |
| `bis.org/doclist/cbspeeches.rss` | Central bankers' speeches incl. regional Fed presidents | RSS |
| 12 regional bank sites | Presidents' speeches, essays, podcasts | per-bank scrape (fallback) |
| FRED `FEDTARMD/RH/RL/CTH/CTL/MDLR`, `UNRATEMD`, `PCECTPIMD`, `GDPC1MD`, … | SEP medians, ranges, central tendency, longer-run | FRED API (existing fetch path) |
| FRED `DFEDTARU` / `DFEDTARL` | Official target range (replaces DFF-rounding heuristic) | FRED API |
| `fomccalendars.htm` | Meeting dates, statement/minutes/SEP/presser links | HTML scrape |

**Important caveat for expertise:** SEP dots are anonymous. We can track the
*distribution* of dots per meeting, but cannot attribute a dot to a member.
Members' implied dots are inferred from their speeches — label them as inferred.

## Data model (Postgres, new tables in `schema.sql`)

```
fomc_participants   id, name, role (chair|vice_chair|vc_supervision|governor|president),
                    institution (Board|NY|Boston|…), appointed_by, appointed_date,
                    term_ends, chair_term_ends, background (academic|markets|
                    regulator|regional|policy), bio_url, active
fomc_voters         year, participant_id, voter_type (permanent|rotating|alternate)
fomc_meetings       meeting_date, has_sep, has_presser, decision_bps,
                    target_low, target_high, statement_url, minutes_url,
                    minutes_release_date, presser_transcript_url
fomc_votes          meeting_date, participant_id, vote (for|against),
                    dissent_preference_bps, dissent_reason (text from statement)
fed_communications  id, participant_id, date, kind (speech|testimony|interview|
                    essay|statement|minutes|presser|beige_book), title, url,
                    source (frb_rss|bis|bank_site|calendar), full_text, text_hash,
                    is_scheduled (from calendar.json, pre-delivery)
comm_analysis       comm_id, prompt_version, model, hawk_dove (-2..+2),
                    rate_view (cut|hold|hike + horizon), confidence,
                    topics[], framework_tags[], key_quotes jsonb,
                    shift_vs_prior (text), analyzed_at
sep_projections     meeting_date, variable, horizon_year, median, range_lo,
                    range_hi, ct_lo, ct_hi, dot_distribution jsonb
```

`dot_distribution` = `{rate: count}` per horizon, scraped from the SEP
materials' Figure 2 table (HTML version), because FRED only carries medians and ranges.

## Features, phased

### Phase 1: Foundation (deterministic, no LLM)

1. **Participant roster + 2027 voter panel.** Seed the 19 participants from the
   Board and bank sites; store appointment lineage and term dates. Voter rotation
   is rule-based: NY is permanent; 2027 rotating seats are **Richmond, Chicago,
   Atlanta, San Francisco**. Card per participant: role, appointed by, term end,
   voter/non-voter, last speech date, current stance badge (once Phase 2 lands).
   A weekly roster check against the Board/bank "leadership" pages flags changes
   (new president, resignation, nomination). Leadership turned over in 2026, so
   the roster must stay current without manual edits.
2. **Meeting ledger + vote record.** Parse each FOMC statement for decision,
   target range, voters, and dissents ("…voted against; preferred to lower by
   25bp"). Backfill 2018 onward so dissent history is deep. Table: meeting ×
   member grid (✓ / dissent-ease / dissent-tighten).
3. **Statement redline.** Diff each statement against the prior one, word-level,
   rendered with strikethrough/insert highlighting. This is the single most-used
   Fed-watcher tool and is pure text diffing.
4. **Dot plot.** Medians and ranges from FRED (existing fetch path, add a
   `fed-sep` group to `config.yaml`), plus scraped dot distribution. Render the
   current dot plot with the prior SEP ghosted behind it, and show the median path
   against the FedWatch-implied path, the "Fed vs market" gap.
5. **Calendar-driven schedule.** Replace the hardcoded `FOMC_MEETINGS` list with
   `fomccalendars.htm` + `calendar.json`. Add `release_schedule` cadences for
   statement (14:00 ET), presser (14:30), minutes (meeting + 21 days), SEP
   (Mar/Jun/Sep/Dec), Beige Book, Monetary Policy Report. The existing 15:00 and
   16:00 intraday sweeps already land after the 14:00 statement on decision days.
6. **Blackout tracker.** Blackout runs from the second Saturday before the
   meeting through midnight the day after. Banner: "Blackout begins Sat Dec 5,"
   and a "last word before blackout" list of each member's final pre-blackout remarks.
7. **Official target range.** Switch `get_current_target_range()` from DFF
   rounding to `DFEDTARL/DFEDTARU`.

### Phase 2: Communications + stance scoring (LLM)

8. **Speech ingest.** Intraday cohort polls the Board RSS feeds and the BIS feed,
   plus regional-bank scrapers where BIS lags. Fetch the full text, then dedupe by
   `text_hash`. `calendar.json` pre-populates *upcoming* appearances, which gives
   a "Fed speakers this week" list.
9. **Per-speech analysis**, with the prompt in `prompts/fed_speech_analysis.txt`
   (source of truth, versioned). Output: hawk/dove score with **verbatim quotes as
   evidence**, implied rate view, topics (inflation, labor, shelter/housing,
   balance sheet, r*, tariffs, financial stability, independence), framework tags
   (data-dependent, forward-looking/models, risk-management, inflation-
   expectations hawk, labor-mandate emphasis, r* believer/skeptic), and "what
   changed vs their last remarks." Only remarks with policy content get scored;
   pure regulatory speeches get topic tags only.
10. **Member profile pages.** Stance trajectory chart (score over time, dissents
    marked), framework summary (regenerated monthly from their last ~10 speeches),
    signature phrases, quote archive, voting record.
11. **Hawk-dove spectrum.** All 19 on one axis, voters highlighted, plus each
    member's 60-day movement arrow. Calibrate the LLM scores against actual
    dissents and SEP shifts so the axis stays honest.
12. **"Who's moving" alerts.** Flag when a voter's score moves materially, or a
    centrist (Chair, Vice Chair, NY president) changes language. Shown in the
    daily briefing.

### Phase 3: Recurring deliverables (prompts in `prompts/`)

13. **Meeting preview** (Fri before blackout ends): consensus call, FedWatch
    odds, what each voter said last, statement language likely to change,
    dissent risk.
14. **Meeting debrief** (decision day, 16:00 cohort): decision, redline
    highlights, dissents, dot shifts, presser key Q&A (transcript posts later;
    rerun next morning), and the 2y-yield/FedWatch market reaction.
15. **Minutes decoder** (minutes day): counts of Fed quantifiers ("a few" <
    "some" < "several" < "many" < "most" < "all") on key questions, compared with the
    prior minutes.
16. **Weekly Fed review** (Sunday cohort, alongside the FedWatch refresh): who
    spoke, stance changes, upcoming speakers, personnel/political news (nominations,
    confirmations, independence fights) via web search.

### Phase 4: Expertise compounding

17. **Track record.** Each speech's implied call vs what the committee did, plus
    which members lead consensus and which lag it.
18. **Fed vs market scorecard.** SEP median path vs futures over time: who was
    right?
19. **Reference glossary.** Living notes on the mechanics: the voting rotation,
    the SEP process, IORB/ON RRP, the balance sheet, and the statement's standard
    paragraphs. LLM-drafted and human-edited.

## On "political leanings"

Store only facts: appointing president, appointment and reappointment dates,
confirmation vote margins, and public statements on Fed independence. Do not
LLM-infer personal party affiliation. That is unreliable and would weaken
anything published from it. Policy stance (hawk/dove) is a separate axis and
is evidenced by quotes.

## Cron fit

| Cohort | Fed work |
|---|---|
| morning (07/09/10:30) | catch overnight speeches; presser transcript; morning debrief rerun |
| intraday (13/15/16) | RSS + BIS poll; statement/redline/votes on decision days; debrief at 16:00 |
| calendar (Sun 06:00) | FedWatch refresh (existing), roster check, calendar.json, weekly review, profile regen |

LLM spend is bounded: roughly 3–8 policy speeches/week × one Sonnet call, plus a few
digests.

## Open decisions

- Backfill depth for speeches (BIS archive goes back years; 2023+ is probably
  enough to establish each current member's baseline).
- Whether member profiles live on the Fed tab or as their own sub-tab.
- Whether debrief/preview are emailed/pushed or briefing-only.
