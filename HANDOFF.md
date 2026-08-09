# econ-data — Handoff

**Written 2026-08-05 (Wednesday) for a fresh Claude instance on a new Anthropic account.**

This file exists because Mike is migrating Anthropic accounts. Everything Claude had
accumulated about this project lived in `~/.claude/projects/-Users-mikesimonsen-projects-econ-data/memory/`,
which does not travel with an account change. This file is that knowledge, written down
in the repo where it survives.

Read this file **and** `CLAUDE.md` before doing anything. `CLAUDE.md` is the operating
manual; this file is the context, history, and hard-won lessons behind it.

---

## Who you're working with

Mike Simonsen. Housing and economic data analyst. He founded Altos Research (built it from
scratch, no longer works there — he's now a customer). His email `mike@altosresearch.com`
is a holdover from that era; don't infer his employer from it.

What matters when working with him:

- **He has deep domain expertise in economic statistics.** He knows that rates and
  percentages need percentage-point differences, not percent changes. Don't explain
  basic econ concepts to him. Do explain infrastructure/DB tradeoffs — he doesn't
  consider himself a database expert.
- **He thinks carefully about what metrics are meaningful for an audience.** When
  something is technically correct but would mislead a reader, say so.
- **He catches sloppiness.** Several of the lessons below exist because he noticed
  something that was quietly wrong and pushed back. When he pushes back on a diagnosis,
  he is usually right — re-verify before defending your conclusion.

Git identity for this repo: `mike@cognitaresearch.com` (Cognita Research). Note that
other projects in his portfolio record different affiliations (see
`~/projects/PORTFOLIO-HANDOFF.md`) — **ask him to confirm his current role** rather than
assuming from any one file.

---

## What this project is

Fetches U.S. economic data (FRED plus ~10 other sources), stores it in managed Postgres,
computes derived series, generates LLM-written analysis, and publishes a daily briefing
to GitHub Pages. Runs unattended in the cloud on a schedule.

It is the most mature and most operationally live project in Mike's portfolio: **464
commits, running daily in production, publishing a public site.** Treat it as production
infrastructure, not a sandbox.

---

## Current status (2026-08-05)

Healthy and running daily. Recent commits are almost all automated
`data: update sheets export YYYY-MM-DD` pushes from the cloud cron.

Open threads, in rough priority order:

1. **Redfin fetcher is pointed at a dead URL.** Redfin migrated their public data center
   from `redfin_market_tracker/*.tsv000.gz` to
   `redfin_data_center/<dataset>/<frequency>/<geography>.csv` around 2026-06-02 and left
   the old files publicly readable but frozen. Our data stops at May 2026. Full detail
   and the new paths are in "Redfin migration" below. **Needs a rewrite of
   `econ_data/fetch_redfin.py`, and Mike has design decisions to make first.**
2. **Phantom OVERDUE rows in `release_schedule`.** `FEDFUNDS` (a monthly series) inherits
   the daily H.15 release calendar and manufactures ~20 false OVERDUE rows a month. MND
   rows are captured with an off-by-N offset — `mark_captured()` appears to consume the
   oldest open row rather than the row matching the period. Both make the briefing's
   "Upcoming" tab cry wolf. Not data loss, but it masks real outages (it masked the
   Redfin one for two months).
3. **Sheets manifest timezone bug.** Detail below under "Known bugs."
4. **Case-Shiller Detroit** April 2026 still unpublished upstream. Everything else
   resolved.

---

## Architecture

```
FRED + ~10 other sources
        │
        ▼
  fetch (schedule-driven)  ──► Postgres (Neon, us-east-1)
        │                        observations, calculated, release_schedule,
        │                        groups, revisions, series_release
        ▼
  calculations (period_pct, yoy_pct)
        │
        ▼
  LLM analyses (daily + housing, Anthropic API)
        │
        ▼
  briefing.py ──► docs/index.html ──► GitHub Pages
  export_sheets.py ──► sheets_data/*.csv + last_updated.json ──► Google Apps Script
        │
        ▼
  git commit + push to origin/main
```

**Key modules** (`econ_data/`):

| File | Role |
|---|---|
| `config.py` | loads `config.yaml` — the single source of truth for series, groups, cadences |
| `fetch.py` | FRED via `fredapi`, gated on `release_schedule` |
| `fetch_*.py` | one per non-FRED source: altos, bls, confboard, factset, mnd, nar, realtor, redfin, web, xactus, zillow |
| `store.py` | Postgres CRUD (`store_sqlite.py` is the retired SQLite path — keep, don't use) |
| `release_schedule.py` | the scheduling brain (see CLAUDE.md "Scheduling") |
| `calculations.py` | derived `period_pct` / `yoy_pct` for every series |
| `daily_analysis.py`, `housing_analysis.py` | LLM commentary |
| `briefing.py` | builds `docs/index.html` |
| `export_sheets.py` | writes `sheets_data/` CSVs + manifest |
| `pipeline.py` | orchestrates the whole run |

**Entrypoints:** `run.py` (morning), `run_intraday.py` (afternoon), `run_calendar.py`
(Sunday schedule refresh).

**`prompts/`** — `daily_analysis.txt`, `housing_analysis.txt`,
`monthly_housing_summary.txt`. Per the working agreement in CLAUDE.md, when a
deliverable's voice or emphasis needs to change, **edit the prompt**, don't one-shot the
output differently.

---

## How it runs in production

`.github/workflows/cron.yml` fires on a fixed UTC schedule (paired EDT/EST crons with an
hour guard), and each firing starts a Fly machine in the `econ-data-cron` app.

| Time (ET) | Cohort | Purpose |
|---|---|---|
| 07:00 | morning | overnight markets, prior-day catchup |
| 09:00 | morning | captures 08:30 ET releases (CPI, PPI, payrolls, claims) |
| 10:30 | morning | captures 10:00 ET releases (JOLTS, ISM, Census, NAR) |
| 13:00 / 15:00 / 16:00 | intraday | FRED API routinely posts 10:00 releases hours late |
| Sun 06:00 | calendar | refresh `release_schedule` + FedWatch |

The Fly machine has **no** `--schedule` flag — that was deliberately removed 2026-04-28
because Fly's "daily" fires ~24h after the previous run and drifts. Region is `iad`, same
metro as Neon's us-east-1, which cut runtime from ~12 min to ~5 min.

Manual trigger: `./trigger-fly.sh` or `gh workflow run cron.yml -f cohort=...`.
`run.sh` is **retired** — it prints a notice and exits 1.

---

## Secrets and accounts — READ THIS DURING THE MIGRATION

The account switch has direct consequences here, because this project calls the Anthropic
API from the cloud on a schedule.

| Secret | Where it lives | Action needed on account change |
|---|---|---|
| `ANTHROPIC_API_KEY` | **Fly secret** on `econ-data-cron` | **Must be rotated to the new account's key, or the daily LLM analyses fail silently.** Not in local `.env`. |
| `DATABASE_URL` | local `.env` + Fly secret | Neon Postgres. Unaffected by the Anthropic change. |
| `FRED_API_KEY` | local `.env` + Fly secret | unaffected |
| `BLS_API_KEY` | local `.env` + Fly secret | unaffected (public API, 25 req/day without a key) |
| `FLY_API_TOKEN` | GitHub repo secret (set 2026-04-28) | unaffected |

To inspect: `fly secrets list -a econ-data-cron` (requires `flyctl auth login`).
To rotate: `fly secrets set ANTHROPIC_API_KEY=... -a econ-data-cron`.

`.env.example` lists `ANTHROPIC_API_KEY` but the local `.env` does not have it — local
runs of the LLM steps will fail until you add it.

---

## Working agreements

These are in `CLAUDE.md` too. Repeated here because they're the ones that matter most.

- **Auto-commit and push.** When you create or edit tracked files as part of a task,
  commit and push without asking. Mike does not want to be the gatekeeper. Exceptions
  that still need confirmation: secrets/credentials, force-pushes, destructive ops, and
  anything he marks WIP.
- **Tracked output locations.** `monthly_summaries/` for monthly drafts
  (`<topic>-YYYY-MM.md`). `summaries/` is gitignored and ephemeral.
- **Prompts are source of truth.** Improve `prompts/*.txt` over time.

---

## Hard-won lessons — do not relearn these

Each of these cost real debugging time. They were Claude's accumulated memories; they're
now yours.

### Never guess the day of the week
Compute it (`datetime.date(...).strftime('%A')`). Claude got this wrong repeatedly and it
eroded trust — especially bad here because the pipeline schedule is weekday-tied. Applies
to code that generates day-of-week labels for LLM prompts too.

### Fetch origin before diagnosing staleness
The Fly cron pushes to `origin/main`. **The local clone on Mike's laptop is frequently
dozens of commits behind.** On 2026-06-24 Claude read local `git log`, saw HEAD six days
old, and declared the publish layer "dead for 6 days" — it was just a stale clone.
Always `git fetch origin` and compare `origin/main`, and/or query Postgres `captured_at`,
before claiming anything stopped. Production truth is Postgres and origin, never the
working copy.

### Always log manual runs
Every manual trigger must leave an operational record.
- `./trigger-fly.sh` → captured by `fly logs -a econ-data-cron`. Nothing more needed.
- `gh workflow run cron.yml` → captured in the Actions run.
- Local `python run.py` → **no automatic logging.** Redirect
  (`>> logs/run.log 2>> logs/run.error.log`) or backfill a timestamped entry immediately.

Mike caught three days of manual runs with no log entries. The logs are the operational
record; gaps make incident diagnosis impossible.

### Local runs race with the cloud cron
Altos/Xactus and other `import_files/` drops must be ingested locally (Fly can't see the
local folder). But the local fetch writes to the **shared** Postgres, so the next Fly
cohort publishes that data on its own. A full local `run.py` takes ~25 min, during which
the cron pushes its own export commit, and the local push then conflicts on
`sheets_data/last_updated.json`, leaving the repo mid-rebase.

Recovery: `git rebase --abort` → `git reset --hard origin/main` → re-export → compare.
The CSVs usually come back byte-identical. **Do not push the locally regenerated
`last_updated.json`** — local timestamps are naive-local and *older* than Fly's UTC ones,
so pushing them regresses the manifest and triggers the TZ bug below.

Better: to import a drop, run only the fetch+save step, not full `run.py`.

### Don't over-engineer temporary scripts
For migration helpers, bridges, one-shots: ask "what's the dumbest thing that guarantees
correctness?" before "what's most efficient?" During the SQLite→Postgres migration Claude
proposed incremental replication and surfaced an edge-case decision to Mike; a full
truncate-and-reload took seconds for 470k rows and removed the entire question. Mike's
words: *"I'm not clear on what the decision is here."* Don't manufacture decisions when
"do the simple thing" is right.

### No Google Sheets as a data destination
Mike explicitly does not want it. The Google API has been a recurring pain point. Propose
SQL databases. (The existing Apps Script path is legacy — don't re-invest in it.)

### Adding new FRED series requires seeding the release calendar first
`fetch_all` gates on `series_due_now()`, which returns False when a series has **no
`release_schedule` rows at all** — which is the state of every newly added series. A new
series is therefore skipped on every run, forever, **without reporting an error**.

After adding series to `config.yaml`, run `refresh_fred_calendar(new_ids)` to seed
`series_release` + PENDING rows, then `fetch_all(series, force=True)` to backfill history
past the schedule gate. Full write-up in `plans/regional-payrolls.md`.

### The static site bakes data in at build time
`docs/index.html` does **not** read the CSVs at runtime. A database fix alone doesn't fix
the UI until the briefing regenerates. The briefing regenerates every run; the LLM
analyses regenerate only when new observations arrive.

---

## Known bugs and their fixes

### Sheets manifest timezone bug (fix written, not applied)
`econ_data/export_sheets.py:201` writes manifest entries with
`datetime.now().isoformat(timespec="seconds")` — naive, local TZ, no `Z`.
`google_apps_script.js:74` compares them as **strings** against a JS
`Date().toISOString()` (UTC with `Z`). Lexicographic comparison then gives the wrong
answer and Apps Script silently skips groups that should re-import.

Real example (2026-04-28): manifest had `"mortgage-rates": "2026-04-27T10:05:21"` (CDT;
real UTC 15:05), stored checkpoint was `"2026-04-27T14:00:00.000Z"`. Lex compare said
skip; real-time order said import.

The fix:
1. Change to `datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")`.
2. One-time: rewrite every entry in `sheets_data/last_updated.json` to current UTC-with-Z
   and commit. Next run re-imports every tab once and resets the baseline cleanly.
3. Verify after the next 07:00 ET run — every group with new CSV data should appear.
4. Optional: have Apps Script store `lastManifestCheck` as the max of what it just
   imported, so clock skew can't cause near-misses.

### Redfin migration (open — needs Mike's decisions)
Redfin moved their public downloads on ~2026-06-02 and left the old files serving stale
data at HTTP 200 forever. Our last capture is 2026-06-03; last data period is 2026-05-31.

New layout, verified live 2026-08-04:
- Base: `https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_data_center`
- `index.json` — machine-readable manifest of every dataset/geography path. Datasets:
  affordability, buyers_and_sellers, cash_loan, contract_cancellations,
  delistings_relistings, ehs, housing_market, investors, luxury, migration, price_drops,
  property_types, rhpi, starter_home.
- `housing_market/monthly/country.csv` — national monthly.
- `housing_market/weekly/country.csv` — **national rolling 4-week.** This is the file
  that fixes a long-standing complaint: the old weekly file had no national row, so our
  monthly figures never matched the numbers in Redfin's own articles (we showed 546k
  pending sales where Redfin published 86k). This one is the 4-week window they quote.
- Schema is a full rewrite: CSV not TSV, human-readable headers with units
  (`MEDIAN SALE PRICE NSA ($)`), precomputed MOM/YOY/WOW columns. **Monthly carries NSA
  only; SA variants exist only in the weekly file** — so the existing `REDFIN_*_SA`
  series need a decision, not a rename.

Discovery method, since S3 bucket listing is AccessDenied: scrape
`https://www.redfin.com/news/data-center/downloads/` and read the inline `<script>` — it
defines `const S3 = '.../redfin_data_center'` and a `CARDS` array with every path.

**Open decisions for Mike:** monthly vs 4-week vs both as primary; what to do with the
SA series; how to align with NAR/Altos so comparisons stay apples-to-apples.

Contact: econdata@redfin.com

### The root-cause pattern worth generalizing
`fetch_redfin.py` treats "HTTP 200 + parsed OK + zero rows newer than `last_dates`" as
success — which is **indistinguishable from "no new data yet."** A dead URL serving a
stale file fails completely silently. It went unnoticed for two months.

**Any fetcher pointed at a static bulk file needs a staleness assertion** — compare max
period against expected cadence, or check `Last-Modified` — rather than trusting a 200.
Consider auditing the other `fetch_*.py` modules for the same hole.

### Case-Shiller sanity guard
On 2026-07-02 FRED served dollar-scale garbage for 16 of 21 Case-Shiller city series
(LA 890,422 instead of a ~250–450 index), producing derived rows like +201,195%. Root
cause was upstream at FRED (series renamed to "S&P Cotality Case-Shiller"). Resolved
2026-07-06 (commit 42c4e10) with a permanent fetch-time guard: `_drop_implausible` /
`CASE_SHILLER_RANGE = (20, 3000)` in `fetch.py`, tests in `tests/test_fetch_sanity.py`.

**Keep the floor ≤ 40 on purpose** — real Case-Shiller history goes down to ~41
(Portland 1987), so a tighter floor wrongly rejects legitimate 1980s values.

### BLS metro CPI
15 BLS metro CPI series added 2026-03-25 because FRED discontinued many metro series after
the 2018 geographic restructuring. BLS still publishes them under new area codes
(e.g. `CUURS49ASA0` for LA). If they show no data, check the BLS API quota (25 req/day
without a key) or whether `BLS_API_KEY` is set.

### Git email
Repo email changed to `mike@cognitaresearch.com` on 2026-03-25. Prior commits use
`mike.simonsen@compass.com`, a different GitHub account, so the contribution graph only
shows repo creation. **Mike explicitly declined rewriting history** — cosmetic only.

---

## Performance playbook

Post-region-move runtime is ~5 min. If Mike complains about cron latency, this is the
sequenced list — **don't propose these preemptively**, it's premature optimization:

| # | Action | Effort | Savings |
|---|---|---|---|
| 2 | Run `daily_analysis` + `housing_analysis` LLM calls in parallel (independent) | 30 min | ~1:15 |
| 3 | Memoize `analyze_series` across `summary` and `briefing` | 1–2 hr | ~1:30 |
| 4 | Batch per-series SELECTs in `compute_all` into one `IN (...)` | 2–3 hr | ~1:30 |
| 5 | Same batching in `summary.py` / `briefing.py`; prefetch into dicts | 1 day | ~3:00 |

(#1 was the `sjc`→`iad` region move, already done.)

---

## First session checklist

1. Read `CLAUDE.md`, then this file.
2. `git fetch origin && git status` — expect the local clone to be well behind. Pull.
3. Confirm the cloud is alive: `git log origin/main -3` should show `data: update sheets
   export` commits from today or yesterday.
4. Confirm `ANTHROPIC_API_KEY` has been rotated on Fly, or the LLM analyses are silently
   dead.
5. Ask Mike which of the open threads he wants next — Redfin rewrite is the biggest.
