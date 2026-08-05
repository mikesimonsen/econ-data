# Portfolio Handoff — Mike Simonsen's Projects

**Written 2026-08-05 (Wednesday), for a fresh Claude instance on a new Anthropic account.**

## What this is and why it exists

Mike is migrating to a new Anthropic account. Everything Claude had learned across six
projects — his preferences, the architectural reasoning, the bugs already diagnosed, the
mistakes already made — lived in per-project memory directories under
`~/.claude/projects/*/memory/`. That does not travel with an account change.

This file is the portfolio-level map. **Each project has its own `HANDOFF.md`** with the
full detail. Start here, then read the one for whatever you're working on.

| Project | What it is | Status | Handoff |
|---|---|---|---|
| **econ-data** | U.S. economic data pipeline → daily briefing on GitHub Pages | **Live in production**, runs daily | `econ-data/HANDOFF.md` |
| **market-reports** | Local market reports for 340K Compass agents | Dormant since 2026-04-08 | `market-reports/HANDOFF.md` |
| **HMDA** | Mortgage data explorer, 138M rows, 2018–2025 | Deployed, stable | `HMDA/HANDOFF.md` |
| **acs-pums** | Census ACS/PUMS explorer, static + DuckDB-WASM | Dormant since 2026-05-02 | `acs-pums/HANDOFF.md` |
| **earnings** | Homebuilder earnings/filings tracker | **Paused**, partly broken | `earnings/HANDOFF.md` |
| **cdao_product_support** | Product spec for a CDAO support function | **Paused** for team review | `cdao_product_support/HANDOFF.md` |

---

## Who Mike is

Housing and economic data analyst. **He founded Altos Research** and built it from scratch;
he no longer works there (he's now a customer). His email `mike@altosresearch.com` is a
holdover — **don't infer his employer from it.**

### One thing to resolve early

His recorded affiliation is **inconsistent across projects**, because the memories were
written at different times:

| Source | Says |
|---|---|
| econ-data | Cognita Research (this is also his git commit email: `mike@cognitaresearch.com`) |
| market-reports | Chief Economist, Compass International Holdings (340K agents, 9 brands) |
| cdao_product_support | Works with Compass's CDAO group; founded Altos, no longer there |
| acs-pums | At Compass, leads a research/data team |
| HMDA | Real estate/mortgage data analyst |

These may all be partly true — Cognita Research could be his own entity alongside a Compass
role. **Ask him rather than assuming.** It affects how you frame work in market-reports and
cdao_product_support especially.

### How he works

- **Deep domain expertise in economic statistics and housing markets.** He knows rates and
  percentages need percentage-point differences, not percent changes. Don't explain the
  fundamentals to him.
- **Not a database expert by his own account.** Do explain storage/infrastructure
  tradeoffs when they drive a recommendation.
- **He thinks about what a metric means to an audience.** If something is technically
  correct but would mislead a reader, say so.
- **His publishing workflow is: explore in a custom tool → download CSV → finalize in
  Flourish.** The tools he builds are internal exploration tools, not the published
  artifact. Prioritize correct numbers and easy CSV export over chart polish.
- **He catches things.** Several lessons below exist because he noticed something quietly
  wrong and pushed back. **When he pushes back on a diagnosis, re-verify before defending
  it** — in the session that produced this file, he was right twice and Claude was wrong
  twice.

### Recurring patterns across his work

- Static sites on **GitHub Pages** (econ-data, HMDA, acs-pums all publish this way)
- Data pipelines that run unattended on a **schedule**
- **Housing and mortgage data** is the through-line in every single project
- Python for data work, TypeScript for the newer web work

---

## Account migration checklist — do this first

Only two projects call the Anthropic API. **Both will silently break until their keys are
rotated.**

| Project | Where the key lives | Action |
|---|---|---|
| **econ-data** | **Fly secret** on the `econ-data-cron` app | `fly secrets set ANTHROPIC_API_KEY=... -a econ-data-cron`. Not in local `.env`. **This one matters most — it runs daily in production and the LLM analyses will fail quietly.** |
| **market-reports** | local `.env`, and probably a GitHub repo secret | Update `.env`; check `gh secret list` for the weekly Action too. |

Unaffected (no LLM calls): HMDA, acs-pums, earnings, cdao_product_support.

Other credentials across the portfolio, none affected by the account change:
`DATABASE_URL` (Neon Postgres), `FRED_API_KEY`, `BLS_API_KEY`, `FLY_API_TOKEN`,
`CENSUS_API_KEY`, `ALTOS_PAI`, `FMP_API_KEY`, `EDGAR_USER_AGENT`.

---

## How Mike wants Claude to work

These were learned on econ-data, but most generalize. The full versions, with the
incidents behind them, are in `econ-data/HANDOFF.md`.

**Never guess the day of the week.** Compute it. Claude got this wrong repeatedly and it
cost trust.

**Verify against production, not your local copy.** Cloud jobs push to `origin`; the local
clone is routinely dozens of commits behind. Claude once declared a pipeline "dead for 6
days" from a stale checkout. `git fetch` first, query the real database, check the live
site.

**Auto-commit and push** work you do on tracked files (this is econ-data's explicit
agreement — confirm before assuming it for other repos). Mike doesn't want to be the
gatekeeper for tracking work. Still confirm for: secrets, force-pushes, destructive
operations, anything marked WIP.

**Log every manual run** of a scheduled pipeline. The logs are the operational record;
gaps make incident diagnosis impossible.

**Don't over-engineer temporary code.** For migrations, bridges, one-shots: ask "what's the
dumbest thing that guarantees correctness?" before "what's most efficient?" And **don't
manufacture decisions for him** when "do the simple thing" is obviously right.

**No Google Sheets as a data destination.** The Google API has been a repeated pain point.
Propose a real SQL database.

**Prompts are source of truth.** Where a recurring deliverable has a prompt file, improve
the prompt rather than one-shotting the output differently.

---

## A pattern worth carrying everywhere

Discovered 2026-08-04 on econ-data's Redfin fetcher, and it's the most transferable lesson
in the portfolio:

> **A fetcher that treats "HTTP 200 + parsed OK + zero new rows" as success cannot
> distinguish a dead source from a quiet one.**

Redfin moved their data to a new URL and left the old files serving stale data at 200
forever. The fetcher reported success every day for two months while ingesting nothing.

Several projects here poll external sources on a schedule. **Any fetcher pointed at a
static bulk file needs an explicit staleness assertion** — compare the max period against
the expected cadence, or check `Last-Modified` — rather than trusting a 200.

---

## Risks worth raising with Mike

Ordered by how much would be lost.

1. **HMDA is 52 GB and almost none of it is in git.** `hmda.db` (138M rows) and ~11 GB of
   source zips are all gitignored. Losing the working directory means re-downloading from
   the CFPB and a multi-hour rebuild. **If he's changing machines as well as accounts,
   flag this loudly.**
2. **`earnings` is not a git repository at all.** 1,126 files, no version control, no
   remote, no backup. The source is small — a ten-minute `git init` removes the risk.
3. **`cdao_product_support` has no version control** — 442 lines of reviewed design work
   existing only as a loose file, with its gstack-tracked original sitting in
   `~/.gstack/` where it won't survive a machine change.
4. **market-reports' design doc lives outside the repo** at
   `~/.gstack/projects/market-reports/mikesimonsen-unknown-design-20260329-090000.md`.
   Same exposure.
5. **market-reports' weekly GitHub Action has been firing unattended since April.** It
   opens editorial PRs. Four months of runs may have succeeded, failed, or piled up
   unreviewed.
6. **A half-installed launchd agent** (`com.earnings.daily.plist`) is still in
   `~/Library/LaunchAgents/` but appears not to be running. So is
   `com.mikesimonsen.fed-expectations.plist`, which econ-data's notes record as **redundant
   since 2026-05-06** — that work moved to the cloud cron and Mike was going to unload and
   delete it.

---

## Suggested first session on the new account

1. Read this file, then the `HANDOFF.md` for whatever you're touching.
2. **Rotate the two Anthropic keys** (econ-data on Fly, market-reports locally + repo
   secret). econ-data is publishing daily right now.
3. Confirm econ-data's cloud cron is still healthy: `git fetch origin && git log
   origin/main -3` should show `data: update sheets export` commits from today or
   yesterday.
4. Ask Mike to confirm his current role/affiliation so the docs can be corrected.
5. Raise the backup risks above — at minimum `git init` for `earnings` and
   `cdao_product_support`.
6. Consider seeding a `CLAUDE.md` in the three repos that lack one (HMDA, earnings,
   acs-pums) so future sessions load context automatically instead of relying on someone
   remembering to open `HANDOFF.md`.

---

## State of things at the moment these files were written (2026-08-05)

Transient facts that won't be obvious later. Delete this section once it's stale.

**Three repos have the handoff file committed but NOT pushed.** Mike has two GitHub
accounts (`mike@cognitaresearch.com` and `mike.simonsen@compass.com`) and was mid-account
migration, so the identity to push under wasn't settled:

| Repo | Commit | Pushed? |
|---|---|---|
| econ-data | `6832846` | ✅ pushed to `origin/main` |
| market-reports | `3055f57` | ❌ local only |
| HMDA | `f07016c` | ❌ local only |
| acs-pums | `469199c` | ❌ local only |

`earnings` and `cdao_product_support` have no git repo, so their `HANDOFF.md` files are
loose and unbacked-up.

**This file's own location.** The canonical, version-controlled copy lives at
`econ-data/PORTFOLIO-HANDOFF.md`. The copy at `~/projects/PORTFOLIO-HANDOFF.md` is a
convenience copy sitting one level above the projects — **`~/projects/` is not a git
repository**, so that copy has no backup. If you edit one, sync the other, and prefer the
tracked copy as the source of truth.

**Also unpushed:** during the session that produced these files, econ-data's Redfin
diagnosis was completed but **no code was changed** — `econ_data/fetch_redfin.py` still
points at the dead URL. The rewrite is waiting on Mike's decisions (monthly vs 4-week,
what to do with the `REDFIN_*_SA` series).

---

## Note on the memory system

These files capture what was in Claude's memory as of 2026-08-05. Going forward on the new
account, memory will start empty and rebuild. **These `HANDOFF.md` files are the durable
version** — when something important is learned, consider writing it here in the repo, not
only into session memory, precisely so the next migration is a non-event.
