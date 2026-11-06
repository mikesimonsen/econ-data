# Manual refresh button and scheduling watchdog

## Goal

When the page shows missing or stale data, Mike can click once and get an answer
on the page: the data was captured and is live, or here is the specific reason
it wasn't. That replaces opening a CLI to diagnose it. A watchdog handles the
same job automatically, so the button is rarely needed.

## Why

GitHub Actions scheduled crons ran 3 to 9.5 hours late across all 60 runs
checked from 2026-10-31 to 2026-11-06. On 2026-11-06 payrolls sat uncaptured
for hours after FRED posted them. A manual `workflow_dispatch` started within
seconds.

## Architecture

The published page is static and public (GitHub Pages), and `econ-data-cron` has
no web service. Add one small always-on Fly app, `econ-data-web`, kept separate
so `cron.yml` never mistakes it for the data machine. Cloudflare was considered
and rejected as an extra vendor and signup.

The service has three jobs:

1. **Button API:** authenticate, record a request, start the data machine.
2. **Status API:** report progress and per-series verdicts to the page.
3. **Watchdog:** every few minutes, find releases that are due but uncaptured
   and start the same job.

If the service is unreachable, the page itself reports "Can't reach Fly."

## Click flow

1. Click Refresh on a series or group, or "Everything due" on a stale banner.
2. The service writes a `refresh_requests` row and starts the machine.
3. The job heartbeats its stage to Postgres. No heartbeat for ~2 minutes means
   the panel shows STALLED and the last stage reached.
4. Only what's needed runs. A series expands to its whole FRED release using
   `series_release`. If nothing is new, stop with no render and no LLM calls.
5. Done means live: confirm the published page carries the new build stamp.

Stages: requested, machine started, fetching, saved, rendering, pushed, live.

## Verdicts

| Verdict | Meaning |
|---|---|
| CAPTURED | New value stored, shown in the panel before the page rebuilds |
| NOT_PUBLISHED | Source hasn't updated since our last capture; retry later |
| NOTHING_DUE | Already have the latest; shows the next scheduled release |
| SOURCE_ERROR | Fetch failed; shows the actual error |
| REJECTED | Sanity guard dropped the value (the Case-Shiller case) |
| NEEDS_MANUAL | Source relies on a manual import, so the cloud can't get it |
| SYSTEM_ERROR | Machine didn't start, or a render/push step failed |

## Rules

- **Access:** the page is public, so the button appears only in a browser holding
  a key stored once on that device. The key never goes in the repo, the service
  checks it on every request, and status output is behind the same key.
- **One run at a time:** a Postgres advisory lock; later clicks queue and the
  running job drains the queue. Test what a late cron's `machine update` does to
  a running machine, since that is a live risk today.
- **Retiring GitHub crons:** only after the watchdog has a proven record. Until
  then they stay as a backstop.

## Build order

### Stage 0: prerequisite fixes (done first)

- Save observations before marking a release `CAPTURED`, so a crash can't lose a
  release.
- Run the pipeline and DB sessions in ET, so "today" and date stamps are right
  after 7 PM ET. Write the Sheets manifest in explicit UTC, which that change
  would otherwise break.
- Reconcile the schedule: close rows superseded by later data, list one overdue
  row per series, and don't flag rows from a calendar denser than the series.
- Record `source_last_updated` at capture.

### Stage 1: single-series button

`econ-data-web`, `refresh_requests`, targeted job, status panel, verdicts.

### Stage 2: group and "Everything due"

Group refresh, stale-data banner, and queue draining.

### Stage 3: watchdog

Automatic triggering, a time-to-capture metric on the page, then retire the
GitHub crons.

## Definition of done

Force each failure and confirm the panel reports it correctly: not published,
source error, machine won't start, double-click, cron landing mid-run. After
launch, the page's time-to-capture numbers show whether it keeps working.

## Open items found during Stage 0

- Redfin has no new data since 2026-05-31 (known redesign; see fetch errors).
- Altos and Xactus have no new periods since 2026-07-03.
- Conference Board consumer confidence errored on 2026-11-06.
