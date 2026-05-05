-- Postgres schema for econ-data.
-- Idempotent: safe to re-run.
-- Apply via:
--   psql "$DATABASE_URL" -f schema.sql
-- (or run schema.sql through psycopg if no psql client is installed)

CREATE TABLE IF NOT EXISTS observations (
    series_id   TEXT             NOT NULL,
    name        TEXT             NOT NULL,
    date        DATE             NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    captured_at TIMESTAMPTZ,
    PRIMARY KEY (series_id, date)
);
CREATE INDEX IF NOT EXISTS observations_date_idx     ON observations (date);
CREATE INDEX IF NOT EXISTS observations_captured_idx ON observations (captured_at);

CREATE TABLE IF NOT EXISTS calculated (
    series_id   TEXT             NOT NULL,
    calc_type   TEXT             NOT NULL,
    date        DATE             NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (series_id, calc_type, date)
);
CREATE INDEX IF NOT EXISTS calculated_series_date_idx ON calculated (series_id, date);

CREATE TABLE IF NOT EXISTS groups (
    group_id TEXT PRIMARY KEY,
    name     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS group_members (
    group_id  TEXT NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    series_id TEXT NOT NULL,
    PRIMARY KEY (group_id, series_id)
);

CREATE TABLE IF NOT EXISTS export_log (
    export_key  TEXT        PRIMARY KEY,
    last_date   DATE        NOT NULL,
    exported_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fetch_log (
    series_id    TEXT PRIMARY KEY,
    last_checked DATE NOT NULL
);

-- Tracks FRED series that failed in the most recent fetch attempt.
-- Morning run inserts on error / deletes on success; intraday run reads
-- this table to decide which series to retry.
CREATE TABLE IF NOT EXISTS fetch_errors (
    series_id  TEXT        PRIMARY KEY,
    errored_at TIMESTAMPTZ NOT NULL,
    error      TEXT
);

CREATE TABLE IF NOT EXISTS revisions (
    series_id   TEXT             NOT NULL,
    date        DATE             NOT NULL,
    old_value   DOUBLE PRECISION NOT NULL,
    new_value   DOUBLE PRECISION NOT NULL,
    pct_change  DOUBLE PRECISION NOT NULL,
    detected_at TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (series_id, date, detected_at)
);
CREATE INDEX IF NOT EXISTS revisions_detected_idx ON revisions (detected_at);

CREATE TABLE IF NOT EXISTS expectations (
    series_id    TEXT             NOT NULL,
    period       TEXT             NOT NULL,  -- mixed format: YYYY-MM or ISO date
    expected     DOUBLE PRECISION,            -- nullable
    compare_type TEXT             NOT NULL,
    source_text  TEXT,
    fetched_at   TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (series_id, period)
);

CREATE TABLE IF NOT EXISTS fed_expectations (
    meeting_date DATE             NOT NULL,
    outcome_bps  INTEGER          NOT NULL,
    probability  DOUBLE PRECISION NOT NULL,
    captured_at  TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (meeting_date, outcome_bps)
);

CREATE TABLE IF NOT EXISTS release_calendar (
    release_date DATE        NOT NULL,
    report       TEXT        NOT NULL,
    series_ids   TEXT        NOT NULL,  -- comma-separated
    confirmed    BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (release_date, report)
);

-- Per-series expected releases. One row per (series_id, scheduled_release) —
-- the date the source intends to publish a new observation. period_end isn't
-- the PK because (a) we often don't know the exact period the release will
-- cover until it lands, and (b) sources occasionally combine periods (e.g.
-- Census published Feb+Mar new home sales together on 2026-05-05 after a
-- delay). Captured_period_end records what actually arrived.
--
-- Status transitions:
--   PENDING  → CAPTURED  when a new obs arrives (or FRED last_updated advances)
--   PENDING  → OVERDUE   when scheduled_release + grace_days < today and no data
--   OVERDUE  → CAPTURED  when late data finally arrives
--   OVERDUE  → DELAYED   when a human acknowledges the delay (gov shutdown etc.);
--                        sweep skips DELAYED so alerts stop firing
CREATE TABLE IF NOT EXISTS release_schedule (
    series_id           TEXT        NOT NULL,
    scheduled_release   DATE        NOT NULL,
    expected_time_et    TIME,
    grace_days          INTEGER     NOT NULL DEFAULT 3,
    status              TEXT        NOT NULL DEFAULT 'PENDING',
    captured_at         TIMESTAMPTZ,
    captured_period_end DATE,
    source_last_updated TIMESTAMPTZ,
    notes               TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (series_id, scheduled_release),
    CHECK (status IN ('PENDING', 'CAPTURED', 'OVERDUE', 'DELAYED'))
);
CREATE INDEX IF NOT EXISTS release_schedule_status_idx
    ON release_schedule (status, scheduled_release);
CREATE INDEX IF NOT EXISTS release_schedule_due_idx
    ON release_schedule (scheduled_release) WHERE status IN ('PENDING','OVERDUE');

-- Maps each FRED series to its release_id (FRED's grouping for batch
-- publications like "Employment Situation" or "New Residential Sales").
-- Populated once via fred/series/release; cached forever (release_id is
-- stable). Used by the calendar refresher to pull schedules in bulk.
CREATE TABLE IF NOT EXISTS series_release (
    series_id  TEXT        PRIMARY KEY,
    release_id INTEGER     NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS series_release_release_idx
    ON series_release (release_id);
