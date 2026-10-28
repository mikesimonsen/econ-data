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

-- Daily/housing analyses are LLM-generated and live in summaries/, which is
-- gitignored — so Fly's fresh-clone runtime can't see prior generations.
-- Persisting them in the DB lets quiet-run firings (no new data) reuse the
-- previously generated analysis without paying for an LLM call.
CREATE TABLE IF NOT EXISTS daily_analyses (
    date_key     DATE        NOT NULL,
    kind         TEXT        NOT NULL,  -- 'daily' or 'housing'
    content      TEXT        NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_key, kind)
);

-- ── Fed Watch (econ_data/fomc.py) ─────────────────────────────────────────
-- Everyone who has sat on the FOMC since 2021 (from vote records) plus the
-- current roster (fomc.htm + Board bios + fed_roster.yaml seed). id is the
-- lowercase last name ("warsh", "paese").
CREATE TABLE IF NOT EXISTS fomc_participants (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    institution    TEXT,             -- 'Board of Governors' or bank city
    role           TEXT,             -- Chairman / Vice Chair / Interim President …
    appointed_by   TEXT,             -- governors: U.S. president; presidents: bank board
    board_term_end DATE,
    chair_term_end DATE,
    bio_url        TEXT,
    bio_text       TEXT,
    verified       BOOLEAN NOT NULL DEFAULT TRUE,  -- FALSE = seed not yet confirmed at source
    active         BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Voting roster per year, captured from fomc.htm while that year is current
-- (the page only ever shows the current year, so this table is the history).
CREATE TABLE IF NOT EXISTS fomc_voters (
    year           INTEGER NOT NULL,
    participant_id TEXT    NOT NULL,
    status         TEXT    NOT NULL,  -- 'member' | 'alternate'
    PRIMARY KEY (year, participant_id)
);

-- Forward bank rotation (fomc.htm table): which banks vote in future years.
CREATE TABLE IF NOT EXISTS fomc_rotation (
    year        INTEGER NOT NULL,
    institution TEXT    NOT NULL,
    status      TEXT    NOT NULL,     -- 'member' | 'alternate'
    PRIMARY KEY (year, institution)
);

CREATE TABLE IF NOT EXISTS fomc_meetings (
    meeting_date    DATE PRIMARY KEY, -- decision (final) day
    start_date      DATE NOT NULL,
    has_sep         BOOLEAN NOT NULL DEFAULT FALSE,
    unscheduled     BOOLEAN NOT NULL DEFAULT FALSE,
    statement_url   TEXT,
    presser_url     TEXT,
    sep_url         TEXT,
    minutes_url     TEXT,
    minutes_release DATE,
    statement_text  TEXT,
    target_low      DOUBLE PRECISION,
    target_high     DOUBLE PRECISION,
    decision_bps    INTEGER,
    votes_for       INTEGER,
    votes_against   INTEGER,
    statement_parsed_at TIMESTAMPTZ,
    sep_parsed_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fomc_votes (
    meeting_date   DATE    NOT NULL,
    participant_id TEXT    NOT NULL,
    vote           TEXT    NOT NULL,  -- 'for' | 'against'
    preferred_bps  INTEGER,           -- dissenter's preferred change; NULL = non-rate dissent
    dissent_text   TEXT,
    inferred       BOOLEAN NOT NULL DEFAULT FALSE,  -- 'for' derived from year roster (post-June-2026 statements name dissenters only)
    PRIMARY KEY (meeting_date, participant_id)
);

-- SEP Table 1: median / central tendency / range by variable and horizon.
CREATE TABLE IF NOT EXISTS sep_projections (
    meeting_date DATE NOT NULL,
    variable     TEXT NOT NULL,       -- gdp | unemployment | pce | core_pce | fed_funds
    horizon      TEXT NOT NULL,       -- '2027' … or 'LR'
    median       DOUBLE PRECISION,
    ct_lo        DOUBLE PRECISION,
    ct_hi        DOUBLE PRECISION,
    range_lo     DOUBLE PRECISION,
    range_hi     DOUBLE PRECISION,
    PRIMARY KEY (meeting_date, variable, horizon)
);

-- SEP Figure 2 (the dot plot): participant count at each rate midpoint.
CREATE TABLE IF NOT EXISTS sep_dots (
    meeting_date DATE             NOT NULL,
    horizon      TEXT             NOT NULL,
    rate         DOUBLE PRECISION NOT NULL,
    n            INTEGER          NOT NULL,
    PRIMARY KEY (meeting_date, horizon, rate)
);
