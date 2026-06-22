-- =============================================================================
-- BDTProject — Gold Layer Schema (TimescaleDB)
-- =============================================================================
-- Tables and hypertables that power the outage intelligence dashboard.
--
-- Design principles:
--   1. Raw signal values stored here, NOT pre-aggregated Spark output.
--      TimescaleDB continuous aggregates handle roll-ups at query time so the
--      Gold Spark job is an idempotent UPSERT, not a destructive overwrite.
--   2. Three core tables matching the PDF's analytical goals:
--        asn_baselines    — RIPE: per-ASN hourly RTT/loss aggregates
--        ioda_signals     — IODA: per-datasource raw 5/10-min signal values
--        outage_events    — correlated outage detections (RIPE + IODA combined)
--   3. Two support tables:
--        asn_names        — static lookup (ASN → ISP name/country)
--        country_coverage — per-country probe counts for data quality UI
--   4. Continuous aggregates for daily roll-ups (avoid re-scanning all rows
--      in the dashboard for wide time windows).
--   5. Indexes tuned to the dashboard's actual query patterns.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Extension (already present in timescale/timescaledb image)
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- 1. ASN BASELINES — RIPE Atlas hourly RTT and packet loss per ASN
-- ---------------------------------------------------------------------------
-- Populated by gold_batch.py from silver/ripe/ping/.
-- Primary analytical table for the ISP ranking and time-series tabs.
-- Stored at hourly granularity; finer-grained data stays in Silver.
--
-- Key design choices vs. original:
--   + rtt_p10_ms / rtt_p90_ms  — inter-quartile spread signals congestion
--   + loss_median_pct           — median loss (less skewed than P95 alone)
--   + icmp_filtered_count       — how many probes had ICMP rate-limiting
--   + probe_count               — distinct anonymised probes (coverage quality)
--   + root_server_count         — how many distinct DNS root servers covered
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asn_baselines (
    time_window          TIMESTAMPTZ       NOT NULL,
    country_code         TEXT              NOT NULL,
    asn                  INTEGER           NOT NULL,
    -- RTT percentiles (ms)
    rtt_p10_ms           DOUBLE PRECISION,   -- low-end baseline
    rtt_median_ms        DOUBLE PRECISION,   -- headline figure
    rtt_p90_ms           DOUBLE PRECISION,   -- degradation indicator
    -- Packet loss
    loss_median_pct      DOUBLE PRECISION,   -- typical loss rate
    loss_p95_pct         DOUBLE PRECISION,   -- tail loss (95th pct)
    -- Coverage quality
    total_measurements   INTEGER,
    probe_count          INTEGER,            -- distinct probes (hashed)
    root_server_count    INTEGER,            -- distinct msm_ids covered
    icmp_filtered_count  INTEGER             -- ICMP-filtered probe count
);

SELECT create_hypertable(
    'asn_baselines', 'time_window',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '7 days'
);

-- Unique index required for ON CONFLICT (time_window, country_code, asn) upsert.
-- TimescaleDB requires unique indexes to include the partitioning column (time_window).
CREATE UNIQUE INDEX IF NOT EXISTS idx_asn_baselines_upsert_key
    ON asn_baselines (time_window, country_code, asn);

-- Indexes for the dashboard WHERE patterns
CREATE INDEX IF NOT EXISTS idx_asn_baselines_country_time
    ON asn_baselines (country_code, time_window DESC);
CREATE INDEX IF NOT EXISTS idx_asn_baselines_asn_time
    ON asn_baselines (asn, time_window DESC);


-- ---------------------------------------------------------------------------
-- 2. IODA SIGNALS — raw per-datasource signal values (5 / 10-min resolution)
-- ---------------------------------------------------------------------------
-- Populated by gold_batch.py from silver/ioda/signals/.
-- Stores raw signal scores at native IODA step resolution (300 s or 600 s),
-- NOT averaged to hourly — hourly roll-ups are handled by the continuous
-- aggregate below so we don't lose the shape of short outage dips.
--
-- Key design choices vs. original:
--   + signal_min / signal_max   — detect sharp within-hour dips
--   + sample_count              — how many raw rows contributed
--   + step_seconds              — native IODA cadence (300 or 600 s)
--   Collection-gap semantics: value IS NULL AND collection_gap = TRUE
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ioda_signals (
    time_bucket          TIMESTAMPTZ       NOT NULL,   -- floored to step_seconds
    country_code         TEXT              NOT NULL,
    datasource           TEXT              NOT NULL,   -- bgp / merit-nt / ping-slash24
    signal_value         DOUBLE PRECISION,
    signal_min           DOUBLE PRECISION,
    signal_max           DOUBLE PRECISION,
    sample_count         INTEGER,
    collection_gap       BOOLEAN           NOT NULL DEFAULT FALSE,
    step_seconds         INTEGER           NOT NULL DEFAULT 300
);

SELECT create_hypertable(
    'ioda_signals', 'time_bucket',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '7 days'
);

CREATE INDEX IF NOT EXISTS idx_ioda_signals_country_ds_time
    ON ioda_signals (country_code, datasource, time_bucket DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ioda_signals_upsert_key
    ON ioda_signals (country_code, datasource, time_bucket);


-- ---------------------------------------------------------------------------
-- 3. OUTAGE EVENTS — correlated outage detections
-- ---------------------------------------------------------------------------
-- Populated by gold_batch.py's correlation pass.
-- Each row is ONE candidate outage event for ONE country, combining evidence
-- from RIPE (RTT/loss spike) and IODA (signal drop across datasources).
-- This is the table that the "Combined view" tab and the PDF's "event
-- detection" goal are built on.
--
-- Confidence scoring logic (see gold_batch.py):
--   ripe_evidence      = 1 if loss_p95_pct > threshold else 0
--   bgp_evidence       = 1 if bgp_signal_pct_drop > 5%    else 0
--   merit_evidence     = 1 if merit_signal_pct_drop > 10%  else 0
--   ping_evidence      = 1 if ping_signal_pct_drop > 10%   else 0
--   confidence_score   = weighted sum of above (0.0 – 1.0)
--   severity           = 'hard_outage' / 'degraded' / 'possible' / 'noise'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outage_events (
    event_id             BIGSERIAL,
    detected_at          TIMESTAMPTZ       NOT NULL,   -- start of the window
    country_code         TEXT              NOT NULL,
    -- RIPE evidence
    ripe_loss_p95        DOUBLE PRECISION,
    ripe_rtt_p90_ms      DOUBLE PRECISION,
    ripe_probe_count     INTEGER,
    ripe_asn_affected    INTEGER,           -- distinct ASNs with loss > threshold
    -- IODA evidence (pct change from rolling 24-h baseline)
    bgp_pct_change       DOUBLE PRECISION,  -- negative = withdrawal
    merit_pct_change     DOUBLE PRECISION,
    ping_pct_change      DOUBLE PRECISION,
    -- Derived
    confidence_score     DOUBLE PRECISION  NOT NULL,
    severity             TEXT              NOT NULL,   -- hard_outage / degraded / possible / noise
    PRIMARY KEY (event_id, detected_at)
);

SELECT create_hypertable(
    'outage_events', 'detected_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '30 days'
);

CREATE INDEX IF NOT EXISTS idx_outage_events_country_time
    ON outage_events (country_code, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_outage_events_severity
    ON outage_events (severity, detected_at DESC);
-- Unique index for ON CONFLICT (detected_at, country_code) upsert dedup.
CREATE UNIQUE INDEX IF NOT EXISTS idx_outage_events_upsert_key
    ON outage_events (detected_at, country_code);


-- ---------------------------------------------------------------------------
-- 4. ASN NAMES — static ISP name lookup
-- ---------------------------------------------------------------------------
-- Populated by populate_asn_names.py (Cymru bulk WHOIS).
-- Referenced by asn_baselines queries via LEFT JOIN.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asn_names (
    asn       INTEGER PRIMARY KEY,
    name      TEXT,
    country   TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- ---------------------------------------------------------------------------
-- 5. COUNTRY COVERAGE — daily probe / measurement count per country
-- ---------------------------------------------------------------------------
-- Light-weight quality-of-data table so the dashboard can warn when a
-- country has unusually few probes on a given day (e.g. IT missing from
-- the sample file above because no IPv4 probes were collected).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS country_coverage (
    coverage_date   DATE         NOT NULL,
    country_code    TEXT         NOT NULL,
    source          TEXT         NOT NULL,  -- 'ripe' or 'ioda'
    measurement_count  INTEGER,
    probe_count        INTEGER,
    asn_count          INTEGER,
    PRIMARY KEY (coverage_date, country_code, source)
);


-- =============================================================================
-- CONTINUOUS AGGREGATES (TimescaleDB-native roll-ups)
-- =============================================================================
-- These replace the "pre-aggregate in Spark" approach. TimescaleDB maintains
-- them incrementally — no reprocessing old data each time a new day lands.

-- ---------------------------------------------------------------------------
-- 5a. Hourly IODA signal average (drives the IODA tab)
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS ioda_signals_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 hour', time_bucket)  AS hour,
    country_code,
    datasource,
    AVG(signal_value)                             AS signal_avg,
    MIN(signal_min)                               AS signal_min,
    MAX(signal_max)                               AS signal_max,
    SUM(sample_count)                             AS sample_count,
    BOOL_OR(collection_gap)                       AS any_gap
FROM ioda_signals
GROUP BY 1, country_code, datasource
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'ioda_signals_hourly',
    start_offset  => INTERVAL '3 days',
    end_offset    => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ---------------------------------------------------------------------------
-- 5b. Daily ASN baseline summary (drives wide-range ISP ranking queries)
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS asn_baselines_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', time_window)    AS day,
    country_code,
    asn,
    AVG(rtt_median_ms)                            AS rtt_median_ms,
    MAX(rtt_p90_ms)                               AS rtt_p90_ms,
    AVG(loss_median_pct)                          AS loss_median_pct,
    MAX(loss_p95_pct)                             AS loss_p95_pct,
    SUM(total_measurements)                       AS total_measurements,
    MAX(probe_count)                              AS peak_probe_count
FROM asn_baselines
GROUP BY 1, country_code, asn
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'asn_baselines_daily',
    start_offset  => INTERVAL '7 days',
    end_offset    => INTERVAL '1 day',
    schedule_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);


-- =============================================================================
-- COMPRESSION POLICIES (keep DB size manageable)
-- =============================================================================
-- Compress chunks older than 7 days — old telemetry is rarely queried at
-- raw resolution; the continuous aggregates cover historical analysis.

SELECT add_compression_policy('asn_baselines',  INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('ioda_signals',   INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('outage_events',  INTERVAL '30 days', if_not_exists => TRUE);

-- Columnar compression settings for the two hot tables
ALTER TABLE asn_baselines   SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'country_code, asn'
);
ALTER TABLE ioda_signals    SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'country_code, datasource'
);