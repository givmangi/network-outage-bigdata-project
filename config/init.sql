CREATE TABLE IF NOT EXISTS asn_baselines (
    time_window     TIMESTAMPTZ     NOT NULL,
    country_code    TEXT            NOT NULL,
    asn             INTEGER         NOT NULL,
    rtt_median_ms   DOUBLE PRECISION,
    loss_95th_pct   DOUBLE PRECISION,
    total_measurements INTEGER
);

SELECT create_hypertable('asn_baselines', 'time_window', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS ioda_signals (
    time_window     TIMESTAMPTZ     NOT NULL,
    country_code    TEXT            NOT NULL,
    datasource      TEXT            NOT NULL,
    signal_value    DOUBLE PRECISION,
    collection_gap  BOOLEAN
);

SELECT create_hypertable('ioda_signals', 'time_window', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS asn_names (
    asn     INTEGER PRIMARY KEY,
    name    TEXT,
    country TEXT
);