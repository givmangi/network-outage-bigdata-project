"""
gold_batch.py — Gold Layer Aggregation (Redesigned)
====================================================
Reads clean Silver Parquet data and writes to TimescaleDB Gold tables.

Pipeline stages (in order):
  1. RIPE → asn_baselines      (hourly RTT/loss per ASN per country)
  2. IODA → ioda_signals       (raw 5/10-min signal values per country/datasource)
  3. Correlation pass          → outage_events (RIPE + IODA evidence joined)
  4. Coverage audit            → country_coverage (data quality per day)

Key design differences from the original gold_batch.py:
  - UPSERT (INSERT … ON CONFLICT DO UPDATE) via a staging table pattern
    instead of mode='overwrite', which was dropping the entire table each run.
  - RIPE: richer aggregates — P10/P50/P90 RTT, median + P95 loss,
    probe_count, root_server_count, icmp_filtered_count.
  - IODA: stored at native resolution (step_seconds), not averaged away.
    The continuous aggregate in TimescaleDB handles hourly roll-ups.
  - Outage correlation pass: joins RIPE and IODA evidence to produce
    outage_events rows with a confidence score and severity label.
  - All JDBC writes go to a _staging table first, then an explicit UPSERT
    merges into the real table — safe for incremental / re-running jobs.

Usage (via docker compose):
    docker compose run --rm --no-deps spark-gold
    docker compose run --rm --no-deps spark-gold --start 2026-06-01 --end 2026-06-21
    docker compose run --rm --no-deps spark-gold --datasets ripe ioda outages coverage
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from textwrap import dedent

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

S3_ENDPOINT   = os.environ["S3_ENDPOINT_URL"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]

DB_USER = os.environ["TIMESCALEDB_USER"]
DB_PASS = os.environ["TIMESCALEDB_PASSWORD"]
DB_URL  = "jdbc:postgresql://timescaledb:5432/outage_intelligence"
DB_PROPS = {
    "user":       DB_USER,
    "password":   DB_PASS,
    "driver":     "org.postgresql.Driver",
    "stringtype": "unspecified",
    "batchsize":  "3000",  
}

# Thresholds for outage correlation
LOSS_OUTAGE_THRESHOLD    = 0.20   # 20% packet loss → RIPE evidence
LOSS_DEGRADED_THRESHOLD  = 0.10   # 10% packet loss → possible
BGP_DROP_THRESHOLD       = 0.05   # 5% BGP signal drop → evidence
MERIT_DROP_THRESHOLD     = 0.10   # 10% darknet drop → evidence
PING_DROP_THRESHOLD      = 0.10   # 10% active-ping drop → evidence

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("gold_batch")


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("gold_batch")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.endpoint",          S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.files.ignoreMissingFiles",    "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# JDBC helpers
# ---------------------------------------------------------------------------

def _jdbc_read(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.jdbc(url=DB_URL, table=table, properties=DB_PROPS)


def _run_sql(spark: SparkSession, sql: str) -> None:
    """
    Execute arbitrary SQL against TimescaleDB using the PostgreSQL JDBC driver
    that is already on the JVM classpath (loaded by spark-submit --packages).

    Uses Py4J to call java.sql.DriverManager directly — no psql binary needed,
    no extra Python packages needed. Works in any apache/spark image.
    """
    jvm   = spark._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user",     DB_USER)
    props.setProperty("password", DB_PASS)
    conn = jvm.java.sql.DriverManager.getConnection(DB_URL, props)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        # No conn.commit() — JDBC opens in autoCommit=true by default;
        # calling commit() explicitly raises PSQLException.
    finally:
        conn.close()

def _upsert_via_staging(df, staging_table, target_table, upsert_sql, spark):
    log.info("Writing to staging %s …", staging_table)
    df.coalesce(2).write.jdbc(
        url=DB_URL,
        table=staging_table,
        mode="overwrite",
        properties=DB_PROPS,
    )
    _run_sql(spark, upsert_sql)
    _run_sql(spark, f"DROP TABLE IF EXISTS {staging_table}") 
    #deleted double computation, very slow processing
    count = spark.read.jdbc(DB_URL, f"(SELECT count(*) AS n FROM {target_table}) t", properties=DB_PROPS).collect()[0][0]
    log.info("Upserted into %s (total rows now: %d).", target_table, count)
    return 0   # return 0 since we no longer track rows-this-run separately


# ---------------------------------------------------------------------------
# Stage 1: RIPE → asn_baselines
# ---------------------------------------------------------------------------

def run_ripe_baselines(spark: SparkSession, date_filter: str | None) -> int:
    """
    Aggregate Silver RIPE ping data into hourly ASN baselines.

    Improvements over original:
    - Filter rtt_avg_ms > 0 (removes -1.0 sentinel) AND packet_loss IS NOT NULL
    - Compute P10/P50/P90 RTT, median packet loss, P95 packet loss
    - Count distinct probe_id_hash and msm_id per window
    - Count icmp_filtered rows per window
    """
    base = f"s3a://{SILVER_BUCKET}/ripe/ping"
    glob = f"{base}/year=*/month=*/day=*"
    if date_filter:
        glob = f"{base}/{date_filter}"

    log.info("[ripe_baselines] reading from %s", glob)
    try:
        df = spark.read.option("basePath", base).parquet(glob)
    except Exception as e:
        log.warning("[ripe_baselines] could not read Silver: %s", e)
        return 0

    if "asn" not in df.columns:
        log.warning("[ripe_baselines] 'asn' column missing — filling NULL")
        df = df.withColumn("asn", F.lit(None).cast(IntegerType()))

    # Filter out failed measurements (sentinel -1.0 and null loss rows)
    df = (
        df
        .filter(F.col("rtt_avg_ms") > 0)
        .filter(F.col("packet_loss").isNotNull())
        .filter(F.col("asn").isNotNull())
    )

    baselines = (
        df
        .withColumn("time_window", F.date_trunc("hour", F.col("ts_utc")))
        .groupBy("time_window", "country_code", "asn")
        .agg(
            F.expr("percentile_approx(rtt_avg_ms, 0.10)").cast(DoubleType()).alias("rtt_p10_ms"),
            F.expr("percentile_approx(rtt_avg_ms, 0.50)").cast(DoubleType()).alias("rtt_median_ms"),
            F.expr("percentile_approx(rtt_avg_ms, 0.90)").cast(DoubleType()).alias("rtt_p90_ms"),
            F.expr("percentile_approx(packet_loss, 0.50)").cast(DoubleType()).alias("loss_median_pct"),
            F.expr("percentile_approx(packet_loss, 0.95)").cast(DoubleType()).alias("loss_p95_pct"),
            F.count("*").cast(IntegerType()).alias("total_measurements"),
            F.countDistinct("probe_id_hash").cast(IntegerType()).alias("probe_count"),
            F.countDistinct("msm_id").cast(IntegerType()).alias("root_server_count"),
            F.sum(F.col("icmp_filtered").cast(IntegerType())).cast(IntegerType()).alias("icmp_filtered_count"),
        )
    )

    upsert_sql = dedent("""
        INSERT INTO asn_baselines
            (time_window, country_code, asn,
             rtt_p10_ms, rtt_median_ms, rtt_p90_ms,
             loss_median_pct, loss_p95_pct,
             total_measurements, probe_count, root_server_count, icmp_filtered_count)
        SELECT
            time_window, country_code, asn,
            rtt_p10_ms, rtt_median_ms, rtt_p90_ms,
            loss_median_pct, loss_p95_pct,
            total_measurements, probe_count, root_server_count, icmp_filtered_count
        FROM asn_baselines_staging
        ON CONFLICT (time_window, country_code, asn)
        DO UPDATE SET
            rtt_p10_ms          = EXCLUDED.rtt_p10_ms,
            rtt_median_ms       = EXCLUDED.rtt_median_ms,
            rtt_p90_ms          = EXCLUDED.rtt_p90_ms,
            loss_median_pct     = EXCLUDED.loss_median_pct,
            loss_p95_pct        = EXCLUDED.loss_p95_pct,
            total_measurements  = EXCLUDED.total_measurements,
            probe_count         = EXCLUDED.probe_count,
            root_server_count   = EXCLUDED.root_server_count,
            icmp_filtered_count = EXCLUDED.icmp_filtered_count;
    """)
    # Note: TimescaleDB hypertables don't support ON CONFLICT with compressed
    # chunks, but the DDL marks no unique constraint here — rely on staging
    # overwrite + merge pattern. For incremental runs, add a unique index on
    # (time_window, country_code, asn) to enable DO UPDATE.

    return _upsert_via_staging(
        baselines, "asn_baselines_staging", "asn_baselines", upsert_sql, spark
    )


# ---------------------------------------------------------------------------
# Stage 2: IODA → ioda_signals
# ---------------------------------------------------------------------------

def run_ioda_signals(spark: SparkSession, date_filter: str | None) -> int:
    """
    Write IODA signal values at native step resolution.

    Improvements over original:
    - Stores min/max per time_bucket so hourly continuous aggregate can track
      within-hour dips (key for short outages that avg() would hide).
    - UNIQUE index on (country_code, datasource, time_bucket) enables real
      UPSERT without duplication.
    - step_seconds preserved so queries know native cadence.
    """
    base = f"s3a://{SILVER_BUCKET}/ioda/signals"
    glob = f"{base}/year=*/month=*/day=*"
    if date_filter:
        glob = f"{base}/{date_filter}"

    log.info("[ioda_signals] reading from %s", glob)
    try:
        df = spark.read.option("basePath", base).parquet(glob)
    except Exception as e:
        log.warning("[ioda_signals] could not read Silver: %s", e)
        return 0

    signals = (
        df
        .filter(F.col("ts_utc").isNotNull())
        .filter(F.col("entity_code").isNotNull())
        # Bucket to native step to reduce row count while preserving resolution
        .withColumn(
            "time_bucket",
            F.to_timestamp(
                (F.unix_timestamp(F.col("ts_utc")) /
                 F.col("step").cast(DoubleType()) *
                 F.col("step").cast(DoubleType())).cast("long")
            )
        )
        .groupBy("time_bucket", "entity_code", "datasource", "step")
        .agg(
            F.avg("value").cast(DoubleType()).alias("signal_value"),
            F.min("value").cast(DoubleType()).alias("signal_min"),
            F.max("value").cast(DoubleType()).alias("signal_max"),
            F.count("*").cast(IntegerType()).alias("sample_count"),
            F.max(F.col("collection_gap").cast(IntegerType())).cast("boolean").alias("collection_gap"),
        )
        .withColumnRenamed("entity_code", "country_code")
        .withColumnRenamed("step", "step_seconds")
        .withColumn("collection_gap",
            F.when(F.col("signal_value").isNull(), F.lit(True))
             .otherwise(F.col("collection_gap"))
        )
    )

    upsert_sql = dedent("""
        INSERT INTO ioda_signals
            (time_bucket, country_code, datasource,
             signal_value, signal_min, signal_max,
             sample_count, collection_gap, step_seconds)
        SELECT
            time_bucket, country_code, datasource,
            signal_value, signal_min, signal_max,
            sample_count, collection_gap, step_seconds
        FROM ioda_signals_staging
        ON CONFLICT (country_code, datasource, time_bucket)
        DO UPDATE SET
            signal_value   = EXCLUDED.signal_value,
            signal_min     = EXCLUDED.signal_min,
            signal_max     = EXCLUDED.signal_max,
            sample_count   = EXCLUDED.sample_count,
            collection_gap = EXCLUDED.collection_gap;
    """)

    return _upsert_via_staging(
        signals, "ioda_signals_staging", "ioda_signals", upsert_sql, spark
    )


# ---------------------------------------------------------------------------
# Stage 3: Correlation → outage_events
# ---------------------------------------------------------------------------

def run_outage_correlation(spark: SparkSession, date_filter: str | None) -> int:
    """
    Join RIPE baselines and IODA signals to detect and score candidate outages.

    Algorithm (mirrors the PDF's Section 7.3 — control-plane correlation):

    For each (country, hour):
      1. Compute rolling 24-h IODA baseline (avg signal, per datasource).
      2. Compute pct_change vs that baseline for BGP, merit-nt, ping-slash24.
      3. Join with RIPE baselines for the same hour and country.
      4. Score evidence:
           bgp_evidence   = 1 if bgp_pct_change   < -BGP_DROP_THRESHOLD
           merit_evidence = 1 if merit_pct_change  < -MERIT_DROP_THRESHOLD
           ping_evidence  = 1 if ping_pct_change   < -PING_DROP_THRESHOLD
           ripe_evidence  = 1 if loss_p95_pct      > LOSS_OUTAGE_THRESHOLD
      5. confidence_score = weighted sum (BGP 0.35, RIPE 0.35, merit 0.20, ping 0.10)
      6. severity:
           >= 0.70 → hard_outage
           >= 0.45 → degraded
           >= 0.20 → possible
           < 0.20  → noise (not stored)

    Source data: TimescaleDB tables (already populated in stages 1 & 2).
    This keeps Spark from re-reading Silver Parquet for every correlation run.
    """
    log.info("[outage_correlation] reading asn_baselines from TimescaleDB …")

    # Read from DB (data already aggregated in stages 1+2)
    try:
        ripe_df = _jdbc_read(spark, "asn_baselines")
        ioda_df = _jdbc_read(spark, "ioda_signals")
    except Exception as e:
        log.warning("[outage_correlation] could not read Gold tables: %s", e)
        return 0

    if ripe_df.rdd.isEmpty() or ioda_df.rdd.isEmpty():
        log.warning("[outage_correlation] empty source tables — skipping")
        return 0

    # Apply date filter if provided
    if date_filter:
        # date_filter is like "year=2026/month=06/day=19"
        # parse it to a date range
        parts = dict(p.split("=") for p in date_filter.split("/"))
        try:
            y, m, d = int(parts["year"]), int(parts["month"]), int(parts["day"])
            day_start = datetime(y, m, d, tzinfo=timezone.utc)
            day_end   = day_start + timedelta(days=1)
            ripe_df = ripe_df.filter(
                (F.col("time_window") >= F.lit(day_start.isoformat())) &
                (F.col("time_window") <  F.lit(day_end.isoformat()))
            )
            ioda_df = ioda_df.filter(
                (F.col("time_bucket") >= F.lit(day_start.isoformat())) &
                (F.col("time_bucket") <  F.lit(day_end.isoformat()))
            )
        except (KeyError, ValueError) as e:
            log.warning("Could not parse date_filter '%s': %s — using all data", date_filter, e)

    # -----------------------------------------------------------------------
    # RIPE: country-level hourly loss/RTT (aggregate across ASNs)
    # -----------------------------------------------------------------------
    ripe_country = (
        ripe_df
        .withColumn("time_window", F.col("time_window").cast("timestamp"))
        .groupBy("time_window", "country_code")
        .agg(
            F.expr("percentile_approx(loss_p95_pct, 0.95)").alias("ripe_loss_p95"),
            F.expr("percentile_approx(rtt_p90_ms,   0.90)").alias("ripe_rtt_p90_ms"),
            F.sum("probe_count").cast(IntegerType()).alias("ripe_probe_count"),
            F.countDistinct("asn").cast(IntegerType()).alias("ripe_asn_affected_raw"),
        )
    )

    # Count ASNs with loss > threshold
    asn_with_loss = (
        ripe_df
        .filter(F.col("loss_p95_pct") > LOSS_DEGRADED_THRESHOLD)
        .withColumn("time_window", F.col("time_window").cast("timestamp"))
        .groupBy("time_window", "country_code")
        .agg(F.countDistinct("asn").cast(IntegerType()).alias("ripe_asn_affected"))
    )
    ripe_country = ripe_country.join(asn_with_loss, ["time_window", "country_code"], "left")

    # -----------------------------------------------------------------------
    # IODA: compute pct change vs 24-h rolling baseline per datasource/country
    # -----------------------------------------------------------------------
    ioda_hourly = (
        ioda_df
        .withColumn("hour", F.date_trunc("hour", F.col("time_bucket")))
        .groupBy("hour", "country_code", "datasource")
        .agg(F.avg("signal_value").alias("signal_hour_avg"))
    )

    # 24-h rolling window of signal values
    w24 = (
        Window
        .partitionBy("country_code", "datasource")
        .orderBy(F.col("hour").cast("long"))
        .rangeBetween(-24 * 3600, -1)   # previous 24 h, excluding current
    )
    ioda_with_baseline = (
        ioda_hourly
        .withColumn("baseline_24h", F.avg("signal_hour_avg").over(w24))
        .withColumn("pct_change",
            F.when(
                F.col("baseline_24h").isNotNull() & (F.col("baseline_24h") > 0),
                (F.col("signal_hour_avg") - F.col("baseline_24h")) / F.col("baseline_24h")
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
    )

    # Pivot to one row per (hour, country) with columns per datasource
    ioda_pivoted = (
        ioda_with_baseline
        .groupBy("hour", "country_code")
        .agg(
            F.avg(F.when(F.col("datasource") == "bgp",          F.col("pct_change"))).alias("bgp_pct_change"),
            F.avg(F.when(F.col("datasource") == "merit-nt",     F.col("pct_change"))).alias("merit_pct_change"),
            F.avg(F.when(F.col("datasource") == "ping-slash24", F.col("pct_change"))).alias("ping_pct_change"),
        )
    )

    # -----------------------------------------------------------------------
    # Join RIPE + IODA and score
    # -----------------------------------------------------------------------
    joined = (
        ripe_country
        .join(
            ioda_pivoted.withColumnRenamed("hour", "time_window"),
            ["time_window", "country_code"],
            "inner",  # only hours where BOTH sources have data
        )
    )

    # Compute evidence flags and confidence score
    scored = (
        joined
        # RIPE evidence
        .withColumn("ripe_evidence",
            F.when(F.col("ripe_loss_p95") > LOSS_OUTAGE_THRESHOLD, F.lit(1.0))
             .when(F.col("ripe_loss_p95") > LOSS_DEGRADED_THRESHOLD, F.lit(0.5))
             .otherwise(F.lit(0.0))
        )
        # IODA control-plane evidence
        .withColumn("bgp_evidence",
            F.when(F.col("bgp_pct_change") < -BGP_DROP_THRESHOLD, F.lit(1.0))
             .otherwise(F.lit(0.0))
        )
        .withColumn("merit_evidence",
            F.when(F.col("merit_pct_change") < -MERIT_DROP_THRESHOLD, F.lit(1.0))
             .otherwise(F.lit(0.0))
        )
        .withColumn("ping_evidence",
            F.when(F.col("ping_pct_change") < -PING_DROP_THRESHOLD, F.lit(1.0))
             .otherwise(F.lit(0.0))
        )
        # Weighted confidence (BGP highest — control-plane is ground truth)
        .withColumn("confidence_score",
            (F.col("ripe_evidence")  * 0.35 +
             F.col("bgp_evidence")   * 0.35 +
             F.col("merit_evidence") * 0.20 +
             F.col("ping_evidence")  * 0.10)
            .cast(DoubleType())
        )
        # Severity classification
        .withColumn("severity",
            F.when(F.col("confidence_score") >= 0.70, F.lit("hard_outage"))
             .when(F.col("confidence_score") >= 0.45, F.lit("degraded"))
             .when(F.col("confidence_score") >= 0.20, F.lit("possible"))
             .otherwise(F.lit("noise"))
        )
        # Only store actionable events (noise is meaningless clutter)
        .filter(F.col("severity") != "noise")
        .select(
            F.col("time_window").alias("detected_at"),
            F.col("country_code"),
            F.col("ripe_loss_p95"),
            F.col("ripe_rtt_p90_ms"),
            F.col("ripe_probe_count"),
            F.coalesce(F.col("ripe_asn_affected"), F.lit(0)).cast(IntegerType()).alias("ripe_asn_affected"),
            F.col("bgp_pct_change"),
            F.col("merit_pct_change"),
            F.col("ping_pct_change"),
            F.col("confidence_score"),
            F.col("severity"),
        )
    )

    upsert_sql = dedent("""
        INSERT INTO outage_events
            (detected_at, country_code,
             ripe_loss_p95, ripe_rtt_p90_ms, ripe_probe_count, ripe_asn_affected,
             bgp_pct_change, merit_pct_change, ping_pct_change,
             confidence_score, severity)
        SELECT
            detected_at, country_code,
            ripe_loss_p95, ripe_rtt_p90_ms, ripe_probe_count, ripe_asn_affected,
            bgp_pct_change, merit_pct_change, ping_pct_change,
            confidence_score, severity
        FROM outage_events_staging
        ON CONFLICT (detected_at, country_code)
        DO UPDATE SET
            ripe_loss_p95    = EXCLUDED.ripe_loss_p95,
            ripe_rtt_p90_ms  = EXCLUDED.ripe_rtt_p90_ms,
            ripe_probe_count = EXCLUDED.ripe_probe_count,
            ripe_asn_affected= EXCLUDED.ripe_asn_affected,
            bgp_pct_change   = EXCLUDED.bgp_pct_change,
            merit_pct_change = EXCLUDED.merit_pct_change,
            ping_pct_change  = EXCLUDED.ping_pct_change,
            confidence_score = EXCLUDED.confidence_score,
            severity         = EXCLUDED.severity;
    """)

    return _upsert_via_staging(
        scored, "outage_events_staging", "outage_events", upsert_sql, spark
    )


# ---------------------------------------------------------------------------
# Stage 4: Coverage audit
# ---------------------------------------------------------------------------

def run_coverage_audit(spark: SparkSession, date_filter: str | None) -> int:
    """
    Write per-country, per-source measurement counts to country_coverage.
    Used by the dashboard to surface days with low probe coverage.
    """
    total = 0

    for source, base_path, entity_col, id_col in [
        ("ripe", f"s3a://{SILVER_BUCKET}/ripe/ping",    "country_code",  "probe_id_hash"),
        ("ioda", f"s3a://{SILVER_BUCKET}/ioda/signals", "entity_code",   "datasource"),
    ]:
        glob = f"{base_path}/year=*/month=*/day=*"
        if date_filter:
            glob = f"{base_path}/{date_filter}"

        try:
            df = spark.read.option("basePath", base_path).parquet(glob)
        except Exception as e:
            log.warning("[coverage][%s] could not read Silver: %s", source, e)
            continue

        ts_col = "ts_utc" if "ts_utc" in df.columns else "time_bucket"
        asn_col = "asn" if "asn" in df.columns else None

        agg_exprs = [
            F.count("*").cast(IntegerType()).alias("measurement_count"),
            F.countDistinct(id_col).cast(IntegerType()).alias("probe_count"),
        ]
        if asn_col:
            agg_exprs.append(F.countDistinct(asn_col).cast(IntegerType()).alias("asn_count"))
        else:
            agg_exprs.append(F.lit(None).cast(IntegerType()).alias("asn_count"))

        cov = (
            df
            .withColumn("coverage_date", F.to_date(F.col(ts_col)))
            .withColumn("source", F.lit(source))
            .groupBy("coverage_date", F.col(entity_col).alias("country_code"), "source")
            .agg(*agg_exprs)
        )

        upsert_sql = dedent(f"""
            INSERT INTO country_coverage
                (coverage_date, country_code, source,
                 measurement_count, probe_count, asn_count)
            SELECT
                coverage_date, country_code, source,
                measurement_count, probe_count, asn_count
            FROM country_coverage_staging
            ON CONFLICT (coverage_date, country_code, source)
            DO UPDATE SET
                measurement_count = EXCLUDED.measurement_count,
                probe_count       = EXCLUDED.probe_count,
                asn_count         = EXCLUDED.asn_count;
        """)

        total += _upsert_via_staging(
            cov, "country_coverage_staging", "country_coverage", upsert_sql, spark
        )

    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

STAGES = {
    "ripe":     run_ripe_baselines,
    "ioda":     run_ioda_signals,
    "outages":  run_outage_correlation,
    "coverage": run_coverage_audit,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gold Layer Batch Job")
    p.add_argument("--start", default=None,
                   help="Start date inclusive (YYYY-MM-DD), for partition filtering.")
    p.add_argument("--end", default=None,
                   help="End date exclusive (YYYY-MM-DD).")
    p.add_argument("--datasets", nargs="+",
                   choices=list(STAGES),
                   default=list(STAGES),
                   help="Which stages to run. Default: all.")
    return p.parse_args()


def _date_filter(start: datetime, end: datetime) -> list[str]:
    """Return Hive-style partition strings for each day in [start, end)."""
    parts, cursor = [], start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor < end:
        parts.append(f"year={cursor.year:04d}/month={cursor.month:02d}/day={cursor.day:02d}")
        cursor += timedelta(days=1)
    return parts


def main() -> None:
    args = parse_args()
    spark = build_spark()

    # Build date filter list
    date_partitions: list[str | None]
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_str = args.end or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if start >= end:
            log.error("--start must be before --end")
            spark.stop(); sys.exit(1)
        date_partitions = _date_filter(start, end)
        log.info("Processing %d partition(s): %s … %s",
                 len(date_partitions), date_partitions[0], date_partitions[-1])
    else:
        date_partitions = [None]   # None → read all available Silver partitions
        log.info("No date range specified — processing all available Silver data.")

    totals: dict[str, int] = {}

    for stage_name in args.datasets:
        log.info("=== Stage: %s ===", stage_name)
        fn = STAGES[stage_name]
        for date_filter in date_partitions:
            totals[stage_name] = totals.get(stage_name, 0) + fn(spark, date_filter)

    log.info("=" * 60)
    log.info("Gold batch complete. Totals: %s", totals)
    log.info("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()