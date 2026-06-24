"""
gold_streaming.py — Gold Layer Structured Streaming Job
========================================================
Reads the Silver Parquet stream (written by silver_streaming.py into MinIO)
and promotes each micro-batch directly into TimescaleDB Gold tables using
a foreachBatch JDBC upsert — no Silver re-scan, no hourly cron needed.

What it does per micro-batch (every GOLD_INTERVAL, default 5 min):
  1. silver/ripe/ping   → asn_baselines    (hourly windows, UPSERT)
  2. silver/ioda/signals → ioda_signals    (native step resolution, UPSERT)
  3. Correlation pass   → outage_events    (RIPE + IODA joined, UPSERT)

Architecture note — why this is different from silver_streaming.py:
  silver_streaming reads from Kafka topics (raw bytes, needs schema parsing).
  gold_streaming reads from the Silver Parquet output in MinIO, which is
  already clean and typed. This means:
    - The Gold streaming job has zero dependency on Kafka.
    - It can be restarted without replaying Kafka offsets.
    - It naturally handles both batch and stream Silver writers, since both
      write to the same MinIO paths.

Trigger: ProcessingTime("5 minutes") — coarser than Silver's 2-minute
micro-batches, giving Silver time to finish writing before Gold reads.

Checkpoints: stored in silver/_checkpoints/gold-{ripe,ioda,outages}/
so each query tracks its own MinIO file offset independently.

Usage (via docker compose):
    docker compose up spark-gold-stream        # always-on service
    docker compose run --rm spark-gold-stream  # one-shot for testing
"""

from __future__ import annotations

import logging
import os
from textwrap import dedent

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
S3_ENDPOINT       = os.environ.get("S3_ENDPOINT_URL",         "http://minio:9000")
S3_ACCESS_KEY     = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY     = os.environ.get("S3_SECRET_KEY")
SILVER_BUCKET     = os.environ.get("S3_BUCKET_SILVER",        "silver")

DB_USER           = os.environ.get("TIMESCALEDB_USER")
DB_PASS           = os.environ.get("TIMESCALEDB_PASSWORD")
DB_HOST           = "timescaledb"
DB_NAME           = "outage_intelligence"
DB_URL            = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"
DB_PROPS          = {
    "user":       DB_USER,
    "password":   DB_PASS,
    "driver":     "org.postgresql.Driver",
    "stringtype": "unspecified",
    "batchsize":  "1000",  
}

GOLD_INTERVAL     = os.environ.get("GOLD_STREAM_INTERVAL", "5 minutes")

# Outage correlation thresholds (match gold_batch.py)
LOSS_OUTAGE_THRESHOLD   = 0.20
LOSS_DEGRADED_THRESHOLD = 0.10
BGP_DROP_THRESHOLD      = 0.05
MERIT_DROP_THRESHOLD    = 0.10
PING_DROP_THRESHOLD     = 0.10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("gold_streaming")


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold_streaming")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint",           S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# JDBC helpers — use Py4J to call the PostgreSQL JDBC driver that is already
# on the JVM classpath (loaded via spark-submit --packages). No psql binary
# or psycopg2 needed inside the apache/spark container.
# ---------------------------------------------------------------------------

def _run_sql(spark: SparkSession, sql: str) -> None:
    """Execute arbitrary SQL via the JDBC driver already on the Spark JVM classpath."""
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


def _upsert_df(spark: SparkSession, df: DataFrame, staging: str, upsert_sql: str) -> int:
    """
    Write df to a staging table (overwrite), merge into the real Gold table
    via upsert_sql, then drop the staging table.
    """
    count = df.count()
    if count == 0:
        return 0
    df.write.jdbc(url=DB_URL, table=staging, mode="overwrite", properties=DB_PROPS)
    _run_sql(spark, upsert_sql)
    _run_sql(spark, f"DROP TABLE IF EXISTS {staging}")
    return count


# ---------------------------------------------------------------------------
# foreachBatch handlers
# ---------------------------------------------------------------------------

def _write_ripe_baselines(micro_batch: DataFrame, batch_id: int) -> None:
    """
    Aggregate one Silver RIPE micro-batch into hourly ASN baselines and
    upsert into asn_baselines.
    """
    if micro_batch.rdd.isEmpty():
        return

    # Ensure asn column exists (older Silver files may lack it)
    if "asn" not in micro_batch.columns:
        micro_batch = micro_batch.withColumn("asn", F.lit(None).cast(IntegerType()))

    # Null-safe filters: remove -1.0 sentinel and null packet_loss rows
    clean = (
        micro_batch
        .filter(F.col("rtt_avg_ms") > 0)
        .filter(F.col("packet_loss").isNotNull())
        .filter(F.col("asn").isNotNull())
        .filter(F.col("country_code").isNotNull())
    )

    if clean.rdd.isEmpty():
        return

    baselines = (
        clean
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
            icmp_filtered_count = EXCLUDED.icmp_filtered_count
    """)

    n = _upsert_df(micro_batch.sparkSession, baselines, "asn_baselines_staging", upsert_sql)
    log.info("[gold/ripe] batch %d → %d baseline rows upserted", batch_id, n)


def _write_ioda_signals(micro_batch: DataFrame, batch_id: int) -> None:
    """
    Bucket one Silver IODA signals micro-batch to native step resolution
    and upsert into ioda_signals.
    """
    if micro_batch.rdd.isEmpty():
        return

    clean = (
        micro_batch
        .filter(F.col("ts_utc").isNotNull())
        .filter(F.col("entity_code").isNotNull())
    )

    if clean.rdd.isEmpty():
        return

    signals = (
        clean
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
            collection_gap = EXCLUDED.collection_gap
    """)

    n = _upsert_df(micro_batch.sparkSession, signals, "ioda_signals_staging", upsert_sql)
    log.info("[gold/ioda] batch %d → %d signal rows upserted", batch_id, n)


def _write_outage_events(ripe_batch: DataFrame, ioda_batch: DataFrame, batch_id: int) -> None:
    """
    Run the outage correlation pass over the current micro-batch windows.

    Strategy: for each country/hour in the current RIPE batch, pull the
    most recent 24-h of IODA signals from TimescaleDB (not from Silver —
    it's already aggregated there), compute pct-change evidence, and
    score the event.

    This avoids needing a Spark streaming join across two streams (which
    requires watermarking and gets complex). Instead we do a point-in-time
    DB lookup per micro-batch, which is fast and correct.
    """
    if ripe_batch.rdd.isEmpty():
        return

    # Compute the time range covered by this RIPE micro-batch
    time_range = ripe_batch.agg(
        F.min("ts_utc").alias("min_ts"),
        F.max("ts_utc").alias("max_ts"),
    ).collect()[0]

    if time_range["min_ts"] is None:
        return

    # Extend range to cover 24-h IODA lookback for baseline
    lookback_start = time_range["min_ts"] - __import__("datetime").timedelta(hours=25)
    batch_end      = time_range["max_ts"]

    # Pull IODA data covering this range from TimescaleDB
    ioda_window_sql = f"""(
        SELECT country_code, datasource, time_bucket, signal_value
        FROM ioda_signals
        WHERE time_bucket >= '{lookback_start.isoformat()}'
          AND time_bucket <= '{batch_end.isoformat()}'
    ) AS ioda_window"""

    try:
        ioda_db = (
            ripe_batch.sparkSession.read
            .jdbc(url=DB_URL, table=ioda_window_sql, properties=DB_PROPS)
        )
    except Exception as e:
        log.warning("[gold/outages] batch %d: could not read IODA from DB: %s", batch_id, e)
        return

    if ioda_db.rdd.isEmpty():
        log.info("[gold/outages] batch %d: no IODA data in window — skipping correlation", batch_id)
        return

    # Aggregate RIPE to country-hour level
    if "asn" not in ripe_batch.columns:
        ripe_batch = ripe_batch.withColumn("asn", F.lit(None).cast(IntegerType()))

    clean_ripe = (
        ripe_batch
        .filter(F.col("rtt_avg_ms") > 0)
        .filter(F.col("packet_loss").isNotNull())
        .filter(F.col("asn").isNotNull())
        .filter(F.col("country_code").isNotNull())
    )
    if clean_ripe.rdd.isEmpty():
        return

    ripe_country = (
        clean_ripe
        .withColumn("time_window", F.date_trunc("hour", F.col("ts_utc")))
        .groupBy("time_window", "country_code")
        .agg(
            F.expr("percentile_approx(packet_loss, 0.95)").alias("ripe_loss_p95"),
            F.expr("percentile_approx(rtt_avg_ms, 0.90)").alias("ripe_rtt_p90_ms"),
            F.countDistinct("probe_id_hash").cast(IntegerType()).alias("ripe_probe_count"),
            #F.sum("probe_count").cast(IntegerType()).alias("ripe_probe_count"),
        )
    )

    asn_with_loss = (
        clean_ripe
        .filter(F.col("packet_loss") > LOSS_DEGRADED_THRESHOLD)
        .withColumn("time_window", F.date_trunc("hour", F.col("ts_utc")))
        .groupBy("time_window", "country_code")
        .agg(F.countDistinct("asn").cast(IntegerType()).alias("ripe_asn_affected"))
    )
    ripe_country = ripe_country.join(asn_with_loss, ["time_window", "country_code"], "left")

    # IODA: compute hourly averages and 24-h rolling baseline
    ioda_hourly = (
        ioda_db
        .withColumn("hour", F.date_trunc("hour", F.col("time_bucket")))
        .groupBy("hour", "country_code", "datasource")
        .agg(F.avg("signal_value").alias("signal_hour_avg"))
    )

    w24 = (
        Window
        .partitionBy("country_code", "datasource")
        .orderBy(F.col("hour").cast("long"))
        .rangeBetween(-24 * 3600, -1)
    )
    ioda_with_pct = (
        ioda_hourly
        .withColumn("baseline_24h", F.avg("signal_hour_avg").over(w24))
        .withColumn("pct_change",
            F.when(
                F.col("baseline_24h").isNotNull() & (F.col("baseline_24h") > 0),
                (F.col("signal_hour_avg") - F.col("baseline_24h")) / F.col("baseline_24h")
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
    )

    ioda_pivoted = (
        ioda_with_pct
        .groupBy("hour", "country_code")
        .agg(
            F.avg(F.when(F.col("datasource") == "bgp",          F.col("pct_change"))).alias("bgp_pct_change"),
            F.avg(F.when(F.col("datasource") == "merit-nt",     F.col("pct_change"))).alias("merit_pct_change"),
            F.avg(F.when(F.col("datasource") == "ping-slash24", F.col("pct_change"))).alias("ping_pct_change"),
        )
    )

    joined = ripe_country.join(
        ioda_pivoted.withColumnRenamed("hour", "time_window"),
        ["time_window", "country_code"],
        "inner",
    )

    scored = (
        joined
        .withColumn("ripe_evidence",
            F.when(F.col("ripe_loss_p95") > LOSS_OUTAGE_THRESHOLD,   F.lit(1.0))
             .when(F.col("ripe_loss_p95") > LOSS_DEGRADED_THRESHOLD,  F.lit(0.5))
             .otherwise(F.lit(0.0))
        )
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
        .withColumn("confidence_score",
            (F.col("ripe_evidence")  * 0.35 +
             F.col("bgp_evidence")   * 0.35 +
             F.col("merit_evidence") * 0.20 +
             F.col("ping_evidence")  * 0.10).cast(DoubleType())
        )
        .withColumn("severity",
            F.when(F.col("confidence_score") >= 0.70, F.lit("hard_outage"))
             .when(F.col("confidence_score") >= 0.45, F.lit("degraded"))
             .when(F.col("confidence_score") >= 0.20, F.lit("possible"))
             .otherwise(F.lit("noise"))
        )
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
            severity         = EXCLUDED.severity
    """)

    n = _upsert_df(ripe_batch.sparkSession, scored, "outage_events_staging", upsert_sql)
    log.info("[gold/outages] batch %d → %d outage event rows upserted", batch_id, n)


# ---------------------------------------------------------------------------
# Streaming reader for Silver Parquet files
# ---------------------------------------------------------------------------

def _silver_stream(spark: SparkSession, path: str, schema):
    """
    Read a Silver Parquet path as a streaming source.
    Uses the 'parquet' format with maxFilesPerTrigger to avoid overwhelming
    the driver on the first read after a long pause.
    """
    return (
        spark.readStream
        .format("parquet")
        .schema(schema)
        .option("path", path)
        .option("maxFilesPerTrigger", 4)
        .load()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("Starting Gold Streaming job")
    log.info("  Trigger interval: %s", GOLD_INTERVAL)
    log.info("  Silver bucket:    s3a://%s/", SILVER_BUCKET)
    log.info("  TimescaleDB:      %s/%s", DB_HOST, DB_NAME)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Infer Silver schemas from a static read (avoids hardcoding schema structs
    # that could drift as Silver evolves — the streaming read uses the same
    # inferred schema for consistency).
    ripe_path  = f"s3a://{SILVER_BUCKET}/ripe/ping"
    ioda_path  = f"s3a://{SILVER_BUCKET}/ioda/signals"
    ckpt_base  = f"s3a://{SILVER_BUCKET}/_checkpoints"

    try:
        ripe_schema = spark.read.parquet(f"{ripe_path}/year=*/month=*/day=*").schema
        log.info("Inferred RIPE Silver schema (%d fields)", len(ripe_schema))
    except Exception as e:
        log.error("Could not infer RIPE Silver schema — is Silver populated? %s", e)
        spark.stop()
        return

    try:
        ioda_schema = spark.read.parquet(f"{ioda_path}/year=*/month=*/day=*").schema
        log.info("Inferred IODA Silver schema (%d fields)", len(ioda_schema))
    except Exception as e:
        log.warning("Could not infer IODA Silver schema — IODA correlation will be skipped: %s", e)
        ioda_schema = None

    # -----------------------------------------------------------------------
    # Query 1: RIPE → asn_baselines  (and outage correlation)
    # -----------------------------------------------------------------------
    ripe_stream = _silver_stream(spark, ripe_path, ripe_schema)

    def ripe_batch_handler(micro_batch: DataFrame, batch_id: int) -> None:
        _write_ripe_baselines(micro_batch, batch_id)
        # Outage correlation also triggered here, using the same RIPE batch
        _write_outage_events(micro_batch, None, batch_id)

    q_ripe = (
        ripe_stream
        .writeStream
        .queryName("gold-ripe-baselines")
        .foreachBatch(ripe_batch_handler)
        .option("checkpointLocation", f"{ckpt_base}/gold-ripe")
        .trigger(processingTime=GOLD_INTERVAL)
        .start()
    )
    log.info("Started query: gold-ripe-baselines")

    queries = [q_ripe]

    # -----------------------------------------------------------------------
    # Query 2: IODA signals → ioda_signals
    # -----------------------------------------------------------------------
    if ioda_schema is not None:
        ioda_stream = _silver_stream(spark, ioda_path, ioda_schema)

        q_ioda = (
            ioda_stream
            .writeStream
            .queryName("gold-ioda-signals")
            .foreachBatch(_write_ioda_signals)
            .option("checkpointLocation", f"{ckpt_base}/gold-ioda")
            .trigger(processingTime=GOLD_INTERVAL)
            .start()
        )
        queries.append(q_ioda)
        log.info("Started query: gold-ioda-signals")
    else:
        log.warning("IODA streaming query not started (schema unavailable).")

    # -----------------------------------------------------------------------
    # Await all queries
    # -----------------------------------------------------------------------
    log.info("All %d Gold streaming queries active. Awaiting termination…", len(queries))
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Interrupt received — stopping Gold streaming queries…")
        for q in queries:
            q.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()