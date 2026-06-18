"""
silver_streaming.py — Silver Layer Structured Streaming Job
============================================================
Consumes five Kafka topics produced by the Bronze ingestion layer,
applies the same normalisation logic as the batch silver jobs, and
writes micro-batch Parquet files to the Silver MinIO layer in
near-real-time.

Topics consumed:
  raw.ioda.alerts   -> silver/ioda/alerts/   (streaming)
  raw.ioda.events   -> silver/ioda/events/   (streaming)
  raw.ioda.signals  -> silver/ioda/signals/  (streaming)
  raw.ripe.ping     -> silver/ripe/ping/      (streaming)

Design decisions:
  - We use Spark Structured Streaming with Kafka as the source.
  - Each topic gets its own streaming query so failures are isolated.
  - Trigger: ProcessingTime("2 minutes") — a 2-minute micro-batch gives
    a good latency/throughput trade-off for outage detection.
  - Output mode: "append" — Silver is write-once-per-micro-batch.
  - Checkpointing: each query has its own checkpoint dir in MinIO so
    offsets survive restarts without data loss or duplication.
  - PII rules (probe ID hash, drop source IP) are identical to the
    batch job — Silver is always PII-clean.

Checkpoints layout (in Silver bucket):
  silver/_checkpoints/ioda-alerts/
  silver/_checkpoints/ioda-events/
  silver/_checkpoints/ioda-signals/
  silver/_checkpoints/ripe-ping/

Usage:
    spark-submit \\
        --master local[*] \\
        --packages \\
            org.apache.hadoop:hadoop-aws:3.3.4,\\
            com.amazonaws:aws-java-sdk-bundle:1.12.262,\\
            org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \\
        --conf spark.hadoop.fs.s3a.access.key=admin \\
        --conf spark.hadoop.fs.s3a.secret.key=password123 \\
        --conf spark.hadoop.fs.s3a.path.style.access=true \\
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
        spark/silver_streaming.py
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
S3_ENDPOINT     = os.environ.get("S3_ENDPOINT_URL",         "http://minio:9000")
S3_ACCESS_KEY   = os.environ.get("S3_ACCESS_KEY",           "admin")
S3_SECRET_KEY   = os.environ.get("S3_SECRET_KEY",           "password123")
BRONZE_BUCKET   = os.environ.get("S3_BUCKET_BRONZE",        "bronze")
SILVER_BUCKET   = os.environ.get("S3_BUCKET_SILVER",        "silver")

MICRO_BATCH_INTERVAL = os.environ.get("MICRO_BATCH_INTERVAL", "2 minutes")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("silver_streaming")


# ---------------------------------------------------------------------------
# Reusable schemas (mirrors the batch jobs)
# ---------------------------------------------------------------------------

PING_RESULT_ITEM = StructType([
    StructField("rtt", DoubleType(),  True),
    StructField("dup", IntegerType(), True),
    StructField("ttl", IntegerType(), True),
])

RIPE_PING_SCHEMA = StructType([
    StructField("fw",           IntegerType(),              True),
    StructField("mver",         StringType(),               True),
    StructField("msm_id",       IntegerType(),              True),
    StructField("prb_id",       IntegerType(),              True),
    StructField("timestamp",    LongType(),                 True),
    StructField("type",         StringType(),               True),
    StructField("proto",        StringType(),               True),
    StructField("dst_name",     StringType(),               True),
    StructField("dst_addr",     StringType(),               True),
    StructField("from",         StringType(),               True),
    StructField("avg",          DoubleType(),               True),
    StructField("min",          DoubleType(),               True),
    StructField("max",          DoubleType(),               True),
    StructField("sent",         IntegerType(),              True),
    StructField("rcvd",         IntegerType(),              True),
    StructField("dup",          IntegerType(),              True),
    StructField("result",       ArrayType(PING_RESULT_ITEM), True),
    StructField("country_code", StringType(),               True),
    StructField("asn",          IntegerType(),              True), # <--- ADDED THIS!
    StructField("err",          StringType(),               True),
])

IODA_ALERT_SCHEMA = StructType([
    StructField("entityType",      StringType(), True),
    StructField("entityCode",      StringType(), True),
    StructField("datasource",      StringType(), True),
    StructField("condition",       StringType(), True),
    StructField("value",           DoubleType(), True),
    StructField("historicalValue", DoubleType(), True),
    StructField("from",            LongType(),   True),
    StructField("until",           LongType(),   True),
    StructField("level",           StringType(), True),
    StructField("method",          StringType(), True),
])

IODA_EVENT_SCHEMA = StructType([
    StructField("entityType",   StringType(), True),
    StructField("entityCode",   StringType(), True),
    StructField("from",         LongType(),   True),
    StructField("until",        LongType(),   True),
    StructField("score",        DoubleType(), True),
    StructField("overallScore", DoubleType(), True),
    StructField("id",           StringType(), True),
])

IODA_SIGNAL_SCHEMA = StructType([
    StructField("entityType", StringType(),  True),
    StructField("entityCode", StringType(),  True),
    StructField("datasource", StringType(),  True),
    StructField("ts",         LongType(),    True),
    StructField("value",      DoubleType(),  True),
    StructField("step",       IntegerType(), True),
    StructField("nativeStep", IntegerType(), True),
])


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_streaming")
        .config("spark.sql.shuffle.partitions", "8")
        # Kafka
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint",           S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Kafka reader helper
# ---------------------------------------------------------------------------

def _kafka_stream(spark: SparkSession, topic: str):
    """
    Return a streaming DataFrame that reads from a single Kafka topic.
    Each row has the Kafka envelope columns: key, value (bytes), topic,
    partition, offset, timestamp, timestampType.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        # Start from the latest offset when there is no checkpoint.
        # On restart the checkpoint restores the exact committed offset.
        .option("startingOffsets", "latest")
        # Limit how many records are pulled per micro-batch to avoid OOM
        .option("maxOffsetsPerTrigger", 50_000)
        # Deserialise the Kafka key (entity_code / country_code)
        .option("includeHeaders", "false")
        .load()
    )


def _parse_json_value(df, schema: StructType):
    """
    Extract the JSON payload from the Kafka 'value' bytes column using
    the provided schema.  Returns a DataFrame where the JSON fields are
    top-level columns (plus the original Kafka metadata columns).
    """
    return (
        df
        .select(
            # Kafka metadata we want to keep for lineage
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            # Parse the JSON payload
            F.from_json(F.col("value").cast(StringType()), schema).alias("data")
        )
        .select("kafka_topic", "kafka_partition", "kafka_offset", "kafka_ts",
                "data.*")
    )


# ---------------------------------------------------------------------------
# Streaming transforms — one per topic
# ---------------------------------------------------------------------------

def transform_ioda_alerts(raw_df):
    """Normalise a streaming micro-batch of IODA alert records."""
    return (
        raw_df
        .withColumn("ts_from",  F.to_timestamp(F.col("from")))
        .withColumn("ts_until", F.to_timestamp(F.col("until")))
        .withColumn("entity_type",      F.upper(F.col("entityType")))
        .withColumn("entity_code",      F.upper(F.col("entityCode")))
        .withColumn("datasource",       F.lower(F.col("datasource")))
        .withColumn("alert_value",      F.col("value").cast(DoubleType()))
        .withColumn("historical_value", F.col("historicalValue").cast(DoubleType()))
        .drop("from", "until", "entityType", "entityCode", "value", "historicalValue")
        .filter(F.col("entity_code").isNotNull())
        .withColumn("year",  F.year("ts_from"))
        .withColumn("month", F.month("ts_from"))
        .withColumn("day",   F.dayofmonth("ts_from"))
    )


def transform_ioda_events(raw_df):
    """Normalise a streaming micro-batch of IODA event records."""
    return (
        raw_df
        .withColumn("ts_from",    F.to_timestamp(F.col("from")))
        .withColumn("ts_until",   F.to_timestamp(F.col("until")))
        .withColumn("duration_sec", F.col("until") - F.col("from"))
        .withColumn("entity_type",  F.upper(F.col("entityType")))
        .withColumn("entity_code",  F.upper(F.col("entityCode")))
        .withColumn("event_id",     F.col("id"))
        .drop("from", "until", "entityType", "entityCode", "id")
        .filter(F.col("entity_code").isNotNull())
        .withColumn("year",  F.year("ts_from"))
        .withColumn("month", F.month("ts_from"))
        .withColumn("day",   F.dayofmonth("ts_from"))
    )


def transform_ioda_signals(raw_df):
    """Normalise a streaming micro-batch of IODA signal records."""
    return (
        raw_df
        .withColumn("ts_utc",       F.to_timestamp(F.col("ts")))
        .withColumn("entity_type",  F.upper(F.col("entityType")))
        .withColumn("entity_code",  F.upper(F.col("entityCode")))
        .withColumn("datasource",   F.lower(F.col("datasource")))
        .withColumn("collection_gap", F.col("value").isNull())
        .drop("entityType", "entityCode")
        .filter(F.col("entity_code").isNotNull())
        .withColumn("year",  F.year("ts_utc"))
        .withColumn("month", F.month("ts_utc"))
        .withColumn("day",   F.dayofmonth("ts_utc"))
    )


def transform_ripe_ping(raw_df):
    """
    Normalise a streaming micro-batch of RIPE Atlas ping records.
    Mirrors the logic in silver_ripe.py (batch job).
    """
    # RTT from result array (firmware >= 5000 fallback)
    valid_rtts   = F.filter(F.col("result"), lambda r: r["rtt"].isNotNull())
    rtts_arr     = F.transform(valid_rtts, lambda r: r["rtt"])
    arr_size     = F.size(rtts_arr)
    avg_from_arr = F.when(arr_size > 0,
                          F.aggregate(rtts_arr, F.lit(0.0),
                                      lambda acc, x: acc + x,
                                      lambda acc: acc / arr_size)
                          ).otherwise(F.lit(None).cast(DoubleType()))
    min_from_arr = F.when(arr_size > 0,
                          F.aggregate(rtts_arr, F.lit(float("inf")),
                                      lambda acc, x: F.least(acc, x))
                          ).otherwise(F.lit(None).cast(DoubleType()))
    max_from_arr = F.when(arr_size > 0,
                          F.aggregate(rtts_arr, F.lit(float("-inf")),
                                      lambda acc, x: F.greatest(acc, x))
                          ).otherwise(F.lit(None).cast(DoubleType()))

    pkt_sent = F.col("sent").cast(DoubleType())
    pkt_rcvd = F.col("rcvd").cast(DoubleType())

    return (
        raw_df
        # RTT resolution
        .withColumn("rtt_avg_ms", F.coalesce(F.col("avg"), avg_from_arr))
        .withColumn("rtt_min_ms", F.coalesce(F.col("min"), min_from_arr))
        .withColumn("rtt_max_ms", F.coalesce(F.col("max"), max_from_arr))
        # Packet loss
        .withColumn("pkt_sent", pkt_sent)
        .withColumn("pkt_rcvd", pkt_rcvd)
        .withColumn(
            "packet_loss",
            F.when(
                pkt_sent.isNotNull() & (pkt_sent > 0),
                F.greatest(F.lit(0.0),
                           F.least(F.lit(1.0),
                                   F.lit(1.0) - (pkt_rcvd / pkt_sent)))
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        # ICMP rate-limit flag
        .withColumn(
            "icmp_filtered",
            (pkt_sent > 0) & (pkt_rcvd == 0) & F.col("rtt_avg_ms").isNull()
        )
        # Error flag
        .withColumn("has_error", F.col("err").isNotNull())
        # PII anonymisation
        .withColumn("probe_id_hash",
                    F.sha2(F.col("prb_id").cast(StringType()), 256))
        .drop("prb_id", "from", "dst_addr", "avg", "min", "max",
              "sent", "rcvd", "result")
        # Timestamp
        .withColumn("ts_utc",   F.to_timestamp(F.col("timestamp")))
        .withColumn("fw_gen",
                    F.when(F.col("fw") >= 5000, F.lit("v5")).otherwise(F.lit("v4")))
        .filter(F.col("country_code").isNotNull())
        .withColumn("year",  F.year("ts_utc"))
        .withColumn("month", F.month("ts_utc"))
        .withColumn("day",   F.dayofmonth("ts_utc"))
    )


# ---------------------------------------------------------------------------
# Write stream helper
# ---------------------------------------------------------------------------

def _write_stream(
    transformed_df,
    output_path: str,
    checkpoint_path: str,
    query_name: str,
    trigger_interval: str = MICRO_BATCH_INTERVAL,
):
    """
    Write a transformed streaming DataFrame to Parquet in Silver.

    We partition by (year, month, day) to match the batch layer layout.
    This means streaming micro-batches and batch re-runs land in the
    same directory structure — a Gold job can read both transparently.
    """
    return (
        transformed_df
        .writeStream
        .queryName(query_name)
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "day")
        .trigger(processingTime=trigger_interval)
        .start()
    )


# ---------------------------------------------------------------------------
# Main: start all streaming queries
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("Starting Silver Streaming job — Kafka → MinIO Silver")
    log.info("  Kafka: %s", KAFKA_BOOTSTRAP)
    log.info("  Silver bucket: s3a://%s/", SILVER_BUCKET)
    log.info("  Micro-batch interval: %s", MICRO_BATCH_INTERVAL)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    queries = []

    # -----------------------------------------------------------------------
    # Query 1: IODA Alerts
    # -----------------------------------------------------------------------
    raw_alerts = _kafka_stream(spark, "raw.ioda.alerts")
    parsed_alerts = _parse_json_value(raw_alerts, IODA_ALERT_SCHEMA)
    silver_alerts = transform_ioda_alerts(parsed_alerts)

    q_alerts = _write_stream(
        silver_alerts,
        output_path=f"s3a://{SILVER_BUCKET}/ioda/alerts",
        checkpoint_path=f"s3a://{SILVER_BUCKET}/_checkpoints/ioda-alerts",
        query_name="silver-ioda-alerts",
    )
    queries.append(q_alerts)
    log.info("Started query: silver-ioda-alerts")

    # -----------------------------------------------------------------------
    # Query 2: IODA Events
    # -----------------------------------------------------------------------
    raw_events = _kafka_stream(spark, "raw.ioda.events")
    parsed_events = _parse_json_value(raw_events, IODA_EVENT_SCHEMA)
    silver_events = transform_ioda_events(parsed_events)

    q_events = _write_stream(
        silver_events,
        output_path=f"s3a://{SILVER_BUCKET}/ioda/events",
        checkpoint_path=f"s3a://{SILVER_BUCKET}/_checkpoints/ioda-events",
        query_name="silver-ioda-events",
    )
    queries.append(q_events)
    log.info("Started query: silver-ioda-events")

    # -----------------------------------------------------------------------
    # Query 3: IODA Signals
    # -----------------------------------------------------------------------
    raw_signals = _kafka_stream(spark, "raw.ioda.signals")
    parsed_signals = _parse_json_value(raw_signals, IODA_SIGNAL_SCHEMA)
    silver_signals = transform_ioda_signals(parsed_signals)

    q_signals = _write_stream(
        silver_signals,
        output_path=f"s3a://{SILVER_BUCKET}/ioda/signals",
        checkpoint_path=f"s3a://{SILVER_BUCKET}/_checkpoints/ioda-signals",
        query_name="silver-ioda-signals",
    )
    queries.append(q_signals)
    log.info("Started query: silver-ioda-signals")

    # -----------------------------------------------------------------------
    # Query 4: RIPE Atlas Ping
    # -----------------------------------------------------------------------
    raw_ripe = _kafka_stream(spark, "raw.ripe.ping")
    parsed_ripe = _parse_json_value(raw_ripe, RIPE_PING_SCHEMA)
    silver_ripe = transform_ripe_ping(parsed_ripe)

    q_ripe = _write_stream(
        silver_ripe,
        output_path=f"s3a://{SILVER_BUCKET}/ripe/ping",
        checkpoint_path=f"s3a://{SILVER_BUCKET}/_checkpoints/ripe-ping",
        query_name="silver-ripe-ping",
    )
    queries.append(q_ripe)
    log.info("Started query: silver-ripe-ping")

    # -----------------------------------------------------------------------
    # Wait for all queries (blocks until manual kill or exception)
    # -----------------------------------------------------------------------
    log.info("All %d streaming queries active. Awaiting termination…", len(queries))
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Interrupt received — stopping streaming queries gracefully…")
        for q in queries:
            q.stop()
        log.info("All queries stopped.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()