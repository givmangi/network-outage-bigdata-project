"""
silver_ripe.py — RIPE Atlas Silver Layer Batch Job
====================================================
Reads raw NDJSON.GZ files from the Bronze MinIO bucket (s3a://bronze/ripe/...),
applies firmware-version-aware RTT extraction, packet-loss calculation,
ICMP rate-limit detection, and SHA-256 probe ID anonymisation, then writes
columnar Parquet to the Silver MinIO bucket (s3a://silver/ripe/...).

Bronze layout consumed:
  s3a://bronze/ripe/ping/year=YYYY/month=MM/day=DD/measurement_<msm_id>_<ts>.ndjson.gz

Silver layout produced:
  s3a://silver/ripe/ping/year=YYYY/month=MM/day=DD/part-*.parquet

Usage (via docker compose):
    docker compose run --rm --no-deps spark-silver-ripe
    docker compose run --rm --no-deps spark-silver-ripe --start 2026-05-28 --end 2026-06-04
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, FloatType, IntegerType, LongType,
    StringType, StructField, StructType, BooleanType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

S3_ENDPOINT   = os.environ.get("S3_ENDPOINT_URL",   "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY",     "admin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY",     "password123")
BRONZE_BUCKET = os.environ.get("S3_BUCKET_BRONZE",  "bronze")
SILVER_BUCKET = os.environ.get("S3_BUCKET_SILVER",  "silver")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("silver_ripe")

# ---------------------------------------------------------------------------
# Bronze RIPE ping schema
# ---------------------------------------------------------------------------
# We cover both firmware generations with a superset schema.
# Fields present only in one firmware version will be null in the other.
# The 'result' array holds per-packet RTT objects in newer firmware.

PING_RESULT_ITEM = StructType([
    StructField("rtt",  DoubleType(), True),
    StructField("dup",  IntegerType(), True),
    StructField("ttl",  IntegerType(), True),
])

RIPE_PING_SCHEMA = StructType([
    # Core measurement metadata
    StructField("fw",          IntegerType(), True),   # firmware version (e.g. 4610, 5020)
    StructField("mver",        StringType(),  True),   # parser version (fw >= 5000)
    StructField("msm_id",      IntegerType(), True),   # measurement ID
    StructField("prb_id",      IntegerType(), True),   # probe ID (to be hashed)
    StructField("timestamp",   LongType(),    True),   # Unix epoch of the measurement
    StructField("type",        StringType(),  True),   # should always be "ping"
    StructField("proto",       StringType(),  True),   # "ICMP" or "UDP"
    # Target
    StructField("dst_name",    StringType(),  True),
    StructField("dst_addr",    StringType(),  True),   # will be dropped (PII-adjacent)
    StructField("from",        StringType(),  True),   # source IP — DROPPED
    # Aggregate RTT fields (firmware < 5000 style)
    StructField("avg",         DoubleType(),  True),
    StructField("min",         DoubleType(),  True),
    StructField("max",         DoubleType(),  True),
    # Packet counts
    StructField("sent",        IntegerType(), True),
    StructField("rcvd",        IntegerType(), True),
    StructField("dup",         IntegerType(), True),
    # Per-packet results array (firmware >= 5000)
    StructField("result", ArrayType(PING_RESULT_ITEM), True),
    # Injected by the ingester from probe_mapping.json
    StructField("country_code", StringType(), True),
    # Error field (firmware >= 5000)
    StructField("err",         StringType(),  True),
])


# ---------------------------------------------------------------------------
# Spark session factory
# ---------------------------------------------------------------------------

def build_spark(app_name: str = "silver_ripe") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
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
# RTT extraction UDF (firmware-version-aware)
# ---------------------------------------------------------------------------
# For firmware >= 5000 the top-level avg/min/max may be absent; instead
# the 'result' array contains per-packet RTTs. We compute the aggregate
# values from the array if the scalar fields are missing.

def _compute_rtt_from_result(result_col: F.Column) -> tuple[F.Column, F.Column, F.Column]:
    """
    Return (avg_rtt, min_rtt, max_rtt) columns computed from the 'result'
    array column, filtering out null RTTs (timeouts / {"x": "*"} entries).
    """
    valid_rtts = F.filter(result_col, lambda r: r["rtt"].isNotNull())
    rtts       = F.transform(valid_rtts, lambda r: r["rtt"])
    avg_from_arr = F.aggregate(rtts, F.lit(0.0), lambda acc, x: acc + x,
                               lambda acc: acc / F.size(rtts))
    min_from_arr = F.aggregate(rtts, F.lit(float("inf")),
                               lambda acc, x: F.least(acc, x))
    max_from_arr = F.aggregate(rtts, F.lit(float("-inf")),
                               lambda acc, x: F.greatest(acc, x))
    return avg_from_arr, min_from_arr, max_from_arr


# ---------------------------------------------------------------------------
# Main transform
# ---------------------------------------------------------------------------

def process_ping(spark: SparkSession, date_partition: str) -> int:
    """
    Read all RIPE ping NDJSON.GZ files for the given date partition, apply
    normalisation and enrichment, and write Parquet to the Silver layer.
    """
    src = f"s3a://{BRONZE_BUCKET}/ripe/ping/{date_partition}/*.ndjson.gz"
    log.info("[ripe/ping] reading from %s", src)

    try:
        raw: DataFrame = (
            spark.read
            .schema(RIPE_PING_SCHEMA)
            .option("multiline", "false")
            .option("mode", "PERMISSIVE")
            .json(src)
        )
    except Exception as exc:
        log.warning("[ripe/ping] could not read bronze: %s", exc)
        return 0

    if raw.rdd.isEmpty():
        log.info("[ripe/ping] no data for %s — skipping", date_partition)
        return 0

    # -----------------------------------------------------------------------
    # Step 1: Firmware-aware RTT resolution
    # -----------------------------------------------------------------------
    # avg/min/max are present in all firmware versions but may be null in
    # newer firmware when packet loss is 100%.  We fall back to computing
    # them from the 'result' array in that case.
    # -----------------------------------------------------------------------
    avg_arr, min_arr, max_arr = _compute_rtt_from_result(F.col("result"))

    df = (
        raw
        .withColumn("rtt_avg_ms", F.coalesce(F.col("avg"), avg_arr))
        .withColumn("rtt_min_ms", F.coalesce(F.col("min"), min_arr))
        .withColumn("rtt_max_ms", F.coalesce(F.col("max"), max_arr))
    )

    # -----------------------------------------------------------------------
    # Step 2: Packet loss calculation
    # -----------------------------------------------------------------------
    # packet_loss = 1 - (rcvd / sent), clipped to [0.0, 1.0].
    # When sent is null or 0 we use null to avoid division by zero and
    # mark these as data quality issues.
    # -----------------------------------------------------------------------
    df = (
        df
        .withColumn("pkt_sent", F.col("sent").cast(DoubleType()))
        .withColumn("pkt_rcvd", F.col("rcvd").cast(DoubleType()))
        .withColumn(
            "packet_loss",
            F.when(
                (F.col("pkt_sent").isNotNull()) & (F.col("pkt_sent") > 0),
                F.greatest(
                    F.lit(0.0),
                    F.least(
                        F.lit(1.0),
                        F.lit(1.0) - (F.col("pkt_rcvd") / F.col("pkt_sent"))
                    )
                )
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
    )

    # -----------------------------------------------------------------------
    # Step 3: ICMP rate-limiting flag
    # -----------------------------------------------------------------------
    # If packets were sent but nothing was received AND avg RTT is null, the
    # destination host is almost certainly rate-limiting ICMP rather than
    # being genuinely offline.  We flag this so the Gold layer can apply a
    # lower confidence weight when scoring this as an outage.
    # -----------------------------------------------------------------------
    df = df.withColumn(
        "icmp_filtered",
        (
            (F.col("pkt_sent") > 0) &
            (F.col("pkt_rcvd") == 0) &
            F.col("rtt_avg_ms").isNull()
        )
    )

    # -----------------------------------------------------------------------
    # Step 4: Error flag (firmware >= 5000)
    # -----------------------------------------------------------------------
    df = df.withColumn("has_error", F.col("err").isNotNull())

    # -----------------------------------------------------------------------
    # Step 5: PII anonymisation
    # -----------------------------------------------------------------------
    # Hash probe_id with SHA-256 (deterministic, irreversible).
    # Drop source IP entirely.
    # -----------------------------------------------------------------------
    df = (
        df
        .withColumn(
            "probe_id_hash",
            F.sha2(F.col("prb_id").cast(StringType()), 256)
        )
        .drop("prb_id", "from", "dst_addr")  # remove PII
    )

    # -----------------------------------------------------------------------
    # Step 6: Timestamp enrichment
    # -----------------------------------------------------------------------
    df = df.withColumn("ts_utc", F.to_timestamp(F.col("timestamp")))

    # -----------------------------------------------------------------------
    # Step 7: Firmware generation flag
    # -----------------------------------------------------------------------
    df = df.withColumn(
        "fw_gen",
        F.when(F.col("fw") >= 5000, F.lit("v5")).otherwise(F.lit("v4"))
    )

    # -----------------------------------------------------------------------
    # Step 8: Select final Silver columns
    # -----------------------------------------------------------------------
    silver = (
        df.select(
            "msm_id",
            "probe_id_hash",
            "country_code",
            "ts_utc",
            "timestamp",
            "proto",
            "dst_name",
            "fw",
            "fw_gen",
            "mver",
            "rtt_avg_ms",
            "rtt_min_ms",
            "rtt_max_ms",
            "pkt_sent",
            "pkt_rcvd",
            "packet_loss",
            "icmp_filtered",
            "has_error",
            "err",
        )
        .filter(F.col("country_code").isNotNull())  # drop unmapped probes
        .dropDuplicates(["msm_id", "probe_id_hash", "timestamp"])
        .withColumn("year",  F.year("ts_utc"))
        .withColumn("month", F.month("ts_utc"))
        .withColumn("day",   F.dayofmonth("ts_utc"))
    )

    dst = f"s3a://{SILVER_BUCKET}/ripe/ping/{date_partition}"
    log.info("[ripe/ping] writing Silver Parquet → %s", dst)

    count = silver.count()
    (
        silver
        .repartition(8, "country_code", "msm_id")
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(dst)
    )
    log.info("[ripe/ping] wrote %d records", count)
    return count


# ---------------------------------------------------------------------------
# Date window helpers
# ---------------------------------------------------------------------------

def _date_partitions(start: datetime, end: datetime) -> list[str]:
    parts = []
    cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor < end:
        parts.append(
            f"year={cursor.year:04d}/month={cursor.month:02d}/day={cursor.day:02d}"
        )
        cursor += timedelta(days=1)
    return parts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RIPE Silver Layer Batch Job")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    today     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    parser.add_argument("--start", default=yesterday)
    parser.add_argument("--end",   default=today)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    if start >= end:
        log.error("--start must be before --end")
        sys.exit(1)

    partitions = _date_partitions(start, end)
    log.info("RIPE Silver job: %d day(s)", len(partitions))

    spark = build_spark()
    total = 0

    for date_part in partitions:
        log.info("=== Processing partition: %s ===", date_part)
        total += process_ping(spark, date_part)

    log.info("=" * 60)
    log.info("RIPE Silver complete. Total records: %d", total)
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()