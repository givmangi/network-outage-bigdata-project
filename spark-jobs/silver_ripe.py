"""
silver_ripe.py — RIPE Atlas Silver Layer Batch Job
====================================================
Reads NDJSON.GZ ping measurement files from s3a://bronze/ripe/ping/,
normalises the schema, computes derived metrics, anonymises probe IDs,
and writes Parquet to s3a://silver/ripe/ping/.

Confirmed bronze schema (from real measurement files, firmware 5080/5090):
{
  "fw": 5080,              # firmware version (all >= 5000 in practice)
  "mver": "2.6.2",         # parser version string
  "lts": 43,               # last time synced (seconds ago)
  "af": 6,                 # address family: 4=IPv4, 6=IPv6
  "msm_id": 2001,          # measurement ID (which root server)
  "msm_name": "Ping",
  "prb_id": 28762,         # probe ID — will be SHA-256 hashed
  "timestamp": 1781710924, # unix epoch of measurement
  "type": "ping",
  "step": 240,             # measurement interval in seconds
  "dst_name": "2001:7fd::1",
  "dst_addr": "2001:7fd::1",
  "src_addr": "...",       # source IP — DROPPED (PII)
  "from": "...",           # same as src_addr — DROPPED (PII)
  "proto": "ICMP",
  "ttl": 60,
  "size": 20,
  "sent": 3,
  "rcvd": 3,
  "dup": 0,
  "min": 14.639,
  "max": 15.196,
  "avg": 14.862,
  "result": [{"rtt": 15.196}, {"rtt": 14.753}, {"rtt": 14.639}],
  "country_code": "DE"     # injected by ingester from probe_mapping.json
}

Silver layout produced:
  s3a://silver/ripe/ping/year=YYYY/month=MM/day=DD/part-*.parquet
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
    DoubleType, IntegerType, LongType, StringType,
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
# Spark session
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
# Transform: RIPE ping
# ---------------------------------------------------------------------------

def process_ping(spark: SparkSession, date_partition: str) -> int:
    """
    Read all RIPE ping NDJSON.GZ files for a date partition and write Silver.

    All files in practice use firmware >= 5000 so avg/min/max are always
    present at the top level. The result[] array contains per-packet RTTs
    which we use to count actual packet-level data (some packets may be
    lost even when rcvd > 0).

    Derived metrics:
    - packet_loss: 1 - rcvd/sent, clipped to [0, 1]
    - icmp_filtered: sent > 0 AND rcvd == 0 AND avg IS NULL
      (host rate-limiting ICMP, not a genuine outage)
    - rtt_stddev: computed from the result[] array where available

    PII: prb_id hashed with SHA-256; src_addr and from (source IP) dropped.
    """
    src = f"s3a://{BRONZE_BUCKET}/ripe/ping/{date_partition}/*.ndjson.gz"
    log.info("[ripe/ping] reading from %s", src)

    try:
        raw: DataFrame = (
            spark.read
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

    # Packet counts
    pkt_sent = F.col("sent").cast(DoubleType())
    pkt_rcvd = F.col("rcvd").cast(DoubleType())

    # RTT stddev from the result array
    valid_rtts = F.filter(F.col("result"), lambda r: r["rtt"].isNotNull())
    rtt_values = F.transform(valid_rtts, lambda r: r["rtt"])
    rtt_count  = F.size(rtt_values)

    silver = (
        raw
        # Measurement identity
        .withColumn("msm_id",       F.col("msm_id").cast(IntegerType()))
        .withColumn("probe_id_hash", F.sha2(F.col("prb_id").cast(StringType()), 256))
        .withColumn("country_code", F.upper(F.col("country_code")))
        # asn: injected by the bronze ingester (ripe_bronze_ingestion.py /
        # ripe_streaming_pipe.py) from ripe_probe_mapping.json. Explicitly
        # cast here so the Parquet schema is always IntegerType regardless of
        # how Spark inferred the raw JSON field. Rows that pre-date the fix
        # (no asn in bronze) will land as NULL and are filtered out by
        # gold_batch.py's .filter(F.col("asn").isNotNull()).
        .withColumn("asn",
            F.when(F.col("asn").isNotNull(), F.col("asn").cast(IntegerType()))
             .otherwise(F.lit(None).cast(IntegerType()))
        )
        .withColumn("ts_utc",       F.to_timestamp(F.col("timestamp").cast(LongType())))
        .withColumn("timestamp",    F.col("timestamp").cast(LongType()))
        # Measurement metadata
        .withColumn("fw",           F.col("fw").cast(IntegerType()))
        .withColumn("mver",         F.col("mver"))
        .withColumn("af",           F.col("af").cast(IntegerType()))   # 4=IPv4, 6=IPv6
        .withColumn("proto",        F.col("proto"))
        .withColumn("dst_name",     F.col("dst_name"))
        .withColumn("step",         F.col("step").cast(IntegerType()))
        # RTT metrics — all firmware >= 5000 has these at top level
        .withColumn("rtt_avg_ms",   F.col("avg").cast(DoubleType()))
        .withColumn("rtt_min_ms",   F.col("min").cast(DoubleType()))
        .withColumn("rtt_max_ms",   F.col("max").cast(DoubleType()))
        # Packet counts
        .withColumn("pkt_sent",     pkt_sent)
        .withColumn("pkt_rcvd",     pkt_rcvd)
        .withColumn("pkt_dup",      F.col("dup").cast(IntegerType()))
        # Derived: packet loss
        .withColumn("packet_loss",
            F.when(
                pkt_sent.isNotNull() & (pkt_sent > 0),
                F.greatest(F.lit(0.0),
                           F.least(F.lit(1.0),
                                   F.lit(1.0) - (pkt_rcvd / pkt_sent)))
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
        # Derived: ICMP rate-limit flag
        .withColumn("icmp_filtered",
            (pkt_sent > 0) & (pkt_rcvd == 0) & F.col("avg").isNull()
        )
        # Derived: per-packet RTT count from result array
        .withColumn("pkt_result_count", rtt_count)
        # Drop PII columns
        .drop("prb_id", "src_addr", "from", "avg", "min", "max",
              "sent", "rcvd", "dup", "result", "dst_addr",
              "lts", "size", "ttl", "msm_name", "type")
        # Only keep records with a known country
        .filter(F.col("country_code").isNotNull())
        .filter(F.col("ts_utc").isNotNull())
        .dropDuplicates(["msm_id", "probe_id_hash", "timestamp"])
        # Zero-padded string partitions
        .withColumn("year",  F.date_format("ts_utc", "yyyy"))
        .withColumn("month", F.date_format("ts_utc", "MM"))
        .withColumn("day",   F.date_format("ts_utc", "dd"))
    )

    dst = f"s3a://{SILVER_BUCKET}/ripe/ping"
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
# Partition discovery via Hadoop FileSystem API (no boto3 needed)
# ---------------------------------------------------------------------------

def _discover_partitions(spark: SparkSession) -> list[str]:
    """List all year=/month=/day= partitions under s3a://bronze/ripe/ping/."""
    jvm  = spark._jvm
    conf = spark._jsc.sc().hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    base = Path(f"s3a://{BRONZE_BUCKET}/ripe/ping")
    fs   = base.getFileSystem(conf)

    day_partitions: set[str] = set()
    try:
        for y in fs.listStatus(base):
            if not y.isDirectory(): continue
            yp = y.getPath()
            for m in fs.listStatus(yp):
                if not m.isDirectory(): continue
                mp = m.getPath()
                for d in fs.listStatus(mp):
                    if not d.isDirectory(): continue
                    day_partitions.add(
                        f"{yp.getName()}/{mp.getName()}/{d.getPath().getName()}"
                    )
    except Exception as exc:
        log.warning("Could not list bronze/ripe/ping: %s", exc)

    return sorted(day_partitions)


def _date_partitions_from_range(start: datetime, end: datetime) -> list[str]:
    parts, cursor = [], start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor < end:
        parts.append(f"year={cursor.year:04d}/month={cursor.month:02d}/day={cursor.day:02d}")
        cursor += timedelta(days=1)
    return parts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RIPE Silver Layer Batch Job")
    p.add_argument("--start", default=None,
                   help="Start date inclusive (YYYY-MM-DD). Omit to auto-discover.")
    p.add_argument("--end",   default=None,
                   help="End date exclusive (YYYY-MM-DD). Omit to auto-discover.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark()

    if args.start is None and args.end is None:
        log.info("Auto-discovering partitions from s3a://%s/ripe/ping/", BRONZE_BUCKET)
        partitions = _discover_partitions(spark)
        if not partitions:
            log.warning("No RIPE bronze partitions found — has ripe-ingester written any data yet?")
            spark.stop()
            return
        log.info("Discovered %d partition(s): %s … %s",
                 len(partitions), partitions[0], partitions[-1])
    else:
        if args.start is None or args.end is None:
            log.error("Provide both --start and --end, or neither.")
            spark.stop()
            sys.exit(1)
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if start >= end:
            log.error("--start must be before --end")
            spark.stop()
            sys.exit(1)
        partitions = _date_partitions_from_range(start, end)
        log.info("Date range: %d partition(s)", len(partitions))

    total = 0
    for date_part in partitions:
        log.info("=== Processing %s ===", date_part)
        total += process_ping(spark, date_part)

    log.info("=" * 60)
    log.info("RIPE Silver complete. Total records: %d", total)
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()