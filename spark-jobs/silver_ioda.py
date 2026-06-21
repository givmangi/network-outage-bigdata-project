"""
silver_ioda.py — IODA Silver Layer Batch Job
=============================================
Reads raw NDJSON.GZ files from the Bronze MinIO bucket (s3a://bronze/ioda/...),
applies schema normalisation, type casting, and deduplication, then writes
columnar Parquet files to the Silver MinIO bucket (s3a://silver/ioda/...).

Bronze layout consumed:
  s3a://bronze/ioda/alerts/year=YYYY/month=MM/day=DD/<entity_type>_<code>_<datasource>.ndjson.gz
  s3a://bronze/ioda/events/year=YYYY/month=MM/day=DD/<entity_type>_<code>.ndjson.gz
  s3a://bronze/ioda/signals/year=YYYY/month=MM/day=DD/<entity_type>_<code>_<datasource>.ndjson.gz

Silver layout produced:
  s3a://silver/ioda/alerts/year=YYYY/month=MM/day=DD/part-*.parquet
  s3a://silver/ioda/events/year=YYYY/month=MM/day=DD/part-*.parquet
  s3a://silver/ioda/signals/year=YYYY/month=MM/day=DD/part-*.parquet

Design principles:
  - Schema-on-read: explicit schemas prevent silent drift if IODA changes field types.
  - Idempotent: output is overwritten by partition — re-running the same day is safe.
  - Partition pruning: output partitioned by (year, month, day) matching the Gold layer.

Usage (via docker compose):
    docker compose run --rm --no-deps spark-silver-ioda
    docker compose run --rm --no-deps spark-silver-ioda --start 2026-05-28 --end 2026-06-04
    docker compose run --rm --no-deps spark-silver-ioda --datasets alerts events
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
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType, BooleanType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

S3_ENDPOINT   = os.environ.get("S3_ENDPOINT_URL",   "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
BRONZE_BUCKET = os.environ.get("S3_BUCKET_BRONZE",  "bronze")
SILVER_BUCKET = os.environ.get("S3_BUCKET_SILVER",  "silver")

DATASOURCES = ["bgp", "ping-slash24", "merit-nt"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("silver_ioda")

# ---------------------------------------------------------------------------
# Confirmed bronze schemas (from real IODA files)
# ---------------------------------------------------------------------------
# These are used only by process_signals (which has stable flat fields).
# Alerts and events use schema inference (PERMISSIVE mode) because the
# nested entity struct is handled dynamically.

SIGNAL_SCHEMA = StructType([
    # Fields as written by _expand_signal in starting_pipe.py
    StructField("entityType",  StringType(),  True),
    StructField("entityCode",  StringType(),  True),
    StructField("datasource",  StringType(),  True),
    StructField("ts",          LongType(),    True),
    StructField("value",       DoubleType(),  True),
    StructField("step",        IntegerType(), True),
    StructField("nativeStep",  IntegerType(), True),
])


# ---------------------------------------------------------------------------
# Spark session factory
# ---------------------------------------------------------------------------

def build_spark(app_name: str = "silver_ioda") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "STATIC")
        # S3A / MinIO settings
        .config("spark.hadoop.fs.s3a.endpoint",           S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Don't fail the job if some bronze partitions don't exist yet
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Bronze path helpers
# ---------------------------------------------------------------------------

def _bronze_glob(layer: str, date_partition: str) -> str:
    """Build the S3A glob pattern for a bronze sub-layer and date partition."""
    return f"s3a://{BRONZE_BUCKET}/ioda/{layer}/{date_partition}/*.ndjson.gz"


def _silver_path(layer: str) -> str:
    """
    Base S3A path for a Silver layer. No date suffix — partitionBy() on the
    DataFrame write call adds year=/month=/day= automatically. Including the
    date in the path AND using partitionBy causes double-nested directories.
    """
    return f"s3a://{SILVER_BUCKET}/ioda/{layer}"


# ---------------------------------------------------------------------------
# Transform: Alerts
# ---------------------------------------------------------------------------

def process_alerts(spark: SparkSession, date_partition: str) -> int:
    """
    Read all alert NDJSON.GZ files for the given date partition, normalise
    the schema, deduplicate, and write Parquet to Silver.

    IODA alert JSON structure (from /outages/alerts endpoint):
      {
        "datasource": "bgp",
        "entity": {"type": "country", "code": "IT", "name": "Italy"},
        "from": 1234567890,
        "until": 1234654290,
        "value": 4300.0,
        "historicalValue": 4350.0,
        "condition": "...",
        "level": "warning",
        "method": "..."
      }
    Entity fields are NESTED under "entity", not flat at the top level.
    We must use entity.type and entity.code, not entityType/entityCode.
    """
    src = _bronze_glob("alerts", date_partition)
    log.info("[alerts] reading from %s", src)

    try:
        # Use permissive + infer schema — the nested entity struct varies
        raw: DataFrame = (
            spark.read
            .option("multiline", "false")
            .option("mode", "PERMISSIVE")
            .json(src)
        )
    except Exception as exc:
        log.warning("[alerts] could not read bronze: %s", exc)
        return 0

    if raw.rdd.isEmpty():
        log.info("[alerts] no data for %s — skipping", date_partition)
        return 0

    available = set(raw.columns)

    # Extract entity fields from the nested struct if present,
    # fall back to flat entityType/entityCode for any records written
    # by the entity-specific endpoint (bronze_ingestion.py style)
    if "entity" in available:
        entity_type_col = F.upper(F.col("entity.type"))
        entity_code_col = F.upper(F.col("entity.code"))
    else:
        entity_type_col = F.upper(F.col("entityType")) if "entityType" in available \
                          else F.lit(None).cast(StringType())
        entity_code_col = F.upper(F.col("entityCode")) if "entityCode" in available \
                          else F.lit(None).cast(StringType())

    silver = (
        raw
        .withColumn("ts_from",          F.to_timestamp(F.col("from")))
        .withColumn("ts_until",         F.to_timestamp(F.col("until")))
        .withColumn("entity_type",      entity_type_col)
        .withColumn("entity_code",      entity_code_col)
        .withColumn("datasource",       F.lower(F.col("datasource")))
        .withColumn("alert_value",      F.col("value").cast(DoubleType()))
        .withColumn("historical_value", F.col("historicalValue").cast(DoubleType())
                    if "historicalValue" in available
                    else F.lit(None).cast(DoubleType()))
        .withColumn("condition",        F.col("condition") if "condition" in available
                    else F.lit(None).cast(StringType()))
        .withColumn("level",            F.col("level") if "level" in available
                    else F.lit(None).cast(StringType()))
        .withColumn("method",           F.col("method") if "method" in available
                    else F.lit(None).cast(StringType()))
        .select("ts_from", "ts_until", "entity_type", "entity_code",
                "datasource", "alert_value", "historical_value",
                "condition", "level", "method")
        .dropDuplicates(["entity_type", "entity_code", "datasource", "ts_from"])
        .filter(F.col("ts_from").isNotNull())
        # Zero-padded string partitions: month=06 not month=6
        .withColumn("year",  F.date_format("ts_from", "yyyy"))
        .withColumn("month", F.date_format("ts_from", "MM"))
        .withColumn("day",   F.date_format("ts_from", "dd"))
    )

    # Write to base layer path — partitionBy adds year=/month=/day= itself
    dst = _silver_path("alerts")
    log.info("[alerts] writing Silver Parquet → %s", dst)

    count = silver.count()
    (
        silver.repartition(4, "entity_code", "datasource")
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(dst)
    )
    log.info("[alerts] wrote %d records", count)
    return count


# ---------------------------------------------------------------------------
# Transform: Events
# ---------------------------------------------------------------------------

def process_events(spark: SparkSession, date_partition: str) -> int:
    """
    Read all event NDJSON.GZ files, flatten the schema, and write Parquet.

    Confirmed bronze schema (from real files):
    {
      "entity":     {"code": "TR", "name": "Turkey", "type": "country",
                     "subnames": [], "attrs": {"fqid": "..."}},
      "from":       1778733000,      # unix epoch
      "until":      1779235200,      # unix epoch
      "score":      609484.8,        # severity score
      "datasource": "gtr",           # which signal triggered
      "method":     "sarima",
      "alerts":     [...]            # nested array — dropped (stored in alerts layer)
    }

    No overallScore, no id field, no flat entityType/entityCode.
    Dedup by (entity_code, ts_from, ts_until) — same event re-written with
    a later ts_until each day as IODA updates ongoing outages.
    """
    src = _bronze_glob("events", date_partition)
    log.info("[events] reading from %s", src)

    try:
        raw: DataFrame = (
            spark.read
            .option("multiline", "false")
            .option("mode", "PERMISSIVE")
            .json(src)
        )
    except Exception as exc:
        log.warning("[events] could not read bronze: %s", exc)
        return 0

    if raw.rdd.isEmpty():
        log.info("[events] no data for %s — skipping", date_partition)
        return 0

    silver = (
        raw
        # Entity is always a nested struct in events
        .withColumn("entity_type",  F.upper(F.col("entity.type")))
        .withColumn("entity_code",  F.upper(F.col("entity.code")))
        .withColumn("entity_name",  F.col("entity.name"))
        # Timestamps
        .withColumn("ts_from",      F.to_timestamp(F.col("from").cast(LongType())))
        .withColumn("ts_until",     F.to_timestamp(F.col("until").cast(LongType())))
        .withColumn("duration_sec", F.col("until").cast(LongType()) - F.col("from").cast(LongType()))
        # Metrics
        .withColumn("score",        F.col("score").cast(DoubleType()))
        .withColumn("datasource",   F.lower(F.col("datasource")))
        .withColumn("method",       F.col("method"))
        # Drop the nested alerts array and raw fields
        .select("entity_type", "entity_code", "entity_name",
                "ts_from", "ts_until", "duration_sec",
                "score", "datasource", "method")
        .filter(F.col("ts_from").isNotNull())
        .filter(F.col("entity_code").isNotNull())
        # Keep all snapshots of ongoing events (ts_until creeps forward each day)
        .dropDuplicates(["entity_code", "ts_from", "ts_until"])
        .withColumn("year",  F.date_format("ts_from", "yyyy"))
        .withColumn("month", F.date_format("ts_from", "MM"))
        .withColumn("day",   F.date_format("ts_from", "dd"))
    )

    dst = _silver_path("events")
    log.info("[events] writing Silver Parquet → %s", dst)

    count = silver.count()
    (
        silver.repartition(2, "entity_code")
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(dst)
    )
    log.info("[events] wrote %d records", count)
    return count


# ---------------------------------------------------------------------------
# Transform: Signals
# ---------------------------------------------------------------------------

def process_signals(spark: SparkSession, date_partition: str) -> int:
    """
    Read all signal NDJSON.GZ files. Each row is already a flattened
    per-timestep record (the Bronze ingester expanded the compact
    'values[]' array). We cast types, filter nulls, and compute
    a z-score flag for anomaly detection by the Gold layer.

    Null 'value' rows are KEPT but flagged (collection_gap=true) because
    a sustained run of nulls can itself indicate a monitoring blackout.
    """
    src = _bronze_glob("signals", date_partition)
    log.info("[signals] reading from %s", src)

    try:
        raw: DataFrame = (
            spark.read
            .schema(SIGNAL_SCHEMA)
            .option("multiline", "false")
            .json(src)
        )
    except Exception as exc:
        log.warning("[signals] could not read bronze: %s", exc)
        return 0

    if raw.rdd.isEmpty():
        log.info("[signals] no data for %s — skipping", date_partition)
        return 0

    silver = (
        raw
        .withColumn("ts",          F.col("ts").cast(LongType()))
        .withColumn("ts_utc",      F.to_timestamp(F.col("ts")))
        .withColumn("entity_type", F.upper(F.col("entityType")))
        .withColumn("entity_code", F.upper(F.col("entityCode")))
        .withColumn("datasource",  F.lower(F.col("datasource")))
        .withColumn("value",       F.col("value").cast(DoubleType()))
        .withColumn("collection_gap", F.col("value").isNull())
        .drop("entityType", "entityCode")
        .dropDuplicates(["entity_type", "entity_code", "datasource", "ts"])
        # Zero-padded string partitions: month=06 not month=6
        .withColumn("year",  F.date_format("ts_utc", "yyyy"))
        .withColumn("month", F.date_format("ts_utc", "MM"))
        .withColumn("day",   F.date_format("ts_utc", "dd"))
    )

    dst = _silver_path("signals")
    log.info("[signals] writing Silver Parquet → %s", dst)

    count = silver.count()
    (
        silver
        .repartition(8, "entity_code", "datasource")
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(dst)
    )
    log.info("[signals] wrote %d records", count)
    return count


# ---------------------------------------------------------------------------
# Partition discovery — uses the Hadoop FileSystem API already in the JVM
# ---------------------------------------------------------------------------

def _discover_partitions(spark: "SparkSession", layer: str) -> list[str]:
    """
    List every year=/month=/day= partition that actually exists under
    s3a://<BRONZE_BUCKET>/ioda/<layer>/ by walking the S3 prefix tree
    via the Hadoop FileSystem API (available through spark._jvm).

    This avoids needing boto3, which is not installed in the apache/spark
    image's Python environment. The S3A connector is already loaded as a
    JAR (via --packages), so its Java classes are accessible through the JVM.

    Returns a sorted list of Hive-style partition strings, e.g.:
      ['year=2026/month=05/day=28', 'year=2026/month=05/day=29', ...]
    """
    jvm  = spark._jvm
    jsc  = spark._jsc.sc()
    conf = jsc.hadoopConfiguration()

    Path = jvm.org.apache.hadoop.fs.Path
    base = f"s3a://{BRONZE_BUCKET}/ioda/{layer}"
    base_path = Path(base)
    fs = base_path.getFileSystem(conf)

    day_partitions: set[str] = set()

    try:
        # Walk year= directories
        for year_status in fs.listStatus(base_path):
            if not year_status.isDirectory():
                continue
            year_path = year_status.getPath()
            # Walk month= directories
            for month_status in fs.listStatus(year_path):
                if not month_status.isDirectory():
                    continue
                month_path = month_status.getPath()
                # Walk day= directories
                for day_status in fs.listStatus(month_path):
                    if not day_status.isDirectory():
                        continue
                    day_path = day_status.getPath()
                    # Build partition string: year=YYYY/month=MM/day=DD
                    partition = (
                        f"{year_path.getName()}/"
                        f"{month_path.getName()}/"
                        f"{day_path.getName()}"
                    )
                    day_partitions.add(partition)
    except Exception as exc:
        log.warning("Could not list s3a://%s/ioda/%s/: %s", BRONZE_BUCKET, layer, exc)

    return sorted(day_partitions)


def _date_partitions_from_range(start: datetime, end: datetime) -> list[str]:
    """Return Hive-style partition strings for each day in [start, end)."""
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
    parser = argparse.ArgumentParser(
        description="IODA Silver Layer Batch Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start", default=None,
        help="Start date inclusive (YYYY-MM-DD). Omit to auto-discover all bronze partitions.",
    )
    parser.add_argument(
        "--end", default=None,
        help=(
            "End date exclusive (YYYY-MM-DD). "
            "Omit to default to yesterday when --start is provided."
        ),
    )
    parser.add_argument(
        "--datasets", nargs="+",
        choices=["alerts", "events", "signals"],
        default=["alerts", "events", "signals"],
        help="Which IODA datasets to process. Default: all three.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Spark must be running before we can use the Hadoop FS API for discovery
    spark = build_spark()

    PROCESSORS = {
        "alerts":  process_alerts,
        "events":  process_events,
        "signals": process_signals,
    }

    # ---------------------------------------------------------------------------
    # Build per-dataset partition lists
    # ---------------------------------------------------------------------------
    if args.start is None and args.end is None:
        # Auto-discover: query each layer's bronze folder independently.
        # A layer whose folder doesn't exist yet (e.g. alerts before any outage
        # has been ingested) gets an empty list and is skipped with a WARNING —
        # it does NOT block the other layers from running.
        log.info(
            "No date range specified — auto-discovering partitions per dataset "
            "from s3a://%s/ioda/", BRONZE_BUCKET,
        )
        dataset_partitions: dict[str, list[str]] = {}
        for layer in args.datasets:
            parts = _discover_partitions(spark, layer)
            if parts:
                log.info(
                    "[%s] discovered %d partition(s): %s … %s",
                    layer, len(parts), parts[0], parts[-1],
                )
            else:
                log.warning(
                    "[%s] no bronze partitions found in s3a://%s/ioda/%s/ — "
                    "skipping (folder may not exist yet).",
                    layer, BRONZE_BUCKET, layer,
                )
            dataset_partitions[layer] = parts

        # Only truly fatal if every requested dataset is empty
        if all(len(p) == 0 for p in dataset_partitions.values()):
            log.error(
                "No bronze partitions found for any of %s. "
                "Has the ingester written any data yet?", args.datasets,
            )
            spark.stop()
            sys.exit(1)

    else:
        if args.start is None:
            log.error(
                "Provide --start when --end is specified, or omit both for auto-discover."
            )
            spark.stop()
            sys.exit(1)

        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.end is None:
            end = (
                datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                - timedelta(days=1)
            )
            log.info("No --end specified; defaulting to yesterday (%s).", end.strftime("%Y-%m-%d"))
        else:
            end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if start >= end:
            log.error("--start must be before --end")
            spark.stop()
            sys.exit(1)
        if end.date() >= datetime.now(timezone.utc).date():
            log.error("--end must be yesterday or earlier (no today/future partitions)")
            spark.stop()
            sys.exit(1)
        shared_partitions = _date_partitions_from_range(start, end)
        log.info("Date range specified: %d partition(s)", len(shared_partitions))
        # Explicit range: all datasets share the same date window.
        # process_* already handle missing/empty files gracefully (return 0).
        dataset_partitions = {layer: shared_partitions for layer in args.datasets}

    # ---------------------------------------------------------------------------
    # Process each dataset over its own partition list
    # ---------------------------------------------------------------------------
    totals: dict[str, int] = {}

    for layer, partitions in dataset_partitions.items():
        if not partitions:
            continue  # already warned above
        log.info("=== [%s] processing %d partition(s) ===", layer, len(partitions))
        for date_part in partitions:
            n = PROCESSORS[layer](spark, date_part)
            totals[layer] = totals.get(layer, 0) + n

    log.info("=" * 60)
    log.info("IODA Silver complete. Record totals: %s", totals)
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()