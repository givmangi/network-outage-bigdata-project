"""
Silver Layer — PySpark transformation job
==========================================
Reads Bronze NDJSON.gz files from ioda-bronze (MinIO), cleans and flattens
all three data types, and writes typed Parquet files to ioda-silver.

Corrections made from real Bronze file analysis (country_IQ_merit-nt):
  1. datasource is NOT limited to ['bgp','ping-slash24','ucsd-nt'].
     The real API also returns 'merit-nt' (Merit Network Telescope) and
     potentially others. The schema and ingester DATASOURCES list must be
     treated as open-ended.

  2. step is NOT always 60 seconds. Confirmed step values per datasource:
       bgp:          60s
       ping-slash24: 600s
       ucsd-nt:      600s
       merit-nt:     300s
     The rolling window for the z-score MUST be computed dynamically per
     row using the actual 'step' field, not hardcoded to 10080 rows.

  3. value is mixed-type: JSON delivers integers for whole numbers (e.g. 3, 6, 11)
     and floats for decimals (e.g. 3.4, 7.8). Spark's schema must declare
     DoubleType and the explicit cast handles both.

  4. Trailing nulls are distinct from mid-series nulls.
     The last N rows of a file often have null value because IODA hasn't
     confirmed the most recent data points yet (ingestion lag). A null in
     the middle of the time series means a genuine collection gap.
     Silver adds 'is_trailing_null' to distinguish these two cases so the
     Gold layer doesn't treat ingestion lag as an outage signal.

  5. The uploaded filename used underscores (country_IQ_merit-nt_ndjson.gz)
     as an artifact of the upload tool. The actual MinIO key uses a dot
     (country_IQ_merit-nt.ndjson.gz). No code change needed — this is a
     naming note only.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("silver_job")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MINIO_ENDPOINT   = os.environ.get("S3_ENDPOINT_URL",  "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY",    "ioda_admin")
MINIO_SECRET_KEY = os.environ.get("S3_SECRET_KEY",    "")
BUCKET_BRONZE    = os.environ.get("S3_BUCKET_BRONZE", "ioda-bronze")
BUCKET_SILVER    = os.environ.get("S3_BUCKET_SILVER", "ioda-silver")

ALL_DATASETS = ["alerts", "events", "signals"]

# ---------------------------------------------------------------------------
# Explicit Bronze schemas
# ---------------------------------------------------------------------------
# Every field is nullable=True because Bronze is raw — we never assume
# a field is present. Cleaning handles nulls explicitly downstream.
#
# value is DoubleType in the schema even though the real file delivers
# integers for whole numbers (3, 6, 11) and floats for decimals (3.4, 7.8).
# Spark's JSON reader will safely upcast int -> double when the schema says
# DoubleType, so no data is lost.
#
# 'step' and 'nativeStep' are kept in the schema because we need 'step'
# to compute the dynamic rolling window. They are dropped from Silver output
# after use.

ALERT_SCHEMA = StructType([
    StructField("entityType",    StringType(),  nullable=True),
    StructField("entityCode",    StringType(),  nullable=True),
    StructField("datasource",    StringType(),  nullable=True),
    StructField("time",          LongType(),    nullable=True),
    StructField("level",         StringType(),  nullable=True),
    StructField("condition",     StringType(),  nullable=True),
    StructField("value",         DoubleType(),  nullable=True),
    StructField("historyValue",  DoubleType(),  nullable=True),
])

EVENT_SCHEMA = StructType([
    StructField("entityType",  StringType(),  nullable=True),
    StructField("entityCode",  StringType(),  nullable=True),
    StructField("from",        LongType(),    nullable=True),
    StructField("until",       LongType(),    nullable=True),
    StructField("score",       DoubleType(),  nullable=True),
    # Read as string; parsed with from_json() in transform_events()
    StructField("alerts",      StringType(),  nullable=True),
])

SIGNAL_SCHEMA = StructType([
    StructField("entityType",  StringType(),  nullable=True),
    StructField("entityCode",  StringType(),  nullable=True),
    StructField("datasource",  StringType(),  nullable=True),
    StructField("ts",          LongType(),    nullable=True),
    StructField("value",       DoubleType(),  nullable=True),  # int or float in JSON
    StructField("step",        IntegerType(), nullable=True),  # 60, 300, or 600
    StructField("nativeStep",  IntegerType(), nullable=True),
])

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_spark(endpoint: str, access_key: str, secret_key: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("ioda-silver-job")
        .config("spark.hadoop.fs.s3a.endpoint",          endpoint)
        .config("spark.hadoop.fs.s3a.access.key",        access_key)
        .config("spark.hadoop.fs.s3a.secret.key",        secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.compression.codec",   "snappy")
        .config("spark.sql.shuffle.partitions",          "8")
        .config("spark.sql.columnNameOfCorruptRecord",   "_corrupt_record")
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def bronze_glob(dataset: str, start: datetime, end: datetime) -> str:
    """
    Build a glob that covers all Bronze partition directories in [start, end].
    Uses the broadest glob that still allows Spark partition pruning to cut
    down the scan when the window is narrow.
    """
    base = f"s3a://{BUCKET_BRONZE}/ioda/{dataset}"
    if start.year == end.year and start.month == end.month:
        return f"{base}/year={start.year:04d}/month={start.month:02d}/*/"
    if start.year == end.year:
        return f"{base}/year={start.year:04d}/*/*/"
    return f"{base}/*/*/*/"


def silver_path(dataset: str) -> str:
    return f"s3a://{BUCKET_SILVER}/{dataset}/"

# ---------------------------------------------------------------------------
# Shared: read Bronze JSON with explicit schema
# ---------------------------------------------------------------------------

def _read_bronze(spark: SparkSession, dataset: str,
                 start: datetime, end: datetime,
                 schema: StructType) -> DataFrame:
    """
    Read Bronze NDJSON.gz files for a dataset.

    Handles two real-world cases confirmed from file inspection:
      - Empty .gz files (valid gzip, zero bytes of content): Spark reads
        these as zero rows — no crash, no warning needed.
      - Mixed int/float 'value' field: the explicit DoubleType schema
        upcasts silently.

    PERMISSIVE mode puts unparseable lines into _corrupt_record instead of
    crashing. We count and log them then drop the column.
    """
    path = bronze_glob(dataset, start, end)
    log.info("[%s] Bronze path: %s", dataset, path)

    df = (
        spark.read
        .schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )

    if "_corrupt_record" in df.columns:
        n = df.filter(F.col("_corrupt_record").isNotNull()).count()
        if n > 0:
            log.warning("[%s] %d corrupt Bronze lines dropped", dataset, n)
        df = df.drop("_corrupt_record")

    return df

# ---------------------------------------------------------------------------
# ALERTS transformation
# ---------------------------------------------------------------------------

def transform_alerts(spark: SparkSession, start: datetime, end: datetime) -> DataFrame:
    """
    Bronze → Silver for alert records.

    Input fields:  entityType, entityCode, datasource, time (Unix long),
                   level, condition, value (double), historyValue (double)

    Output fields:
      alert_ts        TimestampType  — cast from Unix 'time'
      entity_type     string         — lowercased entityType
      entity_code     string         — uppercased entityCode
      datasource      string         — kept as-is (open-ended: bgp, merit-nt, …)
      level           string         — normal / warning / critical
      condition       string         — lte / gte / …
      value           double         — measured value at alert time
      history_value   double         — IODA's baseline for comparison
      deviation_pct   double         — (history-value)/history * 100
                                       positive = drop below baseline (outage signal)
                                       null when history_value is null or zero
      year, month     int            — partition columns extracted from alert_ts
    """
    start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
    df = _read_bronze(spark, "alerts", start, end, ALERT_SCHEMA)

    df = (
        df
        .filter(F.col("time").isNotNull())
        .filter(F.col("time").between(start_ts, end_ts))

        .withColumn("alert_ts",
            F.to_timestamp(F.col("time").cast(LongType())))
        .withColumn("entity_type",  F.lower(F.col("entityType")))
        .withColumn("entity_code",  F.upper(F.col("entityCode")))
        .withColumn("value",        F.col("value").cast(DoubleType()))
        .withColumn("history_value", F.col("historyValue").cast(DoubleType()))

        .withColumn("deviation_pct",
            F.when(
                F.col("history_value").isNotNull() & (F.col("history_value") != 0),
                F.round(
                    (F.col("history_value") - F.col("value"))
                    / F.col("history_value") * 100.0,
                    2
                )
            ).otherwise(F.lit(None).cast(DoubleType()))
        )

        .withColumn("year",  F.year("alert_ts"))
        .withColumn("month", F.month("alert_ts"))

        .drop("time", "entityType", "entityCode", "historyValue")
        .filter(F.col("entity_code").isNotNull())
        .filter(F.col("alert_ts").isNotNull())
    )

    log.info("[alerts] Silver rows: %d", df.count())
    return df

# ---------------------------------------------------------------------------
# EVENTS transformation
# ---------------------------------------------------------------------------

def transform_events(spark: SparkSession, start: datetime, end: datetime) -> DataFrame:
    """
    Bronze → Silver for event records.

    Input fields:  entityType, entityCode, from (Unix long), until (Unix long),
                   score (double), alerts (JSON string array)

    Output fields:
      event_start       TimestampType
      event_end         TimestampType
      entity_type       string
      entity_code       string
      score             double        — IODA composite confidence (0–1)
      duration_minutes  double        — (until - from) / 60
      bgp_fired         boolean       — BGP datasource contributed to event
      ping_fired        boolean       — ping-slash24 contributed
      ucsd_fired        boolean       — ucsd-nt contributed
      merit_nt_fired    boolean       — merit-nt contributed
                                        (discovered from real file: open-ended)
      datasource_count  int           — how many independent signals fired (1–N)
                                        1 = low confidence, 3+ = confirmed
      year, month       int           — partition columns
    """
    start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
    df = _read_bronze(spark, "events", start, end, EVENT_SCHEMA)

    # Parse the nested alerts JSON string into a typed array
    alerts_schema = "array<struct<datasource:string,level:string>>"
    df = df.withColumn("alerts_parsed",
                       F.from_json(F.col("alerts"), alerts_schema))

    df = (
        df
        .filter(F.col("from").isNotNull())
        .filter(F.col("from").between(start_ts, end_ts))

        .withColumn("event_start",
            F.to_timestamp(F.col("from").cast(LongType())))
        .withColumn("event_end",
            F.to_timestamp(F.col("until").cast(LongType())))
        .withColumn("duration_minutes",
            F.round((F.col("until") - F.col("from")) / 60.0, 1).cast(DoubleType()))

        # Per-datasource boolean flags.
        # coalesce(..., false) guards against null array (no alerts embedded).
        # We now include merit-nt because the real data confirmed it exists.
        .withColumn("bgp_fired",
            F.coalesce(
                F.array_contains(F.col("alerts_parsed.datasource"), "bgp"),
                F.lit(False)))
        .withColumn("ping_fired",
            F.coalesce(
                F.array_contains(F.col("alerts_parsed.datasource"), "ping-slash24"),
                F.lit(False)))
        .withColumn("ucsd_fired",
            F.coalesce(
                F.array_contains(F.col("alerts_parsed.datasource"), "ucsd-nt"),
                F.lit(False)))
        .withColumn("merit_nt_fired",
            F.coalesce(
                F.array_contains(F.col("alerts_parsed.datasource"), "merit-nt"),
                F.lit(False)))

        # Total count of independent datasources that fired
        .withColumn("datasource_count",
            F.col("bgp_fired").cast(IntegerType())
            + F.col("ping_fired").cast(IntegerType())
            + F.col("ucsd_fired").cast(IntegerType())
            + F.col("merit_nt_fired").cast(IntegerType()))

        .withColumn("entity_type", F.lower(F.col("entityType")))
        .withColumn("entity_code", F.upper(F.col("entityCode")))
        .withColumn("year",  F.year("event_start"))
        .withColumn("month", F.month("event_start"))

        .drop("from", "until", "entityType", "entityCode",
              "alerts", "alerts_parsed")
        .filter(F.col("entity_code").isNotNull())
        .filter(F.col("event_start").isNotNull())
        .filter(F.col("score").isNotNull())
    )

    log.info("[events] Silver rows: %d", df.count())
    return df

# ---------------------------------------------------------------------------
# SIGNALS transformation
# ---------------------------------------------------------------------------

def transform_signals(spark: SparkSession, start: datetime, end: datetime) -> DataFrame:
    """
    Bronze → Silver for signal time-series records.

    Confirmed from real file (country_IQ_merit-nt):
      - step = 300 (not 60 as previously assumed)
      - value is mixed int/float in JSON
      - last 2 rows have null value (trailing ingestion lag, not outage)
      - datasource = 'merit-nt' (fourth datasource, not in original DATASOURCES list)

    Input fields:  entityType, entityCode, datasource, ts (Unix long),
                   value (double|int|null), step (int), nativeStep (int)

    Output fields:
      signal_ts         TimestampType  — cast from Unix 'ts'
      entity_type       string
      entity_code       string
      datasource        string         — bgp / ping-slash24 / ucsd-nt / merit-nt / …
      value             double         — raw measurement; null preserved
      is_gap            boolean        — true when value is null (collection gap)
      is_trailing_null  boolean        — true when null AND no subsequent non-null
                                         in same entity+datasource partition.
                                         Distinguishes ingestion lag from outage gaps.
      rolling_mean      double         — 7-day rolling mean (dynamic window size
                                         based on actual step value of each row)
      rolling_std       double         — 7-day rolling std dev
      z_score           double         — (value - rolling_mean) / rolling_std
                                         null when value is null, std is null, or std=0
      year, month       int            — partition columns

    Rolling window design:
      7-day window row count = (7 * 24 * 3600) / step
        bgp (step=60):          10080 rows
        merit-nt (step=300):     2016 rows
        ping-slash24 (step=600): 1008 rows
        ucsd-nt (step=600):      1008 rows

      Because step varies per datasource we cannot use a fixed rowsBetween
      value. Instead we compute the window size per datasource using a
      groupBy + join approach:
        1. Compute the mode of 'step' per (entity_code, datasource) group
        2. Join that back onto the full DataFrame
        3. Use a rangeBetween window on the 'ts' column (seconds-based)
           which is naturally step-independent.

      rangeBetween(-7_days_in_seconds, -1) on the 'ts' column works
      regardless of step size: it includes all rows whose ts falls within
      the last 7 days relative to the current row's ts. This is cleaner
      than computing the row count per datasource.
    """
    SEVEN_DAYS_SECONDS = 7 * 24 * 3600

    start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
    df = _read_bronze(spark, "signals", start, end, SIGNAL_SCHEMA)

    # Check for step != nativeStep (IODA resampled data)
    mismatched = df.filter(
        F.col("step").isNotNull()
        & F.col("nativeStep").isNotNull()
        & (F.col("step") != F.col("nativeStep"))
    ).count()
    if mismatched > 0:
        log.warning(
            "[signals] %d rows have step != nativeStep — "
            "IODA resampled; values are averages over the native interval",
            mismatched
        )

    # Log the actual step values seen in this run so surprises are visible
    log.info("[signals] Step values per datasource:")
    df.filter(F.col("step").isNotNull()) \
      .groupBy("datasource", "step") \
      .count() \
      .orderBy("datasource") \
      .show(truncate=False)

    df = (
        df
        .filter(F.col("ts").isNotNull())
        .filter(F.col("ts").between(start_ts, end_ts))
        .withColumn("value",  F.col("value").cast(DoubleType()))
        .withColumn("signal_ts", F.to_timestamp(F.col("ts").cast(LongType())))
        .withColumn("entity_type", F.lower(F.col("entityType")))
        .withColumn("entity_code", F.upper(F.col("entityCode")))
    )

    # ------------------------------------------------------------------
    # Rolling statistics using rangeBetween on ts (seconds-based window)
    # This is step-size-agnostic: it considers all rows whose ts is within
    # the last 7 days, regardless of whether they are 60s or 300s apart.
    # ------------------------------------------------------------------
    rolling_window = (
        Window
        .partitionBy("entity_code", "datasource")
        .orderBy(F.col("ts").cast(LongType()))
        .rangeBetween(-SEVEN_DAYS_SECONDS, -1)  # 7 days back, exclude current
    )

    df = (
        df
        .withColumn("is_gap", F.col("value").isNull())

        .withColumn("rolling_mean",
            F.avg(F.col("value")).over(rolling_window))
        .withColumn("rolling_std",
            F.stddev(F.col("value")).over(rolling_window))

        .withColumn("z_score",
            F.when(
                F.col("value").isNotNull()
                & F.col("rolling_std").isNotNull()
                & (F.col("rolling_std") > 0),
                F.round(
                    (F.col("value") - F.col("rolling_mean"))
                    / F.col("rolling_std"),
                    4
                )
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
    )

    # ------------------------------------------------------------------
    # Trailing null detection
    # A null is "trailing" if there is no subsequent non-null row for the
    # same (entity_code, datasource) within 3 steps of the current row.
    # We use a forward-looking window to find the next non-null value.
    # If it doesn't exist, this null is at the trailing edge of the
    # ingestion window (IODA lag) rather than a genuine collection gap.
    # ------------------------------------------------------------------
    fwd_window = (
        Window
        .partitionBy("entity_code", "datasource")
        .orderBy(F.col("ts").cast(LongType()))
        .rowsBetween(1, 5)          # look at the next 5 rows
    )

    df = (
        df
        .withColumn("_next_non_null_count",
            F.count(
                F.when(F.col("value").isNotNull(), F.lit(1))
            ).over(fwd_window)
        )
        # Trailing null: value is null AND no non-null exists in the next 5 rows
        .withColumn("is_trailing_null",
            F.col("value").isNull() & (F.col("_next_non_null_count") == 0)
        )
        .drop("_next_non_null_count")
    )

    # ------------------------------------------------------------------
    # Final cleanup
    # ------------------------------------------------------------------
    df = (
        df
        .withColumn("year",  F.year("signal_ts"))
        .withColumn("month", F.month("signal_ts"))
        # Drop Bronze originals and metadata not needed in Silver
        .drop("ts", "entityType", "entityCode", "step", "nativeStep")
        .filter(F.col("entity_code").isNotNull())
        .filter(F.col("signal_ts").isNotNull())
    )

    log.info("[signals] Silver rows: %d", df.count())
    return df

# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_silver(df: DataFrame, dataset: str) -> None:
    """
    Write Silver DataFrame to ioda-silver as partitioned Parquet (Snappy).

    partitionOverwriteMode=dynamic means only the partitions present in
    this DataFrame are overwritten — historical partitions are untouched.
    Without this, mode='overwrite' would delete the entire Silver table.
    """
    partition_cols = {
        "alerts":  ["entity_type", "entity_code", "datasource", "year", "month"],
        "events":  ["entity_type", "entity_code", "year", "month"],
        "signals": ["entity_type", "entity_code", "datasource", "year", "month"],
    }

    out = silver_path(dataset)
    log.info("[%s] writing to %s", dataset, out)

    (
        df.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(*partition_cols[dataset])
        .parquet(out)
    )
    log.info("[%s] write complete", dataset)

# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------

def quality_report(df: DataFrame, dataset: str) -> None:
    total = df.count()
    log.info("=== Quality report: %s (%d rows) ===", dataset, total)

    if dataset == "alerts":
        df.groupBy("datasource", "level").count() \
          .orderBy("datasource", "level").show(truncate=False)
        null_v = df.filter(F.col("value").isNull()).count()
        log.info("  Null value rows: %d (%.1f%%)", null_v, 100*null_v/max(total,1))

    elif dataset == "events":
        df.groupBy("datasource_count").count() \
          .orderBy("datasource_count").show()
        df.select(
            F.min("duration_minutes").alias("min_min"),
            F.max("duration_minutes").alias("max_min"),
            F.avg("duration_minutes").alias("avg_min"),
        ).show()
        # Show which datasources are actually firing
        for col in ["bgp_fired","ping_fired","ucsd_fired","merit_nt_fired"]:
            n = df.filter(F.col(col)).count()
            log.info("  %s: %d events", col, n)

    elif dataset == "signals":
        gap_count = df.filter(F.col("is_gap")).count()
        trail_count = df.filter(F.col("is_trailing_null")).count()
        log.info("  Gap rows (mid-series nulls): %d", gap_count - trail_count)
        log.info("  Trailing null rows (ingestion lag): %d", trail_count)
        df.groupBy("datasource").agg(
            F.count("*").alias("rows"),
            F.sum(F.col("is_gap").cast(IntegerType())).alias("gaps"),
            F.avg("value").alias("avg_value"),
            F.min("z_score").alias("min_z"),
            F.max("z_score").alias("max_z"),
        ).orderBy("datasource").show(truncate=False)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IODA Silver Layer PySpark job")
    p.add_argument("--start",
        default=(datetime.now(timezone.utc)-timedelta(days=2)).strftime("%Y-%m-%d"),
        help="Start date YYYY-MM-DD (default: 2 days ago)")
    p.add_argument("--end",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="End date YYYY-MM-DD (default: today)")
    p.add_argument("--datasets", nargs="+", choices=ALL_DATASETS,
        default=ALL_DATASETS)
    p.add_argument("--minio-endpoint",   default=MINIO_ENDPOINT)
    p.add_argument("--minio-access-key", default=MINIO_ACCESS_KEY)
    p.add_argument("--minio-secret-key", default=MINIO_SECRET_KEY)
    return p.parse_args()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = datetime.strptime(args.end,   "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc)

    log.info("Silver job: datasets=%s  window=[%s, %s]",
             args.datasets, args.start, args.end)

    spark = build_spark(args.minio_endpoint, args.minio_access_key,
                        args.minio_secret_key)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    TRANSFORMERS = {
        "alerts":  transform_alerts,
        "events":  transform_events,
        "signals": transform_signals,
    }

    for dataset in args.datasets:
        log.info("--- Processing: %s ---", dataset)
        try:
            df = TRANSFORMERS[dataset](spark, start, end)
            quality_report(df, dataset)
            write_silver(df, dataset)
        except Exception as exc:
            log.error("Failed to process %s: %s", dataset, exc, exc_info=True)

    log.info("Silver job complete.")
    spark.stop()


if __name__ == "__main__":
    main()