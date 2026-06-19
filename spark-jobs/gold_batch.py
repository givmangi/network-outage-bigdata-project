"""
gold_batch.py — Gold Layer Aggregation
=======================================
Reads clean Silver Parquet data, aggregates it into hourly baselines
per ASN/Country for RIPE, and hourly signal averages per country/datasource
for IODA, then writes both to PostgreSQL (TimescaleDB).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# MinIO / S3 Configuration (Strictly pulled from .env via Docker)
S3_ENDPOINT   = os.environ["S3_ENDPOINT_URL"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]

# Database Configuration (Strictly pulled from .env via Docker)
DB_USER = os.environ["TIMESCALEDB_USER"]
DB_PASS = os.environ["TIMESCALEDB_PASSWORD"]

DB_URL = "jdbc:postgresql://timescaledb:5432/outage_intelligence"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified" 
}

def main():
    spark = SparkSession.builder \
        .appName("gold_batch_baselines") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------------------------------------------
    # RIPE: hourly RTT and packet loss baselines per ASN per country
    # -------------------------------------------------------------------------
    
    print(">>> Reading Silver RIPE Ping Parquet files...")
    base_path = f"s3a://{SILVER_BUCKET}/ripe/ping"
    
    try:
        # THE FIX: Explicitly tell Spark to look inside the partition folders
        df = spark.read.option("basePath", base_path).parquet(f"{base_path}/year=*/month=*/day=*")
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read Parquet files. {e}")
        df = None

    # THE SAFETY NET: If we accidentally read old data without ASNs, don't crash.
    if df is not None:
        if "asn" not in df.columns:
            print("WARNING: 'asn' column missing — filling with NULL")
            df = df.withColumn("asn", F.lit(None).cast("integer"))

        # Filter out RIPE sentinel value for failed measurements (-1.0 means no response)
        df = df.filter(F.col("rtt_avg_ms") > 0)

        print(">>> Aggregating RIPE into hourly baselines...")
        baselines = (
            df
            .withColumn("time_window", F.date_trunc("hour", F.col("ts_utc")))
            .groupBy("time_window", "country_code", "asn")
            .agg(
                F.expr("percentile_approx(rtt_avg_ms, 0.5)").alias("rtt_median_ms"),
                F.expr("percentile_approx(packet_loss, 0.95)").alias("loss_95th_pct"),
                F.count("*").cast("int").alias("total_measurements")
            )
            # Drop records that lack an ASN so we don't pollute the database
            .filter(F.col("asn").isNotNull())
        )

        print(">>> Writing RIPE baselines to TimescaleDB...")
        baselines.write.jdbc(
            url=DB_URL,
            table="asn_baselines",
            mode="overwrite",
            properties=DB_PROPERTIES
        )

        print(">>> RIPE baselines written.")
    else:
        print(">>> Skipping RIPE — no data found.")

    # -------------------------------------------------------------------------
    # IODA: hourly signal averages per country per datasource
    # -------------------------------------------------------------------------
    print(">>> Reading Silver IODA Signals Parquet files...")
    ioda_path = f"s3a://{SILVER_BUCKET}/ioda/signals"

    try:
        ioda_df = spark.read.option("basePath", ioda_path).parquet(f"{ioda_path}/year=*/month=*/day=*")
    except Exception as e:
        print(f"WARNING: Could not read IODA Parquet files: {e}")
        ioda_df = None

    if ioda_df is not None:
        print(">>> Aggregating IODA into hourly signals...")
        ioda_gold = (
            ioda_df
            .filter(F.col("ts_utc").isNotNull())
            .filter(F.col("value").isNotNull())
            .withColumn("time_window", F.date_trunc("hour", F.col("ts_utc")))
            .groupBy("time_window", "entity_code", "datasource")
            .agg(
                F.avg("value").alias("signal_value"),
                F.max("collection_gap").alias("collection_gap")
            )
            .withColumnRenamed("entity_code", "country_code")
        )

        print(">>> Writing IODA signals to TimescaleDB...")
        ioda_gold.write.jdbc(
            url=DB_URL,
            table="ioda_signals",
            mode="overwrite",
            properties=DB_PROPERTIES
        )
        print(">>> IODA signals written.")
    else:
        print(">>> Skipping IODA — no data found.")

    print(">>> Gold batch complete!")
    spark.stop()


if __name__ == "__main__":
    main()