"""
gold_batch.py — Gold Layer Aggregation
=======================================
Reads clean Silver Parquet data, aggregates it into hourly baselines 
per ASN/Country, and writes the results to PostgreSQL (TimescaleDB).
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
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    print(">>> Reading Silver RIPE Ping Parquet files...")
    base_path = f"s3a://{SILVER_BUCKET}/ripe/ping"
    
    try:
        # THE FIX: Explicitly tell Spark to look inside the partition folders
        df = spark.read.option("basePath", base_path).parquet(f"{base_path}/year=*/month=*/day=*")
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read Parquet files. {e}")
        spark.stop()
        return

    # THE SAFETY NET: If we accidentally read old data without ASNs, don't crash.
    if "asn" not in df.columns:
        print("================================================================")
        print("WARNING: The 'asn' column is missing from your Parquet files!")
        print("These files were generated from old Bronze data before our ASN fix.")
        print("================================================================")
        df = df.withColumn("asn", F.lit(None).cast("integer"))

    print(">>> Aggregating into hourly baselines...")
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

    print(">>> Writing aggregated baselines to TimescaleDB...")
    baselines.write.jdbc(
        url=DB_URL,
        table="asn_baselines",
        mode="append",
        properties=DB_PROPERTIES
    )

    print(">>> Gold Baseline aggregation complete! Data is ready for Jupyter.")
    spark.stop()

if __name__ == "__main__":
    main()