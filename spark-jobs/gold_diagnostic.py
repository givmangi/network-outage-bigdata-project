# gold_diagnostic.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S3_ENDPOINT   = os.environ["S3_ENDPOINT_URL"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]
DB_USER = os.environ["TIMESCALEDB_USER"]
DB_PASS = os.environ["TIMESCALEDB_PASSWORD"]

spark = SparkSession.builder.appName("gold_diag").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

base_path = f"s3a://{SILVER_BUCKET}/ripe/ping"
df = spark.read.option("basePath", base_path).parquet(f"{base_path}/year=*/month=*/day=*")

print("=== SILVER SCHEMA ===")
df.printSchema()

print("=== SAMPLE ROWS ===")
df.select("ts_utc", "country_code", "asn", "rtt_avg_ms", "packet_loss").show(10, truncate=False)

print("=== AGGREGATION PREVIEW (what gold would write) ===")
baselines = (
    df
    .withColumn("time_window", F.date_trunc("hour", F.col("ts_utc")))
    .groupBy("time_window", "country_code", "asn")
    .agg(
        F.expr("percentile_approx(rtt_avg_ms, 0.5)").alias("rtt_median_ms"),
        F.expr("percentile_approx(packet_loss, 0.95)").alias("loss_95th_pct"),
        F.count("*").cast("int").alias("total_measurements")
    )
    .filter(F.col("asn").isNotNull())
)

print(f"Rows to write to gold: {baselines.count()}")
baselines.show(10, truncate=False)

print("=== TESTING DB CONNECTION ===")
try:
    DB_URL = "jdbc:postgresql://timescaledb:5432/outage_intelligence"
    DB_PROPERTIES = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }
    # Try reading from DB instead of writing — safer diagnostic
    existing = spark.read.jdbc(
        url=DB_URL,
        table="asn_baselines",
        properties=DB_PROPERTIES
    )
    print(f"Rows currently in asn_baselines: {existing.count()}")
except Exception as e:
    print(f"DB CONNECTION ERROR: {e}")

spark.stop()
