#!/usr/bin/env bash
# submit_silver.sh
# ==================
# Wrapper around spark-submit that:
#   1. Pulls the correct S3A / Hadoop-AWS JARs via Maven (first run downloads,
#      subsequent runs use the local .ivy2 cache)
#   2. Injects MinIO credentials from .env or environment variables
#   3. Forwards all extra arguments to silver_job.py
#
# Usage:
#   ./submit_silver.sh                              # last 2 days, all datasets
#   ./submit_silver.sh --start 2026-06-01 --end 2026-06-04
#   ./submit_silver.sh --datasets signals           # signals only
#   ./submit_silver.sh --start 2026-05-01 --end 2026-06-11  # full backfill
#
# Prerequisites:
#   - JAVA_HOME set (Java 11 or 17 recommended)
#   - spark-submit on PATH  (or set SPARK_HOME below)
#   - .env file present in the project root (one level up from spark/)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load .env from project root if present
ENV_FILE="$PROJECT_ROOT/.env"
if [ -f "$ENV_FILE" ]; then
    # Export only the variables we need, skip comments and blank lines
    set -a
    # shellcheck disable=SC1090
    source <(grep -E '^(S3_|MINIO_|KAFKA_)' "$ENV_FILE")
    set +a
    echo "Loaded credentials from $ENV_FILE"
fi

# Resolve spark-submit
SPARK_SUBMIT="${SPARK_HOME:-}/bin/spark-submit"
if ! command -v "$SPARK_SUBMIT" &>/dev/null; then
    SPARK_SUBMIT="spark-submit"
fi

# ---------------------------------------------------------------------------
# S3A JAR packages
# ---------------------------------------------------------------------------
# These three Maven coordinates pull the S3A filesystem connector and its
# AWS SDK dependencies. Version 3.3.4 matches Spark 3.4.x / 3.5.x.
# If you use a different Spark version, match the hadoop-aws version to
# the Hadoop version bundled with your Spark distribution:
#   Spark 3.3.x → hadoop-aws:3.3.2
#   Spark 3.4.x → hadoop-aws:3.3.4
#   Spark 3.5.x → hadoop-aws:3.3.4
PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# ---------------------------------------------------------------------------
# spark-submit
# ---------------------------------------------------------------------------
exec "$SPARK_SUBMIT" \
    --master local[*] \
    --packages "$PACKAGES" \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT_URL:-http://localhost:9000}" \
    --conf "spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY:-ioda_admin}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY:-}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.sql.shuffle.partitions=8" \
    --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
    --conf "spark.driver.memory=2g" \
    "$SCRIPT_DIR/gem_silver.py" \ # needs to be checked 
    "$@"
