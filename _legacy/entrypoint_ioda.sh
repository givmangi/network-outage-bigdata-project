#!/bin/bash
# entrypoint-ioda.sh — spark-submit wrapper for silver_ioda.py
# All spark-submit flags live here. Whatever 'docker compose run' passes
# after the service name lands in "$@" and is forwarded as Python app args
# (i.e. after the .py file), which is exactly what argparse expects.

set -euo pipefail

IVY_DIR="/tmp/.ivy2"
echo "[entrypoint] Using Ivy cache at ${IVY_DIR}"
mkdir -p "${IVY_DIR}/cache" "${IVY_DIR}/jars" "${IVY_DIR}/local"

echo "[entrypoint] App args: $*"

exec /opt/spark/bin/spark-submit \
    --master local[*] \
    --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
    --conf "spark.jars.ivy=${IVY_DIR}" \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT_URL:-http://minio:9000}" \
    --conf "spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.sql.shuffle.partitions=8" \
    --conf "spark.sql.files.ignoreMissingFiles=true" \
    /opt/spark-job/silver_ioda.py \
    "$@"