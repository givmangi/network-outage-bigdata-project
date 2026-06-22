"""
submit.py — universal Spark launcher for all Silver + Gold jobs
================================================================
Replaces the three entrypoint-*.sh scripts with a pure-Python alternative
that works on Windows hosts (no CRLF/LF issues, no chmod needed).

Called by docker-compose as:
    python /opt/spark-jobs/submit.py ioda    [--start X --end Y --datasets ...]
    python /opt/spark-jobs/submit.py ripe    [--start X --end Y]
    python /opt/spark-jobs/submit.py stream
    python /opt/spark-jobs/submit.py gold    [--start X --end Y --datasets ...]
    python /opt/spark-jobs/submit.py gold-stream

The first argument selects which PySpark script to run. Everything after it
is passed through to that script as application arguments.
"""

import os
import subprocess
import sys

# ---------------------------------------------------------------------------
# Ivy cache — /tmp is always writable by any user, no volume ownership issues
# ---------------------------------------------------------------------------
IVY_DIR = "/tmp/.ivy2"
for sub in ("cache", "jars", "local"):
    os.makedirs(f"{IVY_DIR}/{sub}", exist_ok=True)

# ---------------------------------------------------------------------------
# Job registry
# ---------------------------------------------------------------------------
S3_ENDPOINT = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
S3_KEY      = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET   = os.environ.get("S3_SECRET_KEY", "")

BASE_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.6.0"
)
STREAM_PACKAGES = (
    BASE_PACKAGES
    + ",org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

COMMON_CONFS = [
    f"spark.jars.ivy={IVY_DIR}",
    f"spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT}",
    f"spark.hadoop.fs.s3a.access.key={S3_KEY}",
    f"spark.hadoop.fs.s3a.secret.key={S3_SECRET}",
    "spark.hadoop.fs.s3a.path.style.access=true",
    "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
    "spark.sql.shuffle.partitions=8",
    "spark.sql.shuffle.partitions=4",      # fewer partitions = less overhead for small data
    "spark.hadoop.fs.s3a.metrics.system.enabled=false",  # kills the MetricsConfig warning too  
    "spark.sql.files.ignoreMissingFiles=true",
    # DYNAMIC: overwrite only the day partitions touched by each write.
    "spark.sql.sources.partitionOverwriteMode=DYNAMIC",
    "spark.hadoop.fs.s3a.metrics.system.enabled=false",
    # 'directory' committer is bundled in hadoop-aws and correctly enforces
    # partition-level isolation on S3/MinIO.
    "spark.hadoop.fs.s3a.committer.name=directory",
    "spark.hadoop.fs.s3a.committer.staging.conflict-mode=replace",
    "spark.hadoop.fs.s3a.committer.staging.tmp.path=tmp/staging",
]

JOBS = {
    "ioda": {
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-jobs/silver_ioda.py",
        "confs":    COMMON_CONFS,
    },
    "ripe": {
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-jobs/silver_ripe.py",
        "confs":    COMMON_CONFS,
    },
    "stream": {
        "packages": STREAM_PACKAGES,
        "script":   "/opt/spark-jobs/silver_streaming.py",
        "confs":    COMMON_CONFS + ["spark.streaming.stopGracefullyOnShutdown=true"],
    },
    "gold": {
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-jobs/gold_batch.py",
        "confs":    COMMON_CONFS,
        "master":   "local[8]",  # cap at 8 cores since gold is IO bound
    },
    # Gold streaming: reads Silver Parquet from MinIO, promotes to TimescaleDB.
    # Does NOT need Kafka packages (reads files, not topics).
    # Does NOT need the S3A committer confs (it only reads Silver, never writes Parquet).
    "gold-stream": {
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-jobs/gold_streaming.py",
        "confs":    COMMON_CONFS + ["spark.streaming.stopGracefullyOnShutdown=true"],
    },
    "diag": {
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-jobs/gold_diagnostic.py",
        "confs":    COMMON_CONFS,
    },
}

# ---------------------------------------------------------------------------
# Parse job name from first argument
# ---------------------------------------------------------------------------
if len(sys.argv) < 2 or sys.argv[1] not in JOBS:
    print(f"Usage: python submit.py [{' | '.join(JOBS)}] [app args...]", file=sys.stderr)
    sys.exit(1)

job_name = sys.argv[1]
app_args = sys.argv[2:]   # everything after the job name → forwarded to the .py script
job      = JOBS[job_name]

# ---------------------------------------------------------------------------
# Build spark-submit command
# ---------------------------------------------------------------------------
master = job.get("master", "local[2]")
cmd = [
    "/opt/spark/bin/spark-submit",
    "--master", master,
    "--driver-memory", "4g", #trying to use more mem
    "--conf", "spark.driver.maxResultSize=2g",  # default 1g too small

    "--packages", job["packages"],
]

for conf in job["confs"]:
    cmd += ["--conf", conf]

cmd.append(job["script"])
cmd.extend(app_args)

print(f"[submit.py] Launching: {' '.join(cmd)}", flush=True)
result = subprocess.run(cmd)
sys.exit(result.returncode)