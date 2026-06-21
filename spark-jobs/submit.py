"""
submit.py — universal Spark launcher for all Silver jobs
=========================================================
Replaces the three entrypoint-*.sh scripts with a pure-Python alternative
that works on Windows hosts (no CRLF/LF issues, no chmod needed).

Called by docker-compose as:
    python /opt/spark-job/submit.py ioda  [app args forwarded to silver_ioda.py]
    python /opt/spark-job/submit.py ripe  [app args forwarded to silver_ripe.py]
    python /opt/spark-job/submit.py stream

The first argument selects which PySpark script to run. Everything after it
is passed through to that script as application arguments (e.g. --start/--end).
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
    "org.postgresql:postgresql:42.6.0" # <--- ADDED THIS LINE
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
    "spark.sql.files.ignoreMissingFiles=true",
    # DYNAMIC: overwrite only the day partitions touched by each write.
    "spark.sql.sources.partitionOverwriteMode=DYNAMIC",
    # 'directory' committer is bundled in hadoop-aws and correctly enforces
    # partition-level isolation on S3/MinIO. The 'partitioned' committer
    # requires hadoop-cloud-storage (absent from apache/spark:3.5.1) and
    # causes ClassNotFoundException: PathOutputCommitProtocol at write time.
    "spark.hadoop.fs.s3a.committer.name=directory",
    "spark.hadoop.fs.s3a.committer.staging.conflict-mode=replace",
    "spark.hadoop.fs.s3a.committer.staging.tmp.path=tmp/staging",
]

JOBS = {
    "ioda": {
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-job/silver_ioda.py",
        "confs":    COMMON_CONFS,
    },
    "ripe": {
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-job/silver_ripe.py",
        "confs":    COMMON_CONFS,
    },
    "stream": {
        "packages": STREAM_PACKAGES,
        "script":   "/opt/spark-job/silver_streaming.py",
        "confs":    COMMON_CONFS + ["spark.streaming.stopGracefullyOnShutdown=true"],
    },
    "gold": {   # <--- ADDED THIS BLOCK
        "packages": BASE_PACKAGES,
        "script":   "/opt/spark-job/gold_batch.py",
        "confs":    COMMON_CONFS,
    },
    "diag": {
    "packages": BASE_PACKAGES,
    "script":   "/opt/spark-job/gold_diagnostic.py",
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
cmd = ["/opt/spark/bin/spark-submit", "--master", "local[*]",
        "--driver-memory", "2g",
       "--packages", job["packages"]]

for conf in job["confs"]:
    cmd += ["--conf", conf]

cmd.append(job["script"])   # the Python app file
cmd.extend(app_args)         # --start / --end / --datasets etc.

print(f"[submit.py] Launching: {' '.join(cmd)}", flush=True)
result = subprocess.run(cmd)
sys.exit(result.returncode)