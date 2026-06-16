#!/bin/bash
# entrypoint.sh — Spark container entrypoint wrapper
# =====================================================
# The apache/spark:3.5.1 image runs as the 'spark' user (uid 185).
# When Docker mounts a named volume over /opt/spark/.ivy2, the mount
# point is created by the Docker daemon as root:root with mode 755,
# meaning the spark user can read but NOT write into it.
#
# Ivy needs to create files inside .ivy2/cache/ before it can resolve
# --packages dependencies. This script pre-creates the required
# subdirectories as the current user (spark) before handing off to
# spark-submit, so the ownership is always correct regardless of
# how the volume was initialised.
#
# Usage (set as entrypoint in docker-compose, pass spark-submit args):
#   entrypoint.sh --master local[*] --packages ... myapp.py

set -euo pipefail

IVY_DIR="${SPARK_IVY_DIR:-/opt/spark/.ivy2}"

echo "[entrypoint] Ensuring Ivy cache directories exist and are writable..."
mkdir -p \
    "${IVY_DIR}/cache" \
    "${IVY_DIR}/jars" \
    "${IVY_DIR}/local"

echo "[entrypoint] Ivy dir owner: $(stat -c '%U:%G %a' ${IVY_DIR})"
echo "[entrypoint] Cache dir:     $(stat -c '%U:%G %a' ${IVY_DIR}/cache)"
echo "[entrypoint] Launching: /opt/spark/bin/spark-submit $*"

exec /opt/spark/bin/spark-submit "$@"