#!/bin/bash
# entrypoint.sh — Spark container entrypoint wrapper
# =====================================================
# The apache/spark:3.5.1 image runs as the 'spark' user (uid 185).
# Named Docker volumes are always initialised as root:root, so any
# path we try to mkdir inside a mounted volume will fail with
# "Permission denied" unless the volume was pre-populated.
#
# Solution: use /tmp/.ivy2 which is always writable by any user.
# /tmp is ephemeral per container run, so the JARs will be
# re-downloaded each time — acceptable for a dev stack.
# To persist the cache across runs without ownership fights,
# use a bind-mount (host directory) instead of a named volume;
# see the docker-compose.yml comment on spark-ivy-cache.

set -euo pipefail

IVY_DIR="/tmp/.ivy2"

echo "[entrypoint] Using Ivy cache at ${IVY_DIR} (always writable)"
mkdir -p "${IVY_DIR}/cache" "${IVY_DIR}/jars" "${IVY_DIR}/local"

echo "[entrypoint] Launching: /opt/spark/bin/spark-submit $*"
exec /opt/spark/bin/spark-submit \
    --conf "spark.jars.ivy=${IVY_DIR}" \
    "$@"