"""
run_loop.py — container entrypoint
====================================
This is the CMD that Docker runs. It handles three concerns that don't
belong in the ingestion logic itself:

1. Startup wait: Kafka and MinIO may not be fully ready when this container
   starts (Docker's healthcheck grace period can lag behind reality). We
   retry initialisation with exponential backoff.

2. Polling loop: calls ioda_ingest.run_once() every POLL_INTERVAL_SECONDS.

3. Graceful shutdown: catches SIGTERM (sent by `docker compose down`) and
   lets the current run finish before exiting.
"""

import logging
import os
import signal
import sys
import time

from starting_pipe import (
    POLL_INTERVAL_SEC,
    _build_kafka_producer,
    _build_s3_client,
    run_once,
    run_backfill,
    ENTITY_CODES,
    ENTITY_TYPE,
)

log = logging.getLogger("run_loop")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown = False

def _handle_sigterm(signum, frame):
    global _shutdown
    log.info("SIGTERM received — will exit after current run completes")
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)


# ---------------------------------------------------------------------------
# Startup: wait for Kafka and MinIO to be reachable
# ---------------------------------------------------------------------------

def _wait_for_services(max_attempts: int = 10) -> tuple:
    """
    Try to establish connections to Kafka and MinIO with exponential backoff.
    Returns (producer, s3_client) once both are reachable.

    Why not rely on Docker's depends_on + healthcheck alone?
    Docker marks a container healthy when its healthcheck passes, but the
    Kafka broker may still be in leader-election for a few seconds after
    the healthcheck returns 0. Retrying here absorbs that gap cleanly
    without adding an artificial sleep to docker-compose.yml.
    """
    delay = 5
    for attempt in range(1, max_attempts + 1):
        try:
            log.info("Connecting to Kafka and MinIO (attempt %d/%d)…",
                     attempt, max_attempts)

            producer = _build_kafka_producer()
            s3       = _build_s3_client()

            # Probe MinIO: list bucket to confirm credentials and network work
            s3.list_objects_v2(Bucket=os.environ.get("S3_BUCKET_BRONZE", "bronze"),
                                MaxKeys=1)

            log.info("Connected to Kafka and MinIO successfully.")
            return producer, s3

        except Exception as exc:
            log.warning("  Connection failed: %s — retrying in %ds", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 60)   # cap at 60s

    log.error("Could not connect after %d attempts — exiting.", max_attempts)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    # Check if we were invoked with a backfill argument
    # Usage: docker compose run --rm ingester python run_loop.py backfill 7
    if len(sys.argv) >= 2 and sys.argv[1] == "backfill":
        days = int(sys.argv[2]) if len(sys.argv) >= 3 else 7
        log.info("Starting backfill mode: %d days back, entities=%s", days, ENTITY_CODES)
        producer, s3 = _wait_for_services()
        run_backfill(days, producer, s3)
        producer.close()
        log.info("Backfill complete — exiting.")
        return

    # Normal incremental loop
    log.info("Starting incremental polling loop. Interval=%ds  Entities=%s",
             POLL_INTERVAL_SEC, ENTITY_CODES)

    producer, s3 = _wait_for_services()

    while not _shutdown:
        run_start = time.monotonic()

        try:
            run_once(producer, s3)
        except Exception as exc:
            # Log but do not crash — a transient API error should not
            # kill the polling loop. The next run will cover the gap
            # because LOOKBACK_MINUTES > POLL_INTERVAL_SEC / 60.
            log.error("Run failed (will retry next interval): %s", exc, exc_info=True)

        elapsed = time.monotonic() - run_start
        sleep_for = max(0, POLL_INTERVAL_SEC - elapsed)

        log.info("Sleeping %.0fs until next run…", sleep_for)
        # Sleep in 1s increments so SIGTERM is handled promptly
        for _ in range(int(sleep_for)):
            if _shutdown:
                break
            time.sleep(1)

    log.info("Shutdown complete.")
    producer.close()


if __name__ == "__main__":
    main()