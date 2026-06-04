"""
IODA Bronze Ingestion — Kafka + MinIO backend
===============================================
This is the containerised version of ioda_bronze_ingestion.py adapted for
the Docker stack. Two key differences from the original script:

1. Every ingested record is published to a Kafka topic in addition to
   being written to MinIO. Kafka is the real-time transport layer;
   MinIO is the durable persistence layer. Both receive the same data.

2. All configuration (credentials, endpoints, entity codes) is read from
   environment variables injected by Docker Compose — no CLI flags needed
   when running inside the container.

Data flow per ingestion run:
                         ┌─────────────────────────────────────┐
  IODA REST API  ──────► │  Python ingester (this script)      │
                         │                                     │
                         │  _paginate_alerts()                 │
                         │  _paginate_events()                 │
                         │  _fetch_signals()                   │
                         └────────────┬────────────────────────┘
                                      │ raw JSON records
                          ┌───────────┴────────────┐
                          ▼                         ▼
               Kafka topic                    MinIO bucket
          raw.ioda.alerts                  ioda-bronze/
          raw.ioda.events              ioda/alerts/year=.../
          raw.ioda.signals             ioda/events/year=.../
                                       ioda/signals/year=.../

Kafka topics created:
  raw.ioda.alerts   - one message per alert object, key = entity_code
  raw.ioda.events   - one message per event object, key = entity_code
  raw.ioda.signals  - one message per expanded time-step row, key = entity_code

MinIO object layout (Hive-partitioned for Spark):
  ioda-bronze/
    ioda/alerts/year=YYYY/month=MM/day=DD/<entity_type>_<code>_<datasource>.ndjson.gz
    ioda/events/year=YYYY/month=MM/day=DD/<entity_type>_<code>.ndjson.gz
    ioda/signals/year=YYYY/month=MM/day=DD/<entity_type>_<code>_<datasource>.ndjson.gz
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Generator, Iterator

import boto3
import requests
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration — all from environment variables
# ---------------------------------------------------------------------------

IODA_BASE_URL        = os.environ.get("IODA_BASE_URL",         "https://api.ioda.inetintel.cc.gatech.edu/v2")
KAFKA_BOOTSTRAP      = os.environ.get("KAFKA_BOOTSTRAP_SERVERS","kafka:29092")
S3_ENDPOINT          = os.environ.get("S3_ENDPOINT_URL",        "http://minio:9000")
S3_ACCESS_KEY        = os.environ.get("S3_ACCESS_KEY",          "ioda_admin")
S3_SECRET_KEY        = os.environ.get("S3_SECRET_KEY",          "")
S3_BUCKET_BRONZE     = os.environ.get("S3_BUCKET_BRONZE",       "ioda-bronze")
ENTITY_TYPE          = os.environ.get("ENTITY_TYPE",            "country")
ENTITY_CODES         = os.environ.get("ENTITY_CODES",           "GM").split()
LOOKBACK_MINUTES     = int(os.environ.get("LOOKBACK_MINUTES",   "20"))
POLL_INTERVAL_SEC    = int(os.environ.get("POLL_INTERVAL_SECONDS","900"))

DATASOURCES   = ["bgp", "ping-slash24", "ucsd-nt"]
PAGE_SIZE     = 100
REQUEST_DELAY = 1.1   # seconds between IODA API calls

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("ioda_bronze")


# ---------------------------------------------------------------------------
# HTTP client (IODA API)
# ---------------------------------------------------------------------------

def _build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "IODA-BronzeIngestion/1.0 (UniTrento)"
    session.headers["Accept"]     = "application/json"
    return session

HTTP = _build_http_session()


def _api_get(endpoint: str, params: dict[str, Any]) -> Any:
    """
    Single IODA API call. Returns the value of the 'data' key.
    IODA wraps every response in {type, error, queryParameters, data}.
    We unwrap it here so callers only see the payload they care about.
    """
    resp = HTTP.get(f"{IODA_BASE_URL}{endpoint}", params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if body.get("error"):
        raise RuntimeError(f"IODA error on {endpoint}: {body['error']}")
    time.sleep(REQUEST_DELAY)
    return body.get("data", [])


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def _build_kafka_producer() -> KafkaProducer:
    """
    Build a KafkaProducer with JSON serialisation.

    key_serializer:   encodes the string entity code to bytes (e.g. b"IT")
    value_serializer: serialises the full Python dict to a compact JSON bytestring

    acks='all': wait for the broker to acknowledge the write before returning.
    This is slower than acks=1 (leader only) but guarantees the message is
    durable on disk before we move on — important for Bronze data where we
    must not silently drop records.

    retries=5: automatically retry on transient broker errors (leader election,
    network blip). Combined with the acks='all' setting this gives us
    at-least-once delivery semantics.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,        # batch messages for 50ms before sending (throughput)
        compression_type="gzip",  # compress batches in transit
    )


# ---------------------------------------------------------------------------
# MinIO / S3 client
# ---------------------------------------------------------------------------

def _build_s3_client():
    """
    Build a boto3 S3 client pointed at the local MinIO instance.

    endpoint_url overrides the default AWS endpoint — this is the only
    change needed to make boto3 talk to MinIO instead of real S3.
    Everything else (put_object, get_object, list_objects_v2) is identical
    to the AWS API.
    """
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",   # MinIO ignores region but boto3 requires it
    )


# ---------------------------------------------------------------------------
# IODA API pagination helpers
# (same logic as original script — see ioda_bronze_ingestion.py for comments)
# ---------------------------------------------------------------------------

def _paginate_alerts(entity_type: str, entity_code: str, datasource: str,
                     from_ts: int, until_ts: int) -> Generator[dict, None, None]:
    page = 0
    endpoint = f"/outages/alerts"
    while True:
        data = _api_get(endpoint, {"from": from_ts, "until": until_ts,
                                   "datasource": datasource,
                                   "limit": PAGE_SIZE, "page": page, 
                                   "entityCode": entity_code, "entityType": entity_type})
        if not data:
            break
        yield from data
        if len(data) < PAGE_SIZE:
            break
        page += 1


def _paginate_events(entity_type: str, entity_code: str,
                     from_ts: int, until_ts: int) -> Generator[dict, None, None]:
    page = 0
    endpoint = f"/outages/events"
    while True:
        data = _api_get(endpoint, {"from": from_ts, "until": until_ts,
                                   "format": "ioda", "includeAlerts": "true",
                                   "limit": PAGE_SIZE, "page": page,
                                   "entityCode": entity_code, "entityType": entity_type})
        if not data:
            break
        yield from data
        if len(data) < PAGE_SIZE:
            break
        page += 1


def _fetch_signals(entity_type: str, entity_code: str, datasource: str,
                   from_ts: int, until_ts: int) -> list[dict]:
    return _api_get(f"/signals",
                    {"from": from_ts, "until": until_ts,
                    "datasource": datasource, "maxPoints": 1440,
                    "entityCode": entity_code, "entityType": entity_type})


def _expand_signal(records: list[dict]) -> list[dict]:
    """
    Expand the compact (from, step, values[]) signal response into
    one flat dict per time step with an explicit 'ts' field.
    Null values are kept — they indicate IODA collection gaps.
    """
    expanded = []
    for r in records:
        base, step = r.get("from", 0), r.get("step", 60)
        for i, val in enumerate(r.get("values", [])):
            expanded.append({
                "entityType": r.get("entityType"),
                "entityCode": r.get("entityCode"),
                "datasource": r.get("datasource"),
                "ts":         base + i * step,
                "value":      val,
                "step":       step,
                "nativeStep": r.get("nativeStep", step),
            })
    return expanded


# ---------------------------------------------------------------------------
# Dual-write: Kafka + MinIO
# ---------------------------------------------------------------------------

def _s3_key(layer: str, entity_type: str, entity_code: str,
             datasource: str | None, run_date: datetime) -> str:
    """
    Build the S3 object key in Hive-partitioned format.
    This is the MinIO equivalent of the local _partition_path() function.

    Example output:
      ioda/alerts/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
    """
    date_part = f"year={run_date.year:04d}/month={run_date.month:02d}/day={run_date.day:02d}"
    if datasource:
        fname = f"{entity_type}_{entity_code}_{datasource}.ndjson.gz"
    else:
        fname = f"{entity_type}_{entity_code}.ndjson.gz"
    return f"ioda/{layer}/{date_part}/{fname}"


def _dual_write(
    records: Iterator[dict],
    kafka_topic: str,
    kafka_key: str,
    s3_client,
    s3_key: str,
    producer: KafkaProducer,
) -> int:
    """
    Stream records to Kafka (one message per record) and simultaneously
    buffer them for a single bulk MinIO upload at the end.

    Why buffer + bulk upload rather than streaming directly to MinIO?
    MinIO's S3 API is object-based: you cannot append to an existing object.
    You must either upload all bytes at once (put_object) or use the
    multipart upload API for large objects. For the volumes involved here
    (hundreds to tens of thousands of records per run), buffering in memory
    and doing a single put_object is simpler and fast enough.

    The in-memory gzip buffer is created with io.BytesIO, which acts as
    a file-like object. We write NDJSON lines into it through a GzipFile,
    then seek back to position 0 and upload the whole compressed blob.

    Returns the total number of records written.
    """
    # In-memory gzip buffer for the MinIO object
    buf = io.BytesIO()
    count = 0

    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for record in records:
            # 1. Kafka: publish immediately so consumers see real-time data
            producer.send(
                topic=kafka_topic,
                key=kafka_key,
                value=record,
            )

            # 2. Buffer for MinIO
            line = json.dumps(record, separators=(",", ":")) + "\n"
            gz.write(line.encode("utf-8"))
            count += 1

    if count == 0:
        log.debug("  no records — skipping MinIO upload for %s", s3_key)
        return 0

    # Flush Kafka batch before uploading to MinIO.
    # This ensures Kafka consumers can act on the data even if the
    # MinIO upload subsequently fails.
    producer.flush()

    # Seek back to start of buffer and upload
    buf.seek(0)
    s3_client.put_object(
        Bucket=S3_BUCKET_BRONZE,
        Key=s3_key,
        Body=buf,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    log.info("  MinIO ← s3://%s/%s  (%d records)", S3_BUCKET_BRONZE, s3_key, count)
    return count


# ---------------------------------------------------------------------------
# Per-entity ingestion orchestrator
# ---------------------------------------------------------------------------

def ingest_entity(
    entity_type: str,
    entity_code: str,
    from_ts: int,
    until_ts: int,
    run_date: datetime,
    producer: KafkaProducer,
    s3: Any,
) -> dict[str, int]:
    """
    Ingest one entity over one time window: alerts + events + signals.
    Writes to both Kafka and MinIO in a single pass over the data.
    """
    summary: dict[str, int] = {}

    # --- Alerts (per datasource) ------------------------------------------
    for ds in DATASOURCES:
        log.info("[alerts] %s/%s  ds=%s  [%s → %s]",
                 entity_type, entity_code, ds,
                 _ts(from_ts), _ts(until_ts))
        try:
            n = _dual_write(
                records=_paginate_alerts(entity_type, entity_code, ds, from_ts, until_ts),
                kafka_topic="raw.ioda.alerts",
                kafka_key=entity_code,
                s3_client=s3,
                s3_key=_s3_key("alerts", entity_type, entity_code, ds, run_date),
                producer=producer,
            )
            summary[f"alerts_{ds}"] = n
        except Exception as exc:
            log.warning("  WARN alerts %s/%s/%s: %s", entity_type, entity_code, ds, exc)
            summary[f"alerts_{ds}_error"] = 1

    # --- Events (all datasources combined) --------------------------------
    log.info("[events] %s/%s  [%s → %s]",
             entity_type, entity_code, _ts(from_ts), _ts(until_ts))
    try:
        n = _dual_write(
            records=_paginate_events(entity_type, entity_code, from_ts, until_ts),
            kafka_topic="raw.ioda.events",
            kafka_key=entity_code,
            s3_client=s3,
            s3_key=_s3_key("events", entity_type, entity_code, None, run_date),
            producer=producer,
        )
        summary["events"] = n
    except Exception as exc:
        log.warning("  WARN events %s/%s: %s", entity_type, entity_code, exc)
        summary["events_error"] = 1

    # --- Signals (per datasource) -----------------------------------------
    for ds in DATASOURCES:
        log.info("[signals] %s/%s  ds=%s  [%s → %s]",
                 entity_type, entity_code, ds, _ts(from_ts), _ts(until_ts))
        try:
            raw = _fetch_signals(entity_type, entity_code, ds, from_ts, until_ts)
            expanded = _expand_signal(raw)
            n = _dual_write(
                records=iter(expanded),
                kafka_topic="raw.ioda.signals",
                kafka_key=entity_code,
                s3_client=s3,
                s3_key=_s3_key("signals", entity_type, entity_code, ds, run_date),
                producer=producer,
            )
            summary[f"signals_{ds}"] = n
        except Exception as exc:
            log.warning("  WARN signals %s/%s/%s: %s", entity_type, entity_code, ds, exc)
            summary[f"signals_{ds}_error"] = 1

    return summary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(unix: int) -> str:
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


# ---------------------------------------------------------------------------
# One-shot run (used by run_loop.py)
# ---------------------------------------------------------------------------

def run_once(producer: KafkaProducer, s3) -> None:
    """
    Execute one incremental ingestion pass for all configured entities.
    Fetches the last LOOKBACK_MINUTES of data for each entity code.
    """
    until_ts  = _now()
    from_ts   = until_ts - LOOKBACK_MINUTES * 60
    run_date  = datetime.fromtimestamp(from_ts, tz=timezone.utc)

    log.info("=== Incremental run: window=[%s, %s]  entities=%s",
             _ts(from_ts), _ts(until_ts), ENTITY_CODES)

    total: dict[str, int] = {}
    for code in ENTITY_CODES:
        s = ingest_entity(ENTITY_TYPE, code, from_ts, until_ts, run_date, producer, s3)
        for k, v in s.items():
            total[k] = total.get(k, 0) + v

    log.info("Run complete: %s", total)


# ---------------------------------------------------------------------------
# Backfill entrypoint (called directly for historical loads)
# ---------------------------------------------------------------------------

def run_backfill(days_back: int, producer: KafkaProducer, s3) -> None:
    from datetime import timedelta
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    for i in range(days_back, 0, -1):
        day_start = now - timedelta(days=i)
        day_end   = day_start + timedelta(days=1)
        from_ts   = int(day_start.timestamp())
        until_ts  = int(day_end.timestamp())

        log.info("=== Backfill day %s", day_start.strftime("%Y-%m-%d"))
        for code in ENTITY_CODES:
            ingest_entity(ENTITY_TYPE, code, from_ts, until_ts,
                          day_start, producer, s3)