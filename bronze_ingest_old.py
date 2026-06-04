"""
IODA Bronze Layer Ingestion Pipeline
=====================================
Pulls raw data from the IODA v2 API and writes it to a partitioned
Bronze data lake in newline-delimited JSON (NDJSON) format.

Data sources consumed:
  - /outages/alerts    -> raw per-signal anomaly alerts (bgp, ping-slash24, ucsd-nt)
  - /outages/events    -> corroborated outage events with duration and score
  - /signals           -> raw time-series values for a given entity+datasource

Directory layout produced (example for a run on 2026-06-04):
  bronze/
    ioda/
      alerts/
        year=2026/month=06/day=04/
          country_IT_bgp.ndjson.gz
          country_IT_ping-slash24.ndjson.gz
          asn_1234_bgp.ndjson.gz
          ...
      events/
        year=2026/month=06/day=04/
          country_IT.ndjson.gz
          asn_1234.ndjson.gz
          ...
      signals/
        year=2026/month=06/day=04/
          country_IT_bgp.ndjson.gz
          country_IT_ping-slash24.ndjson.gz
          ...

Usage (manual / cron):
  # One-shot historical backfill for Italy, last 7 days:
  python ioda_bronze_ingestion.py \
      --mode backfill \
      --entity-type country \
      --entity-codes IT \
      --days-back 7 \
      --output-dir ./bronze

  # Incremental run (fetches last N minutes, safe to run via cron):
  python ioda_bronze_ingestion.py \
      --mode incremental \
      --entity-type country \
      --entity-codes IT DE FR \
      --lookback-minutes 15 \
      --output-dir ./bronze

  # ASN-level ingestion:
  python ioda_bronze_ingestion.py \
      --mode incremental \
      --entity-type asn \
      --entity-codes 1299 3356 6762 \
      --lookback-minutes 15 \
      --output-dir ./bronze
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://api.ioda.inetintel.cc.gatech.edu/v2"

# All three data sources IODA provides.
# Each represents a different measurement methodology:
#   bgp          -> BGP routing table visibility (control-plane)
#   ping-slash24 -> Active probing of /24 address blocks (data-plane)
#   ucsd-nt      -> Network telescope background radiation (passive)
DATASOURCES = ["bgp", "ping-slash24", "ucsd-nt"]

# Signals resolution: IODA natively emits one data point per minute (60s step).
# We request at most 1440 points (= 24 hours at 1-min resolution) per call
# to stay within API limits.
MAX_SIGNAL_POINTS = 1440

# Pagination: IODA returns at most 100 alert objects per page by default.
# We explicitly request the maximum to minimise round-trips.
PAGE_SIZE = 100

# Polite retry strategy: 3 retries with exponential backoff (1s, 2s, 4s).
# This handles transient 5xx errors and brief network hiccups without
# hammering the API.
RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

# Minimum sleep between successive API requests (seconds).
# IODA does not publish a formal rate-limit, but their usage guidelines
# recommend not exceeding ~1 request/second for sustained polling.
REQUEST_DELAY_SEC = 1.1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ioda_bronze")


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    """
    Build a requests Session with automatic retries and a realistic
    User-Agent so IODA's logs can identify our client.
    """
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=RETRY_STRATEGY)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "IODA-BronzeIngestion/1.0 (academic project; UniTrento)",
        "Accept": "application/json",
    })
    return session


SESSION = build_session()


def _get(endpoint: str, params: dict[str, Any]) -> Any:
    """
    Perform a single authenticated GET request and return the parsed
    JSON payload under the 'data' key.

    IODA always wraps its payload in:
      {
        "type":            <str>,
        "error":           null | <str>,
        "queryParameters": {...},
        "data":            <list | dict>   <- what we care about
      }

    If the 'error' field is non-null we raise immediately so the caller
    knows the request was syntactically valid but semantically rejected
    (e.g. unknown entity code).
    """
    url = f"{BASE_URL}{endpoint}"
    log.debug("GET %s  params=%s", url, params)

    resp = SESSION.get(url, params=params, timeout=30)
    resp.raise_for_status()

    body = resp.json()

    if body.get("error"):
        raise RuntimeError(f"IODA API error for {endpoint}: {body['error']}")

    time.sleep(REQUEST_DELAY_SEC)   # polite throttle
    return body.get("data", [])


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------

def _paginate_alerts(
    entity_type: str,
    entity_code: str,
    datasource: str,
    from_ts: int,
    until_ts: int,
) -> Generator[dict, None, None]:
    """
    Yield every alert object for a given (entity, datasource, time window),
    transparently handling IODA's page-based pagination.

    IODA paginates alerts with a simple page/size scheme:
      ?page=0&limit=100  -> first 100 results
      ?page=1&limit=100  -> next 100 results
      ...
    We keep incrementing 'page' until the response is an empty list,
    which is IODA's signal that there are no more results.

    Why per-datasource?
    Splitting by datasource means each output file represents a single,
    homogeneous signal type. This makes downstream Spark jobs cleaner:
    a reader for the 'bgp' partition never encounters 'ping-slash24'
    schema quirks and vice versa.
    """
    page = 0
    endpoint = f"/outages/alerts/{entity_type}/{entity_code}"

    while True:
        params = {
            "from":       from_ts,
            "until":      until_ts,
            "datasource": datasource,
            "limit":      PAGE_SIZE,
            "page":       page,
        }

        data = _get(endpoint, params)

        # IODA returns an empty list (not an error) when all pages exhausted.
        if not data:
            log.debug("  alerts page %d: empty — pagination complete", page)
            break

        log.debug("  alerts page %d: got %d records", page, len(data))
        for record in data:
            yield record

        # If we received fewer records than the page size we are on the
        # last page and can stop without making a gratuitous extra request.
        if len(data) < PAGE_SIZE:
            break

        page += 1


def _paginate_events(
    entity_type: str,
    entity_code: str,
    from_ts: int,
    until_ts: int,
) -> Generator[dict, None, None]:
    """
    Yield every outage event (aggregated, corroborated) for a given entity
    and time window.

    Events differ from alerts in an important way: an *alert* is a raw
    per-signal anomaly fired by a single datasource (bgp alone, for example).
    An *event* is the result of IODA's correlation engine combining multiple
    alerts across datasources into a single, higher-confidence incident with
    a start timestamp, duration, and composite score.

    We request format=ioda (vs. the CODF format) because it returns cleaner
    machine-readable fields (entityType, entityCode, from, until, score)
    rather than the human-oriented CODF fields.

    We also set includeAlerts=true so the raw constituent alerts are embedded
    inside each event. This means a single Bronze event record is self-contained:
    you can reconstruct which datasources fired without needing a separate join.
    """
    page = 0
    endpoint = f"/outages/events/{entity_type}/{entity_code}"

    while True:
        params = {
            "from":          from_ts,
            "until":         until_ts,
            "format":        "ioda",
            "includeAlerts": "true",
            "limit":         PAGE_SIZE,
            "page":          page,
        }

        data = _get(endpoint, params)

        if not data:
            break

        for record in data:
            yield record

        if len(data) < PAGE_SIZE:
            break

        page += 1


def _fetch_signals(
    entity_type: str,
    entity_code: str,
    datasource: str,
    from_ts: int,
    until_ts: int,
) -> list[dict]:
    """
    Fetch the raw time-series signal for a given (entity, datasource).

    IODA returns one entry per datasource in the response list, each
    containing a 'values' array where each element corresponds to one
    time step (nativeStep seconds apart, typically 60s).

    Why store raw signals in Bronze?
    The 'alerts' and 'events' endpoints only tell you *when* IODA's
    own anomaly detector fired. But for computing our own baselines and
    threshold logic in Spark, we need the underlying continuous numeric
    values (e.g. number of visible /24 prefixes over time for a given ASN).
    These are the raw signals.

    We cap at MAX_SIGNAL_POINTS to avoid enormous single responses. For
    time windows longer than 24 hours the caller should chunk the requests.
    """
    endpoint = f"/signals/{entity_type}/{entity_code}"
    params = {
        "from":       from_ts,
        "until":      until_ts,
        "datasource": datasource,
        "maxPoints":  MAX_SIGNAL_POINTS,
    }

    return _get(endpoint, params)


# ---------------------------------------------------------------------------
# Bronze writer
# ---------------------------------------------------------------------------

def _partition_path(base_dir: Path, layer: str, entity_type: str, entity_code: str,
                    datasource: str | None, run_date: datetime) -> Path:
    """
    Build the output file path following Hive-style partitioning:
      <base_dir>/ioda/<layer>/year=YYYY/month=MM/day=DD/<filename>.ndjson.gz

    Hive-style partitions (year=..., month=..., day=...) are the de-facto
    standard because Apache Spark, Hive, and Presto can all perform
    'partition pruning' — they read only the date-range directories you ask
    for, without scanning the entire dataset. This is critical for performance
    once we accumulate months of data.

    The filename encodes entity + datasource so files are unambiguous even
    when listed in a flat directory view, and so Spark wildcard reads like
    'bronze/ioda/alerts/year=2026/month=06/**/country_IT_bgp.ndjson.gz'
    work cleanly.
    """
    date_partition = (
        f"year={run_date.year:04d}/"
        f"month={run_date.month:02d}/"
        f"day={run_date.day:02d}"
    )
    if datasource:
        filename = f"{entity_type}_{entity_code}_{datasource}.ndjson.gz"
    else:
        filename = f"{entity_type}_{entity_code}.ndjson.gz"

    return base_dir / "ioda" / layer / date_partition / filename


def _write_ndjson_gz(records: Iterator[dict], path: Path) -> int:
    """
    Write an arbitrary iterator of dicts to a gzip-compressed
    newline-delimited JSON file (NDJSON / JSON Lines format).

    Why NDJSON and not a single JSON array?
    - NDJSON is streamable: Spark can read it in parallel by splitting
      on newlines, whereas a single JSON array is a single indivisible blob.
    - Appending new records to an NDJSON file is trivial (just append lines);
      appending to a JSON array requires rewriting the entire file.
    - Individual records are still valid JSON, so debugging with 'zcat | head'
      works perfectly.

    Why gzip?
    JSON is highly compressible (lots of repeated field names). In practice
    NDJSON compresses 8-12x with gzip. A day of IODA alerts that would be
    ~50 MB uncompressed fits in ~5 MB on disk.

    Returns the number of records written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
            count += 1

    return count


# ---------------------------------------------------------------------------
# Ingestion orchestrator
# ---------------------------------------------------------------------------

def ingest_entity(
    entity_type: str,
    entity_code: str,
    from_ts: int,
    until_ts: int,
    output_dir: Path,
    run_date: datetime,
) -> dict[str, int]:
    """
    Full Bronze ingestion for one entity over one time window.

    Runs three sub-tasks in order:
      1. Alerts (per datasource)   -> raw anomaly alerts from each signal
      2. Events (all datasources)  -> corroborated, multi-signal incidents
      3. Signals (per datasource)  -> continuous numeric time-series

    Returns a summary dict with record counts for logging/auditing.
    """
    summary: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Task 1: Alerts
    # Alerts are the most granular anomaly data. Each alert says:
    # "at timestamp T, datasource D detected that entity E had value V,
    #  which was anomalous relative to historical value H (condition: C)."
    #
    # We fetch per-datasource so that:
    # a) each output file is homogeneous in schema
    # b) we can independently retry a failed datasource without re-fetching others
    # c) Spark partition pruning by datasource works at the file level
    # ------------------------------------------------------------------
    for ds in DATASOURCES:
        log.info(
            "  [alerts] entity=%s/%s  datasource=%s  window=[%s, %s]",
            entity_type, entity_code, ds,
            _ts_to_str(from_ts), _ts_to_str(until_ts),
        )
        try:
            alert_gen = _paginate_alerts(entity_type, entity_code, ds, from_ts, until_ts)
            out_path = _partition_path(output_dir, "alerts", entity_type, entity_code, ds, run_date)
            n = _write_ndjson_gz(alert_gen, out_path)
            log.info("    -> wrote %d alert records to %s", n, out_path)
            summary[f"alerts_{ds}"] = n
        except Exception as exc:
            # Log and continue — a failure in one datasource should not
            # abort the entire entity ingestion run.
            log.warning("    WARN: alerts failed for %s/%s/%s: %s", entity_type, entity_code, ds, exc)
            summary[f"alerts_{ds}_error"] = 1

    # ------------------------------------------------------------------
    # Task 2: Events
    # Events are higher-level objects produced by IODA's correlation engine.
    # An event groups one or more alerts across datasources into a single
    # incident with a composite score, start time, and duration.
    #
    # Because events are cross-datasource, we do not split by datasource
    # here — we fetch all events for the entity in one batch.
    #
    # The includeAlerts=true flag embeds the constituent alerts directly
    # inside each event JSON object, giving us a self-contained record
    # in the Bronze layer. This is an intentional data denormalization:
    # in the Bronze layer completeness and self-containedness are more
    # valuable than storage efficiency.
    # ------------------------------------------------------------------
    log.info(
        "  [events] entity=%s/%s  window=[%s, %s]",
        entity_type, entity_code, _ts_to_str(from_ts), _ts_to_str(until_ts),
    )
    try:
        event_gen = _paginate_events(entity_type, entity_code, from_ts, until_ts)
        out_path = _partition_path(output_dir, "events", entity_type, entity_code, None, run_date)
        n = _write_ndjson_gz(event_gen, out_path)
        log.info("    -> wrote %d event records to %s", n, out_path)
        summary["events"] = n
    except Exception as exc:
        log.warning("    WARN: events failed for %s/%s: %s", entity_type, entity_code, exc)
        summary["events_error"] = 1

    # ------------------------------------------------------------------
    # Task 3: Signals
    # Raw numeric time-series for each datasource. These are the values
    # that alerts and events are derived from. Storing them in Bronze
    # gives us full flexibility to re-run our own anomaly detection
    # logic against the original numbers during the Spark Batch phase,
    # rather than being constrained to IODA's own alert thresholds.
    #
    # Structure of a signal response (one per datasource):
    # {
    #   "entityType": "country", "entityCode": "IT",
    #   "datasource": "bgp",
    #   "from": 1234567890, "until": 1234654290,
    #   "nativeStep": 60,    <- seconds between values
    #   "step": 60,
    #   "values": [4300.0, 4300.0, null, 4298.0, ...]
    #                                  ^ null means no data for that step
    # }
    #
    # Important: 'values' is a flat array — the timestamp of element [i]
    # is (from + i * step). There is no per-value timestamp. We enrich
    # each record with its computed timestamp array before writing so that
    # downstream Spark jobs do not need to reconstruct the time axis.
    # ------------------------------------------------------------------
    for ds in DATASOURCES:
        log.info(
            "  [signals] entity=%s/%s  datasource=%s  window=[%s, %s]",
            entity_type, entity_code, ds,
            _ts_to_str(from_ts), _ts_to_str(until_ts),
        )
        try:
            signal_data = _fetch_signals(entity_type, entity_code, ds, from_ts, until_ts)

            # _fetch_signals returns a list, one element per datasource
            # (even when we filtered with ?datasource=X, IODA may still
            #  return an array with a single element — we iterate to be safe).
            enriched_records = _enrich_signal_records(signal_data)

            out_path = _partition_path(output_dir, "signals", entity_type, entity_code, ds, run_date)
            n = _write_ndjson_gz(iter(enriched_records), out_path)
            log.info("    -> wrote %d signal records to %s", n, out_path)
            summary[f"signals_{ds}"] = n
        except Exception as exc:
            log.warning("    WARN: signals failed for %s/%s/%s: %s", entity_type, entity_code, ds, exc)
            summary[f"signals_{ds}_error"] = 1

    return summary


def _enrich_signal_records(signal_data: list[dict]) -> list[dict]:
    """
    IODA returns signals as a compact (from, step, values[]) structure.
    This is efficient for transmission but awkward for Spark: every
    analysis job would need to reconstruct the timestamp for each value.

    This function expands each signal record into a list of per-step
    records, each with an explicit 'ts' field. The result is a flat,
    row-oriented format that maps cleanly to a Spark DataFrame row:

      {
        "entityType": "country",
        "entityCode": "IT",
        "datasource": "bgp",
        "ts": 1748995200,     <- Unix timestamp of this specific step
        "value": 4300.0,      <- numeric measurement (null if no data)
        "step": 60,           <- step size in seconds (for documentation)
        "nativeStep": 60
      }

    We preserve null values (don't drop them) because a sudden run of
    nulls may itself be a signal — it could indicate that IODA's own
    collection infrastructure had a gap, which is important metadata
    for the Veracity assessment step in Spark.
    """
    expanded = []
    for record in signal_data:
        base_ts: int   = record.get("from", 0)
        step: int      = record.get("step", 60)
        values: list   = record.get("values", [])
        entity_type    = record.get("entityType", "")
        entity_code    = record.get("entityCode", "")
        datasource     = record.get("datasource", "")
        native_step    = record.get("nativeStep", step)

        for i, val in enumerate(values):
            expanded.append({
                "entityType":  entity_type,
                "entityCode":  entity_code,
                "datasource":  datasource,
                "ts":          base_ts + i * step,
                "value":       val,         # keep nulls explicitly
                "step":        step,
                "nativeStep":  native_step,
            })

    return expanded


# ---------------------------------------------------------------------------
# Time window helpers
# ---------------------------------------------------------------------------

def _ts_to_str(ts: int) -> str:
    """Human-readable UTC timestamp for log messages."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_utc() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _backfill_windows(days_back: int) -> list[tuple[int, int]]:
    """
    Produce one (from, until) window per calendar day going back N days.
    Splitting by day aligns with the Bronze partition layout (year/month/day)
    and keeps individual API calls bounded in size.
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    windows = []
    for i in range(days_back, 0, -1):
        day_start = now - timedelta(days=i)
        day_end   = day_start + timedelta(days=1)
        windows.append((int(day_start.timestamp()), int(day_end.timestamp())))
    return windows


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="IODA Bronze Layer Ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode", choices=["backfill", "incremental"], required=True,
        help=(
            "backfill: fetch one full day per window going back --days-back days. "
            "incremental: fetch the last --lookback-minutes minutes (for cron)."
        ),
    )
    parser.add_argument(
        "--entity-type", default="country",
        choices=["country", "asn", "region"],
        help="IODA entity type to ingest (default: country).",
    )
    parser.add_argument(
        "--entity-codes", nargs="+", required=True,
        help="One or more entity codes, e.g. IT DE FR  or  1299 3356.",
    )
    parser.add_argument(
        "--days-back", type=int, default=7,
        help="[backfill only] How many calendar days to backfill (default: 7).",
    )
    parser.add_argument(
        "--lookback-minutes", type=int, default=15,
        help="[incremental only] Window size in minutes (default: 15).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("./bronze"),
        help="Root directory for the Bronze data lake (default: ./bronze).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_date = datetime.now(timezone.utc)
    total_summary: dict[str, int] = {}

    # Build time windows depending on run mode.
    if args.mode == "backfill":
        windows = _backfill_windows(args.days_back)
        log.info(
            "Backfill mode: %d days, %d windows, entities=%s",
            args.days_back, len(windows), args.entity_codes,
        )
    else:  # incremental
        until_ts = _now_utc()
        from_ts  = until_ts - args.lookback_minutes * 60
        windows  = [(from_ts, until_ts)]
        log.info(
            "Incremental mode: lookback=%d min, window=[%s, %s], entities=%s",
            args.lookback_minutes,
            _ts_to_str(from_ts), _ts_to_str(until_ts),
            args.entity_codes,
        )

    # Ingest each entity over each time window.
    for from_ts, until_ts in windows:
        # Use a per-window run_date so that backfill data lands in the
        # correct date partition (not today's partition).
        window_date = datetime.fromtimestamp(from_ts, tz=timezone.utc)

        for code in args.entity_codes:
            log.info(
                "Ingesting %s/%s  window=[%s, %s]",
                args.entity_type, code,
                _ts_to_str(from_ts), _ts_to_str(until_ts),
            )
            summary = ingest_entity(
                entity_type=args.entity_type,
                entity_code=code,
                from_ts=from_ts,
                until_ts=until_ts,
                output_dir=args.output_dir,
                run_date=window_date,
            )
            for k, v in summary.items():
                total_summary[k] = total_summary.get(k, 0) + v

    # Print a final summary so operators can quickly verify the run.
    log.info("=" * 60)
    log.info("Ingestion complete. Record counts:")
    for key, count in sorted(total_summary.items()):
        log.info("  %-40s %d", key, count)
    log.info("=" * 60)


if __name__ == "__main__":
    main()