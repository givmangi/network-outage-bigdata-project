"""
RIPE Atlas Bronze Layer Ingestion Pipeline
===========================================
Fetches historical public measurement results (e.g., DNS root pings) from 
the RIPE Atlas REST API. Filters the global stream to only keep results 
originating from our 15 priority countries using our probe mapping file.

Directory layout produced:
  bronze/
    ripe/
      ping/
        year=2026/month=06/day=14/
          measurement_2001.ndjson.gz
          measurement_2009.ndjson.gz
          ...
"""

import argparse
import gzip
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RIPE_RESULTS_URL = "https://atlas.ripe.net/api/v2/measurements/{}/results/"

# RIPE Atlas Built-In Measurement IDs for IPv4 Pings to DNS Root Servers
ROOT_PING_MEASUREMENT_IDS = [
    2009, # A-root
    2010, # B-root
    2011, # C-root
    2012, # D-root
    2013, # E-root
    2004, # F-root
    2014, # G-root
    2015, # H-root
    2005, # I-root
    2016, # J-root
    2001, # K-root
    2008, # L-root
    2006, # M-root
]

# Polite retry strategy (identical to IODA setup)
RETRY_STRATEGY = Retry(
    total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ripe_bronze")

# ---------------------------------------------------------------------------
# HTTP Client & Setup
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=RETRY_STRATEGY))
    session.headers.update({
        "User-Agent": "RIPE-BronzeIngestion/1.0 (academic project; UniTrento)",
        "Accept": "application/json",
    })
    return session

SESSION = build_session()

def load_probe_mapping(mapping_file: str = "ripe_probe_mapping.json") -> dict:
    """Loads the country filter key we generated in Step 1."""
    try:
        with open(mapping_file, "r", encoding="utf-8") as f:
            mapping = json.load(f)
            log.info("Loaded %d probe ID mappings from %s", len(mapping), mapping_file)
            return mapping
    except FileNotFoundError:
        raise RuntimeError(f"Could not find {mapping_file}. Run ripe_recon.py first!")

# ---------------------------------------------------------------------------
# Ingestion Logic
# ---------------------------------------------------------------------------

def fetch_and_filter_results(msm_id: int, from_ts: int, until_ts: int, probe_mapping: dict) -> Iterator[dict]:
    """
    Fetches the chunk of historical data in 2-hour increments to avoid
    crashing the RIPE Atlas API, yielding only records from our target countries.
    """
    CHUNK_SIZE_SEC = 2 * 60 * 60  # 2 hours
    current_start = from_ts

    while current_start < until_ts:
        current_stop = min(current_start + CHUNK_SIZE_SEC, until_ts)
        url = RIPE_RESULTS_URL.format(msm_id)
        params = {
            "start": current_start,
            "stop": current_stop,
            "format": "json"
        }

        log.info("  -> Fetching chunk [%s, %s]...", current_start, current_stop)
        
        try:
            resp = SESSION.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            
            kept = 0
            for record in data:
                probe_id = str(record.get("prb_id"))
                if probe_id in probe_mapping:
                    record["country_code"] = probe_mapping[probe_id]
                    kept += 1
                    yield record
                    
            log.info("     Fetched %d global records, kept %d for our countries.", len(data), kept)
            
        except Exception as e:
            log.error("     Chunk failed: %s. Skipping this 2-hour block.", e)

        # Move to the next chunk
        current_start = current_stop
        time.sleep(1) # Polite delay between chunks

def _partition_path(base_dir: Path, layer: str, msm_id: int, run_date: datetime) -> Path:
    """Mirrors the Hive-style partitioning from the IODA pipeline."""
    date_partition = (
        f"year={run_date.year:04d}/"
        f"month={run_date.month:02d}/"
        f"day={run_date.day:02d}"
    )
    filename = f"measurement_{msm_id}.ndjson.gz"
    return base_dir / "ripe" / layer / date_partition / filename

def _write_ndjson_gz(records: Iterator[dict], path: Path) -> int:
    """Writes compressed NDJSON just like the IODA pipeline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
            count += 1
    return count

def ingest_measurement(msm_id: int, from_ts: int, until_ts: int, output_dir: Path, run_date: datetime, probe_mapping: dict) -> int:
    """Orchestrates fetching, filtering, and saving for a single measurement."""
    log.info("Ingesting Measurement %d window=[%s, %s]", msm_id, from_ts, until_ts)
    
    try:
        record_gen = fetch_and_filter_results(msm_id, from_ts, until_ts, probe_mapping)
        out_path = _partition_path(output_dir, "ping", msm_id, run_date)
        
        n = _write_ndjson_gz(record_gen, out_path)
        log.info("-> Wrote %d records to %s", n, out_path)
        return n
    except Exception as exc:
        log.error("Failed to ingest measurement %d: %s", msm_id, exc)
        return 0

# ---------------------------------------------------------------------------
# CLI & Execution
# ---------------------------------------------------------------------------

def _backfill_windows(days_back: int) -> list[tuple[int, int]]:
    from datetime import timedelta
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    windows = []
    for i in range(days_back, 0, -1):
        day_start = now - timedelta(days=i)
        day_end   = day_start + timedelta(days=1)
        windows.append((int(day_start.timestamp()), int(day_end.timestamp())))
    return windows

def main():
    parser = argparse.ArgumentParser(description="RIPE Atlas Bronze Backfill")
    parser.add_argument("--days-back", type=int, default=7, help="Days to backfill (default: 7)")
    parser.add_argument("--output-dir", type=Path, default=Path("./bronze"), help="Bronze root dir")
    args = parser.parse_args()

    probe_mapping = load_probe_mapping()
    windows = _backfill_windows(args.days_back)
    
    log.info("Starting RIPE Backfill: %d days, %d measurements", args.days_back, len(ROOT_PING_MEASUREMENT_IDS))
    
    total_records = 0
    for from_ts, until_ts in windows:
        window_date = datetime.fromtimestamp(from_ts, tz=timezone.utc)
        
        for msm_id in ROOT_PING_MEASUREMENT_IDS:
            total_records += ingest_measurement(msm_id, from_ts, until_ts, args.output_dir, window_date, probe_mapping)
            time.sleep(1.5) # Polite delay between measurements

    log.info("=" * 50)
    log.info("RIPE Ingestion Complete. Total records written: %d", total_records)
    log.info("=" * 50)

if __name__ == "__main__":
    main()