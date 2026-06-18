"""
RIPE Atlas Bronze Layer Ingestion Pipeline (MinIO Edition)
===========================================================
Fetches historical public measurement results (e.g., DNS root pings) from 
the RIPE Atlas REST API. Filters the global stream to only keep results 
originating from our 15 priority countries using our probe mapping file.

Writes directly to MinIO using an in-memory gzip buffer:
  bronze/
    ripe/
      ping/
        year=2026/month=06/day=14/
          measurement_2001.ndjson.gz
"""

###########################################     THIS DOESN'T ACTUALLY HAVE A DIRECT WAY TO .env SO WE CREATE NEW BUCKET IN DIFFERENT MinIO. GOING TO DO backfill.py AND WE CAN PROBABLY SKIP THIS ONE

import argparse
import gzip
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Iterator

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv

# find_dotenv() automatically searches parent folders until it finds the .env!
load_dotenv(find_dotenv())

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RIPE_RESULTS_URL = "https://atlas.ripe.net/api/v2/measurements/{}/results/"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "admin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "admin123") 
S3_BUCKET_BRONZE = os.environ.get("S3_BUCKET_BRONZE", "bronze")

# RIPE Atlas Built-In Measurement IDs for IPv4 Pings to DNS Root Servers
ROOT_PING_MEASUREMENT_IDS = [2009, 2010, 2011, 2012, 2013, 2004, 2014, 2015, 2005, 2016, 2001, 2008, 2006]

RETRY_STRATEGY = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("ripe_bronze")

# ---------------------------------------------------------------------------
# Infrastructure Clients
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

def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )

def load_probe_mapping(mapping_file: str = "ripe_probe_mapping.json") -> dict:
    try:
        with open(mapping_file, "r", encoding="utf-8") as f:
            mapping = json.load(f)
            return mapping
    except FileNotFoundError:
        raise RuntimeError(f"Could not find {mapping_file}. Run ripe_recon.py first!")

# ---------------------------------------------------------------------------
# Ingestion Logic
# ---------------------------------------------------------------------------

def fetch_and_filter_results(msm_id: int, from_ts: int, until_ts: int, probe_mapping: dict) -> Iterator[dict]:
    """
    Fetches historical data in 2-hour increments to avoid crashing the RIPE API.
    """
    CHUNK_SIZE_SEC = 2 * 60 * 60  # 2 hours
    current_start = from_ts

    while current_start < until_ts:
        current_stop = min(current_start + CHUNK_SIZE_SEC, until_ts)
        url = RIPE_RESULTS_URL.format(msm_id)
        params = {"start": current_start, "stop": current_stop, "format": "json"}

        log.info("  -> Fetching chunk [%s, %s]...", current_start, current_stop)
        
        try:
            resp = SESSION.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            
            kept = 0
            for record in data:
                probe_id = str(record.get("prb_id"))
                if probe_id in probe_mapping:
                    mapping_data = probe_mapping[probe_id]
                    
                    # Safely handle both the new dict format and the old string format
                    if isinstance(mapping_data, dict):
                        record["country_code"] = mapping_data["country_code"]
                        record["asn"] = mapping_data["asn"]
                    else:
                        record["country_code"] = mapping_data
                        
                    kept += 1
                    yield record
                    
            log.info("     Fetched %d global records, kept %d for our countries.", len(data), kept)
        except Exception as e:
            log.error("     Chunk failed: %s. Skipping this 2-hour block.", e)

        current_start = current_stop
        time.sleep(1)

def _s3_key(msm_id: int, run_date: datetime) -> str:
    """Mirrors the Hive-style partitioning from the IODA pipeline."""
    date_part = f"year={run_date.year:04d}/month={run_date.month:02d}/day={run_date.day:02d}"
    return f"ripe/ping/{date_part}/measurement_{msm_id}.ndjson.gz"

def _upload_to_minio(records: Iterator[dict], s3_client, s3_key: str) -> int:
    """Compresses the data stream in memory and uploads directly to MinIO."""
    buf = io.BytesIO()
    count = 0
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for record in records:
            line = json.dumps(record, separators=(",", ":")) + "\n"
            gz.write(line.encode("utf-8"))
            count += 1
            
    if count == 0:
        return 0

    buf.seek(0)
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_BRONZE, Key=s3_key, Body=buf,
            ContentType="application/x-ndjson", ContentEncoding="gzip"
        )
    except ClientError as e:
        log.error("MinIO upload failed: %s", e)
        raise
        
    return count

def ingest_measurement(msm_id: int, from_ts: int, until_ts: int, s3_client, run_date: datetime, probe_mapping: dict) -> int:
    """Orchestrates fetching, filtering, and S3 uploading for a single measurement."""
    log.info("Ingesting Measurement %d window=[%s, %s]", msm_id, from_ts, until_ts)
    
    try:
        record_gen = fetch_and_filter_results(msm_id, from_ts, until_ts, probe_mapping)
        s3_key = _s3_key(msm_id, run_date)
        
        n = _upload_to_minio(record_gen, s3_client, s3_key)
        if n > 0:
            log.info("-> Wrote %d records to s3://%s/%s", n, S3_BUCKET_BRONZE, s3_key)
        else:
            log.info("-> No relevant records found for this window.")
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
    parser.add_argument("--days", type=int, default=7, help="Days to backfill (default: 7)")
    args = parser.parse_args()

    probe_mapping = load_probe_mapping()
    windows = _backfill_windows(args.days)
    s3_client = _build_s3_client()
    
    # ---------------------------------------------------------
    # NEW: Bulletproof Bucket Creation
    # ---------------------------------------------------------
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_BRONZE)
        log.info("MinIO bucket '%s' found and ready.", S3_BUCKET_BRONZE)
    except ClientError:
        log.warning("MinIO bucket '%s' missing! Creating it now...", S3_BUCKET_BRONZE)
        s3_client.create_bucket(Bucket=S3_BUCKET_BRONZE)
        log.info("Bucket '%s' successfully created.", S3_BUCKET_BRONZE)
    # ---------------------------------------------------------
    
    log.info("Starting RIPE Backfill: %d days, %d measurements", args.days, len(ROOT_PING_MEASUREMENT_IDS))
    
    total_records = 0
    for from_ts, until_ts in windows:
        window_date = datetime.fromtimestamp(from_ts, tz=timezone.utc)
        
        for msm_id in ROOT_PING_MEASUREMENT_IDS:
            total_records += ingest_measurement(msm_id, from_ts, until_ts, s3_client, window_date, probe_mapping)
            time.sleep(1.5) # Polite delay between measurements

    log.info("=" * 50)
    log.info("RIPE Backfill Complete. Total records pushed to MinIO: %d", total_records)
    log.info("=" * 50)


if __name__ == "__main__":
    main()