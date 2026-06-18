"""
RIPE Atlas Bronze Streaming Pipeline
====================================
Subscribes to the RIPE Atlas WebSocket stream for our target Measurement IDs.
Filters the incoming stream in real-time using our probe mapping.
Writes incoming records immediately to Kafka, and buffers them for MinIO.

Data Flow:
  RIPE WebSocket -> Filter -> Kafka (raw.ripe.ping)
                           -> MinIO Buffer -> MinIO (bronze/ripe/ping/...)
"""

import gzip
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from threading import Timer

import boto3
from kafka import KafkaProducer
from botocore.exceptions import ClientError
import websocket
from dotenv import load_dotenv  # <--- NEW

# Load environment variables from the .env file in your project root!
load_dotenv() # <--- NEW

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RIPE_WS_URL = "wss://atlas-stream.ripe.net/stream/"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")

# Now it safely pulls directly from your .env file!
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "admin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "admin123") 
S3_BUCKET_BRONZE = os.environ.get("S3_BUCKET_BRONZE", "bronze")

# We flush the MinIO buffer every 15 minutes or 10,000 records, whichever comes first
FLUSH_INTERVAL_SEC = 900
MAX_BUFFER_SIZE = 10000

# The same target measurements
ROOT_PING_MEASUREMENT_IDS = [2009, 2010, 2011, 2012, 2013, 2004, 2014, 2015, 2005, 2016, 2001, 2008, 2006]

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("ripe_stream")

# Global state for buffering
buffer = []
flush_timer = None

# ---------------------------------------------------------------------------
# Infrastructure Clients
# ---------------------------------------------------------------------------
def _build_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        compression_type="gzip",
    )

def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )

try:
    producer = _build_kafka_producer()
    s3_client = _build_s3_client()
    
    # ---> NEW: Bulletproof Bucket Creation <---
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_BRONZE)
        log.info("MinIO bucket '%s' found and ready.", S3_BUCKET_BRONZE)
    except ClientError:
        log.warning("MinIO bucket '%s' missing! Creating it now...", S3_BUCKET_BRONZE)
        s3_client.create_bucket(Bucket=S3_BUCKET_BRONZE)
        log.info("Bucket '%s' successfully created.", S3_BUCKET_BRONZE)
        
except Exception as e:
    log.warning("Could not connect to Kafka/MinIO. Running in local test mode (dry-run). Error: %s", e)
    producer = None
    s3_client = None

# ---------------------------------------------------------------------------
# Buffer & Upload Logic
# ---------------------------------------------------------------------------
def _s3_key(msm_id: int, ts: int) -> str:
    """Build the S3 object key including a timestamp to prevent overwriting chunks."""
    run_date = datetime.fromtimestamp(ts, tz=timezone.utc)
    date_part = f"year={run_date.year:04d}/month={run_date.month:02d}/day={run_date.day:02d}"
    return f"ripe/ping/{date_part}/measurement_{msm_id}_{ts}.ndjson.gz"

def _s3_put_with_retry(bucket, key, body, max_attempts=3):
    if not s3_client:
        return
    delay = 2
    for attempt in range(1, max_attempts + 1):
        try:
            s3_client.put_object(
                Bucket=bucket, Key=key, Body=body,
                ContentType="application/x-ndjson", ContentEncoding="gzip"
            )
            return
        except ClientError as exc:
            if attempt == max_attempts:
                raise
            log.warning("MinIO put failed (attempt %d/%d): %s — retrying in %ds", attempt, max_attempts, exc, delay)
            time.sleep(delay)
            delay *= 2
            body.seek(0)

def flush_buffer():
    """Compresses the current buffer and uploads it to MinIO, grouped by measurement ID."""
    global buffer, flush_timer
    
    if not buffer:
        _reset_timer()
        return

    # Group records by measurement ID before uploading
    grouped_data = {}
    for record in buffer:
        msm_id = record.get("msm_id")
        if msm_id not in grouped_data:
            grouped_data[msm_id] = []
        grouped_data[msm_id].append(record)

    current_ts = int(time.time())
    
    for msm_id, records in grouped_data.items():
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for record in records:
                line = json.dumps(record, separators=(",", ":")) + "\n"
                gz.write(line.encode("utf-8"))
        
        buf.seek(0)
        s3_key = _s3_key(msm_id, current_ts)
        _s3_put_with_retry(S3_BUCKET_BRONZE, s3_key, buf)
        log.info("Flushed %d records to MinIO -> %s", len(records), s3_key)

    # Clear the buffer and reset the timer
    buffer.clear()
    _reset_timer()

def _reset_timer():
    global flush_timer
    if flush_timer:
        flush_timer.cancel()
    flush_timer = Timer(FLUSH_INTERVAL_SEC, flush_buffer)
    flush_timer.start()

# ---------------------------------------------------------------------------
# WebSocket Callbacks
# ---------------------------------------------------------------------------
def on_message(ws, message):
    global buffer
    
    try:
        data = json.loads(message)
    except Exception:
        return

    # Keep the debug line so we can see the glorious success
    # log.info("RAW DATA ARRIVED: %s", str(data)[:200])

    # RIPE sends data as an array: ["event_name", {payload_dict}]
    if isinstance(data, list) and len(data) >= 2:
        event_name = data[0]
        payload = data[1]
        
        # We only care about successful measurement results
        if event_name == "atlas_result" and isinstance(payload, dict):
            probe_id = str(payload.get("prb_id"))
            
            # Filter by our 15 countries using the mapping we made!
            if probe_id in probe_mapping:
                mapping_data = probe_mapping[probe_id]
                
                # Safely handle both the new dict format and the old string format
                if isinstance(mapping_data, dict):
                    payload["country_code"] = mapping_data["country_code"]
                    payload["asn"] = mapping_data["asn"]
                else:
                    payload["country_code"] = mapping_data
                
                # 1. Send immediately to Kafka
                if producer:
                    producer.send(
                        topic="raw.ripe.ping",
                        key=payload["country_code"],
                        value=payload
                    )
                    
                # 2. Add to MinIO buffer
                buffer.append(payload)
                
                if len(buffer) >= MAX_BUFFER_SIZE:
                    log.info("Max buffer size reached. Forcing flush...")
                    flush_buffer()

def on_error(ws, error):
    log.error("WebSocket Error: %s", error)

def on_close(ws, close_status_code, close_msg):
    log.warning("WebSocket Closed: %s - %s", close_status_code, close_msg)
    # Ensure any remaining data is flushed before closing
    flush_buffer()

def on_open(ws):
    log.info("WebSocket Connected. Sending subscription requests...")
    # Subscribe to our target measurements individually using the Array format
    for msm_id in ROOT_PING_MEASUREMENT_IDS:
        sub_msg = [
            "atlas_subscribe",
            {
                "stream_type": "result",
                "msm": msm_id
            }
        ]
        ws.send(json.dumps(sub_msg))
    log.info("Subscriptions sent! Waiting for data...")

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
def load_probe_mapping():
    try:
        with open("ripe_probe_mapping.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log.error("ripe_probe_mapping.json not found! Run ripe_recon.py first.")
        exit(1)

if __name__ == "__main__":
    probe_mapping = load_probe_mapping()
    _reset_timer() # Start the MinIO flush timer
    
    log.info("Starting RIPE Streaming Pipeline for %d measurements...", len(ROOT_PING_MEASUREMENT_IDS))
    
    try:
        # We use run_forever with automatic reconnection
        while True:
            ws = websocket.WebSocketApp(
                RIPE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
            log.info("Reconnecting in 5 seconds...")
            time.sleep(5)
            
    except KeyboardInterrupt:
        log.info("Ctrl+C detected! Shutting down gracefully...")
        if flush_timer:
            flush_timer.cancel()
        flush_buffer() # One last flush before dying
        log.info("Goodbye!")
        exit(0)