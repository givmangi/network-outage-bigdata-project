# Network Outage & Service Quality Intelligence Platform

A big data platform that aggregates network measurements and outage indicators
to detect degraded connectivity, local outages, and persistent performance issues.
Compares providers, regions, and time windows to identify recurring failure patterns.

**Current status:** Bronze ingestion layer complete for both data sources (IODA +
RIPE Atlas). Silver layer (batch + streaming) is running. Gold/unification layer
and dashboard are in progress.

```
┌────────────────────────────────────────────────────────────────────────────┐
│  outage-net (Docker bridge network)                                        │
│                                                                              │
│  ┌──────────────┐   raw.ioda.*   ┌──────────────┐   raw.ripe.ping          │
│  │   ingester   │ ───topics────► │              │ ◄───topics──────┐        │
│  │  (IODA, poll)│                │     kafka     │                │        │
│  │              │── put_object ─►│ (KRaft, 9092) │           ┌────┴───────┐│
│  └──────────────┘      │         └──────┬───────┘           │ripe-ingester││
│                         │                │                  │ (websocket) ││
│                         ▼                ▼                  └────┬────────┘│
│                  ┌────────────────────────────┐  put_object       │        │
│                  │            minio            │◄──────────────────┘       │
│                  │   (S3 API: 9000, console)   │                            │
│                  │   bronze / silver / gold     │                          │
│                  └──────────────┬───────────────┘                          │
│                                  │                                          │
│                     ┌────────────┴─────────────┐                           │
│                     ▼                           ▼                          │
│           spark-silver-ioda /          spark-silver-stream                 │
│           spark-silver-ripe            (always-on, Kafka → silver)         │
│           (on-demand batch jobs)                                           │
│                                                                              │
│  ┌──────────────┐                                                          │
│  │   kafka-ui   │  (web, port 8080)                                        │
│  └──────────────┘                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## Project structure

```
network-outage-bigdata-project/
├── config/
│   ├── kafka/                       # (empty — placeholder for future Kafka config)
│   └── minio/
│       ├── init_minio.py
│       └── Dockerfile
├── ingestion/
│   ├── ioda/                        # IODA data source
│   │   ├── starting_pipe.py         # Docker ingestion logic (Kafka + MinIO)
│   │   ├── run_loop.py              # Container entrypoint + polling/backfill loop
│   │   ├── bronze_ingestion.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── ripe/                        # RIPE Atlas data source
│       ├── ripe_recon.py            # Builds probe→country mapping from TARGET_COUNTRIES
│       ├── ripe_probe_mapping.json  # Generated output of ripe_recon.py (probe_id → country)
│       ├── ripe_streaming_pipe.py   # Always-on websocket ingester (Kafka + MinIO)
│       ├── requirements.txt
│       └── Dockerfile
├── spark-jobs/                      # Silver layer (batch + streaming)
│   ├── silver_ioda.py               # Batch: bronze IODA → silver Parquet
│   ├── silver_ripe.py               # Batch: bronze RIPE → silver Parquet
│   ├── silver_streaming.py          # Streaming: Kafka → silver Parquet (always-on)
│   └── submit.py                    # Universal spark-submit launcher for all three jobs
├── dashboard/                        # Streamlit dashboard (in progress, currently empty)
├── backfill.py                       # Master historical backfill orchestrator (IODA + RIPE)
├── docker-compose.yml
├── .env.example                      # Credential + config template — copy to .env
├── .env                               # Your local credentials (git-ignored, never commit)
└── README.md
```

## Data flow

```
IODA API   → ingester       → Kafka (raw.ioda.alerts / events / signals)
                             → MinIO bronze/ioda/...   (Hive-partitioned NDJSON.gz)

RIPE Atlas → ripe-ingester   → Kafka (raw.ripe.ping)
  (websocket,                → MinIO bronze/ripe/ping/... (Hive-partitioned NDJSON.gz)
   filtered to
   TARGET_COUNTRIES
   via probe mapping)

bronze/
  ioda/
    alerts/year=YYYY/month=MM/day=DD/country_IT_bgp.ndjson.gz
    events/year=YYYY/month=MM/day=DD/country_IT.ndjson.gz
    signals/year=YYYY/month=MM/day=DD/country_IT_bgp.ndjson.gz
  ripe/
    ping/year=YYYY/month=MM/day=DD/measurement_2009_<ts>.ndjson.gz

bronze (both sources) → Spark silver jobs → silver/ioda/...  (Parquet)
                                           → silver/ripe/...  (Parquet)
```

The **15 priority countries** (conflict zones, shutdown-prone governments, and
stable Western baselines) drive both pipelines via a single `.env` value,
`TARGET_COUNTRIES`:

```
IT MM IN PK UA RU PS SY IR TR BD NG US DE GB
```

- **IODA** reads `TARGET_COUNTRIES` directly as its default `ENTITY_CODES` list
  (live ingester and `backfill.py`'s `--source ioda` path), unless overridden
  per-run with `--countries`.
- **RIPE** reads `TARGET_COUNTRIES` in `ripe_recon.py`, which queries the RIPE
  Atlas probe directory and writes `ripe_probe_mapping.json` (probe ID → country
  code) for only those 15 countries. Both the live websocket ingester and the
  RIPE backfill use this mapping file to filter the otherwise-global measurement
  stream down to just these countries. RIPE has no per-country API parameter
  (root server ping measurements are global by nature), so unlike IODA there is
  no per-country loop — scoping happens once, upstream, via this mapping file.

## First-time setup

### 1. Clone the repo

```bash
git clone https://github.com/givmangi/network-outage-bigdata-project.git
cd network-outage-bigdata-project
```

### 2. Generate a Kafka cluster ID

```bash
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```

Copy the output — you need it in the next step.

### 3. Create your .env file

```bash
cp .env.example .env
```

Open `.env` and fill in:
- `KAFKA_CLUSTER_ID` — the UUID you just generated
- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` — choose your own credentials
- `TARGET_COUNTRIES` — space-separated ISO country codes (defaults to the
  15-country recommended set if omitted; see above)

`.env` is git-ignored and never pushed — every teammate creates their own.

### 4. Start the stack

```bash
docker compose up -d
```

Watch startup:
```bash
docker compose logs -f
```

Wait until `minio-init` exits after printing the bucket list, `ingester`
starts its polling loop, and `ripe-ingester` logs `WebSocket Connected`.

### 5. Verify everything is running

```bash
docker compose ps
```

All long-running services should show `healthy` or `running`. Note:
`spark-silver-ioda` and `spark-silver-ripe` are on-demand batch jobs — they
will run once and exit on every `docker compose up`, which is expected; see
the Silver layer section below for how to (re)run them deliberately.

## Web UIs

| Service   | URL                   | Credentials                                     |
|-----------|-----------------------|-------------------------------------------------|
| Kafka UI  | http://localhost:8080 | none                                            |
| MinIO     | http://localhost:9090 | MINIO_ROOT_USER / MINIO_ROOT_PASSWORD from .env |

> MinIO's console port is mapped to **9090** on the host (not the MinIO
> default of 9001) — check `docker-compose.yml` if this ever changes.

## Backfill historical data

`backfill.py` orchestrates historical loads for both sources:

```bash
python3 backfill.py --source ioda --days 7          # IODA only
python3 backfill.py --source ripe --days 7          # RIPE only
python3 backfill.py --source all  --days 30         # Both sources (default)
python3 backfill.py --source ioda --countries IT IQ --days 7   # Specific IODA countries
python3 backfill.py --dry-run                        # Preview without running
```

If `--countries` is omitted, IODA backfill uses `TARGET_COUNTRIES` from `.env`.
If that's also unset, it falls back to fetching and backfilling **all 253**
IODA country codes sequentially — at roughly 75 seconds per country, this
takes approximately 5 hours. For routine work, stick to the default
15-country set or pass `--countries` explicitly.

RIPE backfill has no `--countries` option — it always covers whatever
countries are currently in `ripe_probe_mapping.json` (generated from
`TARGET_COUNTRIES`; regenerate it if you change that list).

### Recommended set

15 countries covering conflict zones, shutdown-prone governments, and stable baselines (~20 min for 7 days, ~1h for 30 days):

```bash
python3 backfill.py --source all --days 30
```

Covers conflict zones (UA, RU, PS, SY), shutdown-prone governments (MM, IN, PK,
IR, TR, BD), Africa's largest market (NG), stable Western baselines (US, DE,
GB), and Italy as the home-country/AGCOM validation reference.

## Inspect the Bronze layer in MinIO

### Via web console
Go to http://localhost:9090, log in, open the `bronze` bucket.
You will see the Hive-partitioned directory tree for both sources:
```
ioda/alerts/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
ioda/events/year=2026/month=06/day=04/country_IT.ndjson.gz
ioda/signals/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
ripe/ping/year=2026/month=06/day=16/measurement_2009_1781596241.ndjson.gz
```

### Via Docker
```bash
# Count total files in bronze
docker compose exec minio mc ls --recursive local/bronze | wc -l

# Download and inspect a file
docker compose exec minio mc cat \
    local/bronze/ioda/signals/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz \
    | gunzip | head -5 | python3 -m json.tool
```

## Inspect Kafka topics

### Via Kafka UI
Go to http://localhost:8080 → Topics. You will see:
- `raw.ioda.alerts`, `raw.ioda.events`, `raw.ioda.signals`
- `raw.ripe.ping`

### Via kcat
```bash
# Install: brew install kcat  or  apt install kafkacat
kcat -b localhost:9092 -t raw.ioda.events -o -10 -e | python3 -m json.tool
kcat -b localhost:9092 -t raw.ripe.ping   -o -10 -e | python3 -m json.tool
```

## Silver layer (Spark)

Two batch jobs (run on demand, exit when finished) and one always-on
structured-streaming job, all launched through a single `submit.py` wrapper
so the spark-submit flags and S3A configuration live in one place.

**Batch — auto-discovers and processes all unprocessed bronze partitions if
no date range is given:**
```bash
docker compose run --rm --no-deps spark-silver-ioda
docker compose run --rm --no-deps spark-silver-ripe
docker compose run --rm --no-deps spark-silver-ioda --start 2026-05-28 --end 2026-06-04
```

Output:
```
silver/ioda/alerts/year=YYYY/month=MM/day=DD/part-*.parquet
silver/ioda/events/year=YYYY/month=MM/day=DD/part-*.parquet
silver/ioda/signals/year=YYYY/month=MM/day=DD/part-*.parquet
silver/ripe/ping/year=YYYY/month=MM/day=DD/part-*.parquet
```

Both jobs overwrite by partition, so re-running them on the same date range
is safe and idempotent.

**Streaming — always running as part of `docker compose up -d`, consumes
both Kafka topic families continuously and checkpoints offsets under
`silver/_checkpoints/` so it resumes cleanly after a restart:**
```bash
docker compose logs -f spark-silver-stream
```

> `spark-silver-ioda` and `spark-silver-ripe` currently start automatically
> on every `docker compose up -d` (they have no `restart` policy, so they run
> once and exit rather than looping) — this is a known rough edge to clean up
> later, not an intended always-on behavior. If you only want to trigger a
> batch run deliberately, use `docker compose run --rm --no-deps <service>`
> as shown above.

## Stop the stack

```bash
# Stop containers but keep data volumes intact
docker compose down

# Stop AND delete all data (full reset)
docker compose down -v
```

## Rebuild after code changes

```bash
docker compose build ingester
docker compose up -d ingester
```

Because the ingestion folders are mounted as volumes, Python file changes
take effect on container restart without a rebuild. Dependency changes
(`requirements.txt`) do require a rebuild — same pattern applies to
`ripe-ingester` and the Spark services via their respective build contexts.

## Connect Spark to MinIO (Silver layer — coming soon)

In your PySpark job, configure the S3A connector:

```python
spark = SparkSession.builder \
    .appName("silver-job") \
    .config("spark.hadoop.fs.s3a.endpoint",          "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key",        "admin") \
    .config("spark.hadoop.fs.s3a.secret.key",        "<MINIO_ROOT_PASSWORD>") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read Bronze IODA signals
df = spark.read.json("s3a://bronze/ioda/signals/year=2026/month=06/")
```