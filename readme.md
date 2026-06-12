# Network Outage & Service Quality Intelligence Platform

A big data platform that aggregates network measurements and outage indicators
to detect degraded connectivity, local outages, and persistent performance issues.
Compares providers, regions, and time windows to identify recurring failure patterns.

**Current status:** Bronze ingestion layer — IODA data source.
RIPE Atlas ingestion coming next.

```
┌──────────────────────────────────────────────────────────────┐
│  outage-net (Docker bridge network)                          │
│                                                              │
│  ┌──────────────┐    raw.ioda.*    ┌─────────────────────┐  │
│  │   ingester   │ ──── topics ───► │      kafka          │  │
│  │  (Python)    │                  │  (KRaft, port 9092) │  │
│  │              │                  └─────────────────────┘  │
│  │  IODA API ──►│                  ┌─────────────────────┐  │
│  │              │ ── put_object ──►│      minio          │  │
│  └──────────────┘                  │  (S3, port 9000)    │  │
│                                    └─────────────────────┘  │
│                                    ┌─────────────────────┐  │
│                                    │    kafka-ui          │  │
│                                    │  (web, port 8080)   │  │
│                                    └─────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## Project structure

```
network-outage-bigdata-project/
├── config/
│   ├── kafka/                   # Kafka setup scripts (coming soon)
│   └── minio/
│       ├── init_minio.py        # One-shot bucket creation on first boot
│       └── Dockerfile
├── ingestion/
│   ├── ioda/                    # IODA data source
│   │   ├── bronze_ingestion.py  # Standalone local ingestion script
│   │   ├── starting_pipe.py     # Docker ingestion logic (Kafka + MinIO)
│   │   ├── run_loop.py          # Container entrypoint + polling loop
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── ripe/                    # RIPE Atlas (coming soon)
├── spark-jobs/                  # Silver/Gold Spark jobs (coming soon)
├── dashboard/                   # Streamlit dashboard (coming soon)
├── docker-compose.yml
├── .env.example                 # Credential template — copy to .env
├── .env                         # Your credentials (git-ignored, never commit)
└── README.md
```

## Data flow

```
IODA API → ingester → Kafka topics (raw.ioda.alerts / events / signals)
                    → MinIO bronze bucket (partitioned NDJSON.gz)

bronze/
  ioda/
    alerts/year=YYYY/month=MM/day=DD/country_IT_bgp.ndjson.gz
    events/year=YYYY/month=MM/day=DD/country_IT.ndjson.gz
    signals/year=YYYY/month=MM/day=DD/country_IT_bgp.ndjson.gz
```

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
- `MINIO_ROOT_PASSWORD` — choose a strong password (min 8 chars)

### 4. Start the stack

```bash
docker compose up -d
```

Watch startup:
```bash
docker compose logs -f
```

Wait until you see `minio-init` print `All buckets ready.` and exit,
and `ingester` print `Connected to Kafka and MinIO successfully.`

### 5. Verify everything is running

```bash
docker compose ps
```

All services should show `healthy` or `running`.

## Web UIs

| Service   | URL                   | Credentials                                     |
|-----------|-----------------------|-------------------------------------------------|
| Kafka UI  | http://localhost:8080 | none                                            |
| MinIO     | http://localhost:9001 | MINIO_ROOT_USER / MINIO_ROOT_PASSWORD from .env |

## Backfill historical data

Use `backfill.py` to load historical data for one or more countries:

    python3 backfill.py --days 7                        # all countries, 7 days
    python3 backfill.py --countries IT IQ               # specific countries, 30 days
    python3 backfill.py --dry-run                       # preview without running

### Recommended set

15 countries covering conflict zones, shutdown-prone governments, and stable baselines (~20 min for 7 days, ~1h for 30 days):

```bash
python3 backfill.py --countries IT MM IN PK UA RU PS SY IR TR BD NG US DE GB --days 30
```

> [!WARNING]
> Running without `--countries` fetches all 253 countries from the IODA API
> and runs them sequentially. At ~75 seconds per country this takes approximately 5 hours.
> For a first run use the recommended set above or specify countries manually.

## Inspect the Bronze layer in MinIO

### Via web console
Go to http://localhost:9001, log in, open the `bronze` bucket.
You will see the Hive-partitioned directory tree:
```
ioda/alerts/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
ioda/events/year=2026/month=06/day=04/country_IT.ndjson.gz
ioda/signals/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
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
- `raw.ioda.alerts`
- `raw.ioda.events`
- `raw.ioda.signals`

### Via kcat
```bash
# Install: brew install kcat  or  apt install kafkacat
kcat -b localhost:9092 -t raw.ioda.events -o -10 -e | python3 -m json.tool
```

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

Because `./ingestion/ioda` is mounted as a volume, Python file changes
take effect on container restart without a rebuild. Dependency changes
(requirements.txt) do require a rebuild.

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