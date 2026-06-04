# IODA Bronze Layer Stack

Docker Compose setup for the IODA ingestion pipeline.
Runs Kafka, MinIO, Kafka UI, and the Python ingester as isolated containers.

```
┌──────────────────────────────────────────────────────────────┐
│  ioda-net (Docker bridge network)                            │
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

## First-time setup

### 1. Generate a Kafka cluster ID

```bash
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```

Copy the output — you need it in the next step.

### 2. Create your .env file

```bash
cp .env.example .env
```

Open `.env` and fill in:
- `KAFKA_CLUSTER_ID` — the UUID you just generated
- `MINIO_ROOT_PASSWORD` — choose a strong password (min 16 chars)
- `ENTITY_CODES` — space-separated country codes or ASNs to ingest

### 3. Start the stack

```bash
docker compose up -d
```

Watch startup:
```bash
docker compose logs -f
```

Wait until you see `ioda-minio-init` print `Buckets ready.` and exit,
and `ioda-ingester` print `Connected to Kafka and MinIO successfully.`

### 4. Verify everything is running

```bash
docker compose ps
```

All services should show `healthy` or `running`.

## Web UIs

| Service    | URL                     | Credentials                      |
|------------|-------------------------|-----------------------------------|
| Kafka UI   | http://localhost:8080   | none                              |
| MinIO      | http://localhost:9001   | MINIO_ROOT_USER / MINIO_ROOT_PASSWORD from .env |

## Backfill historical data

Run a 7-day backfill for Italy:

```bash
docker compose run --rm ingester python run_loop.py backfill 7
```

For a different set of entities (overrides .env for this run only):

```bash
docker compose run --rm -e ENTITY_CODES="IT DE FR" -e ENTITY_TYPE=country \
    ingester python run_loop.py backfill 14
```

ASN-level backfill (Telecom Italia, Fastweb, Vodafone IT):

```bash
docker compose run --rm \
    -e ENTITY_TYPE=asn \
    -e ENTITY_CODES="3269 12874 30722" \
    ingester python run_loop.py backfill 7
```

## Inspect the Bronze layer in MinIO

### Via web console
Go to http://localhost:9001, log in, open the `ioda-bronze` bucket.
You will see the Hive-partitioned directory tree:
```
ioda/alerts/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
ioda/events/year=2026/month=06/day=04/country_IT.ndjson.gz
ioda/signals/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
```

### Via mc (MinIO client)
```bash
# List all objects for today
docker compose exec minio mc ls --recursive local/ioda-bronze/ioda/alerts/

# Download and inspect a file
docker compose exec minio mc cat local/ioda-bronze/ioda/events/year=2026/month=06/day=04/country_IT.ndjson.gz \
    | gunzip | head -5 | python3 -m json.tool
```

## Inspect Kafka topics

### Via Kafka UI
Go to http://localhost:8080 → Topics. You will see:
- `raw.ioda.alerts`
- `raw.ioda.events`
- `raw.ioda.signals`

Click any topic to browse individual messages.

### Via kcat (install separately: `brew install kcat` or `apt install kafkacat`)
```bash
# Print last 10 messages from raw.ioda.events
kcat -b localhost:9092 -t raw.ioda.events -o -10 -e | python3 -m json.tool
```

## Stop the stack

```bash
# Stop containers but keep data volumes intact
docker compose down

# Stop AND delete all data (destructive — only for full reset)
docker compose down -v
```

## Rebuild the ingester after code changes

```bash
docker compose build ingester
docker compose up -d ingester
```

Because `./ingester` is mounted as a volume, Python file changes take effect
on container restart without a rebuild. Dependency changes (requirements.txt)
do require a rebuild.

## Connect Spark to MinIO

In your PySpark job (Phase 3), configure the S3A connector:

```python
spark = SparkSession.builder \
    .appName("ioda-batch") \
    .config("spark.hadoop.fs.s3a.endpoint",            "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key",          "ioda_admin") \
    .config("spark.hadoop.fs.s3a.secret.key",          "<your_password>") \
    .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read the Bronze alerts
df = spark.read.json("s3a://ioda-bronze/ioda/alerts/year=2026/month=06/")
```

## File map

```
ioda-stack/
├── docker-compose.yml       # service definitions
├── .env.example             # credential template (copy to .env)
├── .env                     # your credentials (git-ignored)
├── .gitignore
├── README.md
└── ingester/
    ├── Dockerfile            # Python 3.12 slim image
    ├── requirements.txt      # pinned dependencies
    ├── ioda_ingest.py        # core ingestion logic (Kafka + MinIO)
    └── run_loop.py           # container entrypoint + polling loop
```