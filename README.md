# Network Outage & Service Quality Intelligence Platform

A big data platform that aggregates network measurements and outage indicators
from **RIPE Atlas** and **IODA** to detect degraded connectivity, local outages,
and persistent performance issues. Compares ISPs, regions, and time windows to
identify recurring failure patterns, and exposes the results through a
containerized dashboard.

**Current status:** Bronze, silver, and gold layers are all functional for both
data sources. The gold layer serves RIPE ASN-level baselines and IODA
country-level signals from TimescaleDB. A containerized Streamlit dashboard
reads from the gold layer with four views: time series, ISP ranking, IODA
signals, and a combined RIPE + IODA overlay.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  outage-net (Docker bridge network)                                          │
│                                                                                │
│  ┌──────────────┐   raw.ioda.*   ┌──────────────┐   raw.ripe.ping            │
│  │ ioda-ingester│ ───topics────► │              │ ◄───topics───────┐         │
│  │  (IODA, poll)│                │     kafka     │                 │         │
│  │              │── put_object ─►│ (KRaft, 9092) │            ┌────┴───────┐ │
│  └──────────────┘      │         └──────┬───────┘            │ripe-ingester│ │
│                         │                │                    │ (websocket)│ │
│                         ▼                ▼                    └────┬───────┘ │
│                  ┌────────────────────────────┐  put_object        │         │
│                  │            minio             │◄────────────────┘          │
│                  │   (S3 API: 9000, console)    │                            │
│                  │   bronze / silver / gold      │                          │
│                  └──────────────┬────────────────┘                          │
│                                  │                                            │
│                     ┌────────────┴─────────────┐                             │
│                     ▼                           ▼                            │
│           spark-silver-ioda /          spark-silver-stream                   │
│           spark-silver-ripe            (always-on, Kafka → silver)           │
│           (on-demand batch jobs,                                             │
│            run via the "batch" profile)                                      │
│                     │                                                        │
│                     ▼                                                        │
│               spark-gold                                                     │
│        (on-demand batch job, silver → TimescaleDB)                           │
│                     │                                                        │
│                     ▼                                                        │
│              timescaledb                                                     │
│      (asn_baselines / ioda_signals / asn_names)                              │
│                     │                                                        │
│                     ▼                                                        │
│               dashboard                                                      │
│         (Streamlit, port 8501)                                               │
│                                                                                │
│  ┌──────────────┐                                                            │
│  │   kafka-ui   │  (web, port 8080)                                          │
│  └──────────────┘                                                            │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Project structure

```
network-outage-bigdata-project/
├── _legacy/                            # Superseded files, kept for reference only
├── config/
│   ├── init.sql                        # TimescaleDB schema, auto-applied on first start
│   └── populate_asn_names.py           # One-time script: resolves ASN → provider name
├── dashboard/                          # Containerized Streamlit dashboard
│   ├── app.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md
├── ingestion/
│   ├── ioda/                          # IODA data source
│   │   ├── starting_pipe.py           # Docker ingestion logic (Kafka + MinIO)
│   │   ├── run_loop.py                # Container entrypoint + polling/backfill loop
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── ripe/                          # RIPE Atlas data source
│       ├── ripe_recon.py              # Builds probe→country/ASN mapping
│       ├── ripe_probe_mapping.json    # Generated output of ripe_recon.py
│       ├── ripe_streaming_pipe.py     # Always-on websocket ingester
│       ├── ripe_bronze_ingestion.py   # Historical batch ingestion
│       ├── requirements.txt
│       └── Dockerfile
├── notebooks/                          # Ad-hoc exploration (not part of the running stack)
├── spark-jobs/                        # Silver + Gold layers
│   ├── silver_ioda.py                 # Batch: bronze IODA → silver Parquet
│   ├── silver_ripe.py                 # Batch: bronze RIPE → silver Parquet
│   ├── silver_streaming.py            # Streaming: Kafka → silver Parquet (always-on)
│   ├── gold_batch.py                  # Silver → TimescaleDB (RIPE + IODA), run on demand
│   └── submit.py                      # Universal spark-submit launcher
├── .env.example                        # Credential + config template — copy to .env
├── .env                                 # Your local credentials (git-ignored, never commit)
├── .gitattributes
├── .gitignore
├── backfill.py                         # Master historical backfill orchestrator
├── docker-compose.yml
└── README.md
```

## Data flow

```
IODA API   → ioda-ingester  → Kafka (raw.ioda.alerts / events / signals)
                             → MinIO bronze/ioda/...   (Hive-partitioned NDJSON.gz)

RIPE Atlas → ripe-ingester   → Kafka (raw.ripe.ping)
  (websocket,                → MinIO bronze/ripe/ping/... (Hive-partitioned NDJSON.gz)
   filtered to
   TARGET_COUNTRIES
   via probe mapping)

bronze (both sources) → Spark silver jobs → silver/ioda/...  (Parquet)
                                           → silver/ripe/...  (Parquet)

silver (both sources) → spark-gold        → TimescaleDB asn_baselines (RIPE)
                                           → TimescaleDB ioda_signals (IODA)

TimescaleDB → dashboard (Streamlit, reads asn_baselines, ioda_signals, asn_names)
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
  Atlas probe directory and writes `ripe_probe_mapping.json` (probe ID →
  country code + ASN) for only those 15 countries. Both the live websocket
  ingester and the RIPE backfill use this mapping file to filter the
  otherwise-global measurement stream down to just these countries.

---

## Reproducing this project from scratch — step by step

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

### 3. Create your `.env` file

```bash
cp .env.example .env
```

Open `.env` and fill in:
- `KAFKA_CLUSTER_ID` — the UUID you just generated
- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` — choose your own credentials
- `TIMESCALEDB_USER` / `TIMESCALEDB_PASSWORD` — choose your own credentials
- `TARGET_COUNTRIES` — space-separated ISO country codes (defaults to the
  15-country recommended set if omitted; see above)

`.env` is git-ignored and never pushed — every teammate creates their own.

### 4. Build the RIPE probe mapping (run once)

This fetches all active RIPE Atlas probe IDs and ASNs for your target
countries and saves them locally. Both ingesters use this file to filter
the global measurement stream.

```bash
cd ingestion/ripe
pip install -r requirements.txt
python3 ripe_recon.py
cd ../..
```

This produces `ingestion/ripe/ripe_probe_mapping.json`.

### 5. Start the always-on stack

```bash
docker compose up -d
```

This starts every always-on service: `kafka`, `minio`, `minio-init`,
`timescaledb`, `kafka-ui`, `ioda-ingester`, `ripe-ingester`,
`spark-silver-stream`, and `dashboard`.

> The three batch jobs — `spark-silver-ioda`, `spark-silver-ripe`, and
> `spark-gold` — are tagged with the Docker Compose `batch` profile, so they
> are **not** started by a plain `docker compose up -d`. You trigger them
> deliberately, on demand, as shown in steps 7 and 8 below.

`config/init.sql` is mounted into `timescaledb` and runs automatically the
**first time** the container is created, creating the `asn_baselines`,
`ioda_signals`, and `asn_names` tables. If you already have a `timescaledb`
volume from before this file existed, see the manual table-creation note in
step 8.

Watch startup:
```bash
docker compose logs -f
```

Wait until `minio-init` exits after printing the bucket list, `ioda-ingester`
starts its polling loop, and `ripe-ingester` logs `WebSocket Connected`.

### 6. Verify everything is running

```bash
docker compose ps
```

All long-running services should show `healthy` or `running`.

### 7. Backfill historical data (recommended)

`backfill.py` orchestrates historical loads for both sources:

```bash
python3 backfill.py --source ioda --days 7          # IODA only
python3 backfill.py --source ripe --days 1          # RIPE only — see timing note below
python3 backfill.py --source all  --days 7          # Both sources
python3 backfill.py --dry-run                        # Preview without running
```

> **Timing note:** IODA backfill is fast (~75 seconds per country). RIPE
> backfill is much heavier — roughly **2 hours per day of data** across all
> 13 root-server measurements and the full target-country probe set. Plan
> accordingly; a 30-day RIPE backfill is not realistic on a laptop. One to
> three days of RIPE backfill is enough to validate the pipeline end to end.

If `--countries` is omitted, IODA backfill uses `TARGET_COUNTRIES` from
`.env`. RIPE backfill has no `--countries` option — it always covers
whatever is currently in `ripe_probe_mapping.json`.

### 8. Run the silver batch jobs

Transforms raw bronze JSON/NDJSON into clean, partitioned Parquet.

```bash
docker compose run --rm --no-deps spark-silver-ioda
docker compose run --rm --no-deps spark-silver-ripe
```

Both jobs auto-discover all unprocessed bronze partitions if no date range
is given, or you can scope them explicitly:

```bash
docker compose run --rm --no-deps spark-silver-ioda --start 2026-05-28 --end 2026-06-04
```

Both jobs use Spark's dynamic partition overwrite mode, so re-running them
is safe — only the partitions present in the current run are rewritten,
nothing else in silver is touched.

### 9. Run the gold batch job

Aggregates silver into hourly RIPE ASN baselines and hourly IODA country
signals, then writes both to TimescaleDB.

If you're working against a TimescaleDB volume created **before**
`config/init.sql` existed, create the tables manually once:

```bash
docker exec -it timescaledb psql -U admin -d outage_intelligence -f /docker-entrypoint-initdb.d/init.sql
```

Then run the gold job:

```bash
docker compose run --rm --no-deps spark-gold
```

Verify rows landed:

```bash
docker exec -it timescaledb psql -U admin -d outage_intelligence -P pager=off -c \
  "SELECT country_code, COUNT(*) FROM asn_baselines GROUP BY country_code ORDER BY country_code;"

docker exec -it timescaledb psql -U admin -d outage_intelligence -P pager=off -c \
  "SELECT country_code, datasource, COUNT(*) FROM ioda_signals GROUP BY country_code, datasource ORDER BY country_code;"
```

### 10. Resolve ASN numbers to provider names (run once)

The dashboard shows real ISP names (e.g. "FASTWEB - Fastweb SpA") instead of
bare ASN numbers. This is a one-time lookup against Team Cymru's whois
service, run from your host machine — not from inside a container, since it
needs outbound network access that containers in this stack don't have.

```bash
pip install -r config/requirements.txt
python3 config/populate_asn_names.py
```

This populates the `asn_names` table. Re-run it any time new ASNs show up
in `asn_baselines` that aren't resolved yet.

### 11. Open the dashboard

```
http://localhost:8501
```

If you change dashboard code, rebuild before restarting:

```bash
docker compose up -d --build dashboard
```

---

## Web UIs

| Service   | URL                    | Credentials                                       |
|-----------|-------------------------|----------------------------------------------------|
| Dashboard | http://localhost:8501  | none                                               |
| Kafka UI  | http://localhost:8080  | none                                               |
| MinIO     | http://localhost:9090  | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |

> MinIO's console port is mapped to **9090** on the host (not the MinIO
> default of 9001) — check `docker-compose.yml` if this ever changes.

## Inspect the Bronze layer in MinIO

### Via web console
Go to http://localhost:9090, log in, open the `bronze` bucket. Directory tree:
```
ioda/alerts/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
ioda/events/year=2026/month=06/day=04/country_IT.ndjson.gz
ioda/signals/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz
ripe/ping/year=2026/month=06/day=16/measurement_2009_1781596241.ndjson.gz
```

### Via Docker
```bash
docker compose exec minio mc ls --recursive local/bronze | wc -l

docker compose exec minio mc cat \
    local/bronze/ioda/signals/year=2026/month=06/day=04/country_IT_bgp.ndjson.gz \
    | gunzip | head -5 | python3 -m json.tool
```

## Inspect Kafka topics

### Via Kafka UI
Go to http://localhost:8080 → Topics:
- `raw.ioda.alerts`, `raw.ioda.events`, `raw.ioda.signals`
- `raw.ripe.ping`

### Via kcat
```bash
kcat -b localhost:9092 -t raw.ioda.events -o -10 -e | python3 -m json.tool
kcat -b localhost:9092 -t raw.ripe.ping   -o -10 -e | python3 -m json.tool
```

## The dashboard

Four tabs, all reading from TimescaleDB's gold layer:

- **Time series** — RTT and packet loss over time, filterable by provider
- **ISP ranking** — bar charts comparing providers by RTT and packet loss for the selected country
- **IODA signals** — national-level signal scores (BGP, darknet/merit-nt, active ping) over time
- **Combined view** — RIPE RTT and a chosen IODA signal on the same dual-axis timeline, to visually check whether degraded RTT lines up with an independently-detected IODA signal drop

See `dashboard/README.md` for dashboard-specific details.

## Stop the stack

```bash
# Stop containers but keep data volumes intact
docker compose down

# Stop AND delete all data (full reset)
docker compose down -v
```

## Rebuild after code changes

```bash
docker compose build ioda-ingester
docker compose up -d ioda-ingester
```

Ingestion folders are mounted as volumes, so Python file changes take effect
on container restart without a rebuild. Dependency changes
(`requirements.txt`) do require a rebuild — same pattern applies to
`ripe-ingester`, the Spark services, and `dashboard` via their respective
build contexts.

## Known gaps

- **No live path from streaming silver to gold.** `spark-silver-stream` is
  always-on and continuously writes fresh silver Parquet from live Kafka
  data, but nothing currently reads that and refreshes TimescaleDB
  automatically — `spark-gold` only runs when triggered manually. The
  dashboard therefore always reflects a snapshot from whenever `spark-gold`
  was last run by hand, not true live data. A `stream-gold` job (or a
  scheduled re-run of `spark-gold` every N minutes while the stack is up)
  is the next step to close this gap.
- RIPE Atlas measurement 2014 (G-root) returns 404 from the API —
  likely decommissioned upstream; 12 of 13 root-server measurements work.
- IR, PS, and SY currently have no RIPE gold data — no probe coverage in the
  available backfill window, not a pipeline bug.
- A full 30-day RIPE backfill is impractical locally (~2h/day); the dashboard
  is validated against 1–3 day RIPE windows alongside a full 30-day IODA window.

## Cleanup TODO (pre-presentation)

- [ ] Remove the `jupyter` service from `docker-compose.yml` — superseded by
      the dashboard for all gold-layer inspection.
- [ ] Delete `19.6-progress-how_to_run.md` — fully superseded by this README.
- [ ] Rename containers to drop the redundant `ioda-` prefix on shared
      infrastructure: `ioda-kafka` → `kafka`, `ioda-minio` → `minio`,
      `ioda-minio-init` → `minio-init`, `ioda-timescaledb` → `timescaledb`,
      `ioda-kafka-ui` → `kafka-ui`. `ioda-ingester` and `ripe-ingester` keep
      their names since they're source-specific by design.
- [ ] Rename the Docker network to `outage-net`.
- [ ] Confirm the `batch` Compose profile is applied to `spark-silver-ioda`,
      `spark-silver-ripe`, and `spark-gold` in the version of
      `docker-compose.yml` everyone is running, so they no longer auto-start
      on a plain `docker compose up -d`.