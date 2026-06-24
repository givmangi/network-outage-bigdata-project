# Network Outage & Service Quality Intelligence Platform

Welcome to our repository for the Big Data Technologies project.

This document provides a comprehensive overview of the system architecture, technologies used, data sources, and instructions for running the platform.

## Table of Contents

- [1. Overview](#1-overview)
- [2. System Architecture](#2-system-architecture)
  - [2.1 Architecture Overview](#21-architecture-overview)
  - [2.2 Data Flow Diagram](#22-data-flow-diagram)
  - [2.3 Repository Structure](#23-repository-structure)
  - [2.4 Target Countries](#24-target-countries)
- [3. Data Sources](#3-data-sources)
  - [3.1 RIPE Atlas](#31-ripe-atlas)
  - [3.2 IODA](#32-ioda)
- [4. Technologies Used](#4-technologies-used)
- [5. How to Run](#5-how-to-run)
  - [5.1 Prerequisites](#51-prerequisites)
  - [5.2 Clone the Repository](#52-clone-the-repository)
  - [5.3 Generate a Kafka Cluster ID](#53-generate-a-kafka-cluster-id)
  - [5.4 Create your .env File](#54-create-your-env-file)
  - [5.5 Start the Always-On Stack](#55-start-the-always-on-stack)
  - [5.6 Verify Everything is Running](#56-verify-everything-is-running)
  - [5.7 Build the RIPE Probe Mapping](#57-build-the-ripe-probe-mapping-run-once)
  - [5.8 Backfill Historical Data](#58-backfill-historical-data-recommended)
  - [5.9 Run the Silver Batch Jobs](#59-run-the-silver-batch-jobs)
  - [5.10 Run the Gold Batch Job](#510-run-the-gold-batch-job)
  - [5.11 Resolve ASN Numbers to Provider Names](#511-resolve-asn-numbers-to-provider-names-run-once)
  - [5.12 Open the Dashboard](#512-open-the-dashboard)
  - [5.13 Stopping the Stack](#513-stopping-the-stack)
  - [5.14 Web UIs](#514-web-uis)
- [6. Dashboard](#6-dashboard)
  - [6.1 Overview](#61-overview)
  - [6.2 Providers](#62-providers)
  - [6.3 Signals](#63-signals)
  - [6.4 Correlation](#64-correlation)
  - [6.5 Cross-Country](#65-cross-country)
- [7. Limitations & Future Work](#7-limitations--future-work)
  - [7.1 Geographic and Temporal Scope](#71-geographic-and-temporal-scope)
  - [7.2 Batch and Streaming Path Collision](#72-batch-and-streaming-path-collision)
  - [7.3 Single-Node Spark](#73-single-node-spark)
  - [7.4 RIPE Atlas Probe Coverage](#74-ripe-atlas-probe-coverage)
  - [7.5 No Automated Alerting](#75-no-automated-alerting)
  - [7.6 Manual Setup Steps](#76-manual-setup-steps)
  - [7.7 RIPE Atlas Volunteer Bias](#77-ripe-atlas-volunteer-bias)
  - [7.8 Redundant RIPE Measurement IDs](#78-redundant-ripe-measurement-ids)
- [8. References](#8-references)
- [9. Authors](#9-authors)

---

## 1. Overview

This platform aggregates real-time and historical network measurements from **RIPE Atlas** and outage indicators from **IODA** (Internet Outage Detection and Analysis) to detect degraded connectivity, local outages, and persistent performance issues across 15 target countries. The system ingests two independent data streams — active ping measurements at the ISP level from RIPE Atlas, and country-level control-plane signals from IODA — and processes them through a bronze-to-silver pipeline stored in MinIO, before aggregating into a gold layer served from TimescaleDB. A multi-source confidence scoring model cross-references both streams to classify network events by severity, allowing the system to distinguish genuine outages from isolated measurement noise. Results are exposed through a containerised Streamlit dashboard that enables comparison of providers, regions, and time windows to identify recurring failure patterns.

---

## 2. System Architecture

### 2.1 Architecture Overview
 
![Architecture Pipeline](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/diagrams/architecture_diagram.jpg)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  outage-net (Docker bridge network)                                          │
│                                                                              │
│  ┌──────────────┐   raw.ioda.*   ┌──────────────┐   raw.ripe.ping           │
│  │ ioda-ingester│ ───topics────► │              │ ◄───topics───────┐        │
│  │  (IODA, poll)│                │    kafka      │                 │        │
│  │              │── put_object ─►│ (KRaft, 9092) │            ┌────┴───────┐│
│  └──────────────┘      │         └──────┬───────┘            │ripe-ingester││
│                         │               │                     │ (websocket) ││
│                         ▼               ▼                     └────┬───────┘│
│                  ┌────────────────────────────┐  put_object        │        │
│                  │           minio            │◄──────────────────┘         │
│                  │  (S3 API: 9000, console)   │                             │
│                  │     bronze / silver        │                             │
│                  └──────────────┬─────────────┘                             │
│                                 │                                            │
│                    ┌────────────┴─────────────┐                             │
│                    ▼                           ▼                             │
│          spark-silver-ioda /          spark-silver-stream                   │
│          spark-silver-ripe            (always-on, Kafka → silver)           │
│          (on-demand batch jobs,                │                             │
│           run via the "batch" profile)         │                             │
│                    │                           ▼                             │
│                    │                  spark-gold-stream                      │
│                    │              (always-on, silver → TimescaleDB)          │
│                    │                           │                             │
│                    ▼                           │                             │
│              spark-gold                        │                             │
│       (on-demand batch job, silver → TimescaleDB)                           │
│                    │                           │                             │
│                    └───────────────┬───────────┘                            │
│                                    ▼                                         │
│                             timescaledb                                      │
│      (asn_baselines/ioda_signals/outage_events/country_coverage/asn_names)   │
│                                    │                                         │
│                                    ▼                                         │
│                              dashboard                                       │
│                        (Streamlit, port 8501)                                │
│                                                                              │
│  ┌──────────────┐                                                            │
│  │   kafka-ui   │  (web, port 8080)                                          │
│  └──────────────┘                                                            │
└──────────────────────────────────────────────────────────────────────────────┘
```
 
### 2.2 Data Flow Diagram
 
![Data Flow Diagram](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/diagrams/data_pipeline.jpg)
 
```
IODA API   → ioda-ingester → Kafka (raw.ioda.alerts / events / signals)
                           → MinIO bronze/ioda/...  (Hive-partitioned NDJSON.gz)
 
RIPE Atlas → ripe-ingester → Kafka (raw.ripe.ping)
  (websocket,              → MinIO bronze/ripe/ping/...  (Hive-partitioned NDJSON.gz)
   filtered to
   TARGET_COUNTRIES
   via probe mapping)
 
bronze (both sources) → spark-silver-ioda / spark-silver-ripe → silver/  (Parquet, batch)
                      → spark-silver-stream                    → silver/  (Parquet, streaming)
 
silver (both sources) → spark-gold        → TimescaleDB  (on-demand batch)
                      → spark-gold-stream → TimescaleDB  (always-on streaming)
 
TimescaleDB → dashboard (Streamlit, asn_baselines, ioda_signals, outage_events, country_coverage, asn_names)
```
 
### 2.3 Repository Structure
 
```
network-outage-bigdata-project/
├── _legacy/                            # Superseded files, kept for reference only
├── config/
│   ├── fix_state.py                    # Clears Spark streaming state after silver batch runs
│   └── init.sql                        # TimescaleDB schema, auto-applied on first start
├── dashboard/                          # Containerised Streamlit dashboard
│   ├── Dockerfile
│   ├── README.md
│   ├── app.py
│   ├── populate_asn_names.py           # One-time script: resolves ASN → provider name
│   └── requirements.txt
├── ingestion/
│   ├── ioda/                           # IODA data source
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── run_loop.py                 # Container entrypoint + polling/backfill loop
│   │   └── starting_pipe.py            # Docker ingestion logic (Kafka + MinIO)
│   └── ripe/                           # RIPE Atlas data source
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── ripe_bronze_ingestion.py    # Historical batch ingestion
│       ├── ripe_probe_mapping.json     # Generated output of ripe_recon.py
│       ├── ripe_recon.py               # Builds probe → country/ASN mapping
│       └── ripe_streaming_pipe.py      # Always-on websocket ingester
├── spark-jobs/                         # Silver + Gold layers
│   ├── gold_batch.py                   # Batch: silver → TimescaleDB, run on demand
│   ├── gold_diagnostic.py              # Diagnostic utilities for the gold layer
│   ├── gold_streaming.py               # Streaming: silver → TimescaleDB (always-on)
│   ├── silver_ioda.py                  # Batch: bronze IODA → silver Parquet
│   ├── silver_ripe.py                  # Batch: bronze RIPE → silver Parquet
│   ├── silver_streaming.py             # Streaming: Kafka → silver Parquet (always-on)
│   └── submit.py                       # Universal spark-submit launcher
├── .env.example                        # Credential + config template — copy to .env
├── .gitattributes
├── .gitignore
├── README.md
├── backfill.py                         # Master historical backfill orchestrator
└── docker-compose.yml
```

---
 
## 2.4 Target Countries
 
The platform monitors 15 countries selected to cover a broad spectrum of internet freedom conditions — from active conflict zones and state-imposed shutdowns to stable Western baselines — allowing meaningful cross-regional comparison.
 
| Code | Country | Rationale |
|------|---------|-----------|
| IT | Italy | Home country; provides local ground truth for the team |
| LV | Latvia | Another home country; provides local ground truth for the team |
| SK | Slovakia | Another home country; provides local ground truth for the team |
| VE | Venezuela | Persistent throttling and platform blocks |
| MM | Myanmar | World leader in shutdowns in 2024 with 85 recorded incidents |
| IN | India | Second highest globally with 84 shutdowns in 2024 |
| PK | Pakistan | Third highest globally with 21 shutdowns in 2024 |
| UA | Ukraine | Active conflict zone; documented cross-border Russian disruptions to infrastructure |
| RU | Russia | Major shutdown actor; significant economic and political internet restrictions |
| TR | Turkey | Recurring social media blocks during political events |
| BD | Bangladesh | 5 shutdowns in 2024; Signal blocked nationally |
| NG | Nigeria | Africa's largest internet market; recurring outages and regulatory disruptions |
| US | United States | Major internet hub; baseline for high-capacity "normal" connectivity |
| DE | Germany | European baseline for stable, well-regulated connectivity |
| GB | United Kingdom | Additional Western baseline for comparison |
 
All 15 countries are driven by a single `.env` value, `TARGET_COUNTRIES`, which controls filtering across both the RIPE and IODA ingestion pipelines.
 
---

## 3. Data Sources
 
The platform relies on two complementary and independent data sources, one measuring active network performance at the ISP level, and one detecting outages through passive control-plane signals.
 
### 3.1 RIPE Atlas
 
[RIPE Atlas](https://atlas.ripe.net/) is a global network of hardware probes hosted by volunteers that continuously measure internet connectivity and reachability. The platform queries RIPE Atlas ping measurements to collect round-trip time (RTT) and packet loss data from probes located within the 15 target countries.
 
Each measurement is identified by a measurement ID and linked to a specific probe, which is mapped to a country code and ASN (Autonomous System Number) via `ripe_probe_mapping.json`. This mapping is generated once by `ripe_recon.py` and used by both the live ingester and the batch backfill to filter the otherwise global measurement stream down to the target countries.
 
**Key fields extracted per measurement result:**
 
- `msm_id` — RIPE Atlas measurement identifier
- `probe_id` — unique probe identifier (hashed for storage)
- `ts_utc` — UTC timestamp of the measurement
- `country_code` — derived from the probe mapping
- `asn` — Autonomous System Number of the probe's network
- `rtt_median_ms` — median round-trip time in milliseconds
- `loss_pct` — fraction of packets 

Data is ingested via two paths: a **live WebSocket stream** (`ripe-ingester`) for real-time data, and a **batch backfill** orchestrated via `backfill.py` for loading historical data across a specified date range.
 
---
 
### 3.2 IODA
 
[IODA](https://ioda.inetintel.cc.gatech.edu/) (Internet Outage Detection and Analysis) is a system developed at Georgia Tech that monitors internet outages in near real-time using three independent passive signals:
 
- **BGP routing visibility** — monitors the number of IP prefixes announced in the global routing table. A drop indicates that address blocks have become unreachable at the routing level.
- **Merit darknet traffic** — measures unsolicited traffic arriving at a large block of unused IP addresses. A drop in this background radiation is a strong indicator that hosts in a country can no longer reach the internet.
- **Active ping (/24 blocks)** — actively probes a sample of IP address blocks and measures responsiveness. A drop indicates that previously reachable addresses are no longer responding.

A sustained drop across multiple IODA signals, especially when correlated with RIPE Atlas RTT or loss degradation, is a strong indicator of a real network outage rather than a measurement artefact.
 
**Key fields extracted per signal record:**
 
- `time_window` — UTC timestamp of the observation window
- `country_code` — ISO 3166-1 alpha-2 country code
- `datasource` — one of `bgp`, `merit-nt`, `ping-slash24`
- `signal_value` — normalised score for the signal at that time
- `collection_gap` — flag indicating interpolated or missing data points

Data is ingested via two paths: a **live poller** (`ioda-ingester`) that queries the IODA REST API every 15 minutes for real-time data, and a **batch backfill** orchestrated via `backfill.py` for loading historical data across a specified date range.
 
---
 
## 4. Technologies Used
 
| Technology | Role |
|---|---|
| **Python** | Primary programming language across all components |
| **Apache Kafka** | Message broker for real-time data streaming between ingesters and Spark; runs in KRaft mode (no Zookeeper) |
| **Apache Spark** | Distributed data processing for both batch and streaming silver and gold jobs; runs via PySpark |
| **MinIO** | S3-compatible object storage for the bronze and silver layers; Hive-partitioned NDJSON.gz and Parquet |
| **TimescaleDB** | Time-series optimised PostgreSQL database serving the gold layer (`asn_baselines`, `ioda_signals`, `outage_events`, `country_coverage`, `asn_names`) |
| **Streamlit** | Frontend dashboard for data visualisation and outage exploration |
| **Plotly** | Interactive charting library used in the dashboard |
| **Docker / Docker Compose** | Containerisation and orchestration of all services |
| **boto3 / botocore** | AWS SDK for Python; used by ingesters to write to MinIO via the S3 API |
| **kafka-python** | Kafka producer client used by both ingesters |
| **SQLAlchemy** | SQL toolkit used by the dashboard to query TimescaleDB |
| **psycopg2** | PostgreSQL adapter for Python; used by the dashboard and config scripts |
| **websocket-client** | WebSocket library used by the RIPE Atlas live ingester |
| **pandas** | Data manipulation in the dashboard |
| **python-dotenv** | Loads `.env` credentials into environment variables for the dashboard and one-time scripts |
| **Kafka UI** | Web interface for inspecting Kafka topics (`provectuslabs/kafka-ui`) |
 
---

## 5. How to Run
 
This section covers how to reproduce the project from scratch. All services run inside Docker containers — the only local requirements are Docker, Docker Compose, and Python 3 on the host for the one-time setup scripts.
 
### 5.1 Prerequisites
 
- Docker and Docker Compose installed
- Python 3 installed on the host machine
- At least **10 CPU cores** recommended

---
 
### 5.2 Clone the Repository
 
```bash
git clone https://github.com/givmangi/network-outage-bigdata-project.git
cd network-outage-bigdata-project
```
 
---
 
### 5.3 Generate a Kafka Cluster ID
 
Kafka runs in KRaft mode and requires a unique cluster ID generated before first startup:
 
```bash
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```
 
Copy the output — you will need it in the next step.
 
---
 
### 5.4 Create your `.env` File
 
```bash
cp .env.example .env
```
 
Open `.env` and fill in the following values:
 
- `KAFKA_CLUSTER_ID` — the UUID generated in the previous step
- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` — choose your own credentials
- `TIMESCALEDB_USER` / `TIMESCALEDB_PASSWORD` — choose your own credentials
- `TARGET_COUNTRIES` — space-separated ISO 3166-1 alpha-2 country codes (defaults to the 15-country set if omitted)
- `COMPOSE_PROJECT_NAME` — set to `network-outage-bigdata-project` to ensure Docker volume and network names are consistent across all teammates regardless of the local directory name

`.env` is git-ignored and never pushed — every teammate creates their own local copy.
 
`TARGET_COUNTRIES` drives filtering across both pipelines:
- **IODA** reads it directly as its default `ENTITY_CODES` list for both the live ingester and `backfill.py --source ioda`, unless overridden per-run with `--countries`.
- **RIPE** reads it in `ripe_recon.py`, which queries the RIPE Atlas probe directory and writes `ripe_probe_mapping.json` (probe ID → country code + ASN) for only those countries. Both the live WebSocket ingester and the RIPE backfill use this mapping file to filter the otherwise global measurement stream.

---

### 5.5 Start the Always-On Stack
 
```bash
docker compose up -d
```
 
This starts all always-on services: `kafka`, `minio`, `minio-init`, `timescaledb`, `kafka-ui`, `ioda-ingester`, `ripe-ingester`, `spark-silver-stream`, `spark-gold-stream`, and `dashboard`.
 
> The batch jobs — `spark-silver-ioda`, `spark-silver-ripe`, and `spark-gold` — are tagged with the Docker Compose `batch` profile and are **not** started by a plain `docker compose up -d`. They are triggered manually on demand as shown in the steps below.
 
`config/init.sql` is mounted into `timescaledb` and runs automatically the first time the container is created, setting up the `asn_baselines`, `ioda_signals`, `outage_events`, `country_coverage`, and `asn_names` tables. If you have an existing `timescaledb` volume from before this file existed, run the following once:
 
```bash
docker exec -it timescaledb psql -U admin -d outage_intelligence -f /docker-entrypoint-initdb.d/init.sql
```
 
Watch startup:
 
```bash
docker compose logs -f
```
 
Wait until `minio-init` exits after printing the bucket list, `ioda-ingester` starts its polling loop, and `ripe-ingester` logs `WebSocket Connected`.
 
---

### 5.6 Verify Everything is Running
 
```bash
docker compose ps
```
 
All long-running services should show `healthy` or `running`.
 
---

### 5.7 Build the RIPE Probe Mapping (run once)
 
This fetches all active RIPE Atlas probe IDs and ASNs for your target countries and saves them locally. Both the live ingester and the backfill use this file to filter the global measurement stream down to the target countries.
 
```bash
docker compose exec ripe-ingester python3 /app/ripe_recon.py
```
 
This produces `ingestion/ripe/ripe_probe_mapping.json`. Re-run it any time you change `TARGET_COUNTRIES`.
 
---

### 5.8 Backfill Historical Data (recommended)
 
`backfill.py` orchestrates historical loads for both sources:
 
```bash
python3 backfill.py --source ioda --days 7     # IODA only
python3 backfill.py --source ripe --days 1     # RIPE only — see timing note below
python3 backfill.py --source all  --days 7     # Both sources
python3 backfill.py --dry-run                  # Preview without running
```
 
> **Timing note:** IODA backfill is fast (~75 seconds per country). RIPE backfill is considerably heavier — roughly 2 hours per day of data across all root-server measurements and the full target-country probe set. One to three days of RIPE backfill is enough to validate the pipeline end to end; a full 30-day RIPE backfill is not realistic on a laptop.
 
If `--countries` is omitted, IODA backfill uses `TARGET_COUNTRIES` from `.env`. RIPE backfill has no `--countries` option — it always covers whatever is in `ripe_probe_mapping.json`.
 
---

### 5.9 Run the Silver Batch Jobs
 
Transforms raw bronze NDJSON.gz into clean, Hive-partitioned Parquet in the silver layer.
 
> ⚠️ **Run silver batch jobs with the streaming containers stopped.** Running `spark-silver-ioda` or `spark-silver-ripe` while `spark-silver-stream` or `spark-gold-stream` are active can corrupt the Spark streaming state (the `_spark_metadata` transaction log). Always follow this full sequence:
> ```bash
> # 1. Stop the streaming containers
> docker compose stop spark-silver-stream spark-gold-stream
>
> # 2. Run silver batch jobs
> docker compose run --rm --no-deps spark-silver-ioda
> docker compose run --rm --no-deps spark-silver-ripe
>
> # 3. Run the gold batch job — set --start to match your earliest silver data
> docker compose run --rm spark-gold --start 2026-06-17 #any start date you choose
>
> # 4. Clear the corrupted streaming state
> python3 config/fix_state.py
>
> # 5. Restart the streams
> docker compose up -d spark-silver-stream spark-gold-stream
> ```

Both silver batch jobs auto-discover all available bronze partitions if no date range is given, or you can scope them explicitly:
 
```bash
docker compose run --rm --no-deps spark-silver-ioda --start 2026-05-28 --end 2026-06-04
docker compose run --rm --no-deps spark-silver-ripe  --start 2026-05-28 --end 2026-06-04
```
 
`--end` is optional and defaults to today (exclusive), so `--start 2026-06-17` processes everything from that date through yesterday. To target a single day, set `--end` to the following day:
 
```bash
docker compose run --rm --no-deps spark-silver-ripe --start 2026-06-20 --end 2026-06-21
```
 
Both jobs use Spark's dynamic partition overwrite mode — re-running is safe, only the partitions in the current run are rewritten.
 
---

### 5.10 Run the Gold Batch Job
 
Aggregates silver into hourly RIPE ASN baselines, IODA country signals, outage events, and coverage stats, then writes all to TimescaleDB. A start date is required — set it to match your earliest silver data:
 
```bash
docker compose run --rm spark-gold --start 2026-06-17
docker compose run --rm spark-gold --start 2026-06-19 --end 2026-06-22
```
 
You can scope the run to a date range or run individual stages:
 
```bash
docker compose run --rm spark-gold --start 2026-06-17 --datasets ripe
docker compose run --rm spark-gold --start 2026-06-17 --datasets ioda
docker compose run --rm spark-gold --start 2026-06-17 --datasets outages
docker compose run --rm spark-gold --start 2026-06-17 --datasets coverage
```

Verify rows landed:
 
```bash
docker exec -it timescaledb psql -U admin -d outage_intelligence -P pager=off -c \
  "SELECT country_code, COUNT(*) FROM asn_baselines GROUP BY country_code ORDER BY country_code;"
 
docker exec -it timescaledb psql -U admin -d outage_intelligence -P pager=off -c \
  "SELECT country_code, datasource, COUNT(*) FROM ioda_signals GROUP BY country_code, datasource ORDER BY country_code;"
```
 
---

### 5.11 Resolve ASN Numbers to Provider Names (run once)
 
The dashboard shows real ISP names (e.g. "FASTWEB - Fastweb SpA") instead of bare ASN numbers. This is a one-time lookup against Team Cymru's whois service. The script runs inside the dashboard container, which already has the required dependencies and network access to TimescaleDB.
 
```bash
docker compose exec dashboard python3 /app/populate_asn_names.py
```
 
This populates the `asn_names` table. Re-run it any time new ASNs appear in `asn_baselines` that are not yet resolved.
 
---
 
### 5.12 Open the Dashboard
 
```
http://localhost:8501
```
 
If you make changes to the dashboard code, rebuild before restarting:
 
```bash
docker compose up -d --build dashboard
```
 
---
 
### 5.13 Stopping the Stack
 
```bash
# Stop containers but keep all data volumes intact
docker compose down
 
# Stop and delete all data (full reset)
docker compose down -v
```
 
---
 
### 5.14 Web UIs
 
| Service | URL | Credentials |
|---|---|---|
| Dashboard | http://localhost:8501 | none |
| Kafka UI | http://localhost:8080 | none |
| MinIO console | http://localhost:9090 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |

> MinIO's console port is mapped to **9090** on the host (not the MinIO
> default of 9001) — check `docker-compose.yml` if this ever changes.
 
---

## 6. Dashboard
 
The dashboard is accessible at `http://localhost:8501` after the stack is running. It is built with Streamlit and reads exclusively from the TimescaleDB gold layer. A brief tour of the five tabs is provided below.

### 6.1 Overview
 
![Overview](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/overview.png)
![RTT-Times](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/rtt_times.png)

Country-level health summary for the selected time window. Displays key metrics (median RTT, P95 packet loss, active ISPs, outage events), a status banner indicating whether hard outages or degraded periods were detected, and a timeline of detected outage events with severity classification.
 
### 6.2 Providers
 
![Providers Bubbleplot](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/providers_bubbleplot.png)
![Providers Line Plot Over Time](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/providers_line.png)

ISP-level analysis for the selected country. Shows RTT spread (P10–P90) and packet loss per provider over time, an ISP ranking table with average RTT, standard deviation, and packet loss, and flags providers with unusually high ICMP filtering rates.
 
### 6.3 Signals

![Signals Compare](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/compared_signals.png) 
![Signals Overlayed](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/signals_overlayed.png)

Raw IODA signal traces for the selected country — BGP routing visibility, Merit darknet traffic, and active ping (/24 blocks). Each datasource is plotted independently, with collection gaps flagged. Allows the user to identify which signal layer first detected a degradation.
 
### 6.4 Correlation
 
![Overlay Degradation](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/overlay_degradation.png)
![Loss vs Signals](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/loss_vs_signal.png)
 
Combined RIPE Atlas and IODA overlay. Plots RIPE RTT alongside a chosen IODA signal on a dual-axis chart, with detected outage event markers superimposed. A normalised loss-vs-signal panel allows visual confirmation of correlated drops across independent data sources.
 
### 6.5 Cross-Country
 
![Countries Bubbleplot](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/countries_bubbleplot.png)
![Countries RTT Histogram](https://github.com/givmangi/network-outage-bigdata-project/blob/main/img/dashboard_screenshots/countries_rtt_histogram.png)


Multi-country comparison view. A bubble chart maps all 15 countries by average RTT vs packet loss, with bubble size proportional to detected outage hours. Supports side-by-side RTT time series and outage event alignment across up to four countries simultaneously, to identify whether events are local or affect a shared upstream provider.

See `dashboard/README.md` for dashboard-specific details.

---

## 7. Limitations & Future Work
 
This section outlines the main limitations of the current implementation and gives a brief overview of what could be improved or extended with more time and resources.
 
### 7.1 Geographic and Temporal Scope
 
The platform currently monitors 15 countries with a practical backfill depth of a few days due to the time and computational cost of processing RIPE Atlas data — a full 30-day RIPE backfill across all target countries is not realistic on a laptop. With dedicated infrastructure, expanding to a broader set of countries and a longer historical window would reveal more recurring failure patterns and enable more robust baseline computation for the outage detection model.
 
### 7.2 Batch and Streaming Path Collision
 
Both the silver batch jobs (`spark-silver-ripe`, `spark-silver-ioda`) and the always-on streaming job (`spark-silver-stream`) write to the same MinIO paths. When a batch job overwrites a partition, it bypasses the Spark `_spark_metadata` transaction log that the streaming reader depends on, causing state corruption that requires manual intervention via `config/fix_state.py`. A cleaner architecture would separate batch and streaming output paths and introduce a merge or promotion step, eliminating the need to stop streaming containers before every batch run.
 
### 7.3 Single-Node Spark
 
All Spark jobs run in `local[N]` mode (`local[2]` for silver, `local[4]` for gold batch), which means Spark spawns N threads within a single JVM process on one machine — there is no true distributed execution. This is standard for development and works well at the current data volume, but it is an architectural ceiling. Deploying Spark on a proper cluster (e.g. Kubernetes or YARN) would allow the silver and gold jobs to distribute work across multiple nodes, significantly reducing processing time for large historical backfills and making the platform viable at a much larger scale.
 
### 7.4 RIPE Atlas Probe Coverage
 
RIPE Atlas probe density varies significantly across the target countries. Countries with fewer active probes produce RTT and packet loss figures with lower statistical reliability, which can make it harder to distinguish genuine degradation from measurement noise. Future work could weight measurements by probe count or apply minimum coverage thresholds before surfacing results in the dashboard.
 
### 7.5 No Automated Alerting
 
The platform detects and classifies outages with a confidence score but has no mechanism to push notifications. A natural extension would be an alerting layer — for example, a webhook or email trigger fired when a country's confidence score exceeds a configurable threshold for a sustained period — turning the platform from a monitoring tool into an active early-warning system.
 
### 7.6 Manual Setup Steps
 
Two one-time setup steps currently require manual intervention: running `ripe_recon.py` to build the probe-to-country mapping before first startup, and running `populate_asn_names.py` after each gold batch run to resolve new ASN numbers to provider names. Both could be automated — `ripe_recon.py` could be triggered on stack startup if the mapping file is missing or older than a configurable threshold, and `populate_asn_names.py` could be integrated as a final step in the gold batch job itself — reducing the risk of forgotten steps when onboarding new teammates or rebuilding from scratch.

### 7.7 RIPE Atlas Volunteer Bias
 
RIPE Atlas probes are hosted by volunteers who receive credits in exchange for running the hardware. This self-selection means the probe network is not a representative sample of internet users in a country — probes are disproportionately hosted by technically motivated individuals with above-average connectivity, predominantly in urban areas, and on fixed broadband or institutional networks. In countries like Myanmar, Bangladesh, Nigeria, and Pakistan, where the majority of internet access happens over mobile networks, a real outage affecting mobile users or rural areas may not be visible in RIPE Atlas measurements at all. The RTT and packet loss figures the platform reports should be interpreted as reflecting a technically privileged subset of users rather than the general population.

### 7.8 Redundant RIPE Measurement IDs
 
The platform ingests all 13 built-in RIPE Atlas IPv6 ping measurements, each targeting a different DNS root server (a– through m-root). In practice, outage signals are highly correlated across all 13 — if a country loses connectivity, all measurements drop simultaneously. A representative subset of 5 measurements targeting root servers from different operators and geographic anycast distributions would likely provide equivalent outage detection power at less than half the data volume, making historical backfills significantly faster and reducing the risk of memory exhaustion during long backfill runs.

---

## 8. References
 
- [RIPE Atlas](https://atlas.ripe.net/) — distributed network measurement platform
- [RIPE Atlas REST API](https://atlas.ripe.net/docs/apis/rest-api-manual/) — documentation for the measurement data API used for both live streaming and historical backfill
- [IODA](https://ioda.inetintel.cc.gatech.edu/) — Internet Outage Detection and Analysis, Georgia Tech
- [IODA API](https://api.ioda.inetintel.cc.gatech.edu/v2/) — REST API used to retrieve BGP, darknet, and active ping signals
- [Team Cymru Bulk WHOIS](https://www.team-cymru.com/ip-asn-mapping) — ASN to ISP name resolution used by `populate_asn_names.py`
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [TimescaleDB Documentation](https://docs.timescale.com/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [NetBlocks](https://netblocks.org/) — internet shutdown reporting, referenced for country selection rationale
- [Access Now — #KeepItOn](https://www.accessnow.org/campaign/keepiton/) — global internet shutdown tracker, referenced for shutdown statistics in section 2.4
---
 
## 9. Authors
 
This project was developed by **Group 11** for the Big Data Technologies course.
 
| Name | GitHub |
|---|---|
| Kristine Paegle | [@kristine-p](https://github.com/kristine-p) |
| Giuseppe Pio Mangiacotti | [@givmangi](https://github.com/givmangi) |
| Martin Krisak | [@martin-kri](https://github.com/martin-kri) |
 