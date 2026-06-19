# Network Outage Intelligence — Dashboard

A containerised Streamlit dashboard reading from TimescaleDB (gold layer).
Displays RIPE Atlas network performance data per country and ISP, with IODA
signal overlays coming in Tab 3 and Tab 4.

## Requirements

- Docker and Docker Compose running the full stack
- TimescaleDB populated (run `spark-gold` first)
- ASN names populated (run `populate_asn_names.py` once)

## Running

The dashboard starts automatically with the rest of the stack:

```bash
docker compose up -d
```

Then open http://localhost:8501

To rebuild after code changes:

```bash
docker compose up -d --build dashboard
```

## Tabs

- **Time series** — RTT and packet loss over time per provider, filterable
- **ISP ranking** — bar chart comparing providers by RTT and loss for selected country
- **IODA signals** — national-level signal scores (bgp, merit-nt, ping-slash24) — coming soon
- **Combined view** — RTT overlaid with IODA signal — coming soon

## Data sources

- RIPE data comes from `asn_baselines` table in TimescaleDB
- IODA data will come from `ioda_signals` table in TimescaleDB
- Provider names come from `asn_names` table, populated via `populate_asn_names.py`

## Known gaps

- IODA not yet shown in dashboard (data is in TimescaleDB, tabs not built yet)
- IR, PS, SY have no RIPE data — no probe coverage in the available window
- Only June 17-18 RIPE backfill available (full 30-day backfill takes ~4h per day)