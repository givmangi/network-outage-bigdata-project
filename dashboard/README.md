# Network Outage Intelligence — Dashboard

A containerised Streamlit dashboard reading from TimescaleDB (gold layer).
Displays RIPE Atlas network performance data per country and ISP, with IODA
signal overlays.

## Glossary

**RTT (Round-Trip Time)** — the time it takes for a small data packet to
travel from a RIPE Atlas probe to a target server and back, measured in
milliseconds. Lower is better; it's one of the most basic and reliable
signals of network health, and the core metric behind the Time series and
ISP ranking tabs.

**Packet loss** — the percentage of packets that never received a response.
The dashboard uses the 95th percentile (loss_95th_pct) rather than a simple
average, since averaging hides short, sharp spikes that matter most for
outage detection.

**ASN (Autonomous System Number)** — a unique identifier assigned to a
network operator (an ISP, company, or organization) that controls how
traffic is routed. Each row in the ISP ranking corresponds to one ASN —
i.e. one provider.

**IODA signal score** — a country-level metric from one of three independent
data sources (BGP routing visibility, darknet/merit-nt traffic, or active
ping sweeps) that IODA uses to detect large-scale connectivity events. Not
directly comparable in scale to RTT, which is why the Combined view uses a
dual-axis chart.

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