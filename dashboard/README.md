# Network Outage Intelligence — Dashboard

A containerised Streamlit dashboard reading from the TimescaleDB gold layer.
Aggregates RIPE Atlas network performance data and IODA outage signals across
15 target countries, with multi-source outage detection and cross-country comparison.

## Glossary

**RTT (Round-Trip Time)** — the time it takes for a small data packet to travel
from a RIPE Atlas probe to a target server and back, measured in milliseconds.
Lower is better; it is one of the most reliable signals of network health and
the core metric behind the Overview, Providers, and Correlation tabs.

**Packet loss** — the percentage of packets that never received a response. The
dashboard uses the 95th percentile (`loss_p95_pct`) rather than a simple average,
since averaging hides short, sharp spikes that matter most for outage detection.

**ASN (Autonomous System Number)** — a unique identifier assigned to a network
operator (an ISP, company, or organisation) that controls how traffic is routed.
Each row in the Providers tab corresponds to one ASN — i.e. one provider.

**IODA signal score** — a country-level metric from one of three independent
data sources that IODA uses to detect large-scale connectivity events:
- `bgp` — BGP routing visibility (number of prefixes announced globally)
- `merit-nt` — darknet traffic (unsolicited background radiation to unused IPs)
- `ping-slash24` — active ping sweeps of sampled /24 address blocks

Signal scores are not directly comparable in scale to RTT, which is why the
Correlation tab uses a dual-axis chart.

**Confidence score** — a weighted combination of RIPE and IODA evidence
(BGP 35%, RIPE loss 35%, darknet 20%, active ping 10%) used to classify
each detected event as `hard_outage`, `degraded`, or `possible`.

**ICMP filtering** — some routers rate-limit or silently drop ICMP ping packets,
making measurements appear as packet loss when the network is actually healthy.
The dashboard flags probes exhibiting this pattern to avoid false positives.

## Requirements

- Docker and Docker Compose running the full stack
- TimescaleDB populated — run `spark-gold` first
- ASN names populated — run `populate_asn_names.py` once after the gold job

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

To populate ASN provider names (run once after the gold batch job):

```bash
docker compose exec dashboard python3 /app/populate_asn_names.py
```

## Tabs

- **Overview** — country health at a glance. Key metrics (median RTT, P95 packet
  loss, active ISPs, outage event count), a status banner, and a timeline of
  detected outage events with severity classification.

- **Providers** — ISP-level analysis. RTT spread (P10–P90) and packet loss per
  provider over time, an ISP ranking table with average RTT, standard deviation,
  and loss, and ICMP filtering detection.

- **Signals** — raw IODA signal traces for the selected country. Each of the
  three datasources (BGP, darknet, active ping) plotted independently, with
  collection gaps flagged.

- **Correlation** — RIPE Atlas and IODA combined on a dual-axis chart, with
  detected outage event markers superimposed. A normalised loss-vs-signal panel
  confirms correlated drops across independent sources.

- **Cross-Country** — multi-country comparison. A bubble chart maps all 15
  countries by RTT vs packet loss. Supports side-by-side RTT time series and
  outage event alignment across up to four countries simultaneously.

## Data Sources

| Table | Contents |
|---|---|
| `asn_baselines` | Hourly RIPE RTT and packet loss aggregates per ASN |
| `ioda_signals` | Raw IODA signal values at native resolution (5/10 min) |
| `outage_events` | Correlated outage detections with confidence score and severity |
| `country_coverage` | Daily probe and measurement counts per country (data quality) |
| `asn_names` | ASN → ISP name lookup, populated via `populate_asn_names.py` |