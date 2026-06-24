"""
app.py — Network Outage Intelligence Dashboard
================================================
Streamlit dashboard consuming the Gold TimescaleDB layer.
Designed for: analysts and researchers monitoring internet outages
across 15 target countries using RIPE Atlas + IODA data.

Tabs:
  1. Overview      — country health at a glance, outage timeline
  2. Providers     — ISP ranking, RTT spread, packet loss comparison
  3. Signals       — IODA BGP/darknet/ping raw signal traces
  4. Correlation   — RIPE + IODA overlay, outage event markers
  5. Cross-country — multi-country comparison, recurring failure patterns
"""

import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Net Outage Intelligence",
    page_icon="🛰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Design tokens — dark telemetry aesthetic, monospace data readability
# ---------------------------------------------------------------------------

st.markdown("""
<style>
  /* Import monospace + clean sans */
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Inter:wght@300;400;600&display=swap');

  /* Root overrides */
  html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
  }

  /* Metric cards */
  [data-testid="metric-container"] {
    background: #0f1117;
    border: 1px solid #1e2330;
    border-radius: 6px;
    padding: 16px 20px;
  }
  [data-testid="metric-container"] label {
    font-size: 11px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #5a6070 !important;
    font-family: 'IBM Plex Mono', monospace;
  }
  [data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 28px;
    font-weight: 600;
    color: #e2e8f0;
  }

  /* Alert banners */
  .alert-hard {
    background: #1a0a0a;
    border-left: 3px solid #ef4444;
    padding: 10px 16px;
    border-radius: 4px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
    color: #fca5a5;
    margin-bottom: 8px;
  }
  .alert-degraded {
    background: #1a1000;
    border-left: 3px solid #f59e0b;
    padding: 10px 16px;
    border-radius: 4px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
    color: #fde68a;
    margin-bottom: 8px;
  }
  .alert-ok {
    background: #0a1a0f;
    border-left: 3px solid #22c55e;
    padding: 10px 16px;
    border-radius: 4px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
    color: #86efac;
    margin-bottom: 8px;
  }

  /* Section labels */
  .section-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #3b82f6;
    margin-bottom: 4px;
  }

  /* Sidebar */
  [data-testid="stSidebar"] {
    background: #080b12;
    border-right: 1px solid #1e2330;
  }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Plotly theme — dark, consistent across all charts
# ---------------------------------------------------------------------------

CHART_THEME = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="IBM Plex Mono, monospace", size=11, color="#94a3b8"),
    margin=dict(l=48, r=24, t=40, b=40),
)

SEVERITY_COLORS = {
    "hard_outage": "#ef4444",
    "degraded":    "#f59e0b",
    "possible":    "#eab308",
}

DS_COLORS = {
    "bgp":          "#3b82f6",
    "merit-nt":     "#8b5cf6",
    "ping-slash24": "#06b6d4",
}

# Pre-computed rgba versions for band fills (alpha 0.12)
DS_COLORS_BAND = {
    "bgp":          "rgba(59,130,246,0.12)",
    "merit-nt":     "rgba(139,92,246,0.12)",
    "ping-slash24": "rgba(6,182,212,0.12)",
}


def hex_rgba(color: str, alpha: float = 0.12) -> str:
    """Convert a #rrggbb or rgb(...) color string to rgba() with the given alpha."""
    color = color.strip()
    if color.startswith("#"):
        h = color.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    elif color.startswith("rgb("):
        # e.g. "rgb(99, 110, 250)"
        parts = color[4:-1].split(",")
        r, g, b = int(parts[0]), int(parts[1]), int(parts[2])
    else:
        return f"rgba(148,163,184,{alpha})"   # fallback slate
    return f"rgba({r},{g},{b},{alpha})"

DS_LABELS = {
    "bgp":          "BGP visibility",
    "merit-nt":     "Darknet traffic",
    "ping-slash24": "Active ping",
}

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

DB_USER = os.environ.get("TIMESCALEDB_USER")
DB_PASS = os.environ.get("TIMESCALEDB_PASSWORD")
DB_HOST = os.environ.get("TIMESCALEDB_HOST", "timescaledb")
DB_NAME = os.environ.get("TIMESCALEDB_DB", "outage_intelligence")


@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_countries() -> list[str]:
    with get_engine().connect() as conn:
        rows = conn.execute(text(
            "SELECT DISTINCT country_code FROM asn_baselines ORDER BY country_code"
        )).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=120)
def load_outage_events(country: str, start: str, end: str) -> pd.DataFrame:
    q = text("""
        SELECT detected_at, country_code, severity, confidence_score,
               ripe_loss_p95, ripe_rtt_p90_ms, ripe_probe_count, ripe_asn_affected,
               bgp_pct_change, merit_pct_change, ping_pct_change
        FROM outage_events
        WHERE country_code = :c AND detected_at >= :s AND detected_at < :e
        ORDER BY detected_at DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(q, conn, params={"c": country, "s": start, "e": end})


@st.cache_data(ttl=300)
def load_baselines(country: str, start: str, end: str) -> pd.DataFrame:
    q = text("""
        SELECT b.time_window, b.asn,
               COALESCE(n.name, 'AS' || b.asn::text) AS provider,
               b.rtt_p10_ms, b.rtt_median_ms, b.rtt_p90_ms,
               b.loss_median_pct, b.loss_p95_pct,
               b.probe_count, b.total_measurements, b.icmp_filtered_count
        FROM asn_baselines b
        LEFT JOIN asn_names n ON b.asn = n.asn
        WHERE b.country_code = :c
          AND b.time_window >= :s AND b.time_window < :e
          AND b.rtt_median_ms > 0 AND b.total_measurements >= 3
        ORDER BY b.time_window
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(q, conn, params={"c": country, "s": start, "e": end})


@st.cache_data(ttl=300)
def load_ioda(country: str, start: str, end: str) -> pd.DataFrame:
    # Use time_bucket if it exists (new schema), fall back to time_window
    try:
        q = text("""
            SELECT time_bucket AS time_window, datasource, signal_value,
                   signal_min, signal_max, collection_gap
            FROM ioda_signals
            WHERE country_code = :c
              AND time_bucket >= :s AND time_bucket < :e
            ORDER BY time_bucket
        """)
        with get_engine().connect() as conn:
            return pd.read_sql(q, conn, params={"c": country, "s": start, "e": end})
    except Exception:
        q = text("""
            SELECT time_window, datasource, signal_value, collection_gap
            FROM ioda_signals
            WHERE country_code = :c
              AND time_window >= :s AND time_window < :e
            ORDER BY time_window
        """)
        with get_engine().connect() as conn:
            return pd.read_sql(q, conn, params={"c": country, "s": start, "e": end})


@st.cache_data(ttl=300)
def load_all_countries_summary(start: str, end: str) -> pd.DataFrame:
    """Country-level summary across the date range — drives the cross-country tab."""
    q = text("""
        SELECT
            b.country_code,
            ROUND(AVG(b.rtt_median_ms)::numeric, 1)   AS avg_rtt_ms,
            ROUND(MAX(b.rtt_p90_ms)::numeric, 1)       AS max_p90_rtt_ms,
            ROUND(AVG(b.loss_p95_pct)::numeric, 4)     AS avg_loss,
            COUNT(DISTINCT b.asn)                       AS asn_count,
            SUM(b.total_measurements)                   AS total_measurements,
            COUNT(DISTINCT e.detected_at)               AS outage_hours,
            MAX(e.severity)                             AS worst_severity
        FROM asn_baselines b
        LEFT JOIN outage_events e
          ON e.country_code = b.country_code
         AND e.detected_at >= :s AND e.detected_at < :e
         AND e.severity IN ('hard_outage', 'degraded')
        WHERE b.time_window >= :s AND b.time_window < :e
          AND b.rtt_median_ms > 0
        GROUP BY b.country_code
        ORDER BY outage_hours DESC, avg_loss DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(q, conn, params={"s": start, "e": end})


@st.cache_data(ttl=300)
def load_provider_consistency(country: str, start: str, end: str) -> pd.DataFrame:
    """How consistent is each provider? Std dev of RTT over time."""
    q = text("""
        SELECT
            COALESCE(n.name, 'AS' || b.asn::text) AS provider,
            b.asn,
            ROUND(AVG(b.rtt_median_ms)::numeric, 1)    AS avg_rtt,
            ROUND(STDDEV(b.rtt_median_ms)::numeric, 1) AS rtt_stddev,
            ROUND(AVG(b.loss_p95_pct)::numeric, 4)     AS avg_loss,
            ROUND(MAX(b.loss_p95_pct)::numeric, 4)     AS max_loss,
            SUM(b.total_measurements)                   AS measurements,
            SUM(b.probe_count)                          AS total_probes
        FROM asn_baselines b
        LEFT JOIN asn_names n ON b.asn = n.asn
        WHERE b.country_code = :c
          AND b.time_window >= :s AND b.time_window < :e
          AND b.rtt_median_ms > 0 AND b.total_measurements >= 3
        GROUP BY b.asn, n.name
        HAVING COUNT(*) >= 3
        ORDER BY avg_rtt
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(q, conn, params={"c": country, "s": start, "e": end})


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown('<p class="section-label">🛰 Net Outage Intel</p>', unsafe_allow_html=True)
    st.markdown("**RIPE Atlas + IODA** · 15 countries")
    st.divider()

    try:
        countries = load_countries()
    except Exception as e:
        st.error(f"DB connection failed: {e}")
        st.stop()

    if not countries:
        st.warning("No data yet. Run the Gold batch job first.")
        st.stop()

    selected_country = st.selectbox(
        "Country", countries,
        index=countries.index("IT") if "IT" in countries else 0,
    )

    st.divider()
    st.markdown('<p class="section-label">Time window</p>', unsafe_allow_html=True)
    start_date = st.date_input(
        "From", value=pd.Timestamp.now().normalize() - pd.Timedelta(days=14)
    )
    end_date = st.date_input(
        "To", value=pd.Timestamp.now().normalize() + pd.Timedelta(days=1)
    )

    st.divider()
    st.markdown('<p class="section-label">Compare</p>', unsafe_allow_html=True)
    compare_countries = st.multiselect(
        "Add countries to compare",
        [c for c in countries if c != selected_country],
        default=[],
        max_selections=4,
    )

# ---------------------------------------------------------------------------
# Load primary data
# ---------------------------------------------------------------------------

s, e = str(start_date), str(end_date)
df        = load_baselines(selected_country, s, e)
events_df = load_outage_events(selected_country, s, e)
ioda_df   = load_ioda(selected_country, s, e)

# ---------------------------------------------------------------------------
# Header + status banner
# ---------------------------------------------------------------------------

hard   = events_df[events_df["severity"] == "hard_outage"] if not events_df.empty else pd.DataFrame()
deg    = events_df[events_df["severity"] == "degraded"]    if not events_df.empty else pd.DataFrame()

col_title, col_status = st.columns([3, 1])
with col_title:
    st.markdown(f"## {selected_country} — Network Intelligence")
    st.caption(f"{start_date} → {end_date} · RIPE Atlas + IODA")

with col_status:
    if not hard.empty:
        st.markdown(
            f'<div class="alert-hard">🔴 {len(hard)} hard outage(s) detected</div>',
            unsafe_allow_html=True,
        )
    if not deg.empty:
        st.markdown(
            f'<div class="alert-degraded">🟡 {len(deg)} degraded period(s)</div>',
            unsafe_allow_html=True,
        )
    if hard.empty and deg.empty and not df.empty:
        st.markdown(
            '<div class="alert-ok">🟢 No significant outages</div>',
            unsafe_allow_html=True,
        )

st.divider()

# ---------------------------------------------------------------------------
# Summary metrics row
# ---------------------------------------------------------------------------

if not df.empty:
    rtt_now   = df.sort_values("time_window").tail(24)["rtt_median_ms"].median()
    rtt_all   = df["rtt_median_ms"].median()
    rtt_delta = rtt_now - rtt_all

    loss_now  = df.sort_values("time_window").tail(24)["loss_p95_pct"].mean() * 100
    loss_all  = df["loss_p95_pct"].mean() * 100

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Median RTT",     f"{rtt_all:.1f} ms",
              delta=f"{rtt_delta:+.1f} ms vs period avg",
              delta_color="inverse")
    m2.metric("P95 Packet loss", f"{loss_all:.2f}%")
    m3.metric("Active ISPs",     str(df["asn"].nunique()))
    m4.metric("Outage events",   str(len(events_df)),
              delta=f"{len(hard)} hard" if not hard.empty else None,
              delta_color="inverse")
    m5.metric("Measurements",    f"{df['total_measurements'].sum():,}")
else:
    st.warning(f"No RIPE data for {selected_country} in this window.")

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "🏢 Providers",
    "📡 IODA signals",
    "🔗 Correlation",
    "🌍 Cross-country",
])

# ===========================================================================
# TAB 1 — Overview
# ===========================================================================

with tab1:
    if df.empty:
        st.info("No data in this time window for the selected country.")
        st.stop()

    # Outage event timeline
    if not events_df.empty:
        st.markdown('<p class="section-label">Detected outage events</p>', unsafe_allow_html=True)

        fig_ev = go.Figure()
        for sev, color in SEVERITY_COLORS.items():
            grp = events_df[events_df["severity"] == sev]
            if grp.empty:
                continue
            fig_ev.add_trace(go.Scatter(
                x=grp["detected_at"],
                y=grp["confidence_score"] * 100,
                mode="markers",
                name=sev.replace("_", " ").title(),
                marker=dict(color=color, size=10, symbol="diamond"),
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "Confidence: %{y:.0f}%<br>"
                    "RIPE loss P95: %{customdata[0]:.1%}<br>"
                    "BGP Δ: %{customdata[1]:.1%}<br>"
                    "ASNs affected: %{customdata[2]}<extra></extra>"
                ),
                customdata=grp[["ripe_loss_p95", "bgp_pct_change", "ripe_asn_affected"]].values,
            ))

        fig_ev.add_hline(y=70, line_dash="dot", line_color="#ef4444",
                         annotation_text="Hard outage", annotation_position="right",
                         annotation_font_color="#ef4444")
        fig_ev.add_hline(y=45, line_dash="dot", line_color="#f59e0b",
                         annotation_text="Degraded", annotation_position="right",
                         annotation_font_color="#f59e0b")

        fig_ev.update_layout(
            **CHART_THEME,
            height=260,
            yaxis_title="Confidence score (%)",
            xaxis_title="",
            showlegend=True,
            legend=dict(orientation="h", y=1.15),
        )
        st.plotly_chart(fig_ev, use_container_width=True)

    # Country-wide RTT heatmap by hour-of-day vs day
    st.markdown('<p class="section-label">RTT pattern — hour of day × calendar day</p>',
                unsafe_allow_html=True)
    st.caption("Reveals recurring daily degradation patterns (e.g. peak-hour congestion every evening).")

    hm = (
        df.copy()
        .assign(
            hour=lambda x: pd.to_datetime(x["time_window"]).dt.hour,
            day=lambda x: pd.to_datetime(x["time_window"]).dt.date.astype(str),
        )
        .groupby(["day", "hour"])["rtt_median_ms"]
        .median()
        .reset_index()
    )

    if not hm.empty:
        fig_hm = px.density_heatmap(
            hm, x="hour", y="day", z="rtt_median_ms",
            color_continuous_scale="RdYlGn_r",
            labels={"hour": "Hour (UTC)", "day": "", "rtt_median_ms": "Median RTT (ms)"},
        )
        fig_hm.update_layout(**CHART_THEME, height=max(220, len(hm["day"].unique()) * 22))
        st.plotly_chart(fig_hm, use_container_width=True)

    # Recent event table
    if not events_df.empty:
        st.markdown('<p class="section-label">Event log</p>', unsafe_allow_html=True)
        display = events_df[[
            "detected_at", "severity", "confidence_score",
            "ripe_loss_p95", "ripe_asn_affected", "bgp_pct_change"
        ]].copy()
        display.columns = ["Time (UTC)", "Severity", "Confidence",
                           "RIPE loss P95", "ASNs affected", "BGP Δ%"]
        display["Confidence"] = (display["Confidence"] * 100).round(0).astype(int).astype(str) + "%"
        display["RIPE loss P95"] = (display["RIPE loss P95"] * 100).round(1).astype(str) + "%"
        display["BGP Δ%"] = display["BGP Δ%"].apply(
            lambda x: f"{x*100:.1f}%" if pd.notna(x) else "—"
        )
        st.dataframe(display, use_container_width=True, hide_index=True)

# ===========================================================================
# TAB 2 — Providers
# ===========================================================================

with tab2:
    if df.empty:
        st.info("No data available.")
    else:
        prov_df = load_provider_consistency(selected_country, s, e)

        if prov_df.empty:
            st.info("Not enough per-provider data in this window.")
        else:
            # Provider selector
            top10 = prov_df.head(10)["provider"].tolist()
            sel_provs = st.multiselect(
                "Providers to show in time series",
                sorted(df["provider"].unique()),
                default=top10[:8],
                key="prov_sel",
            )

            # RTT time series with P10-P90 band
            st.markdown('<p class="section-label">RTT over time with spread band (P10–P90)</p>',
                        unsafe_allow_html=True)
            st.caption("The shaded band shows latency variability — a widening band indicates congestion even when the median is stable.")

            filtered = df[df["provider"].isin(sel_provs)]
            fig_rtt = go.Figure()
            colors = px.colors.qualitative.Plotly
            for i, prov in enumerate(sel_provs):
                pdata = filtered[filtered["provider"] == prov].sort_values("time_window")
                if pdata.empty:
                    continue
                col = colors[i % len(colors)]
                # P10-P90 band (only if new schema columns exist)
                if "rtt_p10_ms" in pdata.columns and pdata["rtt_p10_ms"].notna().any():
                    fig_rtt.add_trace(go.Scatter(
                        x=pd.concat([pdata["time_window"], pdata["time_window"][::-1]]),
                        y=pd.concat([pdata["rtt_p90_ms"], pdata["rtt_p10_ms"][::-1]]),
                        fill="toself", fillcolor=hex_rgba(col, 0.10),
                        line=dict(color="rgba(0,0,0,0)"),
                        showlegend=False, hoverinfo="skip", name=f"{prov} band",
                    ))
                fig_rtt.add_trace(go.Scatter(
                    x=pdata["time_window"], y=pdata["rtt_median_ms"],
                    name=prov, line=dict(color=col, width=1.5),
                    hovertemplate=f"<b>{prov}</b><br>%{{x}}<br>RTT: %{{y:.1f}} ms<extra></extra>",
                ))

            fig_rtt.update_layout(**CHART_THEME, height=420,
                                   yaxis_title="RTT (ms)", xaxis_title="",
                                   hovermode="x unified")
            st.plotly_chart(fig_rtt, use_container_width=True)

            # Packet loss time series
            st.markdown('<p class="section-label">P95 packet loss over time</p>',
                        unsafe_allow_html=True)
            fig_loss = px.line(
                filtered, x="time_window", y="loss_p95_pct", color="provider",
                labels={"time_window": "", "loss_p95_pct": "P95 packet loss", "provider": ""},
            )
            fig_loss.update_layout(**CHART_THEME, height=320, hovermode="x unified")
            st.plotly_chart(fig_loss, use_container_width=True)

            st.divider()

            # Provider ranking — scatter: RTT vs consistency (std dev)
            st.markdown('<p class="section-label">Provider quality map — RTT vs consistency</p>',
                        unsafe_allow_html=True)
            st.caption("Bottom-left corner = best. High RTT stddev means erratic performance even when average is acceptable.")

            fig_scatter = px.scatter(
                prov_df,
                x="avg_rtt", y="rtt_stddev",
                size="measurements", color="avg_loss",
                text="provider",
                color_continuous_scale="RdYlGn_r",
                labels={
                    "avg_rtt": "Avg median RTT (ms)",
                    "rtt_stddev": "RTT std dev (ms) — lower = more consistent",
                    "avg_loss": "Avg P95 loss",
                    "measurements": "Measurements",
                },
                size_max=40,
            )
            fig_scatter.update_traces(textposition="top center", textfont_size=9)
            fig_scatter.update_layout(**CHART_THEME, height=480,
                                       coloraxis_colorbar=dict(title="Avg loss"))
            st.plotly_chart(fig_scatter, use_container_width=True)

            # Raw stats table
            st.markdown('<p class="section-label">Provider stats</p>', unsafe_allow_html=True)
            display_prov = prov_df[[
                "provider", "avg_rtt", "rtt_stddev", "avg_loss", "max_loss", "measurements"
            ]].copy()
            display_prov.columns = [
                "Provider", "Avg RTT (ms)", "RTT std dev", "Avg P95 loss", "Max P95 loss", "Measurements"
            ]
            st.dataframe(
                display_prov.style.format({
                    "Avg RTT (ms)": "{:.1f}",
                    "RTT std dev": "{:.1f}",
                    "Avg P95 loss": "{:.4f}",
                    "Max P95 loss": "{:.4f}",
                }),
                use_container_width=True, hide_index=True,
            )

# ===========================================================================
# TAB 3 — IODA signals
# ===========================================================================

with tab3:
    if ioda_df.empty:
        st.info(f"No IODA signal data for {selected_country} in this window.")
    else:
        st.markdown('<p class="section-label">Signal traces — native resolution</p>',
                    unsafe_allow_html=True)
        st.caption(
            "Each datasource measures internet reachability independently. "
            "BGP: routing table withdrawals. Darknet: background radiation traffic. "
            "Active ping: synthetic probing of /24 blocks. "
            "A drop in all three simultaneously is strong evidence of a real outage."
        )

        # One subplot per datasource so scales don't interfere
        from plotly.subplots import make_subplots
        datasources = ioda_df["datasource"].unique().tolist()
        fig_ioda = make_subplots(
            rows=len(datasources), cols=1,
            shared_xaxes=True,
            subplot_titles=[DS_LABELS.get(d, d) for d in datasources],
            vertical_spacing=0.08,
        )
        for i, ds in enumerate(datasources, 1):
            sub = ioda_df[ioda_df["datasource"] == ds].sort_values("time_window")
            col = DS_COLORS.get(ds, "#94a3b8")
            # Min-max band if available
            if "signal_min" in sub.columns and sub["signal_min"].notna().any():
                fig_ioda.add_trace(go.Scatter(
                    x=pd.concat([sub["time_window"], sub["time_window"][::-1]]),
                    y=pd.concat([sub["signal_max"], sub["signal_min"][::-1]]),
                    fill="toself",
                    fillcolor=DS_COLORS_BAND.get(ds, hex_rgba("#94a3b8", 0.12)),
                    line=dict(color="rgba(0,0,0,0)"),
                    showlegend=False, hoverinfo="skip",
                ), row=i, col=1)
            fig_ioda.add_trace(go.Scatter(
                x=sub["time_window"], y=sub["signal_value"],
                name=DS_LABELS.get(ds, ds),
                line=dict(color=col, width=1.5),
                hovertemplate=f"<b>{DS_LABELS.get(ds, ds)}</b><br>%{{x}}<br>%{{y:,.0f}}<extra></extra>",
            ), row=i, col=1)

            # Mark collection gaps
            gaps = sub[sub["collection_gap"] == True]
            if not gaps.empty:
                fig_ioda.add_trace(go.Scatter(
                    x=gaps["time_window"], y=gaps["signal_value"],
                    mode="markers", marker=dict(color="#ef4444", size=5, symbol="x"),
                    name="Gap", showlegend=(i == 1),
                    hovertemplate="Collection gap<br>%{x}<extra></extra>",
                ), row=i, col=1)

        fig_ioda.update_layout(
            **CHART_THEME,
            height=180 * len(datasources),
            showlegend=True,
        )
        st.plotly_chart(fig_ioda, use_container_width=True)

        # Normalised overlay — all sources on same 0-100 scale for comparison
        st.markdown('<p class="section-label">Normalised signal comparison (0–100%)</p>',
                    unsafe_allow_html=True)
        st.caption("Each signal scaled to its own max so drops are visually comparable regardless of raw magnitude.")

        norm_fig = go.Figure()
        for ds in datasources:
            sub = ioda_df[ioda_df["datasource"] == ds].sort_values("time_window")
            mx = sub["signal_value"].max()
            if mx and mx > 0:
                norm_fig.add_trace(go.Scatter(
                    x=sub["time_window"],
                    y=(sub["signal_value"] / mx * 100),
                    name=DS_LABELS.get(ds, ds),
                    line=dict(color=DS_COLORS.get(ds, "#94a3b8"), width=1.5),
                ))
        norm_fig.update_layout(**CHART_THEME, height=340,
                                yaxis_title="% of peak signal", hovermode="x unified")
        st.plotly_chart(norm_fig, use_container_width=True)

# ===========================================================================
# TAB 4 — Correlation
# ===========================================================================

with tab4:
    st.caption(
        "RTT spikes that align with IODA signal drops represent independently-confirmed "
        "network events — data-plane and control-plane evidence converging on the same outage."
    )

    if df.empty or ioda_df.empty:
        st.info("Need both RIPE and IODA data for correlation view.")
    else:
        ds_choice = st.selectbox(
            "IODA signal to overlay",
            options=ioda_df["datasource"].unique().tolist(),
            format_func=lambda x: DS_LABELS.get(x, x),
        )

        ripe_hourly = (
            df.groupby("time_window", as_index=False)
            .agg(
                avg_rtt=("rtt_median_ms", "mean"),
                avg_loss=("loss_p95_pct", "mean"),
            )
        )
        ioda_sel = ioda_df[ioda_df["datasource"] == ds_choice].sort_values("time_window")

        fig_corr = go.Figure()

        # RIPE RTT
        fig_corr.add_trace(go.Scatter(
            x=ripe_hourly["time_window"], y=ripe_hourly["avg_rtt"],
            name="Avg RTT (ms)", yaxis="y1",
            line=dict(color="#60a5fa", width=1.5),
            hovertemplate="RTT: %{y:.1f} ms<extra></extra>",
        ))

        # IODA signal
        fig_corr.add_trace(go.Scatter(
            x=ioda_sel["time_window"], y=ioda_sel["signal_value"],
            name=DS_LABELS.get(ds_choice, ds_choice), yaxis="y2",
            line=dict(color=DS_COLORS.get(ds_choice, "#8b5cf6"), width=1.5),
            hovertemplate="Signal: %{y:,.0f}<extra></extra>",
        ))

        # Outage event markers on RTT axis
        if not events_df.empty:
            rtt_max = ripe_hourly["avg_rtt"].max() * 1.1
            for sev, color in SEVERITY_COLORS.items():
                grp = events_df[events_df["severity"] == sev]
                if grp.empty:
                    continue
                fig_corr.add_trace(go.Scatter(
                    x=grp["detected_at"],
                    y=[rtt_max] * len(grp),
                    mode="markers",
                    marker=dict(symbol="triangle-down", size=14, color=color),
                    yaxis="y1", name=sev.replace("_", " ").title(),
                    hovertemplate=(
                        f"<b>{sev.replace('_',' ').title()}</b><br>"
                        "%{x}<br>"
                        "Confidence: %{customdata:.0%}<extra></extra>"
                    ),
                    customdata=grp["confidence_score"].values,
                ))

        fig_corr.update_layout(
            **CHART_THEME,
            height=480,
            xaxis_title="",
            yaxis_title="Avg RTT (ms)",
            yaxis2=dict(title=DS_LABELS.get(ds_choice, ds_choice),
                        side="right", overlaying="y", showgrid=False),
            hovermode="x unified",
            legend=dict(orientation="h", y=1.08),
        )
        st.plotly_chart(fig_corr, use_container_width=True)

        # Packet loss + IODA normalised together
        st.markdown('<p class="section-label">Loss vs signal drop — normalised</p>',
                    unsafe_allow_html=True)
        st.caption("Both axes 0–100% of their own range. Simultaneous rises/drops confirm shared cause.")

        loss_max = ripe_hourly["avg_loss"].max()
        sig_max  = ioda_sel["signal_value"].max()

        fig_norm = go.Figure()
        if loss_max and loss_max > 0:
            fig_norm.add_trace(go.Scatter(
                x=ripe_hourly["time_window"],
                y=ripe_hourly["avg_loss"] / loss_max * 100,
                name="Packet loss (normalised)", line=dict(color="#f87171", width=1.5),
            ))
        if sig_max and sig_max > 0:
            fig_norm.add_trace(go.Scatter(
                x=ioda_sel["time_window"],
                y=ioda_sel["signal_value"] / sig_max * 100,
                name=f"{DS_LABELS.get(ds_choice)} (normalised)",
                line=dict(color=DS_COLORS.get(ds_choice, "#8b5cf6"), width=1.5),
            ))
        fig_norm.update_layout(**CHART_THEME, height=300,
                                yaxis_title="% of peak", hovermode="x unified")
        st.plotly_chart(fig_norm, use_container_width=True)


# ===========================================================================
# TAB 5 — Cross-country
# ===========================================================================

with tab5:
    st.caption(
        "Compare network health across countries. Identifies whether issues are local "
        "(single country) or regional (multiple countries degraded simultaneously)."
    )

    summary = load_all_countries_summary(s, e)

    if summary.empty:
        st.info("No cross-country data available.")
    else:
        # Bubble chart: RTT vs loss, bubble = outage hours, color = worst severity
        st.markdown('<p class="section-label">Country health map</p>', unsafe_allow_html=True)
        st.caption("Bubble size = detected outage hours. Color = worst severity in period.")

        sev_order = {"hard_outage": 3, "degraded": 2, "possible": 1, None: 0}
        summary["sev_score"] = summary["worst_severity"].map(
            lambda x: sev_order.get(x, 0)
        )
        summary["bubble_size"] = (summary["outage_hours"] + 1).clip(upper=100)

        fig_bubble = px.scatter(
            summary,
            x="avg_rtt_ms", y="avg_loss",
            size="bubble_size", color="sev_score",
            text="country_code",
            color_continuous_scale=["#22c55e", "#eab308", "#f59e0b", "#ef4444"],
            range_color=[0, 3],
            labels={
                "avg_rtt_ms": "Avg RTT (ms)",
                "avg_loss": "Avg P95 packet loss",
                "sev_score": "Severity",
            },
            size_max=50,
        )
        fig_bubble.update_traces(textposition="top center", textfont_size=10)
        fig_bubble.update_layout(
            **CHART_THEME, height=480,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_bubble, use_container_width=True)

        # RTT ranking bar
        st.markdown('<p class="section-label">RTT ranking across all countries</p>',
                    unsafe_allow_html=True)
        fig_rank = px.bar(
            summary.sort_values("avg_rtt_ms"),
            x="country_code", y="avg_rtt_ms",
            color="avg_rtt_ms", color_continuous_scale="RdYlGn_r",
            labels={"country_code": "", "avg_rtt_ms": "Avg RTT (ms)"},
            text="avg_rtt_ms",
        )
        fig_rank.update_traces(texttemplate="%{text:.0f} ms", textposition="outside")
        fig_rank.update_layout(**CHART_THEME, height=340, coloraxis_showscale=False)
        st.plotly_chart(fig_rank, use_container_width=True)

        # Multi-country time series comparison
        if compare_countries:
            all_compare = [selected_country] + compare_countries
            st.markdown(
                f'<p class="section-label">RTT comparison — {" · ".join(all_compare)}</p>',
                unsafe_allow_html=True,
            )
            fig_multi = go.Figure()
            colors = px.colors.qualitative.Plotly
            for i, ctry in enumerate(all_compare):
                ctry_df = load_baselines(ctry, s, e)
                if ctry_df.empty:
                    continue
                hourly = ctry_df.groupby("time_window")["rtt_median_ms"].median().reset_index()
                fig_multi.add_trace(go.Scatter(
                    x=hourly["time_window"], y=hourly["rtt_median_ms"],
                    name=ctry, line=dict(color=colors[i % len(colors)], width=1.5),
                ))
            fig_multi.update_layout(
                **CHART_THEME, height=380,
                yaxis_title="Median RTT (ms)", hovermode="x unified",
            )
            st.plotly_chart(fig_multi, use_container_width=True)

            # Outage event alignment — did events happen at the same time?
            st.markdown('<p class="section-label">Outage event alignment</p>',
                        unsafe_allow_html=True)
            st.caption("Simultaneous events across countries suggest a shared upstream cause (transit, IXP, or cable).")

            fig_align = go.Figure()
            for i, ctry in enumerate(all_compare):
                ev = load_outage_events(ctry, s, e)
                if ev.empty:
                    continue
                fig_align.add_trace(go.Scatter(
                    x=ev["detected_at"],
                    y=[ctry] * len(ev),
                    mode="markers",
                    marker=dict(
                        color=[SEVERITY_COLORS.get(s, "#94a3b8") for s in ev["severity"]],
                        size=12, symbol="square",
                    ),
                    name=ctry,
                    hovertemplate=(
                        f"<b>{ctry}</b><br>%{{x}}<br>"
                        "Severity: %{customdata}<extra></extra>"
                    ),
                    customdata=ev["severity"].values,
                ))
            fig_align.update_layout(
                **CHART_THEME, height=max(200, len(all_compare) * 60),
                xaxis_title="", yaxis_title="",
            )
            st.plotly_chart(fig_align, use_container_width=True)

        else:
            st.markdown('<p class="section-label">Outage hours by country</p>',
                        unsafe_allow_html=True)
            fig_oh = px.bar(
                summary.sort_values("outage_hours", ascending=False),
                x="country_code", y="outage_hours",
                color="outage_hours", color_continuous_scale="RdYlGn_r",
                labels={"country_code": "", "outage_hours": "Detected outage hours"},
            )
            fig_oh.update_layout(**CHART_THEME, height=320, coloraxis_showscale=False)
            st.plotly_chart(fig_oh, use_container_width=True)

            st.caption("Select countries in the sidebar to compare their RTT and outage event timelines side by side.")

        # Summary table
        st.markdown('<p class="section-label">Country summary table</p>', unsafe_allow_html=True)
        disp = summary[[
            "country_code", "avg_rtt_ms", "max_p90_rtt_ms",
            "avg_loss", "asn_count", "outage_hours", "worst_severity"
        ]].copy()
        disp.columns = [
            "Country", "Avg RTT (ms)", "Max P90 RTT (ms)",
            "Avg P95 loss", "ISPs", "Outage hours", "Worst severity"
        ]
        disp["Worst severity"] = disp["Worst severity"].fillna("none")
        st.dataframe(
            disp.style.format({
                "Avg RTT (ms)": "{:.1f}",
                "Max P90 RTT (ms)": "{:.1f}",
                "Avg P95 loss": "{:.4f}",
            }),
            use_container_width=True, hide_index=True,
        )