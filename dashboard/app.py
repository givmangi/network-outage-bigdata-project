import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Network Outage Intelligence",
    page_icon="🌐",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

DB_USER = os.environ.get("TIMESCALEDB_USER", "")
DB_PASS = os.environ.get("TIMESCALEDB_PASSWORD", "")
DB_HOST = os.environ.get("TIMESCALEDB_HOST", "timescaledb")
DB_NAME = os.environ.get("TIMESCALEDB_DB", "outage_intelligence")

@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
    return create_engine(url)


# ---------------------------------------------------------------------------
# Schema detection — gracefully handle old and new Gold schema side by side
# ---------------------------------------------------------------------------

@st.cache_resource
def detect_schema() -> dict:
    """
    Returns a dict of boolean flags indicating which columns/tables exist.
    This lets the dashboard work with both the old schema and the new one
    during a rolling migration, or when Gold has only partially run.
    """
    engine = get_engine()
    flags = {
        "has_rtt_p90":          False,
        "has_loss_median":      False,
        "has_probe_count":      False,
        "has_outage_events":    False,
        "has_country_coverage": False,
        "has_ioda_time_bucket": False,   # new column name vs old 'time_window'
    }
    with engine.connect() as conn:
        # Check asn_baselines columns
        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'asn_baselines'"
        )).fetchall()
        col_names = {r[0] for r in cols}
        flags["has_rtt_p90"]     = "rtt_p90_ms"      in col_names
        flags["has_loss_median"] = "loss_median_pct"  in col_names
        flags["has_probe_count"] = "probe_count"      in col_names

        # Check for new tables
        tables = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )).fetchall()
        table_names = {r[0] for r in tables}
        flags["has_outage_events"]    = "outage_events"    in table_names
        flags["has_country_coverage"] = "country_coverage" in table_names

        # ioda_signals: check whether it uses time_bucket (new) or time_window (old)
        ioda_cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'ioda_signals'"
        )).fetchall()
        ioda_col_names = {r[0] for r in ioda_cols}
        flags["has_ioda_time_bucket"] = "time_bucket" in ioda_col_names

    return flags


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_countries():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT DISTINCT country_code FROM asn_baselines ORDER BY country_code"
        ))
        return [row[0] for row in result]


@st.cache_data(ttl=300)
def load_timeseries(country: str, start: str, end: str) -> pd.DataFrame:
    """
    Load RIPE per-ASN hourly baselines. Uses new schema columns when available,
    falls back to old column names transparently.
    """
    engine  = get_engine()
    schema  = detect_schema()

    # Build the SELECT list based on what actually exists
    rtt_spread = (
        "b.rtt_p10_ms, b.rtt_p90_ms,"
        if schema["has_rtt_p90"] else
        "NULL::double precision AS rtt_p10_ms, NULL::double precision AS rtt_p90_ms,"
    )
    loss_col = (
        "b.loss_p95_pct"
        if schema["has_rtt_p90"] else        # new schema renamed it from loss_95th_pct
        "b.loss_95th_pct AS loss_p95_pct"
    )
    probe_col = (
        "b.probe_count, b.icmp_filtered_count,"
        if schema["has_probe_count"] else
        "NULL::integer AS probe_count, NULL::integer AS icmp_filtered_count,"
    )

    query = text(f"""
        SELECT
            b.time_window,
            b.country_code,
            b.asn,
            COALESCE(n.name, 'AS' || b.asn::text) AS provider,
            b.rtt_median_ms,
            {rtt_spread}
            GREATEST({loss_col}, 0) AS loss_p95_pct,
            {probe_col}
            b.total_measurements
        FROM asn_baselines b
        LEFT JOIN asn_names n ON b.asn = n.asn
        WHERE b.country_code = :country
          AND b.time_window >= :start
          AND b.time_window < :end
          AND b.rtt_median_ms > 0
          AND b.total_measurements >= 5
        ORDER BY b.time_window
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"country": country, "start": start, "end": end})


@st.cache_data(ttl=300)
def load_ioda_signals(country: str, start: str, end: str) -> pd.DataFrame:
    engine = get_engine()
    schema = detect_schema()
    time_col = "time_bucket" if schema["has_ioda_time_bucket"] else "time_window"

    # Use the hourly continuous aggregate if it exists (fast for wide ranges)
    # otherwise fall back to the raw table
    query = text(f"""
        SELECT
            {time_col} AS time_window,
            country_code,
            datasource,
            signal_value,
            collection_gap
        FROM ioda_signals
        WHERE country_code = :country
          AND {time_col} >= :start
          AND {time_col} < :end
        ORDER BY {time_col}
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"country": country, "start": start, "end": end})


@st.cache_data(ttl=60)   # shorter TTL — outage events update frequently
def load_outage_events(country: str, start: str, end: str) -> pd.DataFrame:
    engine = get_engine()
    schema = detect_schema()
    if not schema["has_outage_events"]:
        return pd.DataFrame()

    query = text("""
        SELECT
            detected_at,
            country_code,
            severity,
            confidence_score,
            ripe_loss_p95,
            ripe_rtt_p90_ms,
            ripe_probe_count,
            ripe_asn_affected,
            bgp_pct_change,
            merit_pct_change,
            ping_pct_change
        FROM outage_events
        WHERE country_code = :country
          AND detected_at >= :start
          AND detected_at < :end
        ORDER BY detected_at DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"country": country, "start": start, "end": end})


@st.cache_data(ttl=300)
def load_coverage(country: str) -> pd.DataFrame:
    engine = get_engine()
    schema = detect_schema()
    if not schema["has_country_coverage"]:
        return pd.DataFrame()

    query = text("""
        SELECT coverage_date, source, measurement_count, probe_count, asn_count
        FROM country_coverage
        WHERE country_code = :country
        ORDER BY coverage_date DESC
        LIMIT 30
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"country": country})


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("🌐 Net outage intel")
st.sidebar.caption("RIPE Atlas + IODA")

try:
    countries = load_countries()
except Exception as e:
    st.error(f"Could not connect to TimescaleDB: {e}")
    st.stop()

if not countries:
    st.warning("No data in TimescaleDB yet. Run the Gold batch job first.")
    st.stop()

selected_country = st.sidebar.selectbox(
    "Country",
    countries,
    index=countries.index("IT") if "IT" in countries else 0,
)

st.sidebar.divider()

col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input("From", value=pd.Timestamp.now().normalize() - pd.Timedelta(days=30))
with col2:
    end_date = st.date_input("To", value=pd.Timestamp.now().normalize() + pd.Timedelta(days=1))

# Coverage warning in sidebar
cov_df = load_coverage(selected_country)
if not cov_df.empty:
    ripe_cov = cov_df[cov_df["source"] == "ripe"]
    if not ripe_cov.empty:
        latest_probes = ripe_cov.iloc[0]["probe_count"]
        if latest_probes is not None and latest_probes < 5:
            st.sidebar.warning(f"⚠️ Only {int(latest_probes)} RIPE probe(s) active for {selected_country} — results may be sparse.")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

df = load_timeseries(selected_country, str(start_date), str(end_date))
schema = detect_schema()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🌐 Network Outage Intelligence")
st.caption("RIPE Atlas + IODA · 15 target countries")
st.divider()

if df.empty:
    st.warning(f"No data found for {selected_country} in the selected date range. "
               f"Check that the Gold job has run and that probes are active for this country.")
    st.stop()

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)
col1.metric("Median RTT", f"{df['rtt_median_ms'].median():.1f} ms")
col2.metric("Avg P95 packet loss", f"{df['loss_p95_pct'].mean()*100:.2f}%")
col3.metric("Active providers", str(df["asn"].nunique()))
col4.metric("Total measurements", f"{df['total_measurements'].sum():,}")

# Outage count badge (new schema only)
if schema["has_outage_events"]:
    events_df = load_outage_events(selected_country, str(start_date), str(end_date))
    hard_count = len(events_df[events_df["severity"] == "hard_outage"]) if not events_df.empty else 0
    deg_count  = len(events_df[events_df["severity"] == "degraded"])    if not events_df.empty else 0
    if hard_count > 0:
        st.error(f"🚨 **{hard_count} hard outage(s)** detected in this period for {selected_country}.")
    elif deg_count > 0:
        st.warning(f"⚠️ **{deg_count} degraded connectivity** event(s) detected for {selected_country}.")

st.divider()

# ---------------------------------------------------------------------------
# Tabs — add Outage Events tab when new schema is available
# ---------------------------------------------------------------------------

tab_labels = ["📈 Time series", "🏆 ISP ranking", "📡 IODA signals", "🔗 Combined view"]
if schema["has_outage_events"]:
    tab_labels.append("🚨 Outage events")

tabs = st.tabs(tab_labels)
tab1, tab2, tab3, tab4 = tabs[0], tabs[1], tabs[2], tabs[3]
tab5 = tabs[4] if schema["has_outage_events"] else None


# ---------------------------------------------------------------------------
# Tab 1 — Time series
# ---------------------------------------------------------------------------

with tab1:
    st.subheader(f"RTT over time — {selected_country}")

    all_providers = sorted(df["provider"].unique())
    top_providers = (
        df.groupby("provider")["total_measurements"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )
    selected_providers = st.multiselect(
        "Filter providers", all_providers, default=top_providers, key="tab1_providers"
    )
    filtered = df[df["provider"].isin(selected_providers)]

    if filtered.empty:
        st.warning("No data for selected providers.")
    else:
        # RTT median line
        fig_rtt = px.line(
            filtered, x="time_window", y="rtt_median_ms", color="provider",
            labels={"time_window": "Time (UTC)", "rtt_median_ms": "Median RTT (ms)", "provider": "Provider"},
            title=f"Median RTT per provider — {selected_country}",
        )
        # Overlay P90 as a shaded area when available (new schema)
        if schema["has_rtt_p90"] and "rtt_p90_ms" in filtered.columns:
            for provider in filtered["provider"].unique():
                p_data = filtered[filtered["provider"] == provider].sort_values("time_window")
                fig_rtt.add_trace(go.Scatter(
                    x=pd.concat([p_data["time_window"], p_data["time_window"][::-1]]),
                    y=pd.concat([p_data["rtt_p90_ms"], p_data["rtt_p10_ms"][::-1]]),
                    fill="toself", opacity=0.10, showlegend=False,
                    line=dict(color="rgba(0,0,0,0)"), name=f"{provider} P10–P90 band",
                ))
        fig_rtt.update_layout(height=420, hovermode="x unified")
        st.plotly_chart(fig_rtt, use_container_width=True)

        st.divider()

        fig_loss = px.line(
            filtered, x="time_window", y="loss_p95_pct", color="provider",
            labels={"time_window": "Time (UTC)", "loss_p95_pct": "P95 packet loss", "provider": "Provider"},
            title=f"P95 packet loss per provider — {selected_country}",
        )
        fig_loss.update_layout(height=400, hovermode="x unified")
        st.plotly_chart(fig_loss, use_container_width=True)

        # ICMP filter info (new schema only)
        if schema["has_probe_count"] and "icmp_filtered_count" in filtered.columns:
            total_filtered = filtered["icmp_filtered_count"].sum()
            if total_filtered and total_filtered > 0:
                st.caption(
                    f"ℹ️ {int(total_filtered):,} probe measurements were flagged as ICMP-filtered "
                    f"(100% loss due to router rate-limiting, not a real outage). "
                    f"These are excluded from the loss calculation."
                )


# ---------------------------------------------------------------------------
# Tab 2 — ISP ranking
# ---------------------------------------------------------------------------

with tab2:
    st.subheader(f"ISP ranking — {selected_country}")

    ranking = (
        df.groupby(["asn", "provider"])
        .agg(
            avg_rtt=("rtt_median_ms", "mean"),
            avg_loss=("loss_p95_pct", "mean"),
            total_measurements=("total_measurements", "sum"),
        )
        .reset_index()
        .sort_values("avg_rtt")
    )
    if schema["has_probe_count"] and "probe_count" in df.columns:
        probe_agg = df.groupby("provider")["probe_count"].max().reset_index().rename(columns={"probe_count": "max_probes"})
        ranking = ranking.merge(probe_agg, on="provider", how="left")

    if ranking.empty:
        st.warning("No provider data available.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            fig_rtt_rank = px.bar(
                ranking, x="avg_rtt", y="provider", orientation="h",
                labels={"avg_rtt": "Avg median RTT (ms)", "provider": "Provider"},
                title=f"RTT ranking — {selected_country} (lower is better)",
                color="avg_rtt", color_continuous_scale="RdYlGn_r",
            )
            fig_rtt_rank.update_layout(height=500, showlegend=False, coloraxis_showscale=False)
            fig_rtt_rank.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_rtt_rank, use_container_width=True)

        with c2:
            fig_loss_rank = px.bar(
                ranking.sort_values("avg_loss", ascending=False),
                x="avg_loss", y="provider", orientation="h",
                labels={"avg_loss": "Avg P95 packet loss", "provider": "Provider"},
                title=f"Packet loss ranking — {selected_country} (lower is better)",
                color="avg_loss", color_continuous_scale="RdYlGn_r",
            )
            fig_loss_rank.update_layout(height=500, showlegend=False, coloraxis_showscale=False)
            fig_loss_rank.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_loss_rank, use_container_width=True)

        st.divider()
        st.subheader("Raw provider stats")
        display_cols = ["provider", "avg_rtt", "avg_loss", "total_measurements"]
        rename_map = {"provider": "Provider", "avg_rtt": "Avg RTT (ms)", "avg_loss": "Avg P95 loss", "total_measurements": "Measurements"}
        if "max_probes" in ranking.columns:
            display_cols.append("max_probes")
            rename_map["max_probes"] = "Peak probes"
        fmt = {"Avg RTT (ms)": "{:.1f}", "Avg P95 loss": "{:.4f}"}
        if "Peak probes" in rename_map.values():
            fmt["Peak probes"] = "{:.0f}"
        st.dataframe(
            ranking[display_cols].rename(columns=rename_map).style.format(fmt),
            use_container_width=True, hide_index=True,
        )


# ---------------------------------------------------------------------------
# Tab 3 — IODA signals
# ---------------------------------------------------------------------------

with tab3:
    st.subheader(f"IODA signals — {selected_country}")
    ioda_df = load_ioda_signals(selected_country, str(start_date), str(end_date))

    if ioda_df.empty:
        st.warning(f"No IODA signal data for {selected_country} in this range.")
    else:
        datasource_labels = {
            "bgp":          "BGP routing visibility",
            "merit-nt":     "Merit darknet traffic",
            "ping-slash24": "Active ping (/24 blocks)",
        }
        ioda_df["datasource_label"] = ioda_df["datasource"].map(lambda x: datasource_labels.get(x, x))

        fig_ioda = px.line(
            ioda_df, x="time_window", y="signal_value", color="datasource_label",
            labels={"time_window": "Time (UTC)", "signal_value": "Signal score", "datasource_label": "Data source"},
            title=f"IODA signal scores — {selected_country}",
        )
        fig_ioda.update_layout(height=450, hovermode="x unified")
        st.plotly_chart(fig_ioda, use_container_width=True)

        st.caption(
            "IODA signals are country-level scores from three independent sources: "
            "BGP routing announcements, darknet traffic visibility, and active ping sweeps. "
            "A sustained drop across multiple sources can indicate a real outage."
        )
        gap_count = ioda_df["collection_gap"].sum()
        if gap_count > 0:
            st.info(f"{int(gap_count)} data points were flagged as collection gaps.")


# ---------------------------------------------------------------------------
# Tab 4 — Combined view
# ---------------------------------------------------------------------------

with tab4:
    st.subheader(f"Combined view — {selected_country}")
    st.caption(
        "RIPE Atlas RTT (left axis) overlaid with IODA signal score (right axis). "
        "RTT spikes that align with IODA signal drops are independently-confirmed network events."
    )

    ripe_hourly = df.groupby("time_window", as_index=False).agg(avg_rtt=("rtt_median_ms", "mean"))
    ioda_combined = load_ioda_signals(selected_country, str(start_date), str(end_date))

    if ripe_hourly.empty and ioda_combined.empty:
        st.warning("No data available.")
    else:
        ds_options = ioda_combined["datasource"].unique().tolist() if not ioda_combined.empty else []
        datasource_choice = st.selectbox("IODA data source to overlay", options=ds_options, key="tab4_datasource")
        ioda_filtered = ioda_combined[ioda_combined["datasource"] == datasource_choice] if not ioda_combined.empty else pd.DataFrame()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=ripe_hourly["time_window"], y=ripe_hourly["avg_rtt"],
            name="Avg RTT (ms)", yaxis="y1", line=dict(color="#378ADD"),
        ))
        if not ioda_filtered.empty:
            fig.add_trace(go.Scatter(
                x=ioda_filtered["time_window"], y=ioda_filtered["signal_value"],
                name=f"IODA {datasource_choice}", yaxis="y2", line=dict(color="#D85A30"),
            ))

        # Overlay outage event markers if available
        if schema["has_outage_events"]:
            ev = load_outage_events(selected_country, str(start_date), str(end_date))
            if not ev.empty:
                sev_colors = {"hard_outage": "red", "degraded": "orange", "possible": "gold"}
                for sev, grp in ev.groupby("severity"):
                    fig.add_trace(go.Scatter(
                        x=grp["detected_at"],
                        y=[ripe_hourly["avg_rtt"].max() * 1.05] * len(grp),
                        mode="markers",
                        marker=dict(symbol="triangle-down", size=12, color=sev_colors.get(sev, "gray")),
                        name=sev.replace("_", " ").title(),
                        yaxis="y1",
                        hovertext=grp.apply(
                            lambda r: f"{sev} — confidence {r['confidence_score']:.0%}<br>"
                                      f"BGP: {(r['bgp_pct_change'] or 0)*100:.1f}%  "
                                      f"Loss P95: {(r['ripe_loss_p95'] or 0)*100:.1f}%",
                            axis=1,
                        ),
                        hoverinfo="text+x",
                    ))

        fig.update_layout(
            height=520,
            title=f"RTT vs IODA {datasource_choice} — {selected_country}",
            xaxis=dict(title="Time (UTC)"),
            yaxis=dict(title="Avg RTT (ms)", side="left"),
            yaxis2=dict(title=f"IODA {datasource_choice} score", side="right", overlaying="y"),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 5 — Outage Events (new schema only)
# ---------------------------------------------------------------------------

if tab5 is not None:
    with tab5:
        st.subheader(f"Detected outage events — {selected_country}")
        st.caption(
            "Each row is one hour where both RIPE data-plane and IODA control-plane evidence "
            "combined above the detection threshold. Confidence = weighted sum of BGP (35%), "
            "RIPE loss (35%), darknet (20%), active-ping (10%)."
        )

        events_df = load_outage_events(selected_country, str(start_date), str(end_date))

        if events_df.empty:
            st.success(f"No outage events detected for {selected_country} in this period.")
        else:
            # Summary badges
            sev_counts = events_df["severity"].value_counts()
            ecol1, ecol2, ecol3 = st.columns(3)
            ecol1.metric("Hard outages",        sev_counts.get("hard_outage", 0))
            ecol2.metric("Degraded events",     sev_counts.get("degraded", 0))
            ecol3.metric("Possible incidents",  sev_counts.get("possible", 0))

            st.divider()

            # Confidence score over time
            fig_conf = px.scatter(
                events_df,
                x="detected_at", y="confidence_score",
                color="severity",
                size="confidence_score",
                color_discrete_map={
                    "hard_outage": "#D62728",
                    "degraded":    "#FF7F0E",
                    "possible":    "#BCBD22",
                },
                labels={"detected_at": "Time (UTC)", "confidence_score": "Confidence", "severity": "Severity"},
                title=f"Outage confidence over time — {selected_country}",
                hover_data=["ripe_loss_p95", "bgp_pct_change", "ripe_asn_affected"],
            )
            fig_conf.add_hline(y=0.70, line_dash="dash", line_color="red",   annotation_text="Hard outage threshold")
            fig_conf.add_hline(y=0.45, line_dash="dash", line_color="orange", annotation_text="Degraded threshold")
            fig_conf.update_layout(height=400)
            st.plotly_chart(fig_conf, use_container_width=True)

            st.divider()

            # Evidence breakdown bar chart
            evidence_df = events_df[["detected_at", "bgp_pct_change", "merit_pct_change", "ping_pct_change", "ripe_loss_p95"]].copy()
            evidence_df["bgp_drop_%"]    = (evidence_df["bgp_pct_change"]   * -100).clip(lower=0)
            evidence_df["merit_drop_%"]  = (evidence_df["merit_pct_change"] * -100).clip(lower=0)
            evidence_df["ping_drop_%"]   = (evidence_df["ping_pct_change"]  * -100).clip(lower=0)
            evidence_df["ripe_loss_%"]   = (evidence_df["ripe_loss_p95"]    *  100).clip(lower=0)
            ev_melted = evidence_df.melt(
                id_vars="detected_at",
                value_vars=["bgp_drop_%", "merit_drop_%", "ping_drop_%", "ripe_loss_%"],
                var_name="signal", value_name="magnitude",
            )
            fig_ev = px.bar(
                ev_melted, x="detected_at", y="magnitude", color="signal", barmode="group",
                labels={"detected_at": "Time (UTC)", "magnitude": "Signal change (%)", "signal": "Evidence"},
                title=f"Evidence breakdown per event — {selected_country}",
            )
            fig_ev.update_layout(height=380)
            st.plotly_chart(fig_ev, use_container_width=True)

            st.divider()
            st.subheader("Raw event table")
            display_ev = events_df[[
                "detected_at", "severity", "confidence_score",
                "ripe_loss_p95", "ripe_asn_affected",
                "bgp_pct_change", "merit_pct_change", "ping_pct_change",
            ]].copy()
            display_ev.columns = [
                "Time (UTC)", "Severity", "Confidence",
                "RIPE loss P95", "ASNs affected",
                "BGP Δ%", "Darknet Δ%", "Ping Δ%",
            ]
            for col in ["RIPE loss P95", "BGP Δ%", "Darknet Δ%", "Ping Δ%"]:
                display_ev[col] = (display_ev[col] * 100).round(1).astype(str) + "%"
            display_ev["Confidence"] = (display_ev["Confidence"] * 100).round(0).astype(int).astype(str) + "%"
            st.dataframe(display_ev, use_container_width=True, hide_index=True)