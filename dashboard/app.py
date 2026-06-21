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
    engine = get_engine()
    query = text("""
        SELECT
            b.time_window,
            b.country_code,
            b.asn,
            COALESCE(n.name, 'AS' || b.asn::text) AS provider,
            b.rtt_median_ms,
            GREATEST(b.loss_95th_pct, 0) AS loss_95th_pct,
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
        df = pd.read_sql(query, conn, params={
            "country": country,
            "start": start,
            "end": end,
        })
    return df

@st.cache_data(ttl=300)
def load_ioda_signals(country: str, start: str, end: str) -> pd.DataFrame:
    engine = get_engine()
    query = text("""
        SELECT
            time_window,
            country_code,
            datasource,
            signal_value,
            collection_gap
        FROM ioda_signals
        WHERE country_code = :country
          AND time_window >= :start
          AND time_window < :end
        ORDER BY time_window
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            "country": country,
            "start": start,
            "end": end,
        })
    return df

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

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

df = load_timeseries(
    selected_country,
    str(start_date),
    str(end_date),
)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🌐 Network Outage Intelligence")
st.caption("RIPE Atlas + IODA · 15 target countries")
st.divider()

if df.empty:
    st.warning(f"No data found for {selected_country} in this date range.")
    st.stop()

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)
col1.metric("Median RTT", f"{df['rtt_median_ms'].median():.1f} ms")
col2.metric("Avg packet loss", f"{df['loss_95th_pct'].mean()*100:.2f}%")
col3.metric("Active providers", str(df["asn"].nunique()))
col4.metric("Total measurements", f"{df['total_measurements'].sum():,}")

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Time series",
    "🏆 ISP ranking",
    "📡 IODA signals",
    "🔗 Combined view",
])

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
        "Filter providers",
        all_providers,
        default=top_providers,
        key="tab1_providers",
    )

    filtered = df[df["provider"].isin(selected_providers)]

    if filtered.empty:
        st.warning("No data for selected providers.")
    else:
        fig_rtt = px.line(
            filtered,
            x="time_window",
            y="rtt_median_ms",
            color="provider",
            labels={
                "time_window": "Time (UTC)",
                "rtt_median_ms": "Median RTT (ms)",
                "provider": "Provider",
            },
            title=f"Median RTT per provider — {selected_country}",
        )
        fig_rtt.update_layout(height=400, hovermode="x unified")
        st.plotly_chart(fig_rtt, use_container_width=True)

        st.divider()

        fig_loss = px.line(
            filtered,
            x="time_window",
            y="loss_95th_pct",
            color="provider",
            labels={
                "time_window": "Time (UTC)",
                "loss_95th_pct": "P95 packet loss",
                "provider": "Provider",
            },
            title=f"P95 packet loss per provider — {selected_country}",
        )
        fig_loss.update_layout(height=400, hovermode="x unified")
        st.plotly_chart(fig_loss, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 2 — ISP ranking
# ---------------------------------------------------------------------------

with tab2:
    st.subheader(f"ISP ranking — {selected_country}")

    # Aggregate across the full date range — one row per provider
    ranking = (
        df.groupby(["asn", "provider"])
        .agg(
            avg_rtt=("rtt_median_ms", "mean"),
            avg_loss=("loss_95th_pct", "mean"),
            total_measurements=("total_measurements", "sum"),
        )
        .reset_index()
        .sort_values("avg_rtt")
    )

    if ranking.empty:
        st.warning("No provider data available for this country and date range.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            fig_rtt_rank = px.bar(
                ranking,
                x="avg_rtt",
                y="provider",
                orientation="h",
                labels={
                    "avg_rtt": "Avg median RTT (ms)",
                    "provider": "Provider",
                },
                title=f"RTT ranking — {selected_country} (lower is better)",
                color="avg_rtt",
                color_continuous_scale="RdYlGn_r",
            )
            fig_rtt_rank.update_layout(height=500, showlegend=False, coloraxis_showscale=False)
            fig_rtt_rank.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_rtt_rank, use_container_width=True)

        with col2:
            fig_loss_rank = px.bar(
                ranking.sort_values("avg_loss", ascending=False),
                x="avg_loss",
                y="provider",
                orientation="h",
                labels={
                    "avg_loss": "Avg P95 packet loss",
                    "provider": "Provider",
                },
                title=f"Packet loss ranking — {selected_country} (lower is better)",
                color="avg_loss",
                color_continuous_scale="RdYlGn_r",
            )
            fig_loss_rank.update_layout(height=500, showlegend=False, coloraxis_showscale=False)
            fig_loss_rank.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_loss_rank, use_container_width=True)

        st.divider()
        st.subheader("Raw provider stats")
        st.dataframe(
            ranking[["provider", "avg_rtt", "avg_loss", "total_measurements"]]
            .rename(columns={
                "provider": "Provider",
                "avg_rtt": "Avg RTT (ms)",
                "avg_loss": "Avg P95 loss",
                "total_measurements": "Measurements",
            })
            .style.format({
                "Avg RTT (ms)": "{:.1f}",
                "Avg P95 loss": "{:.4f}",
            }),
            use_container_width=True,
            hide_index=True,
        )

# ---------------------------------------------------------------------------
# Tab 3 — IODA signals
# ---------------------------------------------------------------------------

with tab3:
    st.subheader(f"IODA signals — {selected_country}")

    ioda_df = load_ioda_signals(
        selected_country,
        str(start_date),
        str(end_date),
    )

    if ioda_df.empty:
        st.warning(f"No IODA signal data found for {selected_country} in this date range.")
    else:
        datasource_labels = {
            "bgp": "BGP routing visibility",
            "merit-nt": "Merit darknet traffic",
            "ping-slash24": "Active ping (/24 blocks)",
        }
        ioda_df["datasource_label"] = ioda_df["datasource"].map(
            lambda x: datasource_labels.get(x, x)
        )

        fig_ioda = px.line(
            ioda_df,
            x="time_window",
            y="signal_value",
            color="datasource_label",
            labels={
                "time_window": "Time (UTC)",
                "signal_value": "Signal score",
                "datasource_label": "Data source",
            },
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
            st.info(f"{gap_count} data points were flagged as collection gaps (interpolated or missing).")

# ---------------------------------------------------------------------------
# Tab 4 — Combined View
# ---------------------------------------------------------------------------

with tab4:
    st.subheader(f"Combined view — {selected_country}")
    st.caption(
        "RIPE Atlas RTT (left axis) overlaid with IODA signal score (right axis). "
        "Look for RTT spikes that align with IODA signal drops — that's a sign of a real, "
        "independently-confirmed network event."
    )

    # Reuse the same RIPE data, averaged across all providers per hour
    ripe_hourly = (
        df.groupby("time_window", as_index=False)
        .agg(avg_rtt=("rtt_median_ms", "mean"))
    )

    ioda_combined = load_ioda_signals(
        selected_country,
        str(start_date),
        str(end_date),
    )

    if ripe_hourly.empty and ioda_combined.empty:
        st.warning("No data available for this country and date range.")
    else:
        datasource_choice = st.selectbox(
            "IODA data source to overlay",
            options=ioda_combined["datasource"].unique().tolist() if not ioda_combined.empty else [],
            key="tab4_datasource",
        )

        ioda_filtered = ioda_combined[ioda_combined["datasource"] == datasource_choice] if not ioda_combined.empty else pd.DataFrame()

        fig_combined = go.Figure()

        fig_combined.add_trace(go.Scatter(
            x=ripe_hourly["time_window"],
            y=ripe_hourly["avg_rtt"],
            name="Avg RTT (ms)",
            yaxis="y1",
            line=dict(color="#378ADD"),
        ))

        if not ioda_filtered.empty:
            fig_combined.add_trace(go.Scatter(
                x=ioda_filtered["time_window"],
                y=ioda_filtered["signal_value"],
                name=f"IODA {datasource_choice}",
                yaxis="y2",
                line=dict(color="#D85A30"),
            ))

        fig_combined.update_layout(
            height=500,
            title=f"RTT vs IODA {datasource_choice} — {selected_country}",
            xaxis=dict(title="Time (UTC)"),
            yaxis=dict(title="Avg RTT (ms)", side="left"),
            yaxis2=dict(title=f"IODA {datasource_choice} score", side="right", overlaying="y"),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )

        st.plotly_chart(fig_combined, use_container_width=True)