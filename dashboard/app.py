import os
import streamlit as st
import pandas as pd
import plotly.express as px
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
    start_date = st.date_input("From", value=pd.Timestamp("2026-06-17"))
with col2:
    end_date = st.date_input("To", value=pd.Timestamp("2026-06-20"))

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