import os
from pathlib import Path
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dotenv import load_dotenv

# Page configuration
st.set_page_config(
    page_title="Real-Time Stock Analytics | Snowflake & dbt",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load environment variables (robust search across project locations)
candidate_env_paths = [
    Path(__file__).resolve().parent.parent / "infra" / ".env",
    Path("D:/Data Engineering/Stock_market_data_pull/real-time-stock-data-pipeline/infra/.env"),
    Path(__file__).resolve().parent.parent.parent / "real-time-stock-data-pipeline" / "infra" / ".env",
    Path.cwd() / "real-time-stock-data-pipeline" / "infra" / ".env",
    Path.cwd() / "infra" / ".env",
    Path.cwd() / ".env"
]
for p in candidate_env_paths:
    if p.exists():
        load_dotenv(dotenv_path=p, override=True)
        break

# Custom CSS for modern styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .stApp {
        background-color: #0d1117;
        color: #c9d1d9;
    }
    
    .metric-card {
        background: linear-gradient(135deg, rgba(22, 27, 34, 0.8), rgba(13, 17, 23, 0.95));
        border: 1px solid rgba(48, 54, 61, 0.7);
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        border-color: #58a6ff;
    }
    
    .ticker-symbol {
        font-size: 1.1rem;
        font-weight: 700;
        letter-spacing: 0.05em;
        color: #58a6ff;
    }
    
    .ticker-price {
        font-size: 1.8rem;
        font-weight: 800;
        color: #f0f6fc;
        margin: 4px 0;
    }
    
    .positive-change {
        color: #3fb950;
        font-size: 0.95rem;
        font-weight: 600;
    }
    
    .negative-change {
        color: #f85149;
        font-size: 0.95rem;
        font-weight: 600;
    }
    
    .badge {
        display: inline-block;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        margin-left: 8px;
    }
    
    .badge-live {
        background-color: rgba(63, 185, 80, 0.15);
        color: #3fb950;
        border: 1px solid rgba(63, 185, 80, 0.4);
    }
</style>
""", unsafe_allow_html=True)

def get_config(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

# Snowflake Connection Function
@st.cache_resource
def get_snowflake_connection():
    account = get_config("SNOWFLAKE_ACCOUNT")
    if not account:
        raise ValueError("SNOWFLAKE_ACCOUNT is missing or None. Please ensure it is set in infra/.env or Streamlit Secrets.")
    return snowflake.connector.connect(
        user=get_config("SNOWFLAKE_USER"),
        password=get_config("SNOWFLAKE_PASSWORD"),
        account=account,
        warehouse=get_config("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=get_config("SNOWFLAKE_DB", "STOCK_MDS"),
        schema=get_config("SNOWFLAKE_SCHEMA", "COMMON")
    )

@st.cache_data(ttl=15)
def load_data():
    conn = get_snowflake_connection()
    try:
        kpi_df = pd.read_sql("SELECT * FROM GOLD_KPI ORDER BY SYMBOL", conn)
        candlestick_df = pd.read_sql("SELECT * FROM GOLD_CANDLESTICK ORDER BY SYMBOL, CANDLE_TIME", conn)
        treechart_df = pd.read_sql("SELECT * FROM GOLD_TREECHART ORDER BY SYMBOL", conn)
        recent_df = pd.read_sql("SELECT * FROM SILVER_CLEAN_STOCK_QUOTES ORDER BY FETCHED_AT DESC LIMIT 100", conn)
        return kpi_df, candlestick_df, treechart_df, recent_df, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), str(e)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/bullish.png", width=64)
    st.title("Settings & Status")
    
    st.subheader("🔗 Connection Details")
    st.caption(f"**Account:** `{os.getenv('SNOWFLAKE_ACCOUNT')}`")
    st.caption(f"**Database:** `{os.getenv('SNOWFLAKE_DB', 'STOCK_MDS')}`")
    st.caption(f"**Schema:** `{os.getenv('SNOWFLAKE_SCHEMA', 'COMMON')}`")
    st.caption(f"**Warehouse:** `{os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}`")
    
    st.divider()
    
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    auto_refresh = st.checkbox("⏱️ Auto-refresh (Live Mode)", value=False)
    refresh_interval = st.slider("Refresh Interval (seconds)", min_value=5, max_value=60, value=15, step=5)
    st.caption("Auto-cache TTL: 15 seconds")
    
    st.divider()
    st.subheader("ℹ️ Pipeline Architecture")
    st.markdown("""
    - **Producer**: Finnhub API ➡️ Kafka
    - **Consumer**: Kafka ➡️ MinIO (Bronze S3)
    - **Orchestrator**: Airflow (MinIO ➡️ Snowflake Raw)
    - **Transformation**: dbt (Bronze ➡️ Silver ➡️ Gold)
    - **Frontend**: Streamlit + Plotly
    """)

# Main Content Header
col_header_left, col_header_right = st.columns([3, 1])
with col_header_left:
    st.title("⚡ Real-Time Stock Market Intelligence")
    st.caption("Live streaming analytics powered by Kafka, MinIO, Airflow, Snowflake & dbt")

with col_header_right:
    st.markdown("<div style='text-align: right; padding-top: 15px;'><span class='badge badge-live'>🟢 Live from Snowflake</span></div>", unsafe_allow_html=True)

st.divider()

# Fetch data
with st.spinner("Fetching latest Gold & Silver models from Snowflake..."):
    kpi_df, candlestick_df, treechart_df, recent_df, error = load_data()

if error:
    st.error(f"❌ Failed to query Snowflake: {error}")
    st.info("Please verify that your credentials in `infra/.env` are correct and that Snowflake is accessible.")
    st.stop()

if kpi_df.empty:
    st.warning("⚠️ No data found in `GOLD_KPI` table yet. Make sure Producer and Airflow have ingested data and `dbt run` has executed.")
    st.stop()

# ----------------- 1. LIVE KPI CARDS -----------------
st.subheader("📊 Live Market Quotes (Gold Layer: `GOLD_KPI`)")

kpi_cols = st.columns(len(kpi_df))
for idx, row in kpi_df.iterrows():
    symbol = row["SYMBOL"]
    price = float(row["CURRENT_PRICE"])
    change_amt = float(row["CHANGE_AMOUNT"])
    change_pct = float(row["CHANGE_PERCENT"])
    
    is_positive = change_amt >= 0
    change_class = "positive-change" if is_positive else "negative-change"
    arrow = "▲" if is_positive else "▼"
    
    with kpi_cols[idx]:
        st.markdown(f"""
        <div class="metric-card">
            <div class="ticker-symbol">{symbol}</div>
            <div class="ticker-price">${price:,.2f}</div>
            <div class="{change_class}">
                {arrow} {change_amt:+.2f} ({change_pct:+.2f}%)
            </div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ----------------- 2. INTERACTIVE CHARTS -----------------
tab1, tab2, tab3 = st.tabs(["🕯️ Candlestick & Trendlines", "🌳 Market Volatility (Treemap)", "📋 Recent Stream Feed (Silver Layer)"])

with tab1:
    col_sym, col_info = st.columns([1, 3])
    with col_sym:
        available_symbols = kpi_df["SYMBOL"].unique().tolist()
        selected_symbol = st.selectbox("Select Ticker for Detailed OHLC:", available_symbols, index=0)
    
    symbol_candles = candlestick_df[candlestick_df["SYMBOL"] == selected_symbol].copy()
    
    if not symbol_candles.empty:
        symbol_candles["CANDLE_TIME"] = pd.to_datetime(symbol_candles["CANDLE_TIME"])
        symbol_candles = symbol_candles.sort_values("CANDLE_TIME")
        
        fig_candle = go.Figure()
        
        # Candlestick
        fig_candle.add_trace(go.Candlestick(
            x=symbol_candles["CANDLE_TIME"],
            open=symbol_candles["CANDLE_OPEN"],
            high=symbol_candles["CANDLE_HIGH"],
            low=symbol_candles["CANDLE_LOW"],
            close=symbol_candles["CANDLE_CLOSE"],
            name=f"{selected_symbol} OHLC",
            increasing_line_color="#3fb950",
            decreasing_line_color="#f85149"
        ))
        
        # Trend Line Overlay
        if "TREND_LINE" in symbol_candles.columns:
            fig_candle.add_trace(go.Scatter(
                x=symbol_candles["CANDLE_TIME"],
                y=symbol_candles["TREND_LINE"],
                mode="lines",
                name="Average Trend Line",
                line=dict(color="#58a6ff", width=2, dash="dot")
            ))
            
        fig_candle.update_layout(
            template="plotly_dark",
            paper_bgcolor="#0d1117",
            plot_bgcolor="#161b22",
            height=500,
            title=f"<b>{selected_symbol}</b> Price Action & Technical Trend",
            xaxis_title="Date / Time",
            yaxis_title="Price ($ USD)",
            xaxis_rangeslider_visible=False,
            margin=dict(l=40, r=40, t=50, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig_candle, use_container_width=True)
    else:
        st.info(f"No candlestick historical intervals recorded for {selected_symbol} yet.")

with tab2:
    st.markdown("#### Volatility & Risk Distribution (`GOLD_TREECHART`)")
    
    if not treechart_df.empty:
        col_tree, col_bar = st.columns([1, 1])
        
        with col_tree:
            # Treemap
            fig_tree = px.treemap(
                treechart_df,
                path=["SYMBOL"],
                values="AVG_PRICE",
                color="RELATIVE_VOLATILITY",
                color_continuous_scale="RdYlGn_r",
                title="Market Treemap (Size: Avg Price, Color: Relative Volatility)"
            )
            fig_tree.update_layout(
                template="plotly_dark",
                paper_bgcolor="#0d1117",
                margin=dict(l=20, r=20, t=40, b=20)
            )
            st.plotly_chart(fig_tree, use_container_width=True)
            
        with col_bar:
            # Volatility comparison bar chart
            fig_bar = px.bar(
                treechart_df.sort_values("VOLATILITY", ascending=False),
                x="SYMBOL",
                y="VOLATILITY",
                color="SYMBOL",
                title="Standard Deviation Volatility by Stock",
                text_auto=".2f"
            )
            fig_bar.update_layout(
                template="plotly_dark",
                paper_bgcolor="#0d1117",
                plot_bgcolor="#161b22",
                margin=dict(l=20, r=20, t=40, b=20),
                showlegend=False
            )
            st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No data in `GOLD_TREECHART` yet.")

with tab3:
    st.markdown("#### Latest Cleaned Ticks Feed (`SILVER_CLEAN_STOCK_QUOTES`)")
    if not recent_df.empty:
        st.dataframe(
            recent_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "CURRENT_PRICE": st.column_config.NumberColumn(format="$%.2f"),
                "DAY_HIGH": st.column_config.NumberColumn(format="$%.2f"),
                "DAY_LOW": st.column_config.NumberColumn(format="$%.2f"),
                "DAY_OPEN": st.column_config.NumberColumn(format="$%.2f"),
                "PREV_CLOSE": st.column_config.NumberColumn(format="$%.2f"),
                "CHANGE_AMOUNT": st.column_config.NumberColumn(format="+$%.2f"),
                "CHANGE_PERCENT": st.column_config.NumberColumn(format="+%.2f%%"),
                "FETCHED_AT": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss")
            }
        )
    else:
        st.info("No records found in Silver layer.")

# Live auto-refresh trigger
if auto_refresh:
    import time
    time.sleep(refresh_interval)
    st.rerun()
