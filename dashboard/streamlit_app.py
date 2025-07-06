import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime

# --- Streamlit UI ---
st.set_page_config(page_title="📈 Real-Time Crypto Dashboard", layout="wide")
st.title("🚀 Real-Time Crypto Price Tracker")

# --- Connect to PostgreSQL ---
try:
    conn = psycopg2.connect(
        host="db",           # use 'localhost' if running outside Docker
        dbname="crypto",
        user="postgres",
        password="postgres",
        port=5432
    )

    # --- Load Data ---
    df = pd.read_sql("""
        SELECT * FROM prices 
        ORDER BY timestamp DESC 
        LIMIT 200
    """, conn)

    conn.close()

    # --- Preprocessing ---
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        coins = df['symbol'].unique().tolist()

        # --- Sidebar Filters ---
        st.sidebar.header("🔍 Filters")
        selected_coin = st.sidebar.selectbox("Select Coin", coins)
        time_range = st.sidebar.slider("Max Records", min_value=10, max_value=200, value=100, step=10)

        # --- Filter Data ---
        filtered = df[df['symbol'] == selected_coin].head(time_range)

        if not filtered.empty:
            # --- Metrics ---
            latest = filtered.iloc[0]
            st.metric(f"💰 {selected_coin.upper()} Price", f"${latest['price']:.2f}")
            st.metric(f"📊 Volume", f"{latest['volume']:.2f}")

            # --- Line Chart ---
            st.subheader(f"{selected_coin.upper()} Price Over Time")
            st.line_chart(filtered.set_index('timestamp')['price'])

            # --- Volume Chart ---
            st.subheader("Volume Traded")
            st.area_chart(filtered.set_index('timestamp')['volume'])

            # --- Raw Data ---
            with st.expander("🧾 Show Raw Data"):
                st.dataframe(filtered)
        else:
            st.warning("⚠️ No data available yet for the selected coin.")
    else:
        st.warning("⚠️ No crypto price data available. Waiting for data stream...")

except Exception as e:
    st.error(f"❌ Could not connect to the database or load data: {e}")
