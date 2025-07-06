import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

st.set_page_config(page_title="📈 Real-Time Crypto Dashboard", layout="wide")
st.title("🚀 Real-Time Crypto Price Tracker")

@st.cache_data(ttl=10)
def load_data(limit=200):
    try:
        conn = psycopg2.connect(
            host="db",  # لو تشغل محلي خارج الدوكر، استبدل بـ localhost
            dbname="crypto",
            user="postgres",
            password="postgres",
            port=5432
        )
        # استخدم RealDictCursor لجلب البيانات كقوائم مع القيم
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f"""
            SELECT * FROM prices
            ORDER BY timestamp DESC
            LIMIT %s
        """, (limit,))
        data = cur.fetchall()
        conn.close()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"❌ Could not connect to the database or load data: {e}")
        return pd.DataFrame()

# --- تحميل البيانات مع خاصية التخزين المؤقت لتحسين الأداء ---
max_records = st.sidebar.slider("Max Records", min_value=10, max_value=500, value=200, step=10)
df = load_data(limit=max_records)

if df.empty:
    st.warning("⚠️ No crypto price data available. Waiting for data stream...")
else:
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    coins = df['symbol'].unique().tolist()
    selected_coin = st.sidebar.selectbox("Select Coin", coins)

    filtered = df[df['symbol'] == selected_coin]

    if filtered.empty:
        st.warning(f"⚠️ No data available yet for the selected coin: {selected_coin}")
    else:
        latest = filtered.iloc[0]
        st.metric(f"💰 {selected_coin.upper()} Price", f"${latest['price']:.2f}")
        st.metric(f"📊 Volume", f"{latest['volume']:.2f}")

        st.subheader(f"{selected_coin.upper()} Price Over Time")
        st.line_chart(filtered.set_index('timestamp')['price'])

        st.subheader("Volume Traded")
        st.area_chart(filtered.set_index('timestamp')['volume'])

        with st.expander("🧾 Show Raw Data"):
            st.dataframe(filtered)
