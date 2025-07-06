import streamlit as st
import psycopg2
import pandas as pd

st.set_page_config(page_title="Crypto Dashboard", layout="centered")
st.title("📊 Live Crypto Prices")

try:
    conn = psycopg2.connect(
        dbname="crypto",
        user="postgres",
        password="postgres",
        host="db",
        port="5432"
    )
    query = "SELECT symbol, price, timestamp FROM prices ORDER BY timestamp DESC LIMIT 3"
    df = pd.read_sql(query, conn)

    for _, row in df.iterrows():
        st.metric(label=row['symbol'], value=f"${row['price']}", delta=None)

except Exception as e:
    st.error("⚠️ Could not connect to the database")
    st.text(str(e))
