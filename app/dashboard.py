import streamlit as st
import duckdb
import pandas as pd

# Page Config
st.set_page_config(page_title="Crypto Dashboard", layout="wide")

# Title
st.title('🚀 Crypto Prices Dashboard')

# Connect to DuckDB
con = duckdb.connect('data/crypto_data.duckdb')

# Fetch latest data
query = """
SELECT *
FROM crypto_data
ORDER BY fetched_at DESC
LIMIT 50
"""
df = con.execute(query).fetchdf()

# Close Connection
con.close()

# Show Last Fetched Time
last_fetched = df['fetched_at'].max()
st.write(f"🕒 **Last Updated:** {last_fetched}")

# Search/Filter
search_term = st.text_input('🔎 Search Coin by Name:', '')

if search_term:
    df = df[df['name'].str.contains(search_term, case=False)]

# Show Table
st.dataframe(df)

# Simple Bar Chart: Top 10 Market Cap
st.subheader('📊 Top 10 Coins by Market Cap')
top10 = df.sort_values(by='market_cap', ascending=False).head(10)
st.bar_chart(data=top10, x='name', y='market_cap')

# Simple Line Chart: Price Change Percentage
st.subheader('📈 Price Change Percentage (24h)')
st.line_chart(data=top10, x='name', y='price_change_percentage_24h')
