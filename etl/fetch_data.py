import requests
import pandas as pd
import duckdb
import os
from datetime import datetime

# Step 1: API Fetch
def fetch_crypto_data():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 50,      # Top 50 coins
        'page': 1,
        'sparkline': False
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data

# Step 2: Transform to DataFrame
def transform_data(data):
    df = pd.DataFrame(data)
    df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'price_change_percentage_24h']]
    df['fetched_at'] = datetime.utcnow()  # When was this data fetched
    return df

# Step 3: Save to DuckDB
def save_to_duckdb(df):
    # Make sure 'data/' folder exists
    if not os.path.exists('data'):
        os.makedirs('data')

    con = duckdb.connect(database='data/crypto_data.duckdb', read_only=False)
    
    # Create Table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS crypto_data (
            id VARCHAR,
            symbol VARCHAR,
            name VARCHAR,
            current_price DOUBLE,
            market_cap DOUBLE,
            total_volume DOUBLE,
            price_change_percentage_24h DOUBLE,
            fetched_at TIMESTAMP
        )
    """)
    
    # Insert Data
    con.execute("INSERT INTO crypto_data SELECT * FROM df")
    
    con.close()

if __name__ == "__main__":
    print("🚀 Fetching Crypto Data...")
    raw_data = fetch_crypto_data()
    print("✅ Data Fetched!")

    print("🚀 Transforming Data...")
    df = transform_data(raw_data)
    print("✅ Data Transformed!")

    print("🚀 Saving to DuckDB...")
    save_to_duckdb(df)
    print("✅ Data Saved to DuckDB at 'data/crypto_data.duckdb' 🚀")
