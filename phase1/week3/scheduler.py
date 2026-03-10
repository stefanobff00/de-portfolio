import schedule
import time
import logging
import requests
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_crypto_prices():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1
    }

    try:
        logging.info("Fetching crypto prices...")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        df = pd.DataFrame(response.json())
        df["extracted_at"] = datetime.now().isoformat()
        
        # Append to CSV instead of overwrite
        df.to_csv("phase1/week3/data/crypto_prices.csv", 
                    mode="a", 
                    header=not pd.io.common.file_exists("phase1/week3/data/crypto_prices.csv"),
                    index=False)
        
        logging.info(f"Saved {len(df)} rows at {datetime.now()}")
        
    except Exception as e:
        logging.error(f"Error: {e}")

# Run every 1 minute (for testing — change to 60 for hourly)
schedule.every(1).minutes.do(fetch_crypto_prices)

logging.info("Scheduler started. Press Ctrl+C to stop.")
fetch_crypto_prices()  # run immediately on start

while True:
    schedule.run_pending()
    time.sleep(1)