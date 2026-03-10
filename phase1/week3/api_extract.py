import requests
import pandas as pd
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1
}
try:

    logging.info("Calling CoinGecko API...")
    response = requests.get(url, params=params)
    response.raise_for_status()  # raises exception if status != 200

    df = pd.DataFrame(response.json())
    df["extracted_at"] = datetime.now().isoformat()

    logging.info(f"Fetched {len(df)} coins successfully")
    print(df[["name", "symbol", "current_price", "market_cap", "extracted_at"]])
    
    df.to_csv("phase1/week3/data/crypto_prices.csv", index=False)
    logging.info("Saved to CSV")

except requests.exceptions.Timeout:
    logging.error("Request timed out")
except requests.exceptions.HTTPError as e:
    logging.error(f"HTTP error: {e}")
except Exception as e:
    logging.error(f"Unexpected error: {e}")