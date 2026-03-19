import requests
import pandas as pd
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DBT_PROJECT = Path(__file__).parent / "happiness_project"

def extract():
    logging.info("Extracting data from API...")
    url = "https://restcountries.com/v3.1/all?fields=name,region,population,area"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    rows = []
    for country in data:
        rows.append({
            "country": country["name"]["common"],
            "region": country.get("region", ""),
            "population": country.get("population", 0),
            "area": country.get("area", 0)
        })
    
    df = pd.DataFrame(rows)
    logging.info(f"Extracted {len(df)} countries")
    return df

def load(df):
    output = DBT_PROJECT / "seeds" / "countries_api.csv"
    df.to_csv(output, index=False)
    logging.info(f"Saved to {output}")

def run_dbt(command):
    logging.info(f"Running: dbt {command}")
    result = subprocess.run(
        ["dbt", command],
        cwd=DBT_PROJECT,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        logging.error(result.stdout)
        raise Exception(f"dbt {command} failed")
    logging.info(f"dbt {command} completed successfully")

if __name__ == "__main__":
    df = extract()
    load(df)
    run_dbt("seed")
    run_dbt("run")
    run_dbt("test")
    logging.info("Pipeline completed successfully")