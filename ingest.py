import duckdb
import requests
from pathlib import Path

DATA_PATH = Path("data")
DATA_PATH.mkdir(exist_ok=True)

FILE_NAME = "Airline_Delay_Cause.csv"
FILE_PATH = DATA_PATH / FILE_NAME

URL = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/flights-2k.csv"  # replace with your actual dataset URL


def get_data():
    if not FILE_PATH.exists():
        print("Downloading dataset...")
        response = requests.get(URL)
        response.raise_for_status()
        with open(FILE_PATH, "wb") as f:
            f.write(response.content)
    else:
        print("Dataset already exists.")


if __name__ == "__main__":

    # Step 1: Get data
    get_data()

    # Step 2: Connect to DuckDB
    con = duckdb.connect("zc_project_2.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    # Step 3: Load CSV into DuckDB table
    con.execute(f"""
        CREATE OR REPLACE TABLE prod.airline_delay AS
        SELECT *
        FROM read_csv_auto('{FILE_PATH}')
    """)

    # Optional: preview
    result = con.execute("SELECT * FROM prod.airline_delay LIMIT 5").fetchdf()
    print(result)

    con.close()