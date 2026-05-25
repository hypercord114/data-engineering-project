import os
import sys
import logging
import requests
import json
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

# Pipeline Target: Buffalo, NY coordinates
LATITUDE = "42.8864"
LONGITUDE = "-78.8784"
FORECAST_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=wind_direction_10m,temperature_2m,relative_humidity_2m&temperature_unit=fahrenheit&forecast_days=1&timezone=auto"

# Pull the PostgreSQL connection string from GitHub Secrets securely
DB_CONNECTION_STRING = os.getenv("POSTGRES_DB_URI")

# --- LOGGING CONFIGURATION ---
# Configured to append into the same shared pipeline log file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline-logs/morning_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def create_table_if_not_exists(conn):
    """ Ensures the destination schema exists before attempting to load data. """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hourly_weather_forecast (
                id SERIAL PRIMARY KEY,
                extracted_at TIMESTAMP WITH TIME ZONE,
                forecast_time TIMESTAMP,
                temperature_f NUMERIC,
                relative_humidity NUMERIC,
                wind_direction_deg NUMERIC
            );
        """)
        conn.commit()

def load_to_postgresql(raw_json_text):
    """ Parses the raw json string and loads the hourly metrics into PostgreSQL. """
    if not DB_CONNECTION_STRING:
        logging.warning("Skipping DB Load: POSTGRES_DB_URI environment variable is missing.")
        return

    try:
        # Parse the plain string text into a navigable Python dictionary
        data = json.loads(raw_json_text)
        hourly_data = data["hourly"]
        extracted_timestamp = datetime.now(ZoneInfo("America/New_York"))

        logging.info("Connecting to cloud Neon PostgreSQL database instance...")
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        create_table_if_not_exists(conn)

        inserted_records = 0
        with conn.cursor() as cur:
            # Open-Meteo returns data in parallel arrays; loop using the length of the time index
            for i in range(len(hourly_data["time"])):
                cur.execute("""
                    INSERT INTO hourly_weather_forecast 
                    (extracted_at, forecast_time, temperature_f, relative_humidity, wind_direction_deg)
                    VALUES (%s, %s, %s, %s, %s);
                """, (
                    extracted_timestamp,
                    hourly_data["time"][i],
                    hourly_data["temperature_2m"][i],
                    hourly_data["relative_humidity_2m"][i],
                    hourly_data["wind_direction_10m"][i]
                ))
            conn.commit()
            inserted_records = len(hourly_data["time"])
            
        logging.info(f"Database stage complete! Successfully inserted {inserted_records} rows into Neon.")
        conn.close()

    except Exception as db_err:
        # Log database exceptions as non-critical so if Neon experiences latency, 
        # the workflow still updates and saves the raw json archive to GitHub safely!
        logging.error(f"PIPELINE NON-CRITICAL DATABASE FAILURE: {db_err}")

def fetch_and_save_raw_json():
    local_now = datetime.now(ZoneInfo("America/New_York"))
    today_date = local_now.strftime("%Y-%m-%d")
    folder_name = "raw_json"
    
    # Ensure the directory exists in your GitHub workspace
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, f"forecast_{today_date}.json")
    
    logging.info(f"--- Launching Ingestion Phase for {today_date} ---")
    logging.info("Extracting raw weather payload data from Open-Meteo API...")
    
    try:
        res = requests.get(FORECAST_URL, timeout=15)
        res.raise_for_status()
        
        # Phase 1: Save the exact, un-transformed JSON raw string to GitHub files
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(res.text)
        logging.info(f"Loading stage complete! Raw payload file successfully saved to: {file_path}")
        
        # Phase 2: Relational Extract-Load into Postgres
        load_to_postgresql(res.text)
        
        # Confirmed save to database log update
        logging.info("Pipeline Execution Success: Raw payload archived to GitHub and flat arrays committed to PostgreSQL database.")
        
    except requests.exceptions.RequestException as net_err:
        logging.error(f"PIPELINE CRITICAL API FAILURE: Ingestion sequence aborted. Network Exception details: {net_err}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"CRITICAL EXTRACT FAILURE: Could not write raw JSON payload to storage. Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_save_raw_json()
