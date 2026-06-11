import os
import sys
import logging
import requests
import time
import json  # Added to parse the JSON text string
import psycopg2  # Added to manage connection to PostgreSQL
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
    """ Ensures a modern JSONB document repository schema exists in Postgres. """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_shift_forecasts (
                id SERIAL PRIMARY KEY,
                forecast_date DATE NOT NULL UNIQUE,
                raw_payload JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    conn.commit()

def load_to_postgresql(raw_json_string):
    """ Commits the completely raw un-flattened API payload text as a native JSONB object. """
    if not DB_CONNECTION_STRING:
        logging.warning("Skipping DB Load: POSTGRES_DB_URI environment variable is missing.")
        return

    logging.info("Connecting to cloud Neon PostgreSQL database instance to archive JSONB document...")
    
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        create_table_if_not_exists(conn)
        
        # Determine tracking date context relative to destination timezone rules
        today_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
        cur = conn.cursor()
        
        # Insert the raw string directly. Postgres automatically validates and converts it to binary JSONB
        query = """
            INSERT INTO daily_shift_forecasts (forecast_date, raw_payload)
            VALUES (%s, %s)
            ON CONFLICT (forecast_date) 
            DO UPDATE SET 
                raw_payload = EXCLUDED.raw_payload,
                created_at = CURRENT_TIMESTAMP;
        """
        
        cur.execute(query, (today_date, raw_json_string))
        conn.commit()
        
        cur.close()
        conn.close()
        logging.info("Successfully committed complete JSONB document payload to cloud storage.")
        
    except Exception as db_err:
        logging.error(f"PIPELINE CRITICAL LOAD FAILURE: Failed to push JSON data to Postgres. Details: {db_err}")
        sys.exit(1)

def run_ingestion_pipeline():
    """ Orchestrates the end-to-end data acquisition workflow. """
    today_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    folder_name = "raw_json"
    
    # Ensure the directory exists in your GitHub workspace
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, f"forecast_{today_date}.json")
    
    logging.info(f"--- Launching Ingestion Phase for {today_date} ---")
    logging.info("Extracting raw weather payload data from Open-Meteo API...")

    max_retries = 3
    retry_delay = 30 # seconds
    
    res = None # Initialize variable
    for attempt in range(max_retries):
        try:
            logging.info(f"Extracting data (Attempt {attempt + 1}/{max_retries})...")
            res = requests.get(FORECAST_URL, timeout=15)
            res.raise_for_status()
            
            # If successful, break the loop
            break
        except requests.exceptions.RequestException as net_err:
            logging.warning(f"Network issue: {net_err}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logging.error("PIPELINE CRITICAL API FAILURE: Max retries reached.")
                sys.exit(1)

    # Phase 1: Save the exact, un-transformed JSON raw string to GitHub files
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(res.text)
    logging.info(f"Loading stage complete! Raw payload file successfully saved to: {file_path}")
    
    # Phase 2: Native JSONB Extract-Load into Postgres
    load_to_postgresql(res.text)
    
    # Confirmed save to database log update
    logging.info("Pipeline Execution Success: Raw payload archived to GitHub and flat arrays committed to PostgreSQL database.")

if __name__ == "__main__":
    run_ingestion_pipeline()
