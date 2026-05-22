import os
import sys
import logging
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

# Pipeline Target: Buffalo, NY coordinates
LATITUDE = "42.8864"
LONGITUDE = "-78.8784"
FORECAST_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=wind_direction_10m,temperature_2m,relative_humidity_2m&temperature_unit=fahrenheit&forecast_days=1&timezone=auto"

# --- LOGGING CONFIGURATION ---
# Configured to append into the same shared pipeline log file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("morning_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

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
        
        # Save the exact, un-transformed JSON raw string to disk
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(res.text)
            
        logging.info(f"Loading stage complete! Raw payload file successfully saved to: {file_path}")
        
    except requests.exceptions.RequestException as net_err:
        logging.error(f"PIPELINE CRITICAL API FAILURE: Ingestion sequence aborted. Network Exception details: {net_err}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"CRITICAL EXTRACT FAILURE: Could not write raw JSON payload to storage. Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_and_save_raw_json()
