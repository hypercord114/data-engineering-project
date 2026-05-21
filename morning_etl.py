import math
import sys
import logging
import duckdb
import requests
from datetime import datetime

# Pipeline Target: Buffalo, NY coordinates
LATITUDE = "42.8864"
LONGITUDE = "-78.8784"

# 1. API Endpoint URLs
FORECAST_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=wind_direction_10m,temperature_2m,relative_humidity_2m&temperature_unit=fahrenheit&forecast_days=1&timezone=auto"
METADATA_URL = f"https://api.open-meteo.com/v1/model-updates?latitude={LATITUDE}&longitude={LONGITUDE}"

# Directs entries simultaneously to terminal screens and the pipeline.log file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("morning_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_cardinal_points(wind_degrees):
    """Translates meteorology angles into clear upwind and downwind paths."""
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", 
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((wind_degrees + 11.25) / 22.5) % 16
    return directions[idx], directions[(idx + 8) % 16]

def save_forecast_to_duckdb(today_date, avg_wind, avg_temp, avg_humid, upwind, downwind):
    """Handles DuckDB local database updates and logs skips versus writes."""
    connection = duckdb.connect("environmental_data.db")
    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS daily_shift_forecasts (
                forecast_date DATE PRIMARY KEY,
                predicted_avg_wind_deg DOUBLE,
                predicted_avg_temp_f DOUBLE,
                predicted_avg_humidity_pct DOUBLE,
                upwind_cardinal VARCHAR,
                downwind_cardinal VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Capture the cursor response to evaluate affected rows
        cursor = connection.execute("""
            INSERT INTO daily_shift_forecasts 
                (forecast_date, predicted_avg_wind_deg, predicted_avg_temp_f, predicted_avg_humidity_pct, upwind_cardinal, downwind_cardinal)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (forecast_date) DO NOTHING;
        """, (today_date, avg_wind, avg_temp, avg_humid, upwind, downwind))
        
        # Evaluates whether the database engine actually inserted a row or skipped it
        if cursor.rowcount > 0:
            logging.info("Successfully recorded new forecast data profile to local DuckDB file.")
        else:
            logging.warning(f"Database write skipped. A forecast record for {today_date} already exists in daily_shift_forecasts.")
            
    except Exception as db_err:
        logging.error(f"DATABASE WRITE FAILURE: Failed to save record into DuckDB. Error context: {db_err}")
        raise db_err
    finally:
        connection.close()

def run_morning_etl():
    today_date = datetime.now().strftime("%Y-%m-%d")
    logging.info(f"--- Launching Morning ETL Job Execution Sequence for {today_date} ---")
    
    # --- FIXED: Added robust try-except error catching with request timeouts ---
    try:
        logging.info("Checking Open-Meteo upstream model update metadata...")
        meta_response = requests.get(METADATA_URL, timeout=15)
        meta_response.raise_for_status()
        logging.info(f"Upstream data model verified. Last updated status: {meta_response.json().get('model_updates', {})}")

        logging.info("Fetching hourly forecast parameters...")
        res = requests.get(FORECAST_URL, timeout=15)
        res.raise_for_status()
        hourly = res.json()["hourly"]
        
    except requests.exceptions.RequestException as net_err:
        logging.error(f"PIPELINE CRITICAL API FAILURE: Ingestion sequence aborted. Network/HTTP Exception details: {net_err}")
        sys.exit(1)
        
    except KeyError as key_err:
        logging.error(f"PIPELINE TRANSFORM FAILURE: Expected JSON structure changed. Key Error: {key_err}")
        sys.exit(1)

    shift_wind = []
    shift_temp = []
    shift_humidity = []
    
    # 2. Iterate through 24 hours and filter strictly for the 7 AM - 5 PM shift
    for time_str, wind, temp, humid in zip(hourly["time"], hourly["wind_direction_10m"], hourly["temperature_2m"], hourly["relative_humidity_2m"]):
        dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
        if 7 <= dt.hour <= 17:
            shift_wind.append(wind)
            shift_temp.append(temp)
            shift_humidity.append(humid)

    if not shift_temp:
        logging.error("PIPELINE PROCESSING FAILURE: No valid time-series observations extracted within the shift range.")
        sys.exit(1)

    # 3. Process metrics using circular mean for wind direction
    avg_temp = sum(shift_temp) / len(shift_temp)
    avg_humid = sum(shift_humidity) / len(shift_humidity)
    
    # --- CORRECT CIRCULAR MEAN ALGORITHM FOR WIND DIRECTION ---
    sum_sin = 0.0
    sum_cos = 0.0
    
    for deg in shift_wind:
        rad = math.radians(deg)
        sum_sin += math.sin(rad)
        sum_cos += math.cos(rad)
        
    avg_sin = sum_sin / len(shift_wind)
    avg_cos = sum_cos / len(shift_wind)
    
    avg_wind_rad = math.atan2(avg_sin, avg_cos)
    avg_wind = math.degrees(avg_wind_rad)
    
    if avg_wind < 0:
        avg_wind += 360.0
    # -----------------------------------------------------------

    upwind, downwind = get_cardinal_points(avg_wind)
    
    # FIXED: Converted text display formatting from raw print to structured logger
    logging.info(f"Shift Summary Calculated: Temp: {avg_temp:.1f}F, Wind: {avg_wind:.1f} deg ({upwind}), Humidity: {avg_humid:.1f}%")

    try:
        save_forecast_to_duckdb(today_date, avg_wind, avg_temp, avg_humid, upwind, downwind)
        logging.info(f"--- Pipeline Execution Sequence Successfully Finalized for {today_date} ---")
    except Exception:
        sys.exit(1)

if __name__ == "__main__":
    run_morning_etl()
