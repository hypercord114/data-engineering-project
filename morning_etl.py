import os
import json
import math
import sys
import logging  
import duckdb
from datetime import datetime

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("morning_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_cardinal_points(wind_degrees):
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", 
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((wind_degrees + 11.25) / 22.5) % 16
    return directions[idx], directions[(idx + 8) % 16]

def save_forecast_to_duckdb(today_date, avg_wind, avg_temp, avg_humid, upwind, downwind):
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
        
        cursor = connection.execute("""
            INSERT INTO daily_shift_forecasts 
                (forecast_date, predicted_avg_wind_deg, predicted_avg_temp_f, predicted_avg_humidity_pct, upwind_cardinal, downwind_cardinal)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (forecast_date) DO NOTHING
            RETURNING forecast_date;
        """, (today_date, avg_wind, avg_temp, avg_humid, upwind, downwind))
        
        inserted_rows = cursor.fetchall()
        if len(inserted_rows) > 0:
            logging.info("Successfully recorded new forecast data profile to local DuckDB file.")
        else:
            logging.warning(f"Database write skipped. A forecast record for {today_date} already exists in daily_shift_forecasts.")
            
    except Exception as db_err:
        logging.error(f"DATABASE WRITE FAILURE: Failed to save record into DuckDB. Error context: {db_err}")
        raise db_err
    finally:
        connection.close()

def run_transformation_pipeline():
    local_now = datetime.now(ZoneInfo("America/New_York"))
    today_date = local_now.strftime("%Y-%m-%d")
    logging.info(f"--- Launching Transform Phase for {today_date} ---")
    
    # Target the file created by the Extract/Load phase
    file_path = os.path.join("raw_json", f"forecast_{today_date}.json")
    
    if not os.path.exists(file_path):
        logging.error(f"TRANSFORM CRITICAL ERROR: Staged data file not found at {file_path}. Aborting.")
        sys.exit(1)
        
    try:
        logging.info(f"Reading staged raw JSON payload data from local path: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            response_data = json.load(f)
            
        generation_time = response_data.get("generationtime_ms", "N/A")
        utc_offset = response_data.get("utc_offset_seconds", 0)
        logging.info(f"Local JSON parsed. Source server compute time: {generation_time}ms. Timezone offset: {utc_offset}s.")
        
        hourly = response_data["hourly"]
        
    except (json.JSONDecodeError, KeyError) as parse_err:
        logging.error(f"PIPELINE TRANSFORM FAILURE: Staged JSON payload structure corrupt or invalid: {parse_err}")
        sys.exit(1)

    shift_wind = []
    shift_temp = []
    shift_humidity = []
    
    for time_str, wind, temp, humid in zip(hourly["time"], hourly["wind_direction_10m"], hourly["temperature_2m"], hourly["relative_humidity_2m"]):
        dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
        if 7 <= dt.hour <= 17:
            shift_wind.append(wind)
            shift_temp.append(temp)
            shift_humidity.append(humid)

    if not shift_temp:
        logging.error("PIPELINE PROCESSING FAILURE: No valid observations extracted within the shift range.")
        sys.exit(1)

    # Calculate metrics with your requested 1 decimal rounding rules
    avg_temp = round(sum(shift_temp) / len(shift_temp), 1)
    avg_humid = round(sum(shift_humidity) / len(shift_humidity), 1)
    
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
        
    avg_wind = round(avg_wind, 1)
    # -----------------------------------------------------------

    upwind, downwind = get_cardinal_points(avg_wind)
    logging.info(f"Calculated Metrics from Staged Storage: Temp: {avg_temp}F, Wind: {avg_wind} deg ({upwind}), Humidity: {avg_humid}%")

    try:
        save_forecast_to_duckdb(today_date, avg_wind, avg_temp, avg_humid, upwind, downwind)
        logging.info(f"--- ELT Transformation Sequence Finalized for {today_date} ---")
    except Exception:
        sys.exit(1)

if __name__ == "__main__":
    run_transformation_pipeline()