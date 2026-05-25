import os
import json
import math
import sys
import logging  
import duckdb
import psycopg2  # Used to extract raw data back from Postgres
from datetime import datetime
from zoneinfo import ZoneInfo

# Pull the PostgreSQL connection string from GitHub Secrets securely
DB_CONNECTION_STRING = os.getenv("POSTGRES_DB_URI")

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline-logs/morning_pipeline.log", encoding="utf-8"),
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
                predicted_avg_temp_f DOUBLE,
                predicted_avg_wind_deg DOUBLE,
                predicted_avg_humidity_pct DOUBLE,
                upwind_cardinal VARCHAR,
                downwind_cardinal VARCHAR,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        connection.execute("""
            INSERT OR REPLACE INTO daily_shift_forecasts (
                forecast_date, predicted_avg_wind_deg, predicted_avg_temp_f, 
                predicted_avg_humidity_pct, upwind_cardinal, downwind_cardinal
            ) VALUES (?, ?, ?, ?, ?, ?);
        """, (today_date, avg_wind, avg_temp, avg_humid, upwind, downwind))
        
        logging.info(f"Successfully stored summarized shift targets inside local DuckDB instance.")
    except Exception as e:
        logging.error(f"DuckDB Storage Error Execution Failure: {e}")
        sys.exit(1)
    finally:
        connection.close()

def extract_metrics_from_postgres(db_uri, target_date):
    """ Connects to PostgreSQL and pulls down the complete raw JSONB document blob for processing. """
    logging.info(f"Connecting to cloud Neon PostgreSQL database instance to read metrics for {target_date}...")
    
    try:
        conn = psycopg2.connect(db_uri)
        cur = conn.cursor()
        
        # Self-healing schema validation check to guarantee table context stability
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_shift_forecasts (
                id SERIAL PRIMARY KEY,
                forecast_date DATE NOT NULL UNIQUE,
                raw_payload JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

        # Extract the entire raw document blob directly
        query = """
            SELECT raw_payload 
            FROM daily_shift_forecasts 
            WHERE forecast_date = %s;
        """
        
        cur.execute(query, (target_date,))
        row = cur.fetchone()
        
        cur.close()
        conn.close()
        
        # Return the dictionary payload if found, otherwise return None
        return row[0] if row else None
        
    except Exception as db_err:
        logging.error(f"PIPELINE TRANSFORM CRITICAL EXTRACT FAILURE: Failed to query PostgreSQL. Error: {db_err}")
        sys.exit(1)

def run_transform_pipeline():
    """ Orchestrates the transformation, parsing, analytical calculation, and data warehousing phases. """
    today_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    
    logging.info(f"--- Launching Transform Phase for {today_date} ---")
    
    if not DB_CONNECTION_STRING:
        logging.error("PIPELINE PROCESSING CRITICAL ENVIROMENT BREAK: POSTGRES_DB_URI secret is missing.")
        sys.exit(1)

    # Pull the native document map dictionary from Postgres
    raw_payload = extract_metrics_from_postgres(DB_CONNECTION_STRING, today_date)
    
    if not raw_payload:
        logging.error(f"PIPELINE PROCESSING EXCEPTION: Target dataset for {today_date} was not initialized by Ingestion Layer.")
        sys.exit(1)

    try:
        # Open-Meteo packages its datasets inside a primary 'hourly' key parent array block
        hourly_data = raw_payload["hourly"]
        time_stamps = hourly_data["time"]
        wind_directions = hourly_data["wind_direction_10m"]
        temperatures = hourly_data["temperature_2m"]
        humidities = hourly_data["relative_humidity_2m"]
    except KeyError as parse_err:
        logging.error(f"PIPELINE TRANSFORM CRITICAL FAILURE: Downloaded JSON fields mismatched structure specifications. Missing key: {parse_err}")
        sys.exit(1)

    shift_wind = []
    shift_temp = []
    shift_humidity = []

    # Reformat, filter and segment down observations precisely into the operational shift windows (06:00 to 18:00)
    for i, time_str in enumerate(time_stamps):
        dt = datetime.fromisoformat(time_str)
        hour = dt.hour
        
        if 7 <= hour <= 17:
            shift_wind.append(float(wind_directions[i]))
            shift_temp.append(float(temperatures[i]))
            shift_humidity.append(float(humidities[i]))

    if not shift_temp:
        logging.error("PIPELINE PROCESSING FAILURE: No valid observations extracted within the shift range.")
        sys.exit(1)

    # Perform math with 1 decimal place rounding parameters
    avg_temp = round(sum(shift_temp) / len(shift_temp), 1)
    avg_humid = round(sum(shift_humidity) / len(shift_humidity), 1)
    
    # --- CIRCULAR MEAN ALGORITHM FOR WIND DIRECTION ---
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
    # ---------------------------------------------------

    upwind, downwind = get_cardinal_points(avg_wind)
    logging.info(f"Calculated Metrics from Staged JSONB Document: Temp: {avg_temp}°F | Humid: {avg_humid}% | Vector: {avg_wind}° ({upwind} -> Downwind: {downwind})")

    # Commit data to DuckDB warehouse layer
    save_forecast_to_duckdb(today_date, avg_wind, avg_temp, avg_humid, upwind, downwind)
    logging.info(f"--- Transform Stage Complete for {today_date} ---")

if __name__ == "__main__":
    run_transform_pipeline()
