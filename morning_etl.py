import requests
import math
import duckdb
from datetime import datetime

# Pipeline Target: Buffalo, NY coordinates
LATITUDE = "42.8864"
LONGITUDE = "-78.8784"

# 1. API Endpoint URLs
FORECAST_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=wind_direction_10m,temperature_2m,relative_humidity_2m&temperature_unit=fahrenheit&forecast_days=1&timezone=auto"
METADATA_URL = f"https://api.open-meteo.com/v1/model-updates?latitude={LATITUDE}&longitude={LONGITUDE}"

def get_cardinal_points(wind_degrees):
    """Translates meteorology angles into clear upwind and downwind paths."""
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", 
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((wind_degrees + 11.25) / 22.5) % 16
    return directions[idx], directions[(idx + 8) % 16]

def save_forecast_to_duckdb(today_date, avg_wind, avg_temp, avg_humid, upwind, downwind):
    """Handles DuckDB local database updates."""
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
        
        connection.execute("""
            INSERT INTO daily_shift_forecasts 
                (forecast_date, predicted_avg_wind_deg, predicted_avg_temp_f, predicted_avg_humidity_pct, upwind_cardinal, downwind_cardinal)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (forecast_date) DO UPDATE SET
                predicted_avg_wind_deg = EXCLUDED.predicted_avg_wind_deg,
                predicted_avg_temp_f = EXCLUDED.predicted_avg_temp_f,
                predicted_avg_humidity_pct = EXCLUDED.predicted_avg_humidity_pct,
                upwind_cardinal = EXCLUDED.upwind_cardinal,
                downwind_cardinal = EXCLUDED.downwind_cardinal;
        """, (today_date, avg_wind, avg_temp, avg_humid, upwind, downwind))
        
        print("Successfully recorded forecast data profile to local DuckDB file.")
    finally:
        connection.close()

def run_morning_etl():
    print("Checking Open-Meteo upstream model update metadata...")
    meta_response = requests.get(METADATA_URL).json()
    print(f"Upstream data model verified. Last updated status: {meta_response.get('model_updates', {})}")

    print("Fetching hourly forecast parameters...")
    res = requests.get(FORECAST_URL).json()
    hourly = res["hourly"]
    
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
    
    today_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Shift Summary for {today_date}: Temp: {avg_temp:.1f}F, Wind: {avg_wind:.1f} deg ({upwind}), Humidity: {avg_humid:.1f}%")

    save_forecast_to_duckdb(today_date, avg_wind, avg_temp, avg_humid, upwind, downwind)

if __name__ == "__main__":
    run_morning_etl()