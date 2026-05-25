import time
import json
import random
import logging
import sys
import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from confluent_kafka import Producer

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URI", "kafka-de-kjn0123-kel-6978.d.aivencloud.com:10961") 
TOPIC_NAME = "intraday_pid_telemetry"

LATITUDE = 42.8864
LONGITUDE = -78.8784

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline-logs/kafka_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

COMPASS_NODES = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

pid_sensors = {
    node_id: {
        "baseline": 65.0,
        "current_val": 65.0,
        "spike_duration": 0
    } for node_id in COMPASS_NODES
}

def fetch_current_downwind_zone():
    """ Fetches real-time wind coordinates to orient our downwind plume tracking model. """
    api_url = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=wind_direction_10m&forecast_days=1&timezone=auto"
    try:
        res = requests.get(api_url, timeout=10)
        res.raise_for_status()
        data = res.json()
        
        current_hour_idx = datetime.now().hour
        wind_deg = data["hourly"]["wind_direction_10m"][current_hour_idx]
        
        # Calculate cross-wind downwind direction vector
        downwind_deg = (wind_deg + 180) % 360
        
        directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", 
                      "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        idx = int((downwind_deg + 11.25) / 22.5) % 16
        downwind_cardinal = directions[idx]
        
        # Map 16-point coordinates to our 8 core localized compass boundary segments
        mapping = {
            "N": "N", "NNE": "NE", "NE": "NE", "ENE": "NE",
            "E": "E", "ESE": "SE", "SE": "SE", "SSE": "SE",
            "S": "S", "SSW": "SW", "SW": "SW", "WSW": "SW",
            "W": "W", "WNW": "NW", "NW": "NW", "NNW": "NW"
        }
        target_zone = mapping.get(downwind_cardinal, "N")
        logging.info(f"Meteorological context sync: Wind blowing towards downwind sector {target_zone} ({wind_deg}° raw input).")
        return target_zone
    except Exception as err:
        logging.error(f"Failed to fetch downwind target zone via API. Falling back to default baseline: {err}")
        return "NE"

def generate_telemetry_batch(downwind_zone, current_time_str, event_state):
    """ Synthesizes localized micro-climate baseline drift and realistic plume spikes. """
    batch = []
    
    # 3% overall structural possibility of a chemical fugitive release event starting this interval
    if not event_state["is_active"] and random.random() < 0.03:
        event_state["is_active"] = True
        event_state["cycles_left"] = random.randint(15, 35) # Plume tracking duration boundary parameters
        logging.info(f"--- [PLUME TRANSIENT INTERCEPT] Vapor breakthrough simulated heading downwind towards {downwind_zone} ---")
        
    for node in COMPASS_NODES:
        sensor = pid_sensors[node]
        
        # Standardize the ID format explicitly here to clean the metrics logs
        sensor_id = f"PID-PERIMETER-{node}"
        
        # Normal ambient thermal background noise variations (+/- 0.4 ppb)
        sensor["baseline"] += random.uniform(-0.4, 0.4)
        sensor["baseline"] = max(45.0, min(85.0, sensor["baseline"])) # Bound reasonable ambient thresholds
        
        current_val = sensor["baseline"]
        
        # Inject physics modeling if an active release event is targeting this sector
        if event_state["is_active"] and node == downwind_zone:
            # Steady gas concentration climb curve up to an active alarm floor
            current_val += random.uniform(45.0, 85.0)
            
            # Explicitly guarantee a high spike threshold flag occasionally to check automation response
            if random.random() < 0.25:
                current_val += random.uniform(30.0, 50.0)
        else:
            # Minor atmospheric dispersion spillover to adjacent sensor node arrays
            if event_state["is_active"] and random.random() < 0.30:
                current_val += random.uniform(5.0, 15.0)
                
        reading = {
            "sensor_id": sensor_id, # Cleaned, standardized key
            "timestamp": f"{datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d')} {current_time_str}",
            "tvoc_ppb": round(current_val, 1)
        }
        batch.append(reading)
        
    # Countdown and reset simulated plume event flags
    if event_state["is_active"]:
        event_state["cycles_left"] -= 1
        if event_state["cycles_left"] <= 0:
            event_state["is_active"] = False
            logging.info("--- [PLUME TRANSIENT RESOLVED] Vapor concentrations returned to environmental baseline. ---")
            
    return batch

def delivery_report(err, msg):
    """ Confirms network acknowledgment status from the distributed Aiven Kafka cloud node cluster. """
    if err is not None:
        logging.error(f"Kafka message broker routing failure: {err}")
    # Low-overhead tracking omitted from log files to keep screen buffer readable

def run_simulation_pipeline():
    """ Orchestrates the telemetry burst serialization and cluster push tasks. """
    logging.info("Connecting to a secure cloud-managed Aiven Kafka instance...")
    
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'SSL',
        'ssl.ca.location': 'ssl_credentials/ca.pem',
        'ssl.certificate.location': 'ssl_credentials/service.cert',
        'ssl.key.location': 'ssl_credentials/service.key',
        'client.id': 'buffalo-perimeter-producer-daemon',
        'acks': '1'
    }
    
    try:
        producer = Producer(conf)
    except Exception as init_err:
        logging.critical(f"Could not initialize Kafka Producer Client framework: {init_err}")
        sys.exit(1)
        
    downwind_target = fetch_current_downwind_zone()
    
    ny_tz = ZoneInfo("America/New_York")
    now_local = datetime.now(ny_tz)
    sim_start_time = now_local - timedelta(minutes=120)
    
    logging.info(f"Simulating telemetry window: {sim_start_time.strftime('%H:%M:%S')} to {now_local.strftime('%H:%M:%S')}")
    
    active_event = {"is_active": False, "cycles_left": 0}
    
    try:
        for minute_offset in range(120):
            sim_moment = sim_start_time + timedelta(minutes=minute_offset)
            sim_time_str = sim_moment.strftime("%H:%M:%S")
            
            telemetry_batch = generate_telemetry_batch(downwind_target, sim_time_str, active_event)
            
            for reading in telemetry_batch:
                serialized_data = json.dumps(reading)
                producer.produce(
                    topic=TOPIC_NAME,
                    key=reading["sensor_id"],
                    value=serialized_data,
                    callback=delivery_report
                )
            producer.poll(0)
            
        logging.info("All 120 telemetry intervals successfully calculated and buffered.")
            
    except Exception as run_err:
        logging.error(f"Error during simulation execution: {run_err}")
    finally:
        logging.info("Flushing full historical payload burst to Aiven cloud broker...")
        producer.flush(timeout=10.0)
        logging.info("Data injection phase complete. Producer context offline.")

if __name__ == "__main__":
    run_simulation_pipeline()