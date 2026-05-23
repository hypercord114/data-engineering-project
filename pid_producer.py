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
# Check for a GitHub Secret Environment Variable first, fallback to your hardcoded cluster URI
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URI", "...")  
TOPIC_NAME = "intraday_pid_telemetry"

LATITUDE = 42.8864
LONGITUDE = -78.8784

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

COMPASS_NODES = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

# Base sensor state configuration
pid_sensors = {
    node_id: {
        "baseline": 65.0,
        "current_val": 65.0,
        "spike_duration": 0
    } for node_id in COMPASS_NODES
}

def get_live_downwind_direction():
    """ Fetches live wind direction from Buffalo and converts it to downwind target. """
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current=wind_direction_10m&wind_speed_unit=mph"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        wind_from_deg = data["current"]["wind_direction_10m"]
        downwind_deg = (wind_from_deg + 180) % 360
        
        idx = int((downwind_deg + 22.5) / 45) % 8
        downwind_node = COMPASS_NODES[idx]
        
        logging.info(f"Weather Fetch -> Wind from: {wind_from_deg}°. Vapor plume dispersing TOWARD: {downwind_node} ({round(downwind_deg)}°)")
        return downwind_node
    except Exception as e:
        logging.error(f"Weather API unavailable, defaulting dispersion path to NE. Error: {e}")
        return "NE"

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Kafka Delivery Failed: {err}")
    else:
        pass # Suppress spamming 960 successful log confirmations to GitHub logs

def generate_telemetry_batch(active_downwind_node, sim_time_string, active_event):
    """ Simulates environmental data for a single point in simulated time. """
    payloads = []
    downwind_idx = COMPASS_NODES.index(active_downwind_node)
    
    # 2.5% chance per simulated minute to trigger a process anomaly plume event
    if not active_event["is_active"] and random.random() < 0.025:
        logging.warning(f"[{sim_time_string}] !!! ALERT: Process leak initiated! Plume tracking toward {active_downwind_node} !!!")
        active_event["is_active"] = True
        active_event["cycles_left"] = random.randint(15, 45) # Plume lasts 15-45 minutes

    for idx, node_id in enumerate(COMPASS_NODES):
        state = pid_sensors[node_id]
        
        if active_event["is_active"]:
            distance_to_plume = min((idx - downwind_idx) % 8, (downwind_idx - idx) % 8)
            
            if distance_to_plume == 0:
                state["current_val"] = random.uniform(350.0, 700.0)
            elif distance_to_plume == 1:
                state["current_val"] = random.uniform(120.0, 250.0)
            else:
                drift = random.uniform(-4.0, 4.0)
                state["current_val"] = max(15.0, min(110.0, state["current_val"] + drift))
        else:
            drift = random.uniform(-5.0, 5.0)
            state["current_val"] = max(15.0, min(110.0, state["current_val"] + drift))
            
        payloads.append({
            "sensor_id": f"PID_NODE_{node_id}",
            "timestamp": sim_time_string,
            "tvoc_ppb": round(state["current_val"], 1),
            "unit": "ppb"
        })
        
    if active_event["is_active"]:
        active_event["cycles_left"] -= 1
        if active_event["cycles_left"] <= 0:
            logging.info(f"[{sim_time_string}] Vapor plume successfully cleared from perimeter.")
            active_event["is_active"] = False

    return payloads

def run_producer():
    logging.info("Initializing connection parameters to secure cloud Kafka cluster...")
    
    conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'security.protocol': 'SSL',
            'ssl.ca.location': 'ssl_credentials/ca.pem',
            'ssl.certificate.location': 'ssl_credentials/service.cert',
            'ssl.key.location': 'ssl_credentials/service.key',
            'client.id': 'buffalo-field-producer',
            'queue.buffering.max.messages': 2000 
    }
    
    try:
        producer = Producer(conf)
    except Exception as init_err:
        logging.error(f"Could not build Kafka producer interface. Error: {init_err}")
        sys.exit(1)
        
    # Determine the downwind target vector via live scraping
    downwind_target = get_live_downwind_direction()
    
    # --- TIME CALCULATIONS ---
    # Capture the exact current moment in Buffalo timezone
    current_time = datetime.now(ZoneInfo("America/New_York"))
    # Backdate our simulation starting point by exactly 2 hours (120 minutes)
    sim_start_time = current_time - timedelta(minutes=120)
    
    logging.info(f"Batch Processing Triggered. Simulating 120 minutes from {sim_start_time.strftime('%H:%M:%S')} to {current_time.strftime('%H:%M:%S')}...")
    
    # State tracker for transient gas leaks across the batch generation
    active_event = {"is_active": False, "cycles_left": 0}
    
    try:
        # Loop exactly 120 times, advancing 1 minute per cycle
        for minute_offset in range(120):
            sim_moment = sim_start_time + timedelta(minutes=minute_offset)
            sim_time_str = sim_moment.strftime("%H:%M:%S")
            
            # Generate measurements for all 8 sensors at this specific simulated timestamp
            telemetry_batch = generate_telemetry_batch(downwind_target, sim_time_str, active_event)
            
            for reading in telemetry_batch:
                serialized_data = json.dumps(reading)
                producer.produce(
                    topic=TOPIC_NAME,
                    key=reading["sensor_id"],
                    value=serialized_data,
                    callback=delivery_report
                )
            
            # Serve internal queue callbacks to keep processing memory clear
            producer.poll(0)
            
        logging.info("All 120 telemetry intervals successfully calculated and buffered.")
            
    except Exception as run_err:
        logging.error(f"Error during simulation execution: {run_err}")
    finally:
        logging.info("Flushing full historical payload burst to Aiven cloud broker...")
        producer.flush(timeout=15)
        logging.info("Batch streaming sequence finalized. Shutting down virtual workspace.")

if __name__ == "__main__":
    run_producer()