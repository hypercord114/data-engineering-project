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

# --- UPDATED LOGGING CONFIGURATION ---
# Appends logs to kafka_pipeline.log and mirrors output to standard stdout stream
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("kafka_pipeline.log", encoding="utf-8"),
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

def get_live_downwind_direction():
    """ Fetches live wind direction from Buffalo and converts it to downwind target. """
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current=wind_direction_10m&timezone=auto"
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        wind_deg = data["current"]["wind_direction_10m"]
        
        idx = int((wind_deg + 22.5) / 45) % 8
        upwind_dir = COMPASS_NODES[idx]
        downwind_idx = (idx + 4) % 8
        downwind_dir = COMPASS_NODES[downwind_idx]
        
        logging.info(f"Current Buffalo Weather: Wind coming from {wind_deg}° ({upwind_dir}) -> Plume traveling Downwind toward: {downwind_dir}")
        return downwind_dir
    except Exception as e:
        logging.error(f"Failed to fetch live wind metrics: {e}. Falling back to default baseline (NE).")
        return "NE"

def generate_telemetry_batch(downwind_target, sim_time_str, active_event):
    batch = []
    
    if not active_event["is_active"] and random.random() < 0.15:
        active_event["is_active"] = True
        active_event["cycles_left"] = random.randint(15, 35)
        logging.info(f"[SIMULATION] Random emission spike started at {sim_time_str}. Duration: {active_event['cycles_left']} mins.")

    for node in COMPASS_NODES:
        state = pid_sensors[node]
        
        if active_event["is_active"] and node == downwind_target:
            target_val = random.uniform(115.0, 145.0)
            state["current_val"] += (target_val - state["current_val"]) * 0.3
        else:
            state["current_val"] += (state["baseline"] - state["current_val"]) * 0.2
            
        state["current_val"] += random.uniform(-1.5, 1.5)
        state["current_val"] = max(0.0, state["current_val"])
        
        batch.append({
            "sensor_id": f"PID-PERIMETER-{node}",
            "timestamp": sim_time_str,
            "tvoc_ppb": round(state["current_val"], 1),
            "unit": "ppb"
        })
        
    if active_event["is_active"]:
        active_event["cycles_left"] -= 1
        if active_event["cycles_left"] <= 0:
            active_event["is_active"] = False
            logging.info(f"[SIMULATION] Emission event dissipated natively at {sim_time_str}.")
            
    return batch

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    # Suppressing successful individual logs to keep the text file clean

def run_producer():
    logging.info("--- Starting Intraday 2-Hour Batch Telemetry Generation ---")
    
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'SSL',
        'ssl.ca.location': 'ssl_credentials/ca.pem',
        'ssl.certificate.location': 'ssl_credentials/service.cert',
        'ssl.key.location': 'ssl_credentials/service.key'
    }
    
    try:
        producer = Producer(conf)
    except Exception as e:
        logging.critical(f"Failed to initialize Kafka Producer instance: {e}")
        sys.exit(1)

    downwind_target = get_live_downwind_direction()
    
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
        producer.flush(timeout=15)
        logging.info("Batch streaming sequence finalized. Shutting down virtual workspace.")

if __name__ == "__main__":
    run_producer()
