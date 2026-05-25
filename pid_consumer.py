import os
import sys
import json
import logging
import time  # Added to track duration periods
from datetime import datetime
from zoneinfo import ZoneInfo
from confluent_kafka import Consumer, KafkaError, KafkaException
import psycopg2

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URI")
TOPIC_NAME = "intraday_pid_telemetry"
GROUP_ID = "buffalo-perimeter-analytics-group"

# Secure Cloud Postgres Connection String
DB_CONNECTION_STRING = os.getenv("POSTGRES_DB_URI")

# Max seconds to wait for a new message before deciding the topic is fully drained
MAX_IDLE_TIMEOUT_SECONDS = 15.0

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline-logs/consumer_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def create_telemetry_table_if_not_exists(db_conn):
    """ Ensures the relational real-time telemetry schema exists. """
    with db_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_pid_telemetry (
                id SERIAL PRIMARY KEY,
                sensor_id VARCHAR(50) NOT NULL,
                reading_timestamp TIME NOT NULL,
                tvoc_ppb NUMERIC NOT NULL,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        db_conn.commit()

def insert_telemetry_record(db_conn, record_dict):
    """ Commits a single parsed PID telemetry reading to the Postgres cluster. """
    try:
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO realtime_pid_telemetry (sensor_id, reading_timestamp, tvoc_ppb)
                VALUES (%s, %s, %s);
            """, (
                record_dict["sensor_id"],
                record_dict["timestamp"],
                record_dict["tvoc_ppb"],
            ))
            db_conn.commit()
    except Exception as db_err:
        logging.error(f"PostgreSQL Insertion Failure: {db_err}")
        db_conn.rollback()

def run_consumer():
    logging.info("Initializing connection parameters to secure cloud infrastructure...")

    if not DB_CONNECTION_STRING:
        logging.critical("DATABASE ERROR: POSTGRES_DB_URI environment variable is missing. Halting.")
        sys.exit(1)

    # 1. Initialize PostgreSQL Connection
    try:
        db_conn = psycopg2.connect(DB_CONNECTION_STRING)
        create_telemetry_table_if_not_exists(db_conn)
        logging.info("Successfully connected to Neon PostgreSQL. Destination schema verified.")
    except Exception as conn_err:
        logging.critical(f"Could not connect to PostgreSQL database. Error: {conn_err}")
        sys.exit(1)

    # 2. Configure Kafka Consumer Options
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'SSL',
        'ssl.ca.location': 'ssl_credentials/ca.pem',
        'ssl.certificate.location': 'ssl_credentials/service.cert',
        'ssl.key.location': 'ssl_credentials/service.key',
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',  # Automatically grab back-logged burst data
        'enable.auto.commit': True,
        'session.timeout.ms': 6000
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC_NAME])
        logging.info(f"Kafka Consumer Engine Subscribed to Topic: '{TOPIC_NAME}'. Listening for backlogged message stream...")
    except Exception as init_err:
        logging.critical(f"Could not build Kafka consumer interface. Error: {init_err}")
        db_conn.close()
        sys.exit(1)

    # --- SERVERLESS DRAIN TRACKING ENGINE ---
    last_message_received_time = time.time()
    processed_message_count = 0

    try:
        while True:
            # Poll for incoming data packets with a 1.0-second network window
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # Calculate how long the stream has been empty
                idle_duration = time.time() - last_message_received_time
                if idle_duration >= MAX_IDLE_TIMEOUT_SECONDS:
                    logging.info(f"Topic drain window exceeded. No new data streams observed for {int(idle_duration)}s.")
                    logging.info("Ecosystem indicates data catch-up phase complete.")
                    break
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Kafka Consumer Error Event: {msg.error()}")
                    continue

            # A valid message was located! Reset the timeout heartbeat clock
            last_message_received_time = time.time()
            processed_message_count += 1

            try:
                raw_payload = msg.value().decode('utf-8')
                reading_data = json.loads(raw_payload)
                
                insert_telemetry_record(db_conn, reading_data)
                
                if reading_data.get("tvoc_ppb", 0) >= 120.0:
                    logging.warning(f"[ALARM STAGE] Elevated VOC detected at {reading_data['sensor_id']}: {reading_data['tvoc_ppb']} ppb")
                    
            except Exception as parse_err:
                logging.error(f"Skipping malformed payload. Parsing Error: {parse_err}")

    except KeyboardInterrupt:
        logging.info("Manual termination intercept triggered.")
    finally:
        logging.info(f"Draining phase finished. Total payload entries ingested and relationalized: {processed_message_count}")
        consumer.close()
        db_conn.close()
        logging.info("Ecosystem clean shutdown verified. Runner workspace offline.")

if __name__ == "__main__":
    run_consumer()
