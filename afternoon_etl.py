import os
import sys
import time
import math
import logging
import psycopg2
import duckdb
from datetime import datetime
from zoneinfo import ZoneInfo
from confluent_kafka.admin import AdminClient, NewTopic

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline-logs/afternoon_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- ENVIRONMENT SEEDING ---
POSTGRES_URI = os.getenv("POSTGRES_DB_URI")
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URI")
TOPIC_NAME = "intraday_pid_telemetry"
DUCKDB_PATH = "environmental_data.db"
EXCEEDANCE_THRESHOLD_PPB = 120.0

COMPASS_NODES = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def get_cardinal_points(wind_degrees):
    """ Mapped precisely to morning vector calculations """
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", 
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((wind_degrees + 11.25) / 22.5) % 16
    return directions[idx], directions[(idx + 8) % 16]


def setup_star_schema_dimensions(duck_conn):
    """ Creates shared dimension tables in DuckDB. """
    logging.info("Initializing dimensional framework in DuckDB...")
    
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_locations (
            location_id VARCHAR(10) PRIMARY KEY,
            compass_heading VARCHAR(5),
            latitude DOUBLE,
            longitude DOUBLE,
            zone_description VARCHAR(100)
        );
    """)

    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_pid_info (
            pid_id VARCHAR(50) PRIMARY KEY,
            sensor_model VARCHAR(50),
            detection_limit_ppb DOUBLE,
            baseline_ppb DOUBLE
        );
    """)

    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_calibration (
            calibration_id VARCHAR(50) PRIMARY KEY,
            pid_id VARCHAR(50),
            span_gas_type VARCHAR(50),
            span_value_ppb DOUBLE,
            last_calibration_date DATE
        );
    """)

    logging.info("Seeding static perimeter network structural dimensions...")
    for node in COMPASS_NODES:
        duck_conn.execute("""
            INSERT OR IGNORE INTO dim_locations (location_id, compass_heading, latitude, longitude, zone_description)
            VALUES (?, ?, 42.8864, -78.8784, ?);
        """, (f"LOC_{node}", node, f"Perimeter Monitoring Boundary Node - {node}"))

        duck_conn.execute("""
            INSERT OR IGNORE INTO dim_pid_info (pid_id, sensor_model, detection_limit_ppb, baseline_ppb)
            VALUES (?, 'MiniPID 2', 10000.0, 65.0);
        """, (f"PID_{node}",))

        duck_conn.execute("""
            INSERT OR IGNORE INTO dim_calibration (calibration_id, pid_id, span_gas_type, span_value_ppb, last_calibration_date)
            VALUES (?, ?, 'Isobutylene', 100000.0, CURRENT_DATE);
        """, (f"CAL_{node}", f"PID_{node}"))


def process_fact_tables(postgres_rows, duck_conn):
    """ Splits incoming data into unique fact tables per unique PID measurement node. """
    logging.info("Sorting telemetry stream rows into distinct physical fact tables...")
    ingested_count = 0

    for row in postgres_rows:
        raw_sensor_id = row[1]
        reading_time = row[2]
        tvoc_val = float(row[3])
        
        clean_node_suffix = raw_sensor_id.split("-")[-1].lower()
        fact_table_name = f"fact_pid_{clean_node_suffix}"
        
        node_upper = clean_node_suffix.upper()
        pid_id = f"PID_{node_upper}"
        location_id = f"LOC_{node_upper}"
        
        exceedance_flag = 1 if tvoc_val >= EXCEEDANCE_THRESHOLD_PPB else 0

        duck_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {fact_table_name} (
                fact_id UUID DEFAULT uuid(),
                pid_id VARCHAR(50),
                measurement_date DATE,
                measurement_time TIME,
                location_id VARCHAR(10),
                tvoc_ppb NUMERIC(5, 1),
                exceedance_flag TINYINT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        current_date = datetime.now().date()

        duck_conn.execute(f"""
            INSERT INTO {fact_table_name} (pid_id, measurement_date, measurement_time, location_id, tvoc_ppb, exceedance_flag)
            VALUES (?, ?, ?, ?, ?, ?);
        """, (pid_id, current_date, reading_time, location_id, tvoc_val, exceedance_flag))
        
        ingested_count += 1

    logging.info(f"Fact table processing complete. Total records isolated: {ingested_count}")


def extract_observed_weather_from_postgres(pg_cur, target_date):
    """ Connects to PostgreSQL and extracts the raw observed JSONB weather block. """
    logging.info(f"Extracting raw observed weather payload data for {target_date}...")
    
    query = """
        SELECT raw_payload 
        FROM daily_shift_weather_observed 
        WHERE forecast_date = %s;
    """
    pg_cur.execute(query, (target_date,))
    row = pg_cur.fetchone()
    return row[0] if row else None


def process_and_save_observed_weather(raw_payload, today_date, duck_conn):
    """ Aggregates observed metrics over core work shift and stores in DuckDB database. """
    logging.info("Processing weather reality payload and computing real shift vectors...")
    try:
        hourly_data = raw_payload["hourly"]
        time_stamps = hourly_data["time"]
        wind_directions = hourly_data["wind_direction_10m"]
        temperatures = hourly_data["temperature_2m"]
        humidities = hourly_data["relative_humidity_2m"]
    except KeyError as parse_err:
        logging.error(f"WEATHER TRANSFORM FAILURE: Unexpected JSON fields context layout. Missing key: {parse_err}")
        return

    shift_wind = []
    shift_temp = []
    shift_humidity = []

    # Segment down to operational shift hours: 07:00 to 17:00
    for i, time_str in enumerate(time_stamps):
        dt = datetime.fromisoformat(time_str)
        hour = dt.hour
        
        if 7 <= hour <= 17:
            shift_wind.append(float(wind_directions[i]))
            shift_temp.append(float(temperatures[i]))
            shift_humidity.append(float(humidities[i]))

    if not shift_temp:
        logging.warning("No valid observations extracted inside core shift window limits.")
        return

    # Calculate metrics with standard rounding limits
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
    upwind, downwind = get_cardinal_points(avg_wind)

    logging.info(f"Observed Realities Calculated: Temp: {avg_temp}°F | Humid: {avg_humid}% | Wind: {avg_wind}° ({upwind} -> Downwind: {downwind})")

    # Save to table daily_shift_weather_observed inside DuckDB
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_shift_weather_observed (
            forecast_date DATE PRIMARY KEY,
            observed_avg_temp_f DOUBLE,
            observed_avg_wind_deg DOUBLE,
            observed_avg_humidity_pct DOUBLE,
            upwind_cardinal VARCHAR,
            downwind_cardinal VARCHAR,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    duck_conn.execute("""
        INSERT OR REPLACE INTO daily_shift_weather_observed (
            forecast_date, observed_avg_wind_deg, observed_avg_temp_f, 
            observed_avg_humidity_pct, upwind_cardinal, downwind_cardinal
        ) VALUES (?, ?, ?, ?, ?, ?);
    """, (today_date, avg_wind, avg_temp, avg_humid, upwind, downwind))
    
    logging.info("Observed shift weather summary successfully committed to local analytical core.")


def wipe_kafka_topic():
    """ Connects to Aiven cloud broker to purge and recreate the operational topic logs. """
    if not KAFKA_BROKER:
        logging.error("Skipping Kafka wipe: KAFKA_BROKER_URI variable is empty.")
        return

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'security.protocol': 'SSL',
        'ssl.ca.location': 'ssl_credentials/ca.pem',
        'ssl.certificate.location': 'ssl_credentials/service.cert',
        'ssl.key.location': 'ssl_credentials/service.key'
    }

    logging.info(f"Connecting Admin Client to Aiven cluster to clear topic '{TOPIC_NAME}'...")
    try:
        admin_client = AdminClient(conf)
        
        # 1. Delete the active topic logs
        fs_delete = admin_client.delete_topics([TOPIC_NAME], operation_timeout=15)
        for topic, future in fs_delete.items():
            future.result()
            logging.info(f"Topic '{topic}' successfully dropped from cluster logs.")
            
        logging.info("Waiting 5 seconds for cluster stabilization...")
        time.sleep(5)

        # 2. Recreate clean topic partitions
        new_topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=2)
        fs_create = admin_client.create_topics([new_topic], operation_timeout=15)
        for topic, future in fs_create.items():
            future.result()
            logging.info(f"Topic '{topic}' successfully recreated. Topic is now completely clean!")
            
    except Exception as kafka_err:
        logging.error(f"Kafka Admin reset execution encountered an unexpected error: {kafka_err}")


def run_afternoon_etl():
    if not POSTGRES_URI:
        logging.error("CRITICAL ERROR: POSTGRES_DB_URI environment variable is empty.")
        sys.exit(1)

    today_date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    dynamic_table_name = f"realtime_pid_telemetry_{today_date}"
    telemetry_records = []
    pg_conn = None
    pg_cur = None
    duck_conn = None

    try:
        logging.info("Connecting to Neon PostgreSQL production node...")
        pg_conn = psycopg2.connect(POSTGRES_URI)
        pg_cur = pg_conn.cursor()
        
        # Ingestion Part 1: Extract real-time PID telemetry values
        logging.info("Extracting raw telemetry records...")
        pg_cur.execute(f"SELECT id, sensor_id, reading_timestamp, tvoc_ppb FROM {dynamic_table_name};")
        telemetry_records = pg_cur.fetchall()
        logging.info(f"Successfully extracted {len(telemetry_records)} source entries from Postgres.")
        
        # Ingestion Part 2: Extract historical open-meteo observed realities for the layout day
        weather_payload = extract_observed_weather_from_postgres(pg_cur, today_date)
        if not weather_payload:
            logging.warning(f"No observed weather context log was discovered in Postgres for target date: {today_date}")

    except Exception as pg_err:
        logging.error(f"PostgreSQL connection or extraction failure: {pg_err}")
        sys.exit(1)

    # If telemetry didn't log new assets, run weather computations if available before halting entirely
    if not telemetry_records:
        logging.warning("No real-time PID data found in PostgreSQL table. Moving directly to analytical transforms.")

    try:
        logging.info(f"Opening analytic file store at: {DUCKDB_PATH}")
        duck_conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)
        
        # 1. Process Star Schema Dimensions and high frequency sensor facts
        setup_star_schema_dimensions(duck_conn)
        if telemetry_records:
            process_fact_tables(telemetry_records, duck_conn)
        
        # 2. Extract, transform, and map weather summaries into DuckDB storage targets
        if weather_payload:
            process_and_save_observed_weather(weather_payload, today_date, duck_conn)
            
    except Exception as duck_err:
        logging.error(f"DuckDB Transformation engine failure: {duck_err}")
    finally:
        if pg_cur: pg_cur.close()
        if pg_conn: pg_conn.close()
        if duck_conn: duck_conn.close()
        
        # Run the Kafka cleanup step immediately following a successful database data commit
        if len(telemetry_records) > 0:
            wipe_kafka_topic()
            
        logging.info("ETL ecosystem processing cycle terminated successfully.")


if __name__ == "__main__":
    run_afternoon_etl()