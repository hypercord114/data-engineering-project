import duckdb
import logging
import sys
import os

# --- LOGGING CONFIGURATION ---
log_dir = "pipeline-logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "duckdb-data-cleaning.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_deduplication():
    """
    Connects to DuckDB and cleans duplicates from fact tables.
    """
    db_path = "environmental_data.db" 
    
    try:
        logging.info("Starting DuckDB cleanup process...")
        duck_conn = duckdb.connect(database=db_path, read_only=False)
        suffixes = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

        for suffix in suffixes:
            fact_table_name = f"fact_pid_{suffix}"
            
            # Check if table exists
            table_exists = duck_conn.execute(
                f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{fact_table_name}'"
            ).fetchone()[0] > 0

            if not table_exists:
                logging.info(f"Skipping {fact_table_name}: Table does not exist.")
                continue

            logging.info(f"Initiating deduplication for: {fact_table_name}...")

            # Deduplication query: Partition by measurement columns, keep earliest fact_id
            dedup_query = f"""
                DELETE FROM {fact_table_name}
                WHERE fact_id IN (
                    SELECT fact_id FROM (
                        SELECT fact_id, 
                               ROW_NUMBER() OVER (
                                   PARTITION BY pid_id, measurement_date, measurement_time, 
                                                location_id, tvoc_ppb, exceedance_flag 
                                   ORDER BY fact_id ASC
                               ) as rn
                        FROM {fact_table_name}
                    ) 
                    WHERE rn > 1
                );
            """
            
            duck_conn.execute(dedup_query)
            removed_count = duck_conn.execute("SELECT changes()").fetchone()[0]
            logging.info(f"Cleanup complete for {fact_table_name}. Rows removed: {removed_count}")

        duck_conn.close()
        logging.info("Deduplication process finished successfully.")

    except Exception as e:
        logging.error(f"Critical failure during deduplication: {e}")

if __name__ == "__main__":
    run_deduplication()