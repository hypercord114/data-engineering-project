import duckdb
import os
import logging
import sys

# --- LOGGING CONFIGURATION ---
log_dir = "pipeline-logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "duckdb-export.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def export_all_tables_to_csv(output_dir="exported_data"):
    """
    Connects to DuckDB and exports all tables found in 'main' schema to CSV files.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    db_path = "environmental_data.db" 
    
    try:
        logging.info(f"Connecting to DuckDB: {db_path}")
        duck_conn = duckdb.connect(database=db_path, read_only=True)
        
        # Get list of all tables
        tables = duck_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        
        if not tables:
            logging.info("No tables found in the database.")
            return

        for table in tables:
            table_name = table[0]
            output_file = os.path.join(output_dir, f"{table_name}.csv")
            
            logging.info(f"Exporting '{table_name}' to {output_file}...")
            
            # The COPY command is the fastest way to export in DuckDB
            duck_conn.execute(f"COPY {table_name} TO '{output_file}' (HEADER, DELIMITER ',')")
            
        duck_conn.close()
        logging.info("All tables exported successfully.")

    except Exception as e:
        logging.error(f"Failed to export tables: {e}")

if __name__ == "__main__":
    export_all_tables_to_csv()