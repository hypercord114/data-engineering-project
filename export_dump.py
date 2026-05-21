import duckdb

def dump_database_to_txt():
    db_file = "environmental_data.db"
    output_file = "database_dump.txt"
    
    print(f"Opening {db_file}...")
    connection = duckdb.connect(db_file, read_only=True)
    
    try:
        # CHANGED: We explicitly select and convert 'created_at' from UTC to New York timezone
        query = """
            SELECT 
                forecast_date,
                predicted_avg_wind_deg,
                predicted_avg_temp_f,
                predicted_avg_humidity_pct,
                upwind_cardinal,
                downwind_cardinal,
                created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS created_at
            FROM daily_shift_forecasts 
            ORDER BY forecast_date DESC;
        """
        result = connection.execute(query).fetchall()
        
        # Get column names so our text file has clear headers
        headers = [desc[0] for desc in connection.description]
        
        print(f"Writing contents to {output_file}...")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("==================================================\n")
            f.write("      ENVIRONMENTAL DATABASE RECORD DUMP         \n")
            f.write("==================================================\n\n")
            
            if not result:
                f.write("The database table 'daily_shift_forecasts' is currently empty.\n")
            else:
                for row in result:
                    f.write(f"--- Record Date: {row[0]} ---\n")
                    for header, value in zip(headers, row):
                        if header == "forecast_date":
                            continue
                        f.write(f"  {header}: {value}\n")
                    f.write("\n")
                    
        print("Export complete! Open 'database_dump.txt' to view your records.")
        
    except duckdb.CatalogException:
        print("Error: The table 'daily_shift_forecasts' does not exist in this database file yet.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    dump_database_to_txt()
