import duckdb

def dump_database_to_txt():
    db_file = "environmental_data.db"
    output_file = "database_dump.txt"
    
    print(f"Opening {db_file}...")
    # Connect to the database file in read-only mode to prevent any accidental edits
    connection = duckdb.connect(db_file, read_only=True)
    
    try:
        # Fetch all columns and rows from the shift forecast table
        query = "SELECT * FROM daily_shift_forecasts ORDER BY forecast_date DESC;"
        result = connection.execute(query).fetchall()
        
        # Get column names so our text file has clear headers
        headers = [desc[0] for desc in connection.description]
        
        print(f"Writing contents to {output_file}...")
        with open(output_file, "w", encoding="utf-8") as f:
            # Write a clean title banner
            f.write("==================================================\n")
            f.write("      ENVIRONMENTAL DATABASE RECORD DUMP         \n")
            f.write("==================================================\n\n")
            
            if not result:
                f.write("The database table 'daily_shift_forecasts' is currently empty.\n")
            else:
                # Loop through each record and print them out clearly line-by-line
                for row in result:
                    f.write(f"--- Record Date: {row[0]} ---\n")
                    for header, value in zip(headers, row):
                        # Skip re-printing the date inside the block for cleaner reading
                        if header == "forecast_date":
                            continue
                        f.write(f"  {header}: {value}\n")
                    f.write("\n") # Add a spacing line between records
                    
        print("Export complete! Open 'database_dump.txt' to view your records.")
        
    except duckdb.CatalogException:
        print("Error: The table 'daily_shift_forecasts' does not exist in this database file yet.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    dump_database_to_txt()