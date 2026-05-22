Slowly putting together a data engineering project to showcase.

As of 2026-05-20, Github Actions triggers a morning data ELT and ETL pipeline for collection of weather data from the free Open-Meteo API.
morning_extract_and_load.py script collects JSON file and stores it in pseudo Data Lake, within Github.
morning_etl.py script parses the JSON file, truncates it to the period 07:00-17:00, calculates the average temperature, humidity and wind direction for the working day, and loads data into a DuckDB MySQL database.
Average wind direction is calculated properly using unit conversion to radians and trigonometry.
generate_dashboard.py script parses data from DuckDB table, creates an HTML dashboard and writes it to index.html for viewing.
All process notes are logged in morning_pipeline.log file.
Pipeline is triggered by YAML file/Github Actions set for 4:00AM EST.
index.html file is viewable at:  
