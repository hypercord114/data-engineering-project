Slowly putting together a data engineering project to showcase.

As of 2026-05-20, Github Actions triggers a morning data ELT and ETL pipeline for collection of weather data from the free Open-Meteo API.
morning_extract_and_load.py script collects JSON file and stores it in pseudo Data Lake, within Github.
morning_etl.py script parses the JSON file, truncates it to the period 07:00-17:00, calculates the average temperature, humidity and wind direction for the working day, and loads data into a DuckDB MySQL database.
Average wind direction is calculated properly using unit conversion to radians and trigonometry.
generate_dashboard.py script parses data from DuckDB table, creates an HTML dashboard and writes it to index.html for viewing.
All process notes are logged in morning_pipeline.log file.
Pipeline is triggered by YAML file/Github Actions set for 4:00AM EST.
index.html file is viewable at:  https://hypercord114.github.io/data-engineering-project/

Next, will set up free Kafka cluster on Aiven.io and Github hosted Python script that will relay simulated TVOC measurements from environmental monitors to Kafka producer.
Script will be triggered every 3 hours with YAML file to generate a backlog of 3 hours of continuous TVOC measurements.
Consumer will be established to catch up on Producer data as well, collect data in batches and write to DuckDB MySQL database.
DuckDB database schema will be designed in star method, with TVOC measurements stored in fact table and qualifier data stored in decoupled tables.

YAML file will be used to trigger another set of scripts at 17:00 to perform final data collection, data cleaning, and augmentation of dashboard with 6 line graphs for each monitoring instrument.  Observed environmental measurements will be posted as well, in comparison to the pre-shift predictions.

May attempt to integrate a free Airflow service in order to collect new weather API's when they are published and then weight the monitoring data according to the observed wind direction for the previous 3 hours, simulating real spikes in measurements at the downwind location.

May also attempt to utilize a free online Dashboard module, instead of simply generating HTMl, and may attempt to generate a pre-shift email with predicted measurements in it.  In the case of use of an online Dashboard module, will attempt to store data as parquet file on service outside of Github.

Project should be demonstrative of ability to develop ETL/ELT pipelines, manage MySQL database, utilize Kafka, utilize Airflow, and automate publication of data to a dashboard.

If I can get this working properly, I will probably attempt another project incorporating Spark and ML algorithms where more statistics are used, such as regression/trend analysis, etc.
