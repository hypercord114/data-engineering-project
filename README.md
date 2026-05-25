Slowly putting together a data engineering project to showcase.

As of 2026-05-21, Github Actions triggers a morning data ELT and ETL pipeline for collection of weather data from the free Open-Meteo API.
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

Project should be demonstrative of ability to develop ETL/ELT pipelines, manage MySQL database, utilize Kafka, utilize Airflow, and automate publication of data to a dashboard.

If I can get this working properly, I will probably attempt another project incorporating Spark and ML algorithms where more statistics are used, such as regression/trend analysis, etc.

May also attempt to utilize a free online Dashboard module, instead of simply generating HTML, and may attempt to generate a pre-shift email with predicted measurements in it.  In the case of use of an online Dashboard module, will attempt to store data as parquet file on service outside of Github.

2026-05-22; looks like there are no free, serverless, Airflow services online.  Dang.  I think that was the most interesting part of the Data Engineering stuff I've learned so far, setting up DAGs.  Oh well...  Will need to use GitHub Actions alone then.  Well, I tried to demonstrate that I understand the need for Airflow.

I've started setting up Kafka service and it seems to work so far.  May go back and store the JSON files collected during the ELT step in a PSQL database instead of just staging them, just to demonstrate I can do that.

Still need to flesh out the star schema for the set of tables holding all the TVOC measurements for the day.

This environmental related project may turn noses.  Should probably do something financial related so that it will pique more interest...  This was just the first thing that came to my mind.

+++++++++++++++++++++++++++++++++++++++++++++++++++++++

2026-05-25; well i think the entire pipeline works.  it works when i manually trigger the GitHub Actions anyway.  there are certainly kinks and idempotency issues to work out, but i'm going to leave it alone for now.

i will create a more eloquent description soon, but BASICALLY what i'm doing is this:

MORNING PIPELINE - triggered by YAML file at 4:13AM New York time
+ morning_extract_and_load.py: collects Open Meteo API and stages it in raw_json/ folder.  also uploads unformatted JSONB object into PostgreSQL database hosted on Neon.com.  writes logging info to morning_pipeline.log in pipeline-logs/ folder.
+ morning_etl.py: reads JSONB file from Neon.com, calculates predicted average value for day's temperature, humidity and wind direction.  appropriate calculation used for average wind direction calculation.  relevant data is input to DuckDB database table for shift forecasts.  writes logging info to morning_pipeline.log in pipeline-logs/ folder.
+ generate dashboard.py: queries DuckDB shift forecast table, parses out values for the current date, and write an HTML webpage.  this script only posts the predicted values for the day in cells at the top of the webpage.  writes logging info to morning_pipeline.log in pipeline-logs/ folder.

KAFKA PIPELINE - triggered every 2 hours starting at 9:00AM New York time; consumer script YAML is dependent on producer script YAML
+ pid_producer.py: calls Open Meteo API and extracts current wind direction, then generates simulated TVOC measurements for eight PID nodes at all eight cardinal directions (N, NE, E, SE, S, SW, W, NW).  PID nodes downwind of current wind direction are subject to increased chance of elevated TVOC measurements, simulating real effect of wind direction of excation where TVOC vapors are released.  the PID data is fed to KAFKA server producer in bursts, 120 measurements per instrument, simulating two hours of measurements.  writes logging info to kafka_pipeline.log in pipeline-logs/ folder.
+ pid_consumer.py: accesses Kafka producer, listens, and collects either all messages from earliest message from the day or all messages since last group login.  raw message data is superficially parsed and fed into Neon.com PostgreSQL server.  a table is created specific to the current date and the raw message data is appended to that table for the duraction of the current day.  writes logging info to consumer_pipeline.log in pipeline-logs/ folder.

AFTERNOON PIPELINE - triggered at 6:18PM New York time
+ afternoon_extract_and_load.py: same as morning_extract_and_load.py, except the API call pulls all of the observed weather measurements for the day.  stages raw JSON file in raw_json/ folder, then uploads JSONB file to Neon.com PostgreSQL database under table name shift-weather-observed.  writes logging info to afternoon_pipeline.log in pipeline-logs/ folder.
+ afternoon_etl.py: big meaty file.  sets up star schema fact tables and dimension tables for each PID node (need to adjust this, don't need to adjust most of the dim tables cyclically) in DuckDB database, reads raw measurement data from current day's PID measurement table on Neon.com PostgreSQL database, extrapolates data into instrument specific datasets, then populates the instrument specific fact tables in DuckDB database.  need to toy with adjusting the calibration tables cyclically but leaving the other dim tables as static data (instrument specs and location info).  finally, once all the data is extracted from the PostgreSQL database, the Kafka topic is reset for the following day.  writes logging info to afternoon_pipeline.log in pipeline-logs/ folder.
+ afternoon_generate_dashboard.py: reads all the instrument specific measurements for today's date out of the fact tables in DuckDB database as well as the observed weather measurements from the observed table in the DuckDB database and updates the index.html webpage with line graphs for each instrument and values for the observed weather measurements.  writes logging info to afternoon_pipeline.log in pipeline-logs/ folder.

