from etl.config import logger
from etl.flights import run_extract_flights_phase, run_transform_flights_phase
from etl.weather import run_weather_phase
from etl.database import run_load_phase

def main():
    logger.info("Starting full ETL pipeline...")
    run_extract_flights_phase()
    run_transform_flights_phase()
    run_weather_phase()
    run_load_phase()
    logger.info("Pipeline execution finished.")

if __name__ == "__main__":
    main()