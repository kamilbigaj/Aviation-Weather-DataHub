# AVIATION & WEATHER ETL PIPELINE

import requests
import pandas as pd
import json
import os
import time
import boto3
from datetime import datetime, timedelta
from meteostat import Point, Hourly
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

# 1. PROFESSIONAL LOGGER CONFIGURATION
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

AERO_API_KEY = os.getenv('AERO_API_KEY')
AIRPORTS = ['EPWA', 'EGLL', 'EDDF']
RAW_DIR = "data/raw"

logger.info("Libraries loaded. Environment variables secured and ready.")


# 2. DATA LAKE: AWS S3 Upload Function
def upload_raw_to_s3(data, file_prefix):
    """Persists raw JSON payloads directly to AWS S3 (Data Lake layer)."""
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not bucket_name:
        logger.warning("S3_BUCKET_NAME not found in .env. Skipping S3 upload.")
        return

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )

        today_str = datetime.now().strftime('%Y-%m-%d')
        file_name = f"raw_data/{today_str}/{file_prefix}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data)
        )
        logger.info(f"SUCCESS (S3): Raw data saved to Data Lake -> s3://{bucket_name}/{file_name}")
    except Exception as e:
        logger.error(f"FAILED (S3): Error uploading file {file_prefix}. Reason: {e}")


# 3. ROBUST API FETCH FUNCTIONS (RESILIENCE)
# @retry decorator ensures that in case of a network error, the script
# waits 10 seconds and retries. Max 3 attempts.
@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def fetch_flight_data_from_api(airport, time_from, time_to, headers):
    """Fetches data from AeroDataBox with built-in retry mechanism."""
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{airport}/{time_from}/{time_to}"
    querystring = {"withLeg": "true", "direction": "Arrival", "withCancelled": "false", "withCodeshared": "true",
                   "withCargo": "false", "withPrivate": "false", "withLocation": "false"}

    response = requests.get(url, headers=headers, params=querystring)

    # CRITICAL: This method forces an exception if the status is e.g., 500 or 429.
    # This ensures the 'tenacity' library knows it must retry!
    response.raise_for_status()

    return response.json()


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def fetch_weather_data_from_api(location, start_time, end_time):
    """Fetches historical data from Meteostat with built-in retry mechanism."""
    data = Hourly(location, start_time, end_time)
    return data.fetch()


# 4. MAIN ETL PROCESSING PHASES
def run_extract_flights_phase():
    logger.info("STARTING PHASE: EXTRACT (Flights)")
    os.makedirs(RAW_DIR, exist_ok=True)
    today_str = datetime.now().strftime('%Y-%m-%d')
    time_from, time_to = f"{today_str}T00:00", f"{today_str}T11:59"
    headers = {"X-RapidAPI-Key": AERO_API_KEY, "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}

    for airport in AIRPORTS:
        local_file = os.path.join(RAW_DIR, f"aero_{airport}_{today_str}.json")
        file_prefix = f"aero_{airport}"

        if os.path.exists(local_file):
            logger.info(f"Cache found: {local_file}. Skipping API call.")
        else:
            logger.info(f"Fetching data for airport: {airport}...")
            try:
                # Using our robust function with @retry
                json_data = fetch_flight_data_from_api(airport, time_from, time_to, headers)

                # Local save
                with open(local_file, 'w', encoding='utf-8') as file:
                    json.dump(json_data, file, ensure_ascii=False, indent=4)

                # Save to AWS S3
                upload_raw_to_s3(json_data, file_prefix)

                # Rate limit protection
                time.sleep(2)
            except Exception as e:
                logger.error(f"Critical error fetching data for {airport} after 3 attempts: {e}")


def run_transform_flights_phase():
    logger.info("STARTING PHASE: TRANSFORM (Flights in Pandas)")
    today_str = datetime.now().strftime('%Y-%m-%d')
    dataframes_list = []

    for airport in AIRPORTS:
        local_file = os.path.join(RAW_DIR, f"aero_{airport}_{today_str}.json")
        try:
            with open(local_file, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            flights_list = json_data.get('arrivals', [])
            if not flights_list:
                continue
            temp_df = pd.json_normalize(flights_list)
            temp_df['arrival_airport'] = airport
            dataframes_list.append(temp_df)
        except FileNotFoundError:
            logger.warning(f"Missing file {local_file}. Run EXTRACT phase first!")

    if dataframes_list:
        raw_flights_df = pd.concat(dataframes_list, ignore_index=True)
        column_mapping = {
            'number': 'flight_number', 'status': 'flight_status', 'airline.name': 'airline',
            'departure.airport.iata': 'departure_airport', 'arrival_airport': 'arrival_airport',
            'arrival.scheduledTime.utc': 'scheduled_arrival', 'arrival.revisedTime.utc': 'actual_arrival'
        }
        existing_columns = [col for col in column_mapping.keys() if col in raw_flights_df.columns]
        clean_flights_df = raw_flights_df[existing_columns].rename(columns=column_mapping)

        if 'flight_status' in clean_flights_df.columns:
            landed_flights = clean_flights_df[clean_flights_df['flight_status'] == 'Arrived'].copy()
        else:
            landed_flights = clean_flights_df.copy()

        if 'scheduled_arrival' in landed_flights.columns:
            landed_flights['scheduled_arrival'] = pd.to_datetime(landed_flights['scheduled_arrival'], utc=True)
        if 'actual_arrival' in landed_flights.columns:
            landed_flights['actual_arrival'] = pd.to_datetime(landed_flights['actual_arrival'], utc=True)
            landed_flights.dropna(subset=['actual_arrival', 'scheduled_arrival'], inplace=True)

        if 'scheduled_arrival' in landed_flights.columns and 'actual_arrival' in landed_flights.columns:
            landed_flights['delay_minutes'] = (landed_flights['actual_arrival'] - landed_flights[
                'scheduled_arrival']).dt.total_seconds() / 60
            landed_flights['delay_minutes'] = landed_flights['delay_minutes'].apply(lambda x: x if x > 0 else 0).astype(
                int)
        else:
            landed_flights['delay_minutes'] = 0

        landed_flights.to_csv('clean_flights.csv', index=False)
        logger.info(f"Transformation successful! Saved {len(landed_flights)} rows to clean_flights.csv.")
    else:
        logger.warning("No flight data to process.")


def run_weather_phase():
    logger.info("STARTING PHASE: EXTRACT & TRANSFORM (Weather)")
    airport_locations = {
        'EPWA': Point(52.1657, 20.9671, 110),
        'EGLL': Point(51.4700, -0.4543, 25),
        'EDDF': Point(50.0333, 8.5705, 111)
    }
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    weather_frames = []

    for airport_code, location in airport_locations.items():
        logger.info(f"Fetching historical weather for airport: {airport_code}...")
        try:
            # Using our robust function with @retry
            data = fetch_weather_data_from_api(location, start_time, end_time)
            if not data.empty:
                df_airport = data.reset_index()
                df_airport['airport_code'] = airport_code
                weather_frames.append(df_airport)
        except Exception as e:
            logger.error(f"Critical error fetching weather for {airport_code}: {e}")

    if weather_frames:
        weather_df = pd.concat(weather_frames, ignore_index=True)
        columns_to_keep = ['airport_code', 'time', 'temp', 'prcp', 'wspd', 'coco']
        weather_df = weather_df[columns_to_keep]
        weather_df.rename(columns={
            'time': 'weather_timestamp', 'temp': 'temperature_c', 'prcp': 'precipitation_mm',
            'wspd': 'wind_speed_kmh', 'coco': 'condition_code'
        }, inplace=True)
        weather_df['precipitation_mm'] = weather_df['precipitation_mm'].fillna(0)

        weather_df.to_csv('clean_weather.csv', index=False, encoding='utf-8')
        logger.info(f"Weather processed! Saved {len(weather_df)} rows to clean_weather.csv.")
    else:
        logger.warning("No weather data generated.")


def run_load_phase():
    logger.info("STARTING PHASE: LOAD (Data Warehouse AWS RDS)")
    db_url = os.getenv('DATABASE_URL')

    if not db_url:
        logger.error("DATABASE_URL missing in .env! Aborting data load.")
        return

    try:
        engine = create_engine(db_url)

        # IDEMPOTENT DATA LOADING: FLIGHTS
        if os.path.exists('clean_flights.csv'):
            flights_df = pd.read_csv('clean_flights.csv')
            if not flights_df.empty:
                # 1. Create target table with UNIQUE CONSTRAINT if it doesn't exist
                with engine.begin() as conn:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS flights (
                            flight_number VARCHAR,
                            flight_status VARCHAR,
                            airline VARCHAR,
                            departure_airport VARCHAR,
                            arrival_airport VARCHAR,
                            scheduled_arrival TIMESTAMP WITH TIME ZONE,
                            actual_arrival TIMESTAMP WITH TIME ZONE,
                            delay_minutes FLOAT,
                            UNIQUE (flight_number, scheduled_arrival)
                        );
                    """))

                # 2. Load data into a temporary staging table
                flights_df.to_sql('stg_flights', engine, if_exists='replace', index=False)

                # 3. Perform a safe UPSERT (Insert ON CONFLICT DO NOTHING)
                with engine.begin() as conn:
                    columns = ", ".join(flights_df.columns)
                    conn.execute(text(f"""
                        INSERT INTO flights ({columns})
                        SELECT {columns} FROM stg_flights
                        ON CONFLICT (flight_number, scheduled_arrival) DO NOTHING;
                    """))
                    # Clean up staging table
                    conn.execute(text("DROP TABLE stg_flights;"))
                    logger.info(f"Success: {len(flights_df)} flights processed idempotently.")

        # IDEMPOTENT DATA LOADING: WEATHER
        if os.path.exists('clean_weather.csv'):
            weather_df = pd.read_csv('clean_weather.csv')
            if not weather_df.empty:
                # 1. Create target table with UNIQUE CONSTRAINT
                with engine.begin() as conn:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS weather (
                            airport_code VARCHAR,
                            weather_timestamp TIMESTAMP WITH TIME ZONE,
                            temperature_c FLOAT,
                            precipitation_mm FLOAT,
                            wind_speed_kmh FLOAT,
                            condition_code FLOAT,
                            UNIQUE (airport_code, weather_timestamp)
                        );
                    """))

                # 2. Load data into a temporary staging table
                weather_df.to_sql('stg_weather', engine, if_exists='replace', index=False)

                # 3. Perform UPSERT
                with engine.begin() as conn:
                    columns = ", ".join(weather_df.columns)
                    conn.execute(text(f"""
                        INSERT INTO weather ({columns})
                        SELECT {columns} FROM stg_weather
                        ON CONFLICT (airport_code, weather_timestamp) DO NOTHING;
                    """))
                    # Clean up staging table
                    conn.execute(text("DROP TABLE stg_weather;"))
                    logger.info(f"Success: {len(weather_df)} weather records processed idempotently.")

        logger.info("ETL PIPELINE EXECUTED SUCCESSFULLY!")
    except Exception as e:
        logger.error(f"CRITICAL ERROR during database load: {e}")


# 5. ORCHESTRATION (Script Execution)
if __name__ == "__main__":
    logger.info("Starting full ETL process...")
    run_extract_flights_phase()
    run_transform_flights_phase()
    run_weather_phase()
    run_load_phase()