# ENVIRONMENT SETUP & CONFIGURATION

import requests
import pandas as pd
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environmental configurations from the local secure .env file
load_dotenv()

# Initialize API credentials and project scopes
AERO_API_KEY = os.getenv('AERO_API_KEY')
AIRPORTS = ['EPWA', 'EGLL', 'EDDF']

print("Libraries loaded. AeroDataBox API key secured and ready.")


# PHASE: EXTRACT (Flight Data Ingestion via AeroDataBox API)

import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
AERO_API_KEY = os.getenv('AERO_API_KEY')
AIRPORTS = ['EPWA', 'EGLL', 'EDDF']

print("Starting AeroDataBox data extraction...\n")

# Establish landing zone directory for immutable raw data storage (Data Lake pattern)
RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

# Generate current timestamps for dynamic FIDS tracking windows
today_str = datetime.now().strftime('%Y-%m-%d')
time_from = f"{today_str}T00:00"
time_to = f"{today_str}T11:59"

headers = {
    "X-RapidAPI-Key": AERO_API_KEY,
    "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
}

for airport in AIRPORTS:
    # Build unique, date-stamped file path for daily caching optimization
    local_file = os.path.join(RAW_DIR, f"aero_{airport}_{today_str}.json")

    if os.path.exists(local_file):
        print(f"Cache found: {local_file}. Skipping API call.")
    else:
        print(f"--- Fetching FIDS data for {airport} ---")

        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{airport}/{time_from}/{time_to}"
        querystring = {"withLeg": "true", "direction": "Arrival", "withCancelled": "false", "withCodeshared": "true",
                       "withCargo": "false", "withPrivate": "false", "withLocation": "false"}

        response = requests.get(url, headers=headers, params=querystring)

        if response.status_code == 200:
            json_data = response.json()
            with open(local_file, 'w', encoding='utf-8') as file:
                json.dump(json_data, file, ensure_ascii=False, indent=4)
            print(f"Success! Data saved to {local_file}")

            # RATE LIMIT PROTECTION: Sleep execution to respect BASIC tier 1 rps quota
            print("Sleeping for 2 seconds to respect API rate limits...")
            time.sleep(2)
        else:
            print(f"Error fetching {airport}: {response.status_code} - {response.text}")

print("\nExtract Phase (AeroDataBox) completed!")



# PHASE: TRANSFORM (Flight Data Normalization & Feature Engineering)

import pandas as pd
import json
import os
from datetime import datetime

print("Starting AeroDataBox data transformation in Pandas...\n")
dataframes_list = []
AIRPORTS = ['EPWA', 'EGLL', 'EDDF']
RAW_DIR = "data/raw"
today_str = datetime.now().strftime('%Y-%m-%d')

# Parse daily raw JSON files from the local Landing Zone
for airport in AIRPORTS:
    local_file = os.path.join(RAW_DIR, f"aero_{airport}_{today_str}.json")
    try:
        with open(local_file, 'r', encoding='utf-8') as file:
            json_data = json.load(file)

        flights_list = json_data.get('arrivals', [])

        if not flights_list:
            print(f"No arrivals found for {airport} in the JSON file.")
            continue

        # Flatten nested JSON hierarchy into tabular format
        temp_df = pd.json_normalize(flights_list)
        temp_df['arrival_airport'] = airport
        dataframes_list.append(temp_df)
    except FileNotFoundError:
        print(f"File {local_file} not found. Run extraction first!")

if dataframes_list:
    raw_flights_df = pd.concat(dataframes_list, ignore_index=True)

    # EXACT schema mapping based on live production API payloads
    column_mapping = {
        'number': 'flight_number',
        'status': 'flight_status',
        'airline.name': 'airline',
        'departure.airport.iata': 'departure_airport',
        'arrival_airport': 'arrival_airport',
        'arrival.scheduledTime.utc': 'scheduled_arrival',
        'arrival.revisedTime.utc': 'actual_arrival'
    }

    existing_columns = [col for col in column_mapping.keys() if col in raw_flights_df.columns]
    clean_flights_df = raw_flights_df[existing_columns].rename(columns=column_mapping)

    # Filter out active/scheduled air traffic to keep completed flights only
    if 'flight_status' in clean_flights_df.columns:
        landed_flights = clean_flights_df[clean_flights_df['flight_status'] == 'Arrived'].copy()
    else:
        landed_flights = clean_flights_df.copy()

    # Standardize temporal features into pandas UTC datetime objects
    if 'scheduled_arrival' in landed_flights.columns:
        landed_flights['scheduled_arrival'] = pd.to_datetime(landed_flights['scheduled_arrival'], utc=True)

    if 'actual_arrival' in landed_flights.columns:
        landed_flights['actual_arrival'] = pd.to_datetime(landed_flights['actual_arrival'], utc=True)
        landed_flights.dropna(subset=['actual_arrival', 'scheduled_arrival'], inplace=True)

    # FEATURE ENGINEERING: Compute precise flight delay duration in minutes
    if 'scheduled_arrival' in landed_flights.columns and 'actual_arrival' in landed_flights.columns:
        landed_flights['delay_minutes'] = (landed_flights['actual_arrival'] - landed_flights[
            'scheduled_arrival']).dt.total_seconds() / 60
        # Rectify negative values (early arrivals) to 0 minutes
        landed_flights['delay_minutes'] = landed_flights['delay_minutes'].apply(lambda x: x if x > 0 else 0).astype(int)
    else:
        landed_flights['delay_minutes'] = 0

    print(f"Transformation complete! {len(landed_flights)} landed flights processed.")

    # Persist structured staging dataset to local directory
    landed_flights.to_csv('clean_flights.csv', index=False)
    print("clean_flights.csv successfully updated with real timestamps.")
else:
    print("No dataframes were created.")



    # PHASE: WEATHER EXTRACT (Meteostat Historical Meteorological Data)

from datetime import datetime, timedelta
from meteostat import Point, Hourly
import pandas as pd

print("\nStarting historical weather data extraction...\n")

# Map geographical coordinates for precise weather spatial index queries
airport_locations = {
    'EPWA': Point(52.1657, 20.9671, 110),  # Warsaw Chopin
    'EGLL': Point(51.4700, -0.4543, 25),  # London Heathrow
    'EDDF': Point(50.0333, 8.5705, 111)  # Frankfurt
}

# Define data extraction time bounds (24-hour lookback window)
end_time = datetime.now()
start_time = end_time - timedelta(days=1)

weather_frames = []

for airport_code, location in airport_locations.items():
    print(f"Fetching hourly weather data for {airport_code}...")

    # Query Meteostat physical stations
    data = Hourly(location, start_time, end_time)
    data = data.fetch()

    if not data.empty:
        # Re-index dataframe to convert timestamp into an operational column
        df_airport = data.reset_index()
        # Append partition key for relational integrity downstream
        df_airport['airport_code'] = airport_code
        weather_frames.append(df_airport)
    else:
        print(f"WARNING: No weather data found for {airport_code} in the given timeframe.")

if weather_frames:
    # Concatenate regional data arrays into a unified weather dataframe
    weather_df = pd.concat(weather_frames, ignore_index=True)

    # Prune target features for target Data Warehouse warehouse schema
    columns_to_keep = ['airport_code', 'time', 'temp', 'prcp', 'wspd', 'coco']
    weather_df = weather_df[columns_to_keep]

    # Normalize business definitions to match target PostgreSQL database fields
    weather_df.rename(columns={
        'time': 'weather_timestamp',
        'temp': 'temperature_c',
        'prcp': 'precipitation_mm',
        'wspd': 'wind_speed_kmh',
        'coco': 'condition_code'
    }, inplace=True)

    # Impute missing values for precipitation metrics (NaN -> 0.0 mm)
    weather_df['precipitation_mm'] = weather_df['precipitation_mm'].fillna(0)

    print(f"Weather dataframe created! Total rows: {len(weather_df)}")
    print(weather_df[['airport_code', 'weather_timestamp', 'temperature_c', 'wind_speed_kmh']].head().to_string())

    print("\nSaving cleaned datasets to CSV format...")
    # Cache transformed entities to final staging tables
    if 'landed_flights' in locals():
        landed_flights.to_csv('clean_flights.csv', index=False, encoding='utf-8')
    weather_df.to_csv('clean_weather.csv', index=False, encoding='utf-8')

    print("Extraction Complete! CSV files are updated and ready for SQL.")
else:
    print("Failed to create weather dataframe.")



    # PHASE: LOAD (Data Warehouse Ingestion with Duplicate Protection)

import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

print("Starting Load Phase: Ingesting data into PostgreSQL...\n")

# 1. Acquire target database URL credentials securely
load_dotenv()
db_url = os.getenv('DATABASE_URL')

if not db_url:
    print("ERROR: DATABASE_URL missing in .env file!")
else:
    try:
        # 2. Establish high-performance relational database engine mapping
        engine = create_engine(db_url)

        # 3. Read processed datasets from the Staging Layer
        print("Reading cleaned CSV datasets...")
        flights_df = pd.read_csv('clean_flights.csv')
        weather_df = pd.read_csv('clean_weather.csv')

        # 4. Stream flight records to database row-by-row to handle unique constraints
        print("Uploading flight data to database...")
        flights_loaded = 0
        for _, row in flights_df.iterrows():
            try:
                pd.DataFrame([row]).to_sql(name='flights', con=engine, if_exists='append', index=False)
                flights_loaded += 1
            except IntegrityError:
                continue
        print(f"Success: {flights_loaded} new flights loaded (duplicates ignored).")

        # 5. Stream weather records to database row-by-row
        print("Uploading weather data to database...")
        weather_loaded = 0
        for _, row in weather_df.iterrows():
            try:
                pd.DataFrame([row]).to_sql(name='weather', con=engine, if_exists='append', index=False)
                weather_loaded += 1
            except IntegrityError:
                continue
        print(f"Success: {weather_loaded} new weather records loaded (duplicates ignored).")

        print("\nETL PIPELINE EXECUTED SUCCESSFULLY WITH IDEMPOTENCY!")

    except Exception as e:
        # Log infrastructure or transaction level errors for debugging
        print(f"\nCRITICAL ERROR during database load: {e}")