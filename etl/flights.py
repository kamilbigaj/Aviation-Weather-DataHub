import requests
import pandas as pd
import json
import os
import time
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed
from etl.config import AERO_API_KEY, AIRPORTS, RAW_DIR, logger
from etl.storage import upload_raw_to_s3

@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def fetch_flight_data_from_api(airport, time_from, time_to, headers):
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{airport}/{time_from}/{time_to}"
    querystring = {"withLeg": "true", "direction": "Arrival", "withCancelled": "false", "withCodeshared": "true", "withCargo": "false", "withPrivate": "false", "withLocation": "false"}
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def run_extract_flights_phase():
    logger.info("STARTING PHASE: EXTRACT (Flights - Full 24h Day)")
    os.makedirs(RAW_DIR, exist_ok=True)
    today_str = datetime.now().strftime('%Y-%m-%d')
    time_windows = [(f"{today_str}T00:00", f"{today_str}T11:59"), (f"{today_str}T12:00", f"{today_str}T23:59")]
    headers = {"X-RapidAPI-Key": AERO_API_KEY, "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}

    for airport in AIRPORTS:
        local_file = os.path.join(RAW_DIR, f"aero_{airport}_{today_str}.json")
        if os.path.exists(local_file):
            logger.info(f"Cache found: {local_file}. Skipping API call.")
            continue
            
        all_arrivals = []
        try:
            for time_from, time_to in time_windows:
                json_data = fetch_flight_data_from_api(airport, time_from, time_to, headers)
                if 'arrivals' in json_data:
                    all_arrivals.extend(json_data['arrivals'])
                time.sleep(2)
            
            final_json_data = {"arrivals": all_arrivals}
            with open(local_file, 'w', encoding='utf-8') as file:
                json.dump(final_json_data, file, ensure_ascii=False, indent=4)
            upload_raw_to_s3(final_json_data, f"aero_{airport}")
        except Exception as e:
            logger.error(f"Critical error fetching data for {airport}: {e}")

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
            if flights_list:
                temp_df = pd.json_normalize(flights_list)
                temp_df['arrival_airport'] = airport
                dataframes_list.append(temp_df)
        except FileNotFoundError:
            logger.warning(f"Missing file {local_file}. Run EXTRACT first!")

    if dataframes_list:
        raw_flights_df = pd.concat(dataframes_list, ignore_index=True)
        column_mapping = {
            'number': 'flight_number', 'status': 'flight_status', 'airline.name': 'airline',
            'departure.airport.iata': 'departure_airport', 'arrival_airport': 'arrival_airport',
            'arrival.scheduledTime.utc': 'scheduled_arrival', 'arrival.revisedTime.utc': 'actual_arrival'
        }
        existing_cols = [col for col in column_mapping.keys() if col in raw_flights_df.columns]
        clean_flights_df = raw_flights_df[existing_cols].rename(columns=column_mapping)

        landed_flights = clean_flights_df[clean_flights_df['flight_status'] == 'Arrived'].copy() if 'flight_status' in clean_flights_df.columns else clean_flights_df.copy()

        if 'scheduled_arrival' in landed_flights.columns and 'actual_arrival' in landed_flights.columns:
            landed_flights['scheduled_arrival'] = pd.to_datetime(landed_flights['scheduled_arrival'], utc=True)
            landed_flights['actual_arrival'] = pd.to_datetime(landed_flights['actual_arrival'], utc=True)
            landed_flights.dropna(subset=['actual_arrival', 'scheduled_arrival'], inplace=True)
            landed_flights['delay_minutes'] = (landed_flights['actual_arrival'] - landed_flights['scheduled_arrival']).dt.total_seconds() / 60
            landed_flights['delay_minutes'] = landed_flights['delay_minutes'].apply(lambda x: x if x > 0 else 0).astype(int)
        else:
            landed_flights['delay_minutes'] = 0

        landed_flights.to_csv('clean_flights.csv', index=False)
        logger.info(f"Transformation successful! Saved {len(landed_flights)} rows.")