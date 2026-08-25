import pandas as pd
import os
from datetime import datetime, timedelta
from meteostat import Point, Hourly
from tenacity import retry, stop_after_attempt, wait_fixed
from etl.config import logger

@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def fetch_weather_data_from_api(location, start_time, end_time):
    data = Hourly(location, start_time, end_time)
    return data.fetch()

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
        try:
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
        logger.info(f"Weather processed! Saved {len(weather_df)} rows.")