import os
import pandas as pd
from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL, logger

def run_load_phase():
    logger.info("STARTING PHASE: LOAD (Data Warehouse AWS RDS)")
    if not DATABASE_URL:
        logger.error("DATABASE_URL missing in .env! Aborting data load.")
        return

    try:
        engine = create_engine(DATABASE_URL)

        if os.path.exists('clean_flights.csv'):
            flights_df = pd.read_csv('clean_flights.csv')
            if not flights_df.empty:
                flights_df = flights_df.astype(object).where(pd.notnull(flights_df), None)
                records = flights_df.to_dict(orient='records')
                
                with engine.begin() as conn:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS flights (
                            flight_number VARCHAR, flight_status VARCHAR, airline VARCHAR,
                            departure_airport VARCHAR, arrival_airport VARCHAR,
                            scheduled_arrival TIMESTAMP WITH TIME ZONE, actual_arrival TIMESTAMP WITH TIME ZONE,
                            delay_minutes FLOAT, UNIQUE (flight_number, scheduled_arrival)
                        );
                    """))
                    
                    insert_query = text("""
                        INSERT INTO flights (flight_number, flight_status, airline, departure_airport, arrival_airport, scheduled_arrival, actual_arrival, delay_minutes)
                        VALUES (:flight_number, :flight_status, :airline, :departure_airport, :arrival_airport, :scheduled_arrival, :actual_arrival, :delay_minutes)
                        ON CONFLICT (flight_number, scheduled_arrival) DO NOTHING;
                    """)
                    conn.execute(insert_query, records)
                    
                logger.info(f"Success: {len(flights_df)} flights processed idempotently.")

        if os.path.exists('clean_weather.csv'):
            weather_df = pd.read_csv('clean_weather.csv')
            if not weather_df.empty:
                weather_df = weather_df.astype(object).where(pd.notnull(weather_df), None)
                records = weather_df.to_dict(orient='records')
                
                with engine.begin() as conn:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS weather (
                            airport_code VARCHAR, weather_timestamp TIMESTAMP WITH TIME ZONE,
                            temperature_c FLOAT, precipitation_mm FLOAT, wind_speed_kmh FLOAT,
                            condition_code FLOAT, UNIQUE (airport_code, weather_timestamp)
                        );
                    """))
                    
                    insert_query = text("""
                        INSERT INTO weather (airport_code, weather_timestamp, temperature_c, precipitation_mm, wind_speed_kmh, condition_code)
                        VALUES (:airport_code, :weather_timestamp, :temperature_c, :precipitation_mm, :wind_speed_kmh, :condition_code)
                        ON CONFLICT (airport_code, weather_timestamp) DO NOTHING;
                    """)
                    conn.execute(insert_query, records)
                    
                logger.info(f"Success: {len(weather_df)} weather records processed idempotently.")

    except Exception as e:
        logger.error(f"CRITICAL ERROR during database load: {e}")