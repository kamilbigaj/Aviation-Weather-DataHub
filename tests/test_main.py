import pandas as pd
import numpy as np
import pytest

# ETL PIPELINE UNIT TESTS

# We test the core data transformation logic (business rules) without 
# making actual API calls. This ensures our logic is sound.

# TEST 1: Flight Delay Calculation
def test_flight_delay_calculation():
    """Tests if the delay is correctly calculated in minutes and negative delays become 0."""
    # 1. ARRANGE (Create dummy data)
    data = {
        'scheduled_arrival': [pd.to_datetime('2026-08-14 10:00:00', utc=True), pd.to_datetime('2026-08-14 12:00:00', utc=True)],
        'actual_arrival': [pd.to_datetime('2026-08-14 10:15:00', utc=True), pd.to_datetime('2026-08-14 11:50:00', utc=True)]
    }
    df = pd.DataFrame(data)

    # 2. ACT (Apply the exact logic from our main.py)
    df['delay_minutes'] = (df['actual_arrival'] - df['scheduled_arrival']).dt.total_seconds() / 60
    # Negative values (early arrivals) should be set to 0
    df['delay_minutes'] = df['delay_minutes'].apply(lambda x: x if x > 0 else 0).astype(int)

    # 3. ASSERT (Verify the results)
    assert df['delay_minutes'].iloc[0] == 15  # 15 minutes late
    assert df['delay_minutes'].iloc[1] == 0   # 10 minutes early (should round to 0)


# TEST 2: Filter 'Arrived' Flights Only
def test_filter_arrived_flights():
    """Tests if the pipeline correctly filters out non-arrived flights (e.g. Expected, Canceled)."""
    # 1. ARRANGE
    data = {
        'flight_number': ['LO123', 'BA456', 'LH789'],
        'flight_status': ['Arrived', 'Expected', 'Canceled']
    }
    df = pd.DataFrame(data)

    # 2. ACT
    landed_flights = df[df['flight_status'] == 'Arrived'].copy()

    # 3. ASSERT
    assert len(landed_flights) == 1
    assert landed_flights['flight_number'].iloc[0] == 'LO123'


# TEST 3: Weather Data Imputation (Missing Values)
def test_weather_data_imputation():
    """Tests if missing precipitation values (NaN) from Meteostat are correctly imputed as 0.0."""
    # 1. ARRANGE
    data = {
        'airport_code': ['EPWA', 'EGLL'],
        'precipitation_mm': [5.5, np.nan]  # The second row is missing data
    }
    df = pd.DataFrame(data)

    # 2. ACT
    df['precipitation_mm'] = df['precipitation_mm'].fillna(0)

    # 3. ASSERT
    assert df['precipitation_mm'].iloc[0] == 5.5
    assert df['precipitation_mm'].iloc[1] == 0.0