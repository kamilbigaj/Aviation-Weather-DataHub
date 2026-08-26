import pandas as pd
from unittest.mock import patch
from etl.weather import run_weather_phase

@patch('etl.weather.fetch_weather_data_from_api')
def test_real_weather_imputation(mock_fetch):
    # 1. Simulate Meteostat returning data with missing precipitation
    fake_weather_df = pd.DataFrame({
        'time': ['2023-10-01 10:00:00'],
        'temp': [15.5],
        'prcp': [float('nan')],  # Changed from None to float('nan') to silence Pandas warning
        'wspd': [10.2],
        'coco': [3]
    }).set_index('time')
    
    # The fake_weather_df will be returned for each of the airports
    mock_fetch.return_value = fake_weather_df

    captured_df = None

    def mock_to_csv(self, *args, **kwargs):
        nonlocal captured_df
        captured_df = self

    # 2. Execute the REAL function
    with patch('pandas.DataFrame.to_csv', new=mock_to_csv):
        run_weather_phase()

    # 3. Validate the results
    assert captured_df is not None, "DataFrame was not generated!"
    assert 'precipitation_mm' in captured_df.columns, "Precipitation column is missing!"
    
    # If NaN was converted to 0.0, the imputation logic in etl/weather.py works correctly!
    assert captured_df['precipitation_mm'].iloc[0] == 0.0, "Precipitation NaN imputation failed!"