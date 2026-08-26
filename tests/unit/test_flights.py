import json
import pandas as pd
from unittest.mock import patch, mock_open
from etl.flights import run_transform_flights_phase

@patch('etl.flights.AIRPORTS', ['EPWA'])
@patch('os.path.exists', return_value=True)
def test_real_flight_delay_calculation(mock_exists):
    # 1. Simulate API response from AeroDataBox
    fake_api_response = {
        "arrivals": [
            {
                "number": "LO123",
                "status": "Arrived",
                "airline": {"name": "LOT"},
                "departure": {"airport": {"iata": "LHR"}},
                "arrival": {
                    "scheduledTime": {"utc": "2023-10-01 10:00Z"},
                    "revisedTime": {"utc": "2023-10-01 10:15Z"}  # 15 minutes delay
                }
            },
            {
                "number": "LH456",
                "status": "Canceled",  # This flight should be filtered out!
                "airline": {"name": "Lufthansa"},
                "departure": {"airport": {"iata": "FRA"}},
                "arrival": {
                    "scheduledTime": {"utc": "2023-10-01 11:00Z"},
                    "revisedTime": {"utc": "2023-10-01 11:00Z"}
                }
            }
        ]
    }

    captured_df = None

    # HACK: Intercept to_csv to prevent writing to disk and capture the DF instead
    def mock_to_csv(self, *args, **kwargs):
        nonlocal captured_df
        captured_df = self

    # 2. Execute the REAL function from the ETL module!
    with patch('builtins.open', mock_open(read_data=json.dumps(fake_api_response))):
        with patch('pandas.DataFrame.to_csv', new=mock_to_csv):
            run_transform_flights_phase()

    # 3. Validate the results
    assert captured_df is not None, "DataFrame was not generated!"
    assert len(captured_df) == 1, "Function failed to filter out canceled flights!"
    assert captured_df.iloc[0]['delay_minutes'] == 15, "Incorrect delay calculation!"