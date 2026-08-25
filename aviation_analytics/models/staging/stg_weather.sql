SELECT
    airport_code,
    weather_timestamp,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    condition_code
FROM {{ source('raw_data', 'weather') }}