{{ config(materialized='table') }}

SELECT
    f.flight_number,
    f.airline,
    f.departure_airport,
    f.arrival_airport,
    f.scheduled_arrival,
    f.actual_arrival,
    f.delay_minutes,
    w.temperature_c,
    w.wind_speed_kmh,
    w.precipitation_mm,
    w.condition_code
FROM public.flights f
LEFT JOIN public.weather w
    ON f.arrival_airport = w.airport_code
    -- Join by date and round the arrival time to the nearest hour to match the hourly weather data
    AND DATE_TRUNC('hour', CAST(f.actual_arrival AS TIMESTAMP)) = DATE_TRUNC('hour', CAST(w.weather_timestamp AS TIMESTAMP))
WHERE f.flight_status = 'Arrived'