SELECT
    f.flight_number,
    f.airline,
    f.departure_airport,
    f.arrival_airport,
    f.scheduled_arrival,
    f.actual_arrival,
    f.delay_minutes,
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,
    w.condition_code
FROM {{ ref('stg_flights') }} f
LEFT JOIN {{ ref('stg_weather') }} w
    ON f.arrival_airport = w.airport_code
    AND DATE_TRUNC('hour', f.actual_arrival) = DATE_TRUNC('hour', w.weather_timestamp)