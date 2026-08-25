SELECT
    flight_number,
    flight_status,
    airline,
    departure_airport,
    arrival_airport,
    scheduled_arrival,
    actual_arrival,
    delay_minutes
FROM {{ source('raw_data', 'flights') }}