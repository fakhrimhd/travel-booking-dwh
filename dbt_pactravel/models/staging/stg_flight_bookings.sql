select
    trip_id,
    flight_number,
    seat_number,
    departure_time,
    departure_date,
    flight_duration,
    travel_class,
    price,
    customer_id,
    airline_id,
    aircraft_id,
    airport_src,
    airport_dst
from {{ source('dwh_pactravel', 'flight_bookings') }}
