select
    trip_id,
    customer_id,
    hotel_id,
    check_in_date,
    check_out_date,
    price,
    breakfast_included
from {{ source('dwh_pactravel', 'hotel_bookings') }}
