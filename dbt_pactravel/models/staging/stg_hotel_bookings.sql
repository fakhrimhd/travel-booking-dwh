select
    booking_id,
    customer_id,
    hotel_id,
    check_in_date,
    check_out_date,
    duration_nights,
    price
from {{ source('dwh_pactravel', 'hotel_bookings') }}
