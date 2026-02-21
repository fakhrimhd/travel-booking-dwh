select
    hotel_id,
    hotel_name,
    hotel_address,
    city,
    country,
    hotel_score
from {{ source('dwh_pactravel', 'hotel') }}
