select
    airport_id,
    airport_name,
    city,
    country,
    latitude,
    longitude
from {{ source('dwh_pactravel', 'airports') }}
