select
    airport_id,
    airport_name,
    city,
    latitude,
    longitude
from {{ source('dwh_pactravel', 'airports') }}
