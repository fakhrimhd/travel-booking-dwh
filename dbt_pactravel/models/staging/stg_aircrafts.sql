select
    aircraft_id,
    aircraft_name
from {{ source('dwh_pactravel', 'aircrafts') }}
