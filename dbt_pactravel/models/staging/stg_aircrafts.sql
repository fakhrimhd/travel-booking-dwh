select
    aircraft_id,
    aircraft_name,
    aircraft_iata,
    aircraft_icao
from {{ source('dwh_pactravel', 'aircrafts') }}
