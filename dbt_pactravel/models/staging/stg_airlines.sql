select
    airline_id,
    airline_name,
    country,
    airline_iata,
    airline_icao,
    alias
from {{ source('dwh_pactravel', 'airlines') }}
