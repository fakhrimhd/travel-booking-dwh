select
    airline_id,
    airline_name
from {{ source('dwh_pactravel', 'airlines') }}
