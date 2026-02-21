select
    customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_country
from {{ source('dwh_pactravel', 'customers') }}
