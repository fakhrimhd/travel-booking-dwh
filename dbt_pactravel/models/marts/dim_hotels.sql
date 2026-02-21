{{
  config(
    unique_key='sk_hotel_id'
  )
}}

with deduplicated_hotels as (
  select 
    hotel_id as nk_hotel_id,
    hotel_name,
    hotel_address,
    city,
    country,
    hotel_score,
    -- Add any timestamp column if available for versioning:
    -- updated_at,
    row_number() over (
      partition by hotel_id
      order by hotel_id  -- or use updated_at DESC if available
    ) as rn
  from {{ ref('stg_hotels') }}
),

final_dim_hotels as (
  select
    {{ dbt_utils.generate_surrogate_key([
        'nk_hotel_id',
        'hotel_name',
        'city',
        'country'
    ]) }} as sk_hotel_id,
    nk_hotel_id,
    hotel_name,
    hotel_address,
    city,
    country,
    hotel_score,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at,
    -- Optional metadata columns:
    'source_system' as record_source,
    current_timestamp as dwh_loaded_at
  from deduplicated_hotels
  where rn = 1  -- Ensures one record per hotel
)

select * from final_dim_hotels