{{
  config(
    materialized='incremental',
    unique_key='sk_hotel_booking_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
  )
}}

with stg_fct_hotel_bookings as (
    select
        hb.trip_id as nk_trip_id,
        hb.customer_id as nk_customer_id,
        hb.hotel_id as nk_hotel_id,
        hb.check_in_date::date as check_in_date,
        hb.check_out_date::date as check_out_date,
        hb.price,
        hb.breakfast_included,
        {{ dbt.datediff("hb.check_in_date", "hb.check_out_date", "day") }} as duration_nights,
        row_number() over (
            partition by trip_id, check_in_date
            order by check_in_date
        ) as rn
    from {{ ref('stg_hotel_bookings') }} hb
    {% if is_incremental() %}
    -- Only process new records since the last run
    -- Look back 3 days to handle late-arriving data
    where hb.check_in_date::date >= (
        select coalesce(max(check_in_date) - interval '3 days', '1900-01-01'::date)
        from {{ this }}
    )
    {% endif %}
),

final_fct_hotel_bookings as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'nk_trip_id',
            'check_in_date',
            'check_out_date'
        ]) }} as sk_hotel_booking_id,
        dc.sk_customer_id,
        dh.sk_hotel_id,
        dci.date_id as check_in_date_id,
        dco.date_id as check_out_date_id,
        stg.nk_trip_id,
        stg.check_in_date,
        stg.check_out_date,
        stg.price,
        stg.breakfast_included,
        stg.duration_nights,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at,
        -- Additional date attributes
        dci.week_of_year as check_in_week,
        dci.year_actual as check_in_year,
        dci.quarter_actual as check_in_quarter,
        dci.month_name as check_in_month,
        dci.weekend_indr as check_in_weekend
    from stg_fct_hotel_bookings stg
    left join {{ ref('dim_customers') }} dc 
        on stg.nk_customer_id = dc.nk_customer_id
    left join {{ ref('dim_hotels') }} dh 
        on stg.nk_hotel_id = dh.nk_hotel_id
    left join {{ ref('dim_date') }} dci 
        on stg.check_in_date = dci.date_actual
    left join {{ ref('dim_date') }} dco 
        on stg.check_out_date = dco.date_actual
    where stg.rn = 1  -- Deduplication
)

select * from final_fct_hotel_bookings