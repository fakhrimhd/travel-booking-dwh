{{
  config(
    materialized='incremental',
    unique_key='sk_flight_booking_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
  )
}}

with stg_fct_flight_bookings as (
    select
        fb.trip_id as nk_trip_id,
        fb.flight_number,
        fb.seat_number,
        fb.departure_time::time as departure_time,
        fb.departure_date::date as departure_date,
        fb.flight_duration,
        fb.travel_class,
        fb.price,
        fb.customer_id as nk_customer_id,
        fb.airline_id as nk_airline_id,
        fb.aircraft_id as nk_aircraft_id,
        fb.airport_src as nk_airport_src_id,
        fb.airport_dst as nk_airport_dst_id,
        row_number() over (
            partition by trip_id, departure_date, departure_time
            order by departure_date
        ) as rn
    from {{ ref('stg_flight_bookings') }} fb
    {% if is_incremental() %}
    -- Only process new records since the last run
    -- Look back 3 days to handle late-arriving data
    where fb.departure_date::date >= (
        select coalesce(max(departure_date) - interval '3 days', '1900-01-01'::date)
        from {{ this }}
    )
    {% endif %}
),

final_fct_flight_bookings as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'nk_trip_id',
            'departure_date',
            'departure_time',
            'flight_number'
        ]) }} as sk_flight_booking_id,
        dc.sk_customer_id,
        da.sk_airline_id,
        dac.sk_aircraft_id,
        ds.sk_airport_id as sk_airport_src_id,
        dd.sk_airport_id as sk_airport_dst_id,
        dt.date_id,
        t.time_id,
        stg.nk_trip_id,
        stg.flight_number,
        stg.seat_number,
        stg.departure_time,
        stg.departure_date,
        stg.flight_duration,
        stg.travel_class,
        stg.price,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at,
        dt.week_of_year,
        dt.quarter_actual,
        dt.year_actual,
        dt.weekend_indr,
        dt.month_name_abbreviated
    from stg_fct_flight_bookings stg
    left join {{ ref('dim_customers') }} dc 
        on stg.nk_customer_id = dc.nk_customer_id
    left join {{ ref('dim_airlines') }} da 
        on stg.nk_airline_id = da.nk_airline_id
    left join {{ ref('dim_aircrafts') }} dac 
        on stg.nk_aircraft_id = dac.nk_aircraft_id
    left join {{ ref('dim_airports') }} ds 
        on stg.nk_airport_src_id = ds.nk_airport_id
    left join {{ ref('dim_airports') }} dd 
        on stg.nk_airport_dst_id = dd.nk_airport_id
    left join {{ ref('dim_date') }} dt 
        on stg.departure_date::date = dt.date_actual::date
    left join {{ ref('dim_time') }} t 
        on stg.departure_time::time = t.time_actual::time
    where stg.rn = 1  -- Deduplication
)

select * from final_fct_flight_bookings