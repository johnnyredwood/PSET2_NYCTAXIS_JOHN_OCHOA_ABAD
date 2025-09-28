{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
        cluster_by = ['pickup_date_sk', 'pickup_zone_sk', 'service_type_sk']
    )
}}

with silver_data as (
    select * from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
),

date_pickup as (
    select pickup_date_sk, pickup_date 
    from {{ ref('dim_date_pickup') }}
),

date_dropoff as (
    select dropoff_date_sk, dropoff_date 
    from {{ ref('dim_date_dropoff') }}
),

time_pickup as (
    select pickup_time_sk, hour as pickup_hour, minute as pickup_minute, second as pickup_second
    from {{ ref('dim_time_pickup') }}
),

time_dropoff as (
    select dropoff_time_sk, hour as dropoff_hour, minute as dropoff_minute, second as dropoff_second
    from {{ ref('dim_time_dropoff') }}
),

zone_pickup as (
    select pickup_zone_sk, pickup_location_id
    from {{ ref('dim_zone_pickup') }}
),

zone_dropoff as (
    select dropoff_zone_sk, dropoff_location_id
    from {{ ref('dim_zone_dropoff') }}
),

vendor_dim as (
    select vendor_sk, vendor_id
    from {{ ref('dim_vendor') }}
),

rate_code_dim as (
    select rate_code_sk, rate_code_id
    from {{ ref('dim_rate_code') }}
),

payment_type_dim as (
    select payment_type_sk, payment_type_description
    from {{ ref('dim_payment_type') }}
),

service_type_dim as (
    select service_type_sk, service_type
    from {{ ref('dim_service_type') }}
),

trip_type_dim as (
    select trip_type_sk, duration_flag, speed_flag
    from {{ ref('dim_trip_type') }}
)

select
    
    -- Key
    t.trip_id,
    
    -- Foreign Keys a dimensiones
    dp.pickup_date_sk,
    dd.dropoff_date_sk,
    tp.pickup_time_sk,
    td.dropoff_time_sk,
    zp.pickup_zone_sk,
    zd.dropoff_zone_sk,
    v.vendor_sk,
    rc.rate_code_sk,
    pt.payment_type_sk,
    st.service_type_sk,
    tt.trip_type_sk,
    
    -- Métricas del viaje
    t.trip_duration_seconds,
    t.avg_speed_mph,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    
    -- Métricas extas
    case 
        when t.fare_amount > 0 then (t.tip_amount / t.fare_amount) * 100 
        else 0 
    end as tip_percentage,
    
    
    -- Atributos descriptivos
    t.store_and_fwd_flag,
    
    -- Metadatos
    t.run_id,
    t.ventana_temporal_utc,
    t.ventana_temporal_nyc,
    t.lote_mes

from silver_data t
left join date_pickup dp on t.pickup_datetime_nyc::date = dp.pickup_date
left join date_dropoff dd on t.dropoff_datetime_nyc::date = dd.dropoff_date
left join time_pickup tp on 
    extract(hour from t.pickup_datetime_nyc) = tp.pickup_hour 
    and extract(minute from t.pickup_datetime_nyc) = tp.pickup_minute 
    and extract(second from t.pickup_datetime_nyc) = tp.pickup_second
left join time_dropoff td on 
    extract(hour from t.dropoff_datetime_nyc) = td.dropoff_hour 
    and extract(minute from t.dropoff_datetime_nyc) = td.dropoff_minute 
    and extract(second from t.dropoff_datetime_nyc) = td.dropoff_second
left join zone_pickup zp on t.pickup_location_id = zp.pickup_location_id
left join zone_dropoff zd on t.dropoff_location_id = zd.dropoff_location_id
left join vendor_dim v on t.vendor_id = v.vendor_id
left join rate_code_dim rc on t.rate_code_id = rc.rate_code_id
left join payment_type_dim pt on t.payment_type_description = pt.payment_type_description
left join service_type_dim st on t.service_type = st.service_type
left join trip_type_dim tt on t.duration_flag = tt.duration_flag and t.speed_flag = tt.speed_flag

where
    -- Reglas de calidad
    t.trip_distance >= 0
    and t.trip_id is not null
    and t.passenger_count > 0
    and t.fare_amount >= 0
    and t.extra >= 0
    and t.mta_tax >= 0
    and t.tip_amount >= 0
    and t.tolls_amount >= 0
    and t.improvement_surcharge >= 0
    and t.total_amount >= 0
    and t.avg_speed_mph > 0
    and t.avg_speed_mph <= 100