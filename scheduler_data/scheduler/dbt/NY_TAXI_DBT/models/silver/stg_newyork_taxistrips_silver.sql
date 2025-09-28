{{
    config(
        materialized='table',
        schema='SILVER',
        tags=['silver']
    )
}}

with green as (
    select
        *
    from {{ source('bronze', 'RAW_NEWYORK_TAXIS_GREEN_BRONZE') }}
    where 
        lpep_pickup_datetime is not null 
        and lpep_dropoff_datetime is not null
),

yellow as (
    select
        *
    from {{ source('bronze', 'RAW_NEWYORK_TAXIS_YELLOW_BRONZE') }}
    where 
        tpep_pickup_datetime is not null 
        and tpep_dropoff_datetime is not null
),

all_trips as (
    select 
        vendor_id,
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id as rate_code_id,
        store_and_fwd_flag,
        pulocation_id,
        dolocation_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        run_id,
        ventana_temporal,
        lote_mes,
        month,
        year,
        service_type
    from green
    
    union all
    
    select  
        vendor_id,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id as rate_code_id,
        store_and_fwd_flag,
        pulocation_id,
        dolocation_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        run_id,
        ventana_temporal,
        lote_mes,
        month,
        year,
        service_type
    from yellow
),

taxi_zones as (
    select 
        *
    from {{ source('bronze', 'RAW_NEWYORK_TAXIS_ZONES') }}
),

standardized as (
    select
        -- Identificador único para cada viaje
        md5(
            coalesce(cast(all_trips.pickup_datetime as varchar), '') || 
            coalesce(cast(all_trips.dropoff_datetime as varchar), '') ||
            coalesce(cast(all_trips.pulocation_id as varchar), '') ||
            coalesce(cast(all_trips.dolocation_id as varchar), '') ||
            coalesce(cast(all_trips.vendor_id as varchar), '') ||
            coalesce(cast(all_trips.rate_code_id as varchar), '') ||
            coalesce(cast(all_trips.payment_type as varchar), '') ||  
            coalesce(cast(all_trips.total_amount as varchar), '') ||     
            coalesce(cast(all_trips.trip_distance as varchar), '') || 
            coalesce(all_trips.service_type, '') ||
            coalesce(cast(all_trips.passenger_count as varchar), '') ||
            cast(
                row_number() over (
                    partition by 
                        all_trips.pickup_datetime,
                        all_trips.dropoff_datetime,
                        all_trips.pulocation_id,
                        all_trips.dolocation_id,
                        all_trips.vendor_id,
                        all_trips.rate_code_id,
                        all_trips.payment_type,
                        all_trips.total_amount,
                        all_trips.trip_distance,
                        all_trips.service_type,
                        all_trips.passenger_count
                    order by all_trips.pickup_datetime
                ) as varchar
            )
        ) as trip_id,
        
        -- Información de pickup location con zonas
        cast(all_trips.pulocation_id as integer) as pickup_location_id,
        taxi_zones.BOROUGH as pickup_borough,  
        taxi_zones.ZONE as pickup_zone,        
        taxi_zones.SERVICE_ZONE as pickup_service_zone,
        
        all_trips.dolocation_id,

        -- Estándarizar zonas horarias

        TO_TIMESTAMP_NTZ(all_trips.pickup_datetime / 1000000000) AS pickup_datetime_nyc,
        TO_TIMESTAMP_NTZ(all_trips.dropoff_datetime / 1000000000) AS dropoff_datetime_nyc,

        -- Convertimos a UTC desde NY
        CONVERT_TIMEZONE('America/New_York', 'UTC', TO_TIMESTAMP_NTZ(all_trips.pickup_datetime / 1000000000)) AS pickup_datetime_utc,
        CONVERT_TIMEZONE('America/New_York', 'UTC', TO_TIMESTAMP_NTZ(all_trips.dropoff_datetime / 1000000000)) AS dropoff_datetime_utc,

        -- Calcular duración del viaje
        ((dropoff_datetime - pickup_datetime) / 1000000000) AS trip_duration_seconds,
        
        -- Calcular velocidad promedio
        case
        when ((dropoff_datetime - pickup_datetime) / 1000000000) > 0
        then trip_distance * 3600 / ((dropoff_datetime - pickup_datetime) / 1000000000)
        else NULL
        end as avg_speed_mph,

        -- Tipificación y normalización de campos
        cast(all_trips.vendor_id as integer) as vendor_id,
        cast(all_trips.passenger_count as integer) as passenger_count,
        cast(all_trips.trip_distance as float) as trip_distance,
        cast(all_trips.rate_code_id as integer) as rate_code_id,
        all_trips.store_and_fwd_flag,
        cast(all_trips.fare_amount as float) as fare_amount,
        cast(all_trips.extra as float) as extra,
        cast(all_trips.mta_tax as float) as mta_tax,
        cast(all_trips.tip_amount as float) as tip_amount,
        cast(all_trips.tolls_amount as float) as tolls_amount,
        cast(all_trips.improvement_surcharge as float) as improvement_surcharge,
        cast(all_trips.total_amount as float) as total_amount,
        cast(all_trips.congestion_surcharge as float) as congestion_surcharge,
        
        -- Payment type legible con categorías oficiales
        case all_trips.payment_type
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
            else 'Other'
        end as payment_type_description,
        
        -- Rate code legible
        case all_trips.rate_code_id
            when 1 then 'Standard rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated fare'
            when 6 then 'Group ride'
            else 'Unknown'
        end as rate_code_description,
        
        -- Metadatos de procesamiento
        all_trips.run_id,
        CONVERT_TIMEZONE('America/Guayaquil', 'UTC', TO_TIMESTAMP_NTZ(all_trips.ventana_temporal / 1000000000)) AS ventana_temporal_utc,
        CONVERT_TIMEZONE('America/Guayaquil', 'America/New_York', TO_TIMESTAMP_NTZ(all_trips.ventana_temporal / 1000000000)) AS ventana_temporal_nyc,
        all_trips.lote_mes,
        all_trips.month,
        all_trips.year,
        all_trips.service_type

    from all_trips
    left join taxi_zones on all_trips.pulocation_id = taxi_zones.LOCATIONID
), 

standarized_plus_zones as (
    select
        standardized.trip_id,
        standardized.pickup_location_id,
        standardized.pickup_borough,
        standardized.pickup_zone,
        standardized.pickup_service_zone,
        
        -- Información de pickup location con zonas
        cast(standardized.dolocation_id as integer) as dropoff_location_id,
        taxi_zones.BOROUGH as dropoff_borough,  
        taxi_zones.ZONE as dropoff_zone,        
        taxi_zones.SERVICE_ZONE as dropoff_service_zone,  
        
        standardized.pickup_datetime_utc,
        standardized.dropoff_datetime_utc,
        standardized.pickup_datetime_nyc,
        standardized.dropoff_datetime_nyc,
        standardized.trip_duration_seconds,
        standardized.avg_speed_mph,
        standardized.vendor_id,
        standardized.passenger_count,
        standardized.trip_distance,
        standardized.rate_code_id,
        standardized.store_and_fwd_flag,
        standardized.fare_amount,
        standardized.extra,
        standardized.mta_tax,
        standardized.tip_amount,
        standardized.tolls_amount,
        standardized.improvement_surcharge,
        standardized.total_amount,
        standardized.congestion_surcharge,
        standardized.payment_type_description,
        standardized.rate_code_description,
        standardized.run_id,
        standardized.ventana_temporal_utc,
        standardized.ventana_temporal_nyc,
        standardized.lote_mes,
        standardized.month,
        standardized.year,
        standardized.service_type

    from standardized
    left join taxi_zones on standardized.dolocation_id = taxi_zones.LOCATIONID

)

select
    trip_id,
    pickup_datetime_utc,
    dropoff_datetime_utc,
    pickup_datetime_nyc,
    dropoff_datetime_nyc,
    trip_duration_seconds,
    avg_speed_mph,
    vendor_id,
    passenger_count,
    trip_distance,
    
    -- Información de pickup enriquecida
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    
    -- Información de dropoff enriquecida
    dropoff_location_id,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone,
    
    rate_code_id,
    rate_code_description,
    store_and_fwd_flag,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    payment_type_description,
    run_id,
    ventana_temporal_utc,
    ventana_temporal_nyc,
    lote_mes,
    month,
    year,
    service_type,
    
    case 
        when trip_duration_seconds < 60 then 'Very short trip (<1min)'
        when trip_duration_seconds > 3600 then 'Very long trip (>1h)'
        else 'Normal trip'
    end as duration_flag,
    
    case
        when avg_speed_mph > 80 then 'High speed'
        when avg_speed_mph < 5 then 'Low speed'
        else 'Normal speed'
    end as speed_flag

from standarized_plus_zones
where
    -- Reglas de calidad
    trip_distance >= 0
    and passenger_count > 0
    and fare_amount >= 0
    and extra >= 0
    and mta_tax >= 0
    and tip_amount >= 0
    and tolls_amount >= 0
    and improvement_surcharge >= 0
    and total_amount >= 0
    and avg_speed_mph > 0
    and pickup_datetime_utc < dropoff_datetime_utc
    and pickup_datetime_utc >= TO_TIMESTAMP_NTZ('2015-01-01 00:00:00')
    and pickup_datetime_utc < TO_TIMESTAMP_NTZ('2026-01-01 00:00:00')
    and pickup_datetime_nyc >= TO_TIMESTAMP_NTZ('2015-01-01 00:00:00')
    and pickup_datetime_nyc < TO_TIMESTAMP_NTZ('2026-01-01 00:00:00')
    and dropoff_datetime_utc >= TO_TIMESTAMP_NTZ('2015-01-01 00:00:00')
    and dropoff_datetime_utc < TO_TIMESTAMP_NTZ('2026-01-01 00:00:00')
    and dropoff_datetime_nyc >= TO_TIMESTAMP_NTZ('2015-01-01 00:00:00')
    and dropoff_datetime_nyc < TO_TIMESTAMP_NTZ('2026-01-01 00:00:00')
    and ventana_temporal_nyc >= TO_TIMESTAMP_NTZ('2015-01-01 00:00:00')
    and ventana_temporal_nyc < TO_TIMESTAMP_NTZ('2026-01-01 00:00:00')
    and ventana_temporal_utc >= TO_TIMESTAMP_NTZ('2015-01-01 00:00:00')
    and ventana_temporal_utc < TO_TIMESTAMP_NTZ('2026-01-01 00:00:00') 
    and vendor_id is not null
    and pickup_location_id > 0
    and dropoff_location_id > 0