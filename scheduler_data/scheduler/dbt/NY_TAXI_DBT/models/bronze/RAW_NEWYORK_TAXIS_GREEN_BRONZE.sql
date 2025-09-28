{{ config(
    materialized='table',
    schema='BRONZE',
    tags=['bronze']
) }}

SELECT
    VENDORID as vendor_id,
    LPEP_PICKUP_DATETIME as lpep_pickup_datetime,
    LPEP_DROPOFF_DATETIME as lpep_dropoff_datetime,
    PASSENGER_COUNT as passenger_count,
    TRIP_DISTANCE as trip_distance,
    RATECODEID as ratecode_id,
    STORE_AND_FWD_FLAG as store_and_fwd_flag,
    PULOCATIONID as pulocation_id,
    DOLOCATIONID as dolocation_id,
    PAYMENT_TYPE as payment_type,
    FARE_AMOUNT as fare_amount,
    EXTRA as extra,
    MTA_TAX as mta_tax,
    TIP_AMOUNT as tip_amount,
    TOLLS_AMOUNT as tolls_amount,
    IMPROVEMENT_SURCHARGE as improvement_surcharge,
    TOTAL_AMOUNT as total_amount,
    CONGESTION_SURCHARGE as congestion_surcharge,
    RUN_ID as run_id,
    VENTANA_TEMPORAL as ventana_temporal,
    LOTE_MES as lote_mes,
    MONTH as month,
    YEAR as year,
    'green' as service_type
FROM {{ source('raw', 'NY_TAXIS_GREEN_BRONZE') }}