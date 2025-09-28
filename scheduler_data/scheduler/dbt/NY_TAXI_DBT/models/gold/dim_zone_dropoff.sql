{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
    )
}}

with zones as (
    select distinct 
        dropoff_location_id,
        dropoff_borough,
        dropoff_zone,
        dropoff_service_zone
    from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
)

select
    row_number() over(order by dropoff_location_id) as dropoff_zone_sk,
    dropoff_location_id,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone
from zones
