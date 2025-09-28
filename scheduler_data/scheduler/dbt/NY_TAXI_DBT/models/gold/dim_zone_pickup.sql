{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
    )
}}

with zones as (
    select distinct 
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        pickup_service_zone
    from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
)

select
    row_number() over(order by pickup_location_id) as pickup_zone_sk,
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    pickup_service_zone
from zones