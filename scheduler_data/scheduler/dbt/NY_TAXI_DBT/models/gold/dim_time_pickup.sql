{{ config(
    materialized='table',
    schema='GOLD',
    tags=['gold']
) }}

with times as (
    select distinct
        extract(hour from pickup_datetime_nyc) as hour,
        extract(minute from pickup_datetime_nyc) as minute,
        extract(second from pickup_datetime_nyc) as second,
        to_varchar(pickup_datetime_nyc, 'HH24:MI:SS') as pickup_time_formatted
    from {{ source('silver','STG_NEWYORK_TAXISTRIPS_SILVER') }}
)

select
    hour * 10000 + minute * 100 + second as pickup_time_sk,
    hour,
    minute,
    second,
    pickup_time_formatted
from times
order by pickup_time_sk
