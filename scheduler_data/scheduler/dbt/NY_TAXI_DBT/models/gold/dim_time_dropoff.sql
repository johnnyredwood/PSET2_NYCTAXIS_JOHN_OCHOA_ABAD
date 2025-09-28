{{ config(
    materialized='table',
    schema='GOLD',
    tags=['gold']
) }}

with times as (
    select distinct
        extract(hour from dropoff_datetime_nyc) as hour,
        extract(minute from dropoff_datetime_nyc) as minute,
        extract(second from dropoff_datetime_nyc) as second,
        to_varchar(dropoff_datetime_nyc, 'HH24:MI:SS') as dropoff_time_formatted
    from {{ source('silver','STG_NEWYORK_TAXISTRIPS_SILVER') }}
)

select
    hour * 10000 + minute * 100 + second as dropoff_time_sk,
    hour,
    minute,
    second,
    dropoff_time_formatted
from times
order by dropoff_time_sk
