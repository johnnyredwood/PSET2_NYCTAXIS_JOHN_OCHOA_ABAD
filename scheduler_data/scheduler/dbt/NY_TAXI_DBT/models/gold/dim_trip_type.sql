{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
    )
}}

select
    row_number() over(order by duration_flag, speed_flag) as trip_type_sk,
    duration_flag,
    speed_flag,
    concat(duration_flag, ' | ', speed_flag) as trip_type_description
from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
group by duration_flag, speed_flag
