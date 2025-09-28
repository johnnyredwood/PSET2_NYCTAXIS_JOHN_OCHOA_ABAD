{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
    )
}}

select
    row_number() over(order by service_type) as service_type_sk,
    service_type
from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
group by service_type
