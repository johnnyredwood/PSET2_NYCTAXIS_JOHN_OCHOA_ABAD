{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
    )
}}

select
    row_number() over(order by rate_code_id) as rate_code_sk,
    rate_code_id,
    rate_code_description
from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
group by rate_code_id, rate_code_description