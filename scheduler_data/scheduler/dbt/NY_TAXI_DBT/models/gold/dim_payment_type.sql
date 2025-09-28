{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
    )
}}

select
    row_number() over(order by payment_type_description) as payment_type_sk,
    payment_type_description
from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
group by payment_type_description