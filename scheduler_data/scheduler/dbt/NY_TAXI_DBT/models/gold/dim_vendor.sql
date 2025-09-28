{{
    config(
        materialized='table',
        schema='GOLD',
        tags=['gold'],
    )
}}

select
  row_number() over(order by vendor_id) as vendor_sk,
  vendor_id
from {{ source('silver', 'STG_NEWYORK_TAXISTRIPS_SILVER') }}
group by vendor_id