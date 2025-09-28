{{ config(
    materialized='table',
    schema='BRONZE',
    tags=['bronze']
) }}

select
    "LocationID" as LOCATIONID,
    "Borough"    as BOROUGH,
    "Zone"       as ZONE,
    "service_zone" as SERVICE_ZONE
from {{ source('raw','NEWYORK_TAXIS_ZONES') }}
