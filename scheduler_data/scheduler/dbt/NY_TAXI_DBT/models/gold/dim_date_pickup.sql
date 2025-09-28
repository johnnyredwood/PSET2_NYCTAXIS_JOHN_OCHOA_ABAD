{{ config(
    materialized='table',
    schema='GOLD',
    tags=['gold']
) }}

with pickup_dates as (
    select distinct pickup_datetime_nyc::date as calendar_date
    from {{ source('silver','STG_NEWYORK_TAXISTRIPS_SILVER') }}
)

select
    to_number(to_char(calendar_date, 'YYYYMMDD')) as pickup_date_sk,
    calendar_date as pickup_date,
    extract(year from calendar_date) as year,
    extract(month from calendar_date) as month,
    extract(day from calendar_date) as day,
    case extract(dow from calendar_date)
    when 0 then 'domingo'
    when 1 then 'lunes'
    when 2 then 'martes'
    when 3 then 'miércoles'
    when 4 then 'jueves'
    when 5 then 'viernes'
    when 6 then 'sábado'
    end as day_of_week
from pickup_dates
order by calendar_date