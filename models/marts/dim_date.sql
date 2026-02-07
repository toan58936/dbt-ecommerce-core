{{ config(materialized='table') }}

with dates as (
  select generate_series(
    '2015-01-01'::date,
    (current_date + interval '1 year')::date,
    interval '1 day'
  ) as date_day
)

select
  to_char(date_day, 'YYYYMMDD')::int as date_key,
  date_day as date,
  extract(year from date_day)::int as year,
  extract(month from date_day)::int as month,
  extract(quarter from date_day)::int as quarter,
  extract(doy from date_day)::int as day_of_year,
  extract(isodow from date_day)::int as day_of_week,
  trim(to_char(date_day, 'Day')) as day_name,
  trim(to_char(date_day, 'Mon')) as month_name,
  (extract(isodow from date_day) in (6,7)) as is_weekend
from dates
