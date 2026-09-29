-- A role-playing calendar dimension. The fact will join this same relation
-- twice: once for pickup date and once for dropoff date.
{{ config(materialized='table') }}

with bounds as (
    select
        min(to_date(pickup_datetime)) as first_trip_date,
        max(to_date(dropoff_datetime)) as last_trip_date
    from {{ ref('silver_yellow_taxi_trips') }}
),
date_spine as (
    -- 10,000 days covers more than 27 years, far beyond the current data range.
    select dateadd(day, seq4(), first_trip_date)::date as full_date
    from bounds, table(generator(rowcount => 10000))
    where dateadd(day, seq4(), first_trip_date)::date <= last_trip_date
)

select
    to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
    full_date,
    year(full_date) as year_number,
    quarter(full_date) as quarter_number,
    month(full_date) as month_number,
    monthname(full_date) as month_name,
    to_char(full_date, 'YYYY-MM') as year_month,
    day(full_date) as day_of_month,
    dayofweekiso(full_date) as day_of_week_number,
    dayname(full_date) as day_name,
    iff(dayofweekiso(full_date) in (6, 7), true, false) as is_weekend
from date_spine

/*
READ-AFTER-CODE: calendar dimension flow

Silver provides the earliest pickup and latest dropoff date. A Snowflake date
spine then generates every calendar day in that inclusive interval. The final
SELECT derives reusable calendar attributes once, rather than repeating date
logic in every analytical query.

    Silver trip timestamps -> date bounds -> daily spine -> dim_date

date_key is YYYYMMDD, a stable integer key. It is role-playing: the fact uses
pickup_date_key and dropoff_date_key, both pointing at this same dimension.
*/
