{{
    config(
        alias='DIM_DATE',
        tags=['gold', 'dimension', 'date']
    )
}}

with relevant_dates as (
    select observation_date as full_date
    from {{ ref('smart_null_processed_all_years') }}

    union

    select failure_date as full_date
    from {{ ref('ssd_failure_labels') }}
),
date_bounds as (
    select min(full_date) as minimum_date, max(full_date) as maximum_date
    from relevant_dates
),
date_spine as (
    select dateadd(day, generated_date.value::integer, minimum_date)::date as full_date
    from date_bounds,
    lateral flatten(
        input => array_generate_range(
            0,
            datediff(day, minimum_date, maximum_date) + 1
        )
    ) as generated_date
)

select
    to_number(to_char(full_date, 'YYYYMMDD'), 8, 0) as date_key,
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    monthname(full_date) as month_name,
    weekiso(full_date) as week_of_year,
    day(full_date) as day_of_month,
    dayofweekiso(full_date) as day_of_week,
    dayname(full_date) as day_name,
    dayofweekiso(full_date) in (6, 7) as is_weekend
from date_spine
