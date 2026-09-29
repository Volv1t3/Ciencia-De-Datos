-- Persist profile evidence so quality monitoring need not recreate this query.
{{ config(materialized='table') }}

-- Nulls are profiled before Silver exclusion and passenger-count imputation.
with source_rows as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
),
profile as (
    -- Each UNION ALL deliberately creates one metric per month and monitored
    -- column. UNION ALL preserves those distinct rows.
    select source_month, 'passenger_count' as column_name, count(*) as row_count,
        count_if(passenger_count is null) as null_count from source_rows group by 1
    union all
    select source_month, 'rate_code_id', count(*), count_if(rate_code_id is null)
    from source_rows group by 1
    union all
    select source_month, 'store_and_fwd_flag', count(*), count_if(store_and_fwd_flag is null)
    from source_rows group by 1
    union all
    select source_month, 'congestion_surcharge_amount', count(*), count_if(congestion_surcharge_amount is null)
    from source_rows group by 1
    union all
    select source_month, 'airport_fee_amount', count(*), count_if(airport_fee_amount is null)
    from source_rows group by 1
)
select source_month, column_name, row_count, null_count,
    -- NULLIF avoids a divide-by-zero failure for an empty future month.
    null_count / nullif(row_count, 0)::float as null_rate
from profile

/*
READ-AFTER-CODE: data flow through the null-profile model

The model measures missingness before Silver filtering and before passenger
imputation. That timing is essential: a profile built from final Silver would
show no passenger nulls by design and would hide the source condition that led
to the correction policy.

    stg_yellow_taxi_trips
              |
              v
         source_rows CTE
              |
              +--> passenger_count metric
              +--> rate_code_id metric
              +--> store_and_fwd_flag metric
              +--> congestion_surcharge_amount metric
              +--> airport_fee_amount metric
                         |
                         v
          UNION ALL metric rows per month
                         |
                         v
      null_count / row_count = null_rate

Each UNION ALL branch emits one row for a source month and a named field. It
contains the total staged row count and the number that are null. UNION ALL is
not an accidental SQL choice: two columns can have equal metrics, and those are
still different observations that must not be deduplicated.

The output is a compact monitoring table rather than a claim that fields are
missing together. Equal null counts only suggest a possible relationship. The
next model tests row-level overlap and provides the evidence needed for the
shared-missingness decision.
*/
