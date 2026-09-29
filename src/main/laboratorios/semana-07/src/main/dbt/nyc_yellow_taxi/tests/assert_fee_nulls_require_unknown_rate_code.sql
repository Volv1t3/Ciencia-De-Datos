-- This data test returns rows only when the source missingness assumption is
-- broken. dbt interprets returned rows as a test failure.
--
-- The current source profile shows airport/congestion fee nulls occur only with
-- a null RatecodeID. If a future delivery contains a known rate with a missing
-- fee, it must be reviewed; the Silver model must not silently call it zero.
select
    trip_id,
    source_month,
    rate_code_id,
    airport_fee_amount,
    congestion_surcharge_amount
from {{ ref('stg_yellow_taxi_trips') }}
where rate_code_id is not null
  and (
      airport_fee_amount is null
      or congestion_surcharge_amount is null
  )
