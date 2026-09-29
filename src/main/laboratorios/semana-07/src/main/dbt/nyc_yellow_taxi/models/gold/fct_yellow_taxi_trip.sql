-- Grain: exactly one valid, de-duplicated Silver Yellow Taxi trip per trip_id.
-- All dimensions are joined by their Gold keys; tests verify the FK contracts.
{{ config(materialized='table') }}

with silver_trips as (
    select * from {{ ref('silver_yellow_taxi_trips') }}
)

select
    -- Degenerate primary key: stable source-row identity retained in the fact.
    s.trip_id,
    pickup_date.date_key as pickup_date_key,
    dropoff_date.date_key as dropoff_date_key,
    vendor.vendor_key,
    rate.rate_code_key,
    store_and_fwd.store_and_fwd_key,
    pickup_location.location_key as pickup_location_key,
    dropoff_location.location_key as dropoff_location_key,
    payment.payment_type_key,
    source_file.source_file_key,

    -- Atomic trip timestamps and provenance remain useful fact attributes.
    s.source_row_number,
    s.pickup_datetime,
    s.dropoff_datetime,

    -- Core trip measures.
    1 as trip_count,
    s.trip_duration_seconds,
    round(s.trip_duration_seconds / 60.0, 2) as trip_duration_minutes,
    s.trip_distance_miles,
    s.passenger_count_observed,
    s.passenger_count,
    s.fare_amount,
    s.extra_amount,
    s.mta_tax_amount,
    s.tip_amount,
    s.tolls_amount,
    s.improvement_surcharge_amount,
    s.congestion_surcharge_amount,
    s.airport_fee_amount,
    s.cbd_congestion_fee_amount,
    -- A NULL component keeps this total NULL: unknown money is never called zero.
    s.mta_tax_amount
        + s.improvement_surcharge_amount
        + s.congestion_surcharge_amount
        + s.airport_fee_amount
        + s.cbd_congestion_fee_amount as total_tax_and_surcharge_amount,
    s.total_amount,

    -- Additive 0/1 quality metrics make BI rates straightforward to calculate.
    iff(s.passenger_count_was_imputed, 1, 0) as passenger_count_imputed_trip_count,
    iff(s.rate_code_id = 99, 1, 0) as rate_code_unknown_trip_count,
    iff(s.airport_fee_was_missing, 1, 0) as airport_fee_missing_trip_count,
    iff(s.congestion_surcharge_was_missing, 1, 0) as congestion_surcharge_missing_trip_count,

    -- Audit attributes preserve all Silver correction and normalization lineage.
    s.passenger_count_imputation_method,
    s.rate_code_id_observed,
    s.rate_code_id_was_missing,
    s.rate_code_id_normalization_method,
    s.store_and_fwd_flag_observed,
    s.store_and_fwd_flag_was_missing,
    s.store_and_fwd_flag_normalization_method,
    s.congestion_surcharge_amount_observed,
    s.congestion_surcharge_was_missing,
    s.congestion_surcharge_missingness_method,
    s.airport_fee_amount_observed,
    s.airport_fee_was_missing,
    s.airport_fee_missingness_method
from silver_trips s
-- Role-playing joins connect each timestamp to the same calendar dimension.
left join {{ ref('dim_date') }} pickup_date
    on to_date(s.pickup_datetime) = pickup_date.full_date
left join {{ ref('dim_date') }} dropoff_date
    on to_date(s.dropoff_datetime) = dropoff_date.full_date
left join {{ ref('dim_vendor') }} vendor
    on coalesce(s.vendor_id, -1) = vendor.vendor_key
left join {{ ref('dim_rate_code') }} rate
    on s.rate_code_id = rate.rate_code_key
left join {{ ref('dim_store_and_forward') }} store_and_fwd
    on case s.store_and_fwd_flag when 'Y' then 1 when 'N' then 2 else 99 end
        = store_and_fwd.store_and_fwd_key
-- One location dimension plays both pickup and dropoff roles.
left join {{ ref('dim_location') }} pickup_location
    on s.pickup_location_id = pickup_location.location_id
left join {{ ref('dim_location') }} dropoff_location
    on s.dropoff_location_id = dropoff_location.location_id
left join {{ ref('dim_payment_type') }} payment
    on coalesce(s.payment_type, -1) = payment.payment_type_key
left join {{ ref('dim_source_file') }} source_file
    on sha2(concat_ws('|', s.source_month, s.source_file, s.source_content_key, s.source_loaded_at), 256)
        = source_file.source_file_key

/*
READ-AFTER-CODE: fact-table data flow

The fact begins with exactly one Silver trip per trip_id. It does not aggregate
or change trip grain. Instead, it resolves each descriptive code into a Gold
dimension key, keeps atomic trip measures, and carries audit information that
explains how Silver handled missing source values.

      Silver trip (one row)
              |
              +--> pickup/dropoff dates ----> dim_date, twice
              +--> vendor/rate/flag/payment -> code dimensions
              +--> pickup/dropoff zones ----> dim_location, twice
              +--> file lineage ------------> dim_source_file
              |
              v
      fct_yellow_taxi_trip (one row, same trip_id)

trip_count and the 0/1 quality fields are additive helper measures. They let a
BI measure compute total trips, imputation rate, unknown-rate rate, and missing
fee rate through SUM rather than row-by-row logic. Duration minutes is a
convenience derivative of the canonical seconds measure. The fee-component sum
is intentionally NULL when any component is unknown, preserving the difference
between missing money and a known zero amount.
*/
