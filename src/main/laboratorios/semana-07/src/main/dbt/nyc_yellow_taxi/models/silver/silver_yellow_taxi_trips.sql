-- Persist consumer-ready Silver trips. MERGE by trip_id makes reruns idempotent
-- and schema sync permits deliberate model evolution.
{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with curated as (
    select
        trip_id,
        source_month,
        source_file,
        source_row_number,
        source_content_key,
        source_loaded_at,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        -- Input eligibility guarantees non-reversed timestamps.
        datediff('second', pickup_datetime, dropoff_datetime) as trip_duration_seconds,
        passenger_count_observed,
        passenger_count,
        passenger_count_imputation_method,
        passenger_count_was_imputed,
        trip_distance_miles,
        -- Keep the raw rate alongside the semantic rate. TLC assigns 99 to an
        -- unknown/null rate, so a source NULL is normalized to that official code.
        rate_code_id as rate_code_id_observed,
        coalesce(rate_code_id, 99) as rate_code_id,
        rate_code_id is null as rate_code_id_was_missing,
        case
            when rate_code_id is null then 'source_null_mapped_to_unknown_99'
            else 'reported'
        end as rate_code_id_normalization_method,
        -- Keep the raw flag alongside its analyst-friendly semantic category.
        store_and_fwd_flag as store_and_fwd_flag_observed,
        -- Preserve only valid Y/N flags. A missing or malformed source value is
        -- explicitly not reported; it is never guessed as Y or N.
        case
            when store_and_fwd_flag in ('Y', 'N') then store_and_fwd_flag
            else 'NOT_REPORTED'
        end as store_and_fwd_flag,
        store_and_fwd_flag is null as store_and_fwd_flag_was_missing,
        case
            when store_and_fwd_flag is null then 'source_null_mapped_to_not_reported'
            when store_and_fwd_flag not in ('Y', 'N') then 'invalid_value_mapped_to_not_reported'
            else 'reported'
        end as store_and_fwd_flag_normalization_method,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        fare_amount,
        extra_amount,
        mta_tax_amount,
        tip_amount,
        tolls_amount,
        improvement_surcharge_amount,
        total_amount,
        -- Financial NULL is retained as unknown. A zero fee means a known zero
        -- and must not be inferred from a missing source value.
        congestion_surcharge_amount as congestion_surcharge_amount_observed,
        congestion_surcharge_amount,
        congestion_surcharge_amount is null as congestion_surcharge_was_missing,
        case
            when congestion_surcharge_amount is null then 'not_imputed_rate_code_unknown'
            else 'reported'
        end as congestion_surcharge_missingness_method,
        airport_fee_amount as airport_fee_amount_observed,
        airport_fee_amount,
        airport_fee_amount is null as airport_fee_was_missing,
        case
            when airport_fee_amount is null then 'not_imputed_rate_code_unknown'
            else 'reported'
        end as airport_fee_missingness_method,
        cbd_congestion_fee_amount
        -- Rate/fee validation is a dbt data test: a fee NULL with a known rate
        -- code fails the build and requires policy review.
    from {{ ref('int_yellow_taxi_passenger_count_imputation') }}
    -- Final consumers receive reported or resolved counts, never unresolved ones.
    where passenger_count is not null
)

select *
from curated

{% if is_incremental() %}
-- Reprocess watermark boundary rows because source rows can share a load time;
-- the unique MERGE key makes that safe.
where source_loaded_at >= (
    select coalesce(max(source_loaded_at), to_timestamp_ntz('1900-01-01 00:00:00'))
    from {{ this }}
)
{% endif %}

/*
READ-AFTER-CODE: data flow through the final Silver trips model

This model receives rows that have already passed the basic trip-validity
boundary and the passenger-count policy. It turns that intermediate relation
into the consumer-facing Silver table while preserving the evidence needed to
tell reported values from inferred values.

    eligible + passenger-imputation view
                    |
                    v
             curated CTE
    (lineage + duration + normalized flag + missingness flags)
                    |
                    v
     passenger_count IS NOT NULL filter
                    |
                    v
       incremental source-load watermark
                    |
                    v
    Snowflake MERGE into SILVER_YELLOW_TAXI_TRIPS
          keyed by trip_id

The curated CTE mainly carries fields forward. It adds trip_duration_seconds
only after the upstream check has guaranteed that dropoff is not before pickup.
It normalizes a missing RatecodeID to TLC's official unknown code 99, while
retaining rate_code_id_observed and a normalization method. It normalizes a
store-and-forward source NULL to NOT_REPORTED, while retaining the observed
value and missingness/method columns. NOT_REPORTED never means an inferred Y or
N; it says that this source record did not provide the operational value.

The missingness columns preserve source completeness information for a future
Gold star schema. An airport-fee value of zero is different from an airport-fee
value TLC did not report. For that reason, airport and congestion fee NULLs are
not mapped to zero: their observed values, missingness flags, and methods say
explicitly that the source fee was unavailable with an unknown rate code.

The final filter removes only the rare eligible rows that remain without any
passenger-count value after all fallback levels. On a full build every curated
row is selected. On an incremental build, the source_loaded_at watermark limits
work to new or potentially revised Bronze loads. The greater-than-or-equal
boundary is intentional: two source rows can share a timestamp, and MERGE by
trip_id safely reprocesses the boundary without duplicating it.
*/
