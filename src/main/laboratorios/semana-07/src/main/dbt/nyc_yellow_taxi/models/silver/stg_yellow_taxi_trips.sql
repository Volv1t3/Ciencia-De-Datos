-- A view provides a typed, queryable projection without persisting a duplicate
-- copy of the source-faithful Bronze data.
{{ config(materialized='view') }}

-- Keep one current version of each row. Re-uploaded source files are resolved
-- by latest load time, with deterministic secondary tie-breakers.
with bronze_deduplicated as (
    select
        source_month,
        source_file,
        source_row_number,
        source_content_key,
        raw_record,
        loaded_at as source_loaded_at
    from {{ source('bronze', 'yellow_taxi_trips_raw') }}
    qualify row_number() over (
        partition by source_month, source_row_number
        order by loaded_at desc, source_content_key desc nulls last, source_file desc
    ) = 1
),

typed as (
    select
        -- Stable pipeline key from TLC source-row identity, not a guessed
        -- real-world business key.
        sha2(concat_ws('|', source_month, source_row_number), 256) as trip_id,
        source_month,
        source_file,
        source_row_number,
        source_content_key,
        source_loaded_at,

        -- TRY_* casts make malformed source values visible as NULL rather than
        -- failing the full dbt run.
        try_to_number(raw_record:"VendorID"::string) as vendor_id,
        try_to_timestamp_ntz(raw_record:"tpep_pickup_datetime"::string) as pickup_datetime,
        try_to_timestamp_ntz(raw_record:"tpep_dropoff_datetime"::string) as dropoff_datetime,
        try_to_number(raw_record:"passenger_count"::string) as passenger_count,
        try_to_decimal(raw_record:"trip_distance"::string, 18, 3) as trip_distance_miles,
        try_to_number(raw_record:"RatecodeID"::string) as rate_code_id,
        -- Normalize flag text and make blank input a true SQL NULL.
        nullif(upper(trim(raw_record:"store_and_fwd_flag"::string)), '') as store_and_fwd_flag,
        try_to_number(raw_record:"PULocationID"::string) as pickup_location_id,
        try_to_number(raw_record:"DOLocationID"::string) as dropoff_location_id,
        try_to_number(raw_record:"payment_type"::string) as payment_type,
        try_to_decimal(raw_record:"fare_amount"::string, 18, 2) as fare_amount,
        try_to_decimal(raw_record:"extra"::string, 18, 2) as extra_amount,
        try_to_decimal(raw_record:"mta_tax"::string, 18, 2) as mta_tax_amount,
        try_to_decimal(raw_record:"tip_amount"::string, 18, 2) as tip_amount,
        try_to_decimal(raw_record:"tolls_amount"::string, 18, 2) as tolls_amount,
        try_to_decimal(raw_record:"improvement_surcharge"::string, 18, 2) as improvement_surcharge_amount,
        try_to_decimal(raw_record:"total_amount"::string, 18, 2) as total_amount,
        try_to_decimal(raw_record:"congestion_surcharge"::string, 18, 2) as congestion_surcharge_amount,
        try_to_decimal(raw_record:"Airport_fee"::string, 18, 2) as airport_fee_amount,
        try_to_decimal(raw_record:"cbd_congestion_fee"::string, 18, 2) as cbd_congestion_fee_amount
    -- Type conversion follows de-duplication so only current source rows flow on.
    from bronze_deduplicated
)

-- Staging remains pre-curation so null-analysis models measure incoming data.
select *
from typed

/*
READ-AFTER-CODE: data flow through this staging model

This statement starts with the Bronze table rather than a file. Kestra has
already loaded each Parquet record into RAW_RECORD as a Snowflake VARIANT and
attached file-level lineage. That choice is important: the original record is
never discarded just because a typed version later turns out to be missing or
malformed.

    Bronze table
    (one or more loaded versions of a source row)
                 |
                 v
    bronze_deduplicated
    (one chosen current version per month + source row)
                 |
                 v
    typed
    (VARIANT fields safely cast into analytical columns)
                 |
                 v
    stg_yellow_taxi_trips view

The first CTE resolves duplicate uploads before type conversion. Its partition
is the monthly TLC source-row identity: SOURCE_MONTH and SOURCE_ROW_NUMBER.
When the same input file is uploaded again, the latest LOADED_AT wins. This is
why a rerun cannot multiply a trip in later models. Content key and filename
are deterministic tie-breakers if two candidate versions share a timestamp.

The typed CTE then reads individual values from RAW_RECORD. TRY casts are a
deliberate fault boundary: an invalid string becomes NULL for that column, but
the surrounding record, source identifiers, and all other readable values keep
flowing. The next models can distinguish missing/unparseable data from a dbt
execution failure. trip_id is derived only from the stable source identity, so
an updated content version still refers to the same pipeline row.

No final quality rule is applied here. In particular, nulls, negative values,
and reversed timestamps are intentionally visible in staging. The null-profile
and null-correlation models must see this pre-curation population; otherwise a
filter could hide the very source-quality pattern the project is measuring.
*/
