-- Provider codes are conformed attributes, not repeated descriptive text in
-- every fact row. The -1 member represents a source record with no vendor code.
{{ config(materialized='table') }}

with vendor_codes as (
    select distinct coalesce(vendor_id, -1) as vendor_key
    from {{ ref('silver_yellow_taxi_trips') }}
)

select
    vendor_key,
    case vendor_key
        when 1 then 'Creative Mobile Technologies, LLC'
        when 2 then 'Curb Mobility, LLC'
        when 6 then 'Myle Technologies Inc'
        when 7 then 'Helix'
        when -1 then 'NOT_REPORTED'
        else 'UNRECOGNIZED_VENDOR_CODE'
    end as vendor_name
from vendor_codes

/*
READ-AFTER-CODE: vendor dimension flow

Distinct provider codes are extracted from Silver and mapped once to their TLC
labels. The fact stores only vendor_key. A missing source code becomes -1 and
NOT_REPORTED, so no trip loses its vendor relationship because of a null code.
*/
