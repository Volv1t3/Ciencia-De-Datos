-- Payment codes are sourced from facts so new values remain visible, then
-- given TLC-readable labels where the code is known.
{{ config(materialized='table') }}

with payment_codes as (
    select distinct coalesce(payment_type, -1) as payment_type_key
    from {{ ref('silver_yellow_taxi_trips') }}
)

select
    payment_type_key,
    case payment_type_key
        when 0 then 'Flex Fare trip'
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        when -1 then 'NOT_REPORTED'
        else 'UNRECOGNIZED_PAYMENT_CODE'
    end as payment_type_description
from payment_codes

/*
READ-AFTER-CODE: payment dimension flow

Distinct Silver payment codes define the key population. Known codes receive
labels; a missing code maps to -1/NOT_REPORTED and unfamiliar future codes stay
visible instead of disappearing. The fact therefore has a stable foreign key
for every trip while analysts receive descriptive categories.
*/
