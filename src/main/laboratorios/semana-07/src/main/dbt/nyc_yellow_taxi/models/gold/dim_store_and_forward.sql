-- Small fixed dimension for the normalized Silver flag domain.
{{ config(materialized='table') }}

select column1::number as store_and_fwd_key, column2::varchar as store_and_fwd_flag,
    column3::varchar as store_and_fwd_description
from values
    (1, 'Y', 'Stored in vehicle memory before vendor transmission'),
    (2, 'N', 'Transmitted without store-and-forward handling'),
    (99, 'NOT_REPORTED', 'Source record did not report the flag')

/*
READ-AFTER-CODE: store-and-forward dimension flow

This fixed reference table defines the complete Gold domain: Y, N, and
NOT_REPORTED. The fact converts the Silver categorical value to its integer
key, preserving a readable dimension without guessing a missing operational
state.
*/
