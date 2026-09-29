-- Rate code 99 is the TLC-defined null/unknown member already normalized in
-- Silver. Other unexpected source codes remain visible for data-quality review.
{{ config(materialized='table') }}

with rate_codes as (
    select distinct rate_code_id as rate_code_key
    from {{ ref('silver_yellow_taxi_trips') }}
)

select
    rate_code_key,
    case rate_code_key
        when 1 then 'Standard rate'
        when 2 then 'JFK'
        when 3 then 'Newark'
        when 4 then 'Nassau or Westchester'
        when 5 then 'Negotiated fare'
        when 6 then 'Group ride'
        when 99 then 'Null/unknown'
        else 'Unrecognized rate code'
    end as rate_code_description,
    iff(rate_code_key = 99, true, false) as is_unknown_rate_code
from rate_codes

/*
READ-AFTER-CODE: rate-code dimension flow

Silver supplies the normalized code set. This dimension gives each code a
business label and explicitly marks the TLC unknown member 99. The fact joins
by rate_code_key, enabling route/rate analysis without repeating labels.
*/
