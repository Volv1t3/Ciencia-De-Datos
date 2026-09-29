-- Persist pairwise co-missingness evidence for reproducible data-quality review.
{{ config(materialized='table') }}

-- Pairwise co-missingness: 1.0 conditional probability means fields are always
-- missing together within the source month.
with null_flags as (
    -- Store the five null tests in one object per source row so the generic
    -- pairwise calculation below does not repeat separate SQL per pair.
    select source_month, object_construct(
        'passenger_count', passenger_count is null,
        'rate_code_id', rate_code_id is null,
        'store_and_fwd_flag', store_and_fwd_flag is null,
        'congestion_surcharge_amount', congestion_surcharge_amount is null,
        'airport_fee_amount', airport_fee_amount is null
    ) as flags
    from {{ ref('stg_yellow_taxi_trips') }}
),
pair_counts as (
    select source_month,
        left_missing.key::string as left_column_name,
        right_missing.key::string as right_column_name,
        count(*) as row_count,
        count_if(left_missing.value::boolean) as left_null_count,
        count_if(right_missing.value::boolean) as right_null_count,
        count_if(left_missing.value::boolean and right_missing.value::boolean) as both_null_count
    from null_flags,
        -- Flatten twice to form every possible field pair on every source row.
        lateral flatten(input => flags) left_missing,
        lateral flatten(input => flags) right_missing
    -- Remove self-pairs and mirrored duplicates such as (A,B) and (B,A).
    where left_missing.key < right_missing.key
    group by 1, 2, 3
)
select source_month, left_column_name, right_column_name, row_count,
    left_null_count, right_null_count, both_null_count,
    -- Conditional probabilities and Jaccard distinguish exact shared nulls
    -- from only similar null rates.
    both_null_count / nullif(left_null_count, 0)::float as p_right_null_given_left_null,
    both_null_count / nullif(right_null_count, 0)::float as p_left_null_given_right_null,
    both_null_count / nullif(left_null_count + right_null_count - both_null_count, 0)::float
        as null_jaccard_similarity
from pair_counts

/*
READ-AFTER-CODE: data flow through the null-correlation model

This query turns five per-row null checks into all unique pairs, then measures
whether the two members of every pair are absent on the same records. It runs
on staging, so it sees the raw source pattern before curation changes it.

    one staged trip row
              |
              v
    object: { field_name: is_null, ... }
              |
              v
       flatten on left x flatten on right
              |
              v
      every field pair for that trip row
              |
              | discard self/mirrored pairs
              v
    group by source month + field pair
              |
              v
    left null count, right null count, both null count
              |
              v
    conditional probabilities + Jaccard similarity

OBJECT_CONSTRUCT lets the statement define the monitored fields once. The two
LATERAL FLATTEN operations generate a Cartesian pairing of its keys. The lexical
comparison of keys retains, for example, passenger_count versus airport_fee once
but drops passenger_count versus itself and the mirrored airport_fee versus
passenger_count version.

The resulting counts support three complementary measures. P(right null given
left null) asks whether every left-null row is also right-null. The reverse
measure asks the opposite question. Jaccard similarity divides shared-null rows
by rows null on either side. When both conditionals and Jaccard are 1.0, the two
columns have exactly the same null records. That was the observed result for the
monitored source fields and is why the Silver policy preserves, rather than
fabricates, their operational values.
*/
