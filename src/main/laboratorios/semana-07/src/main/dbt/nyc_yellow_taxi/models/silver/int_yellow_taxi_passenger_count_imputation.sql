-- Keep this intermediate policy virtual; the final Silver model persists data.
{{ config(materialized='view') }}

-- Impute only passenger_count, using positive reported counts and peer groups
-- that deliberately omit the fields with correlated missingness.
with eligible_trips as (
    select
        *,
        -- Map NULL vendor/payment values to a stable match key for peer joins.
        coalesce(vendor_id, -1) as vendor_id_key,
        coalesce(payment_type, -1) as payment_type_key,
        date_part('hour', pickup_datetime) as pickup_hour
    from {{ ref('stg_yellow_taxi_trips') }}
    -- A row must first represent a usable trip before becoming donor or target.
    where pickup_datetime is not null
      and dropoff_datetime is not null
      and dropoff_datetime >= pickup_datetime
      and pickup_location_id between 1 and 265
      and dropoff_location_id between 1 and 265
      -- Remove negatives. NULL distance remains NULL in output; zero is only
      -- used here to avoid excluding a row for missing distance alone.
      and coalesce(trip_distance_miles, 0) >= 0
      -- Source zero/negative passenger counts are invalid. NULL values remain
      -- eligible because this is the one field the policy can resolve.
      and (passenger_count > 0 or passenger_count is null)
),

mode_reference as (
    -- Use only positive observed values as donors; never feed prior imputations
    -- back into the reference distribution.
    select * from eligible_trips where passenger_count > 0
),

fine_mode_counts as (
    -- Strongest peer definition: same month/vendor/route/payment/hour.
    select source_month, vendor_id_key, pickup_location_id, dropoff_location_id,
        payment_type_key, pickup_hour, passenger_count,
        count(*) as passenger_count_frequency
    from mode_reference
    group by 1, 2, 3, 4, 5, 6, 7
),

fine_modes as (
    -- Count donor support and select the mode. Lower count resolves a frequency
    -- tie deterministically.
    select *, sum(passenger_count_frequency) over (
        partition by source_month, vendor_id_key, pickup_location_id,
        dropoff_location_id, payment_type_key, pickup_hour
    ) as peer_group_size
    from fine_mode_counts
    qualify row_number() over (
        partition by source_month, vendor_id_key, pickup_location_id,
        dropoff_location_id, payment_type_key, pickup_hour
        order by passenger_count_frequency desc, passenger_count asc
    ) = 1
),

coarse_mode_counts as (
    -- Relax route/hour detail for targets without a sufficiently large fine group.
    select source_month, vendor_id_key, pickup_location_id, payment_type_key,
        passenger_count, count(*) as passenger_count_frequency
    from mode_reference
    group by 1, 2, 3, 4, 5
),

coarse_modes as (
    select *, sum(passenger_count_frequency) over (
        partition by source_month, vendor_id_key, pickup_location_id, payment_type_key
    ) as peer_group_size
    from coarse_mode_counts
    qualify row_number() over (
        partition by source_month, vendor_id_key, pickup_location_id, payment_type_key
        order by passenger_count_frequency desc, passenger_count asc
    ) = 1
),

month_mode_counts as (
    -- Final fallback uses all positive reported counts within the same month.
    select source_month, passenger_count, count(*) as passenger_count_frequency
    from mode_reference
    group by 1, 2
),

month_modes as (
    select * from month_mode_counts
    qualify row_number() over (
        partition by source_month
        order by passenger_count_frequency desc, passenger_count asc
    ) = 1
)

select
    e.trip_id, e.source_month, e.source_file, e.source_row_number,
    e.source_content_key, e.source_loaded_at, e.vendor_id, e.pickup_datetime,
    e.dropoff_datetime, e.passenger_count as passenger_count_observed,
    -- Policy order: reported, supported fine mode, supported coarse mode, month.
    coalesce(
        e.passenger_count,
        iff(fine.peer_group_size >= 20, fine.passenger_count, null),
        iff(coarse.peer_group_size >= 20, coarse.passenger_count, null),
        month.passenger_count
    ) as passenger_count,
    -- Keep the decision visible to analysts and future Gold models.
    case
        when e.passenger_count is not null then 'reported'
        when fine.peer_group_size >= 20 then 'mode_location_route_hour'
        when coarse.peer_group_size >= 20 then 'mode_location_payment'
        when month.passenger_count is not null then 'mode_month'
        else 'unresolved'
    end as passenger_count_imputation_method,
    -- This says the source value was absent, whether or not it resolved later.
    e.passenger_count is null as passenger_count_was_imputed,
    e.trip_distance_miles, e.rate_code_id, e.store_and_fwd_flag,
    e.pickup_location_id, e.dropoff_location_id, e.payment_type,
    e.fare_amount, e.extra_amount, e.mta_tax_amount, e.tip_amount,
    e.tolls_amount, e.improvement_surcharge_amount, e.total_amount,
    e.congestion_surcharge_amount, e.airport_fee_amount, e.cbd_congestion_fee_amount
from eligible_trips e
-- LEFT JOIN retains a valid target when a peer group is unavailable.
left join fine_modes fine
    on e.source_month = fine.source_month
   and e.vendor_id_key = fine.vendor_id_key
   and e.pickup_location_id = fine.pickup_location_id
   and e.dropoff_location_id = fine.dropoff_location_id
   and e.payment_type_key = fine.payment_type_key
   and e.pickup_hour = fine.pickup_hour
left join coarse_modes coarse
    on e.source_month = coarse.source_month
   and e.vendor_id_key = coarse.vendor_id_key
   and e.pickup_location_id = coarse.pickup_location_id
   and e.payment_type_key = coarse.payment_type_key
-- Do not borrow passenger behaviour across delivery months.
left join month_modes month on e.source_month = month.source_month

/*
READ-AFTER-CODE: data flow through passenger-count imputation

The model first decides which staged rows are trustworthy enough to analyse.
An eligible row has usable pickup and dropoff times in the correct order, TLC
zone IDs in the 1--265 range, and no negative distance. A reported passenger
count of zero or below is rejected. A missing passenger count is different: it
survives as a target for a documented replacement rule.

    typed staging rows
             |
             v
    eligible_trips ------------------------------+
             |                                   |
             | positive reported counts           | missing passenger counts
             v                                   |
    mode_reference                                |
             |                                   |
             +--> fine mode / coarse mode / month mode
                                                  |
                                                  v
                     LEFT JOIN mode lookups + COALESCE priority
                                                  |
                                                  v
                    rows with observed and imputed passenger lineage

mode_reference is intentionally smaller than eligible_trips. It contains only
positive, reported counts, so a previous imputation can never become evidence
for a later imputation. This prevents the model from amplifying its own guess.
The three mode CTE families calculate the same statistic at progressively less
specific levels. The fine group is month/vendor/origin/destination/payment/hour;
the coarse group removes destination and hour; the final group is the whole
source month. Each mode is the passenger value with the largest frequency. A
tie selects the smaller count, so repeated runs choose the same value.

The final SELECT does not replace the source blindly. COALESCE keeps a reported
value first. For a source null, it accepts a fine or coarse mode only if its
group has at least twenty observed donor rows. If neither is sufficiently
supported, the source-month mode is used. The CASE expression writes the path
chosen into passenger_count_imputation_method, and the original source value
remains in passenger_count_observed.

The related missing fields are not input to the peer grouping and are passed
through unchanged. The null study showed they occur as the same missing block,
so this model avoids manufacturing separate rate-code, flag, or fee values from
incomplete evidence. An eligible row with no possible monthly mode leaves this
model as unresolved; the final Silver model is the one that excludes it.
*/
