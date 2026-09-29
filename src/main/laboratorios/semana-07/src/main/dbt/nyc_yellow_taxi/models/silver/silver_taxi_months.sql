-- Persist small monthly observability metrics for tests and downstream queries.
{{ config(materialized='table') }}

select
    source_month,
    -- These are Bronze lineage times, not dbt execution timestamps.
    min(source_loaded_at) as first_bronze_loaded_at,
    max(source_loaded_at) as last_bronze_loaded_at,
    count(*) as valid_trip_count
-- Count curated valid trips, rather than raw staged rows.
from {{ ref('silver_yellow_taxi_trips') }}
group by source_month

/*
READ-AFTER-CODE: data flow through the monthly observability model

This is a small table produced from the final Silver trips table, not directly
from Bronze. Therefore its count describes usable curated trips after validity
and passenger-count handling, rather than raw downloaded records.

    SILVER_YELLOW_TAXI_TRIPS
              |
              | group by source_month
              v
    SILVER_TAXI_MONTHS
    one row per TLC delivery month
    + first Bronze load time
    + last Bronze load time
    + valid Silver trip count

MIN and MAX source_loaded_at do not measure the dbt run. They describe the
range of Bronze ingestion timestamps represented in each monthly Silver slice.
This makes the table useful for operational review: a missing month, unexpected
volume, or changed load window becomes visible without scanning 74 million trip
rows. It also supplies the allowed source_month values for the relationship test
on the final trips model.
*/
