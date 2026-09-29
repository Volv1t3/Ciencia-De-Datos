-- The official TLC taxi-zone lookup is loaded with `dbt seed` before this model.
{{ config(materialized='table') }}

select
    locationid::number as location_key,
    locationid::number as location_id,
    borough,
    zone,
    service_zone
from {{ ref('taxi_zone_lookup') }}

/*
READ-AFTER-CODE: location dimension flow

The TLC seed supplies one descriptive row for each official LocationID. The
fact uses this single conformed dimension twice: pickup_location_key and
dropoff_location_key. This is called a role-playing dimension and avoids two
duplicated location tables.
*/
