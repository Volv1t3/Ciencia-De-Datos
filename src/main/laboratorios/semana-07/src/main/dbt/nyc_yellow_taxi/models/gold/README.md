# Gold star schema

`fct_yellow_taxi_trip` has one row per valid, de-duplicated Silver trip. Its
foreign keys reference date (twice, for pickup/dropoff), vendor, rate code,
store-and-forward, location (twice, for pickup/dropoff), payment type, and
source-file lineage dimensions.

The fact contains atomic trip and financial measures plus additive helper
metrics such as `trip_count` and `passenger_count_imputed_trip_count`. Ratios
such as average fare and tip percentage belong in the BI semantic layer, where
they can respond correctly to report filters.

`dim_location` is built from the official TLC taxi-zone lookup seed. Run dbt
seed before dbt build so the source data exists in `S_CDATOS_GOLD`.
