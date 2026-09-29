# Silver models

`stg_yellow_taxi_trips` retains source lineage and casts each Yellow Taxi field
from Bronze's `RAW_RECORD` VARIANT. It eliminates repeated Bronze merge keys by
keeping the latest loaded source row.

`silver_yellow_taxi_trips` is an idempotent Snowflake incremental merge keyed by
`trip_id`. It excludes records that cannot represent a usable trip:

- missing, unparseable, or reversed pickup/dropoff timestamps;
- pickup or dropoff zone IDs outside TLC's 1–265 range;
- zero or negative passenger counts, and negative distances.

Before curation, `silver_yellow_taxi_null_profile` measures null rates and
`silver_yellow_taxi_null_correlations` measures pairwise co-missingness,
conditional probabilities, and Jaccard similarity. These models make a shared
null pattern visible instead of treating it as random missingness.

Passenger count is the only statistically imputed attribute. When it is missing, the
intermediate model chooses the mode from valid, positive passenger counts using
the most specific eligible peer group: month/vendor/pickup/dropoff/payment/hour,
then month/vendor/pickup/payment, then the source-month mode. Groups require at
least 20 reference rows. `passenger_count_observed`,
`passenger_count_was_imputed`, and `passenger_count_imputation_method` preserve
the decision. Rate code, store-and-forward, congestion surcharge, and airport
fee are not imputed: their missingness indicators remain analytical features.

`RatecodeID` follows the TLC's explicit semantic domain: a missing source value
is normalized to code `99` (null/unknown). `rate_code_id_observed`,
`rate_code_id_was_missing`, and `rate_code_id_normalization_method` distinguish
that mapping from a source-provided value. `store_and_fwd_flag` is normalized to
`Y`, `N`, or `NOT_REPORTED`; it retains its observed value, missingness flag, and
normalization method. `NOT_REPORTED` means unavailable source data, never a
guessed operational flag.

Airport fee and congestion surcharge nulls remain null. Zero means a known zero
fee and must not be inferred from missing data. A dbt data test requires any
missing airport/congestion fee to have a missing RatecodeID; a future violation
fails the build and forces an explicit policy review.

`store_and_fwd_flag` is standardized to `Y`/`N`; other values become `NULL`.
The model carries the Bronze month, staged file, row number, content key, and
load timestamp for traceability.

`silver_taxi_months` exposes the allowed source-month key used by the trip model's
dbt relationship test, plus monthly valid-trip counts and Bronze load times.
