# NYC Yellow Taxi Gold fact implementation guide

## Purpose

This guide documents the first Gold star-schema slice for semana-07. It builds
one atomic trip fact and seven conformed dimensions in S_CDATOS_GOLD.

Silver owns parsing, source de-duplication, validity rules, passenger-count
imputation, and semantic normalization. Gold owns analytical structure:
dimension keys, reusable labels, atomic measures, and tested relationships.

## Fact grain

fct_yellow_taxi_trip has one row for one valid, de-duplicated Silver trip. The
stable trip_id is its degenerate primary key.

A row is not a passenger, payment, or daily aggregate. A trip with two
passengers is still one fact row; passenger count is a measure.

~~~text
Silver: one curated trip
          |
          v
Gold: one fact row with same trip_id
          |
          +-- foreign keys to descriptive dimensions
          +-- atomic trip and monetary measures
          +-- Silver audit lineage
~~~

## Star schema

~~~text
                 dim_date
            / pickup     \ dropoff
dim_vendor --+             +-- dim_rate_code
dim_payment_type             dim_store_and_forward
            \               /
             fct_yellow_taxi_trip
            /               \
   dim_location              dim_source_file
 pickup + dropoff
~~~

Date and location are role-playing dimensions: each physical dimension is
built once but the fact references it twice.

## Dimension contract

| Dimension | Primary key | Main attributes | Fact foreign key |
|---|---|---|---|
| dim_date | date_key | date, year, quarter, month, weekday, weekend | pickup_date_key, dropoff_date_key |
| dim_vendor | vendor_key | TPEP provider name | vendor_key |
| dim_rate_code | rate_code_key | TLC rate name and unknown flag | rate_code_key |
| dim_store_and_forward | store_and_fwd_key | Y, N, NOT_REPORTED | store_and_fwd_key |
| dim_payment_type | payment_type_key | payment description | payment_type_key |
| dim_location | location_key | LocationID, borough, zone, service zone | pickup_location_key, dropoff_location_key |
| dim_source_file | source_file_key | source month, path, content key, load time | source_file_key |

dim_location comes from the official TLC Taxi Zone Lookup seed, not a long
hardcoded SQL CASE expression. The seed is taxi_zone_lookup.csv.

## Fact column decisions

### Keys and lineage

| Fact column | Reason |
|---|---|
| trip_id | Stable one-row-per-trip primary/degenerate key. |
| pickup_date_key, dropoff_date_key | Associate actual trip dates to the calendar dimension. Source month is delivery lineage, not trip time. |
| vendor_key, rate_code_key, store_and_fwd_key, payment_type_key | Replace repeated categorical labels with conformed dimensions. |
| pickup_location_key, dropoff_location_key | Two roles of the same official taxi-zone dimension. |
| source_file_key | Moves repeated file metadata into a provenance dimension. |
| source_row_number | Remains in the fact for exact forensic lineage within the file. |
| pickup_datetime, dropoff_datetime | Exact trip timestamps remain atomic fact attributes. |

### Measures

| Group | Columns |
|---|---|
| Volume and duration | trip_count, trip_duration_seconds, trip_duration_minutes, trip_distance_miles |
| Passenger values | passenger_count_observed, passenger_count |
| Fare components | fare_amount, extra_amount, mta_tax_amount, tip_amount, tolls_amount, improvement_surcharge_amount |
| Fees and total | congestion_surcharge_amount, airport_fee_amount, cbd_congestion_fee_amount, total_tax_and_surcharge_amount, total_amount |
| Quality helpers | passenger_count_imputed_trip_count, rate_code_unknown_trip_count, airport_fee_missing_trip_count, congestion_surcharge_missing_trip_count |

trip_count is always one. The quality helper columns are additive zero-or-one
measures, so a BI report can calculate a rate using SUM(helper) divided by
SUM(trip_count).

total_tax_and_surcharge_amount stays null when a component fee is null. A known
zero fee and unavailable source money are different facts.

### Audit attributes

The fact retains passenger imputation method, observed RatecodeID and its
normalization metadata, observed store-and-forward value and metadata, and the
observed/missingness metadata for airport and congestion fees.

These are attributes rather than business measures. They let an analyst compare
reported-only results with results including Silver corrections.

## Row-level columns versus Power BI measures

Gold keeps canonical row-level values. Filter-sensitive ratios belong in the
Power BI semantic layer.

~~~text
Gold fact columns                 Power BI measures
-----------------                 -----------------
trip_count                        Total Trips
total_amount                      Total Revenue
fare_amount                       Average Fare per Trip
tip_amount + fare_amount          Tip Percentage
trip_duration_seconds             Average Trip Duration
trip_distance_miles               Average Distance per Trip
quality helper columns            Imputation / unknown-fee rates
~~~

Tip percentage should be total tips divided by total fare, rather than an
average of individual trip percentages. Average fare should likewise be total
revenue divided by total trips. This makes both respond correctly to report
filters.

## Data flow

~~~text
SILVER_YELLOW_TAXI_TRIPS
          |
          +--> dim_date, twice
          +--> vendor/rate/store/payment dimensions
          +--> dim_location, twice
          +--> dim_source_file
          |
          v
FCT_YELLOW_TAXI_TRIP
one output row for each input trip_id
~~~

The fact uses left joins. If a dimension unexpectedly lacks a member, the fact
shows a null key and dbt tests fail; no trip is silently discarded.

## Integrity contract

The Gold schema YAML enforces the real primary-key and foreign-key contract:

- Every dimension key is unique and non-null.
- trip_id is unique and non-null.
- Every fact foreign key is non-null and has a dbt relationship test.
- Additive helper metrics are non-null.

This is stronger than standard Snowflake PK/FK declarations, which are usually
informational on ordinary Snowflake tables.

## Run commands

Create the schema first:

~~~sql
CREATE SCHEMA IF NOT EXISTS S_CDATOS_LABORATORIOINT.S_CDATOS_GOLD;
~~~

From laboratorios/semana-07, load the official lookup seed and build Gold:

~~~bash
docker compose --env-file src/res/env/.env run --rm dbt-core seed --select taxi_zone_lookup
docker compose --env-file src/res/env/.env run --rm dbt-core build --select path:models/gold
~~~

The seed command must run first because dim_location depends on it. The build
then creates every Gold dimension and fact model and executes their dbt tests.

## Scope

This first version keeps audit fields in the fact for clarity and traceability.
A future version may consolidate related methods and flags into a compact data
quality dimension after observing real combinations in production.

