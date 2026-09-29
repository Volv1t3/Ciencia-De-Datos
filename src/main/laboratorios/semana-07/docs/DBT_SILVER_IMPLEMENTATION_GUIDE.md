# NYC Yellow Taxi: Bronze-to-Silver dbt implementation guide

## Purpose

This is the complete code companion for the dbt Silver layer in
laboratorios/semana-07. It explains project settings, source contract, each
model and SQL block, tests, validation queries, and the data-quality decisions
behind them.

Source:

~~~text
S_CDATOS_LABORATORIOINT.S_CDATOS_BRONZE.YELLOW_TAXI_TRIPS_RAW
~~~

Target:

~~~text
S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER
~~~

## Plain-language overview

This is not machine learning. It is a transparent cleaning workflow with one
statistical rule. If a valid trip has a missing passenger count, the pipeline
uses the most common positive count among similar complete trips. That most
common value is called the mode.

The pipeline does not quietly delete or overwrite information:

- passenger_count_observed keeps the original reported source value.
- passenger_count is the curated usable value.
- passenger_count_was_imputed identifies an originally missing count.
- passenger_count_imputation_method says which rule supplied a replacement.
- Missing RatecodeID becomes official TLC unknown code 99; the observed value,
  missingness flag, and normalization method remain available.
- Missing store-and-forward flag becomes NOT_REPORTED; it also retains observed
  value, missingness flag, and normalization method.
- Missing airport/congestion fees remain null with observed/missingness/method
  lineage because a missing fee is not evidence of a known zero.

This production implementation carries forward the approach used in the
earlier notebooks: compare a missing record with relevant complete records,
then use the modal value. SQL makes the grouping, threshold, fallback order,
and audit fields repeatable.

## Dependency map

~~~text
Bronze RAW_RECORD (VARIANT)
          |
          v
stg_yellow_taxi_trips ----------------> silver_yellow_taxi_null_profile
          |  \------------------------> silver_yellow_taxi_null_correlations
          v
int_yellow_taxi_passenger_count_imputation
          |
          v
silver_yellow_taxi_trips -------------> silver_taxi_months
~~~

A dbt build selecting models/silver follows this lineage, builds models, then
runs the assertions in schema.yml.

## Decisions and supporting evidence

| Decision | Reason | Implementation |
|---|---|---|
| Preserve Bronze faithfully | Reruns and source corrections need traceability. | Keep source file, row, content key, raw VARIANT, and load time. |
| Parse safely | One malformed value must not stop the month. | TRY casts turn invalid source text into measurable nulls. |
| Reject unusable trips | Analysis needs valid time order and TLC zones. | Require timestamps, non-reversed order, zone IDs 1--265, non-negative distance. |
| Treat passenger zero/negative as invalid | It is not a usable passenger observation for this project. | Keep positive counts and null candidates only. |
| Impute passenger count statistically | Notebook-derived peer groups supply a supported mode when source count is missing. | Use a fine, coarse, then monthly mode and preserve method lineage. |
| Normalize missing RatecodeID | TLC reserves code 99 for null/unknown. | Map source null to 99 while retaining observed value, missingness flag, and method. |
| Normalize missing store-and-forward flag | A readable category is useful, but missing input does not mean Y or N. | Map source null to NOT_REPORTED and preserve observed value, missingness flag, and method. |
| Preserve missing financial fees | Zero is a known monetary amount, not a synonym for absent source data. | Retain fee nulls with observed/missingness/method lineage and validate their rate-code relationship. |
| Require 20 peer records | One or two donor records make an unstable mode. | Fine and coarse modes require peer_group_size >= 20. |
| Use monthly fallback | A valid row can lack close peers. | Use source-month positive passenger mode last. |
| Preserve missingness | Gold consumers must distinguish missing from known. | Include was_missing fields. |
| Make reruns safe | Files can be uploaded again. | Stage de-duplicates; Silver merges by trip_id. |

### What the null study proved

The supplied correlation results showed exactly the same null rows for:

- passenger_count
- rate_code_id
- store_and_fwd_flag
- congestion_surcharge_amount
- airport_fee_amount

For every monitored pair, both conditional null probabilities and Jaccard
similarity were 1.0. This is perfect co-missingness: the values disappear
together, not merely at similar rates. Therefore, only passenger count is
filled; the other fields remain explicitly unknown.

The completed validation found 74,743,065 Silver trips, zero invalid passenger
counts, and zero reversed timestamps. There were 18,406,224 imputed passenger
counts, 24.63% of Silver. In 2025-01, 540,031 of 3,450,447 trips, 15.65%, were
imputed. Those records remain in Silver; they were not removed.

January source profiling had 540,149 passenger nulls. The 118-record difference
from January's imputed count is expected: a source record can fail another
eligibility rule before becoming an imputation candidate. It is not a failed
imputation count.

### Semantic normalization revision

The project later introduced deterministic normalization for two categorical
fields. This is deliberately different from passenger-count imputation:

| Source condition | Final value | Why it is defensible | Lineage retained |
|---|---|---|---|
| `rate_code_id IS NULL` | `99` | TLC defines 99 as null/unknown. | `rate_code_id_observed`, `rate_code_id_was_missing`, `rate_code_id_normalization_method` |
| `store_and_fwd_flag IS NULL` | `NOT_REPORTED` | This states data availability without guessing Y or N. | `store_and_fwd_flag_observed`, `store_and_fwd_flag_was_missing`, `store_and_fwd_flag_normalization_method` |
| Airport or congestion fee is null | Remains null | A zero is a known fee; source absence is unknown. | observed amount, `*_was_missing`, and `*_missingness_method` |

The accompanying data test fails when a staged airport or congestion fee is
null while its RatecodeID is known. That makes the current no-financial-
imputation policy explicit: if a future delivery breaks the observed
co-missingness pattern, the pipeline requires review instead of silently
writing a zero.

# Project and connection configuration

## dbt_project.yml

| Lines | Setting | Explanation |
|---|---|---|
| 1 | project name | dbt namespace: nyc_yellow_taxi. |
| 2 | version 1.0.0 | Project release label, not a data setting. |
| 3 | config-version 2 | Current dbt project configuration format. |
| 4 | profile nyc_yellow_taxi | Uses the identically named connection profile. |
| 6--8 | model, macro, test paths | Where dbt discovers project components. |
| 9 | clean targets | Local build files dbt may remove; it does not delete Snowflake tables. |
| 11--15 | Silver default | Default view materialization in S_CDATOS_SILVER. Individual models can override it. |
| 16--18 | Gold default | Future Gold models default to physical tables in GOLD. |

## profiles.yml

Path: src/res/config/dbt/profiles/profiles.yml.

| Lines | Setting | Explanation |
|---|---|---|
| 1--4 | project profile, dev target | Defines the connection chosen by local dbt and dbt UI. |
| 5 | Snowflake adapter | Selects Snowflake connection behaviour. |
| 6--11 | SNOWFLAKE environment variables | Account, user, password, role, warehouse, and database are supplied by container environment, not hard-coded. |
| 12 | Silver schema default | Uses S_CDATOS_SILVER unless intentionally overridden. |
| 13 | threads default 4 | Makes execution parallelism configurable. |
| 14 | keepalive false | Idle Snowflake sessions close normally. |

dbt uses ordinary SNOWFLAKE environment variables. Kestra uses a separate
secret mechanism. Never put literal credentials in models, profiles, or Git.

## macros/generate_schema_name.sql

| Lines | Code | Explanation |
|---|---|---|
| 1 | macro declaration | Overrides dbt schema resolution. |
| 2--3 | no custom schema | Uses the profile target schema. |
| 4--5 | custom schema | Uses the trimmed configured name exactly. This avoids a default dbt prefix and ensures S_CDATOS_SILVER is the real schema. |
| 6--7 | close blocks | Ends the Jinja conditional and macro. |

# Bronze source contract

## models/sources/sources.yml

| Lines | Code block | Explanation |
|---|---|---|
| 1 | version 2 | dbt source YAML format. |
| 3--5 | source bronze | Defines the name used by source references. |
| 6 | database environment variable | Keeps deployment portable without model edits. |
| 7 | S_CDATOS_BRONZE | Physical Bronze schema. |
| 8--11 | raw table identifier | Maps the logical dbt name to uppercase YELLOW_TAXI_TRIPS_RAW. |
| 12--24 | descriptions | Defines lineage fields: month, file, row, content key, raw record, and load time. |

The natural source-row identity is SOURCE_MONTH plus SOURCE_ROW_NUMBER.
SOURCE_CONTENT_KEY and LOADED_AT distinguish a later replacement of the same
source row.

# Model walkthrough

## 1. stg_yellow_taxi_trips.sql

The staging model is a view, deliberately prior to curation. It types and
de-duplicates the source while leaving the null study able to inspect incoming
data.

| Lines | Block | Explanation |
|---|---|---|
| 1 | view configuration | Avoids a redundant physical copy of Bronze. |
| 3--11 | bronze_deduplicated CTE | Reads source metadata and renames loaded_at to source_loaded_at. |
| 12--15 | row_number qualify | Keeps one row per source month and source row. Latest load wins; content key and file path break ties deterministically. This removes duplicate rerun uploads. |
| 18--25 | typed CTE metadata | Carries lineage and makes trip_id as SHA-256 of month and source row. |
| 27 | VendorID TRY number | Invalid source text becomes null instead of failing the build. |
| 28--29 | timestamp TRY casts | Parse pickup and dropoff independently without failing a whole month. |
| 30 | passenger TRY number | Keeps a readable reported count; no fill happens here. |
| 31 | distance decimal 18,3 | Stores miles at three decimal places. |
| 32 | rate-code number | Parses the source rate code. Final Silver later maps only source nulls to official unknown code 99, preserving the observed value and method. |
| 33 | normalized store-forward | Trims/uppercases flags and turns empty string into null. |
| 34--36 | zone and payment fields | Prepare categorical values for quality checks and peer grouping. |
| 37--46 | monetary TRY decimals | Parse values to cents and preserve values rather than secretly correcting them. |
| 47 | source from CTE | Makes typing happen after de-duplication. |
| 50--51 | final select | Exposes typed staging rows. |

TRY casts are an observability choice: invalid values become visible nulls,
rather than causing one damaged field to stop the full transformation.

## 2. int_yellow_taxi_passenger_count_imputation.sql

This intermediate view contains the full passenger-count policy.

### Eligibility and donor data

| Lines | Block | Explanation |
|---|---|---|
| 1 | view configuration | Keep intermediate calculation virtual. |
| 3--4 | comments | State the guardrail: only passenger count is imputed. |
| 5--11 | eligible_trips and keys | Coalesce null vendor/payment to key -1; derive pickup hour as a similarity attribute. |
| 12--14 | timestamp predicates | Require two usable timestamps and non-reversed order. |
| 15--16 | zone predicates | Keep valid TLC zone IDs from 1 through 265. |
| 17 | distance predicate | Remove negative distance. Null distance temporarily behaves as zero for the predicate only; it is still null in output. |
| 18 | passenger predicate | Remove reported zero/negative counts; retain positive counts and missing candidates. |
| 21--23 | mode_reference | Donors must have a positive observed count. Imputed values never become donors. |

### Fine, coarse, and monthly modes

| Lines | Block | Explanation |
|---|---|---|
| 25--31 | fine_mode_counts | Count passenger values in same month, vendor, pickup zone, dropoff zone, payment type, and pickup hour. |
| 33--44 | fine_modes | Count donors in each group and choose its most frequent value. A frequency tie chooses lower passenger count deterministically. |
| 46--51 | coarse_mode_counts | Relax similarity by omitting dropoff zone and pickup hour. |
| 53--62 | coarse_modes | Choose coarse-group mode with the same deterministic tie rule. |
| 64--68 | month_mode_counts | Count positive passenger values in every source month. |
| 70--76 | month_modes | Choose that month's most frequent positive count as final fallback. |

Priority is: fine peer group, then coarse peer group, then source month, then
unresolved. Fine and coarse options must each have at least 20 donor records.

### Final output and joins

| Lines | Block | Explanation |
|---|---|---|
| 78--81 | lineage plus observed count | Retain source identifiers and original count. |
| 82--87 | coalesce replacement | Use reported value first, then eligible fine mode, coarse mode, and monthly mode. |
| 88--94 | method case | Label reported, mode_location_route_hour, mode_location_payment, mode_month, or unresolved. |
| 95 | boolean flag | Records an original source null independently of final result. |
| 96--100 | passthrough columns | Preserve other attributes for final semantic normalization. No financial amount is statistically inferred. |
| 101 | eligible source | Enforces valid-trip scope. |
| 102--108 | fine left join | Finds strongest peer data without discarding a row with no group. |
| 109--113 | coarse left join | Finds the documented relaxed peer data. |
| 114 | monthly left join | Supplies the final fallback. |

A resolved null is imputed and retained. A record is removed only because it
fails eligibility or remains unresolved after every fallback.

## 3. silver_yellow_taxi_trips.sql

| Lines | Block | Explanation |
|---|---|---|
| 1--8 | incremental configuration | Persist table, MERGE by trip_id, and synchronize schema changes. This overrides project view default. |
| 10--20 | curated source lineage | Retain evidence needed to trace every final trip. |
| 21 | duration calculation | Derive seconds from already validated non-reversed timestamps. |
| 22--25 | passenger audit fields | Expose original, final, method, and imputed flag together. |
| categorical normalization block | Preserve `rate_code_id_observed`, map a source null to official code 99, and record the missingness/method. |
| flag normalization block | Preserve `store_and_fwd_flag_observed`; emit Y, N, or NOT_REPORTED and record why it was normalized. |
| fee lineage block | Keep airport/congestion fees unchanged, preserve observed values and missingness/method columns, and never equate an unknown fee to zero. |
| fee/rate data test | Fails if a staged fee is null while RatecodeID is known, forcing policy review before any fee imputation. |
| 49 | intermediate reference | Builds dbt dependency and order. |
| 50 | final passenger filter | Keeps only reported or successfully resolved passenger counts. |
| 53--54 | final result | Returns curated relation. |
| 56--61 | incremental watermark | Reprocesses source rows at or beyond current max load timestamp. Greater-than-or-equal avoids missed boundary rows; merge makes reprocessing safe. 1900 creates full first load. |

## 4. silver_taxi_months.sql

| Lines | Block | Explanation |
|---|---|---|
| 1 | table configuration | Persist small reusable monthly observability data. |
| 3--7 | month, min/max, count | Compute first/last Bronze source load and valid-trip count. |
| 8 | final Silver reference | Metrics describe curated trips, not raw records. |
| 9 | grouping | Guarantees one row per source month. |

## 5. silver_yellow_taxi_null_profile.sql

| Lines | Block | Explanation |
|---|---|---|
| 1 | table configuration | Persist data-quality evidence. |
| 3 | comment | Explicitly profiles before filtering and imputation. |
| 4--6 | source_rows CTE | Names staging input for repeated aggregation. |
| 7--21 | five UNION ALL branches | Produce a month/column row for each investigated nullable field. UNION ALL preserves deliberately distinct metric rows. |
| 23--25 | rate calculation | Divide nulls by rows; NULLIF prevents divide-by-zero. |

## 6. silver_yellow_taxi_null_correlations.sql

| Lines | Block | Explanation |
|---|---|---|
| 1 | table configuration | Persist the co-missingness study. |
| 3--4 | comment | Define conditional probability 1.0 as always missing together. |
| 5--14 | object construct | Put five null flags into one object for each source row. |
| 15--28 | double lateral flatten | Expand the object twice to generate pairs. Left key less than right key removes self-pairs and mirrored duplicates. Count each side and joint nulls. |
| 29--34 | conditional rates/Jaccard | Calculate both directional conditional probabilities and joint-over-union similarity. NULLIF handles no-null denominator safely. |

# Tests and documentation: models/silver/schema.yml

schema.yml documents the catalog and turns central promises into executable
tests. After the categorical-normalization revision, the completed dbt build
reported six models and 27 successful tests, including the fee/rate safeguard.

| Lines | Block | Guarantee |
|---|---|---|
| 1 | version | dbt YAML format. |
| 3--8 | staging description | Staging is typed/de-duplicated but pre-curation. |
| 9--14 | final-trip description | States cleaning boundary and preservation of operational nulls. |
| 16--20 | trip_id tests | Every final trip has a unique non-null merge key. |
| 21--27 | source_month tests | Every trip has a month that exists in silver_taxi_months. |
| 28--33 | lineage tests | Source row number and source load timestamp cannot be null. |
| 34--37 | passenger test | Final passenger count cannot be null. |
| 38--49 | timestamp/zone tests | Required trip timestamps and zones cannot be null. |
| 51--60 | month table tests | Month key is unique/non-null; valid count is non-null. |
| 62--73 | profile tests | Null metrics always identify month, column, and count. |
| 75--86 | correlation tests | Correlation metrics always identify month and both columns. |

# Validation SQL

Run these after a successful dbt build.

## A. Silver trip count and passenger imputation

~~~sql
SELECT
  source_month,
  COUNT(*) AS silver_trip_count,
  COUNT_IF(passenger_count_was_imputed) AS imputed_passenger_count,
  ROUND(
    100.0 * COUNT_IF(passenger_count_was_imputed) / NULLIF(COUNT(*), 0),
    2
  ) AS imputed_pct
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER.SILVER_YELLOW_TAXI_TRIPS
GROUP BY 1
ORDER BY 1;
~~~

COUNT star counts curated trips. COUNT_IF counts original passenger nulls
retained with lineage. NULLIF protects the rate if a month has zero rows.
Grouping and ordering by column one produce one chronological line per month.

## B. Read the persisted null profile

~~~sql
SELECT
  source_month,
  column_name,
  row_count,
  null_count,
  ROUND(100.0 * null_rate, 2) AS null_pct
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER.SILVER_YELLOW_TAXI_NULL_PROFILE
ORDER BY source_month, column_name;
~~~

Equal null counts suggest a relationship but do not prove they are the same
records. Query C makes that distinction.

## C. Prove or reject co-missingness

~~~sql
SELECT
  source_month,
  left_column_name,
  right_column_name,
  row_count,
  left_null_count,
  right_null_count,
  both_null_count,
  p_right_null_given_left_null,
  p_left_null_given_right_null,
  null_jaccard_similarity
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER.SILVER_YELLOW_TAXI_NULL_CORRELATIONS
ORDER BY source_month, left_column_name, right_column_name;
~~~

| Result | Meaning |
|---|---|
| Both conditional probabilities and Jaccard equal 1 | Exact same null rows. |
| One conditional equals 1 but Jaccard is below 1 | One field is always null with the other, but not vice versa. |
| Both conditionals below 1 | Overlapping rather than shared missingness. |
| Conditional is null | No nulls existed on that denominator side. |

## D. Confirm Silver invariants

~~~sql
SELECT
  COUNT(*) AS silver_trip_count,
  COUNT_IF(passenger_count <= 0 OR passenger_count IS NULL)
    AS invalid_passenger_count,
  COUNT_IF(dropoff_datetime < pickup_datetime)
    AS reversed_timestamp_count
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER.SILVER_YELLOW_TAXI_TRIPS;
~~~

The observed output was 74,743,065 total trips, zero invalid passenger counts,
and zero reversed timestamps.

## E. Audit imputation methods

~~~sql
SELECT
  source_month,
  passenger_count_imputation_method,
  COUNT(*) AS trip_count
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER.SILVER_YELLOW_TAXI_TRIPS
GROUP BY 1, 2
ORDER BY 1, 2;
~~~

An increase in mode_month means detailed peer groups may be sparse or source
data behaviour may have changed.

## F. Reconcile staged data to eligibility

~~~sql
SELECT
  source_month,
  COUNT(*) AS staged_rows,
  COUNT_IF(
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND dropoff_datetime >= pickup_datetime
    AND pickup_location_id BETWEEN 1 AND 265
    AND dropoff_location_id BETWEEN 1 AND 265
    AND COALESCE(trip_distance_miles, 0) >= 0
    AND (passenger_count > 0 OR passenger_count IS NULL)
  ) AS eligible_before_imputation
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER.STG_YELLOW_TAXI_TRIPS
GROUP BY 1
ORDER BY 1;
~~~

This separates raw volume from valid imputation candidates and explains
differences between raw null counts and final imputed counts.

## G. Audit categorical normalization and fee safeguards

~~~sql
SELECT
  source_month,
  COUNT_IF(rate_code_id_was_missing) AS rate_code_mapped_to_99,
  COUNT_IF(store_and_fwd_flag_was_missing) AS store_flag_mapped_to_not_reported,
  COUNT_IF(airport_fee_was_missing) AS airport_fee_retained_unknown,
  COUNT_IF(congestion_surcharge_was_missing) AS congestion_fee_retained_unknown
FROM S_CDATOS_LABORATORIOINT.S_CDATOS_SILVER.SILVER_YELLOW_TAXI_TRIPS
GROUP BY 1
ORDER BY 1;
~~~

This query confirms that categorical source nulls were normalized without
erasing their lineage, while financial source nulls remained unknown. The dbt
test `assert_fee_nulls_require_unknown_rate_code` independently verifies that
no staged fee null appeared with a known RatecodeID.

# Supporting scripts

## src/main/kestra/encode_snowflake_secrets.py

| Block | Explanation |
|---|---|
| required keys | Requires Snowflake account, user, password, role, and warehouse. |
| environment parser | Ignores comments/blank lines, accepts export, splits on first equals sign, and unquotes matching outer quotes. |
| missing-value check | Stops incomplete secret configuration from being emitted. |
| base64 output | Emits SECRET_SNOWFLAKE key/value lines for Kestra; it does not edit the source environment file. |
| optional file path | Defaults to project .env but supports a specified input file. |

Base64 is encoding, not encryption. Treat emitted entries as secrets. dbt keeps
using normal environment variables through profiles.yml.

## src/main/kestra/scripts/download_parquet.py

dbt assumes Bronze holds intact Parquet-derived records. This helper protects
that source contract.

| Block | Explanation |
|---|---|
| PAR1 constants | Valid Parquet files begin and end with these bytes; detects incomplete/sparse files. |
| streaming download | Write chunks to .part, check HTTP status, hash stream, flush, and fsync. |
| validation | Check header/footer and compare local hash with streamed hash. |
| atomic rename | Only a validated partial file becomes destination file. |
| retry loop | Retry download/validation; zero attempts means retry until a valid file exists. |
| final validation | Recheck destination after rename before Snowflake upload. |

A Docker named volume puts download, validation, and upload in one
container-managed filesystem. The validation still matters because a volume
does not prove an HTTP transfer completed.

# Limits and Gold guidance

- An imputed passenger count is plausible for analysis, not a recovered fact.
  Filter passenger_count_was_imputed to false for reported-only analysis.
- The mode never borrows observations from other delivery months.
- Twenty peers is a documented policy decision and should be revisited if
  source volume or analytic use changes.
- Monetary amounts are parsed, not corrected. Any outlier policy deserves its
  own business definition and audit fields.
- Gold models should retain nullable operational fields and missingness flags.
  Use an explicit unknown/not-reported dimension member rather than inventing
  values.
- trip_id is a source-row lineage key, not a universal real-world trip ID.

# Reproducible checklist

1. Create the database and S_CDATOS_SILVER schema.
2. Supply SNOWFLAKE environment variables to the dbt container.
3. Run dbt debug.
4. Run dbt build selecting models/silver.
5. Confirm all DAG nodes are green.
6. Run validation queries A through D and method audit E.
7. Record material changes in null rates or imputation-method distribution
   before building Gold facts and dimensions.

The guiding rule for future corrections is simple: preserve the original value
when possible, record whether and how it changed, and support each rule with a
repeatable query.
