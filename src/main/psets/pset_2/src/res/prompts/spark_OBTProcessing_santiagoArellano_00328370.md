# MC1 OBT Implementation Specification

## 1. Canonical implementation artifact

The canonical executable is:

```text
src/main/python/spark/jobs/build_mc1_obt.py
```

It is a normal version-controlled Python program using:

```python
snowflake.snowpark_connect
pyspark.sql.DataFrame
pyspark.sql.functions
pyspark.sql.Window
```

It must not generate the feature transformation as Snowflake SQL.

It must not use the classic:

```text
net.snowflake.spark.snowflake
```

Spark connector.

The transformation contract is:

```text
build_mc1_obt.py
        │
        │ PySpark DataFrame API
        ▼
Snowpark Connect for Spark
        │
        ▼
Snowflake warehouse
        │
        ├── reads Gold tables
        ├── evaluates Spark transformations
        └── writes OBT staging tables
                 │
                 ▼
        validated publication
                 │
                 ▼
S_CDATOS_PSET2_OBT
```

The Snowflake database must be supplied by configuration rather than hardcoded.

---

# 2. Runtime configuration

## Required environment variables

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
```

Optional:

```text
SNOWFLAKE_ROLE
SNOWFLAKE_GOLD_SCHEMA
SNOWFLAKE_OBT_SCHEMA
```

Defaults:

```text
SNOWFLAKE_GOLD_SCHEMA=S_CDATOS_PSET2_GOLD
SNOWFLAKE_OBT_SCHEMA=S_CDATOS_PSET2_OBT
```

If `SNOWFLAKE_DATABASE` is absent, the implementation may fall back to:

```text
DBT_SNOWFLAKE_DATABASE
```

before failing.

Credentials must never:

```text
be logged
appear in RUN_ID
appear in exception output
be stored in the audit table
be committed to source control
```

---

# 3. Snowpark Connect session

Session initialization must use:

```python
from snowflake import snowpark_connect
from pyspark import SparkConf
```

Conceptually:

```python
conf = (
    SparkConf()
    .set("spark.sql.session.timeZone", "UTC")
    .set("spark.sql.ansi.enabled", "true")
)

spark = snowpark_connect.init_spark_session(
    conf=conf,
    connection_parameters={
        "account": ...,
        "user": ...,
        "password": ...,
        "warehouse": ...,
        "database": ...,
        "schema": ...,
        "role": ...,
    },
    app_name=f"pset2-mc1-obt-{run_suffix}",
)
```

Use:

```text
UTC
```

for Spark/Snowflake temporal calculations.

The application name must contain the run identifier suffix so the workload can be identified in Snowflake query history.

---

# 4. Authoritative Snowflake inputs

Resolve fully-qualified names from configuration.

## Dimension

```text
<database>.S_CDATOS_PSET2_GOLD.DIM_SSD
<database>.S_CDATOS_PSET2_GOLD.DIM_DATE
```

## Facts

```text
<database>.S_CDATOS_PSET2_GOLD.FCT_SMART_DAILY_MC1
<database>.S_CDATOS_PSET2_GOLD.FCT_FAILURE_EVENT_MC1
```

No Silver or Bronze relation may be read by this job.

---

# 5. Authoritative outputs

Final canonical relations:

```text
<database>.S_CDATOS_PSET2_OBT.OBT_MC1_RN
<database>.S_CDATOS_PSET2_OBT.OBT_MC1_R
<database>.S_CDATOS_PSET2_OBT.OBT_MC1_N
<database>.S_CDATOS_PSET2_OBT.OBT_MC1_RUN_AUDIT
```

Construction order is strictly:

```text
RN
↓
validate RN
↓
R projection from persisted RN
N projection from persisted RN
↓
validate R/N parity
↓
publish all variants
```

The R and N temporal features must never be independently recalculated.

---

# 6. Gold source contract

`FCT_SMART_DAILY_MC1` has grain:

> One MC1 SSD observation on one calendar date.

Required columns:

```text
SMART_DAILY_KEY
SSD_KEY
OBSERVATION_DATE_KEY
OBSERVATION_DATE
```

Validated current baseline:

```text
115,159,020 observations
199,655 SSD_KEY values
```

These values are audit baselines, not hardcoded row-count requirements.

`DIM_SSD` provides:

```text
SSD_KEY
DISK_ID
MODEL_CODE
```

`DIM_DATE` provides at minimum:

```text
DATE_KEY
FULL_DATE
```

`FCT_FAILURE_EVENT_MC1` provides:

```text
FAILURE_EVENT_KEY
SSD_KEY
FAILURE_DATE_KEY
FAILURE_DATE
FAILURE_AT
FAILURE_COUNT
```

---

# 7. MC1 SMART registry

Expected SMART IDs are exactly:

```python
SMART_IDS = [
    1, 5, 9, 12,
    170, 171, 172, 173, 174, 180,
    183, 184, 187, 188,
    194, 195, 196, 197, 198, 199,
    206, 211,
]
```

Expected predictor columns:

```text
R_1 / N_1
R_5 / N_5
...
R_211 / N_211
```

Total:

```text
22 SMART attributes
44 current SMART columns
```

Before transformation, inspect the source schema.

Extract SMART fields using the equivalent of:

```text
^[RN]_[0-9]+$
```

The discovered registry must equal the expected registry exactly.

Blocking failures:

```text
missing expected R_x
missing expected N_x
incomplete R/N pair
unexpected R_x or N_x
```

Partially NULL retained features remain valid.

---

# 8. Common OBT grain

All outputs preserve:

> One MC1 SSD observation per calendar date.

Required unique identifiers:

```text
SMART_DAILY_KEY
```

and equivalently:

```text
SSD_KEY + OBSERVATION_DATE
```

Every OBT must contain exactly these common metadata fields:

```text
SMART_DAILY_KEY
SSD_KEY
DISK_ID
MODEL_CODE
OBSERVATION_DATE_KEY
OBSERVATION_DATE
```

`MODEL_CODE` must always equal:

```text
MC1
```

---

# 9. Source enrichment

Read the SMART fact using:

```python
spark.table(FCT_SMART_DAILY_MC1_FQN)
```

Read dimensions similarly.

Join `DIM_SSD` by:

```text
FCT_SMART_DAILY_MC1.SSD_KEY
=
DIM_SSD.SSD_KEY
```

to obtain:

```text
DISK_ID
MODEL_CODE
```

Use `DIM_DATE` to validate:

```text
OBSERVATION_DATE_KEY → DATE_KEY
```

and:

```text
OBSERVATION_DATE = FULL_DATE
```

The date dimension need not contribute additional columns to the final OBT.

Dimension enrichment must preserve the SMART observation count exactly.

---

# 10. Failure-event validation

Validate:

```text
FAILURE_DATE_KEY → DIM_DATE.DATE_KEY
SSD_KEY → DIM_SSD.SSD_KEY
```

Failure events are not joined to SMART observations using equal calendar dates.

Their association is based exclusively on:

```text
SSD_KEY
+
future temporal position
```

---

# 11. Run identifier

At startup generate:

```text
STARTED_AT_UTC
NONCE
```

Then:

```text
RUN_ID =
SHA256(
    "MC1"
    + STARTED_AT_UTC
    + NONCE
)
```

The complete SHA-256 value is stored in the audit relation.

For object names use a safe shortened suffix, for example:

```text
RUN_SUFFIX = first 16 hex characters of RUN_ID
```

Run-scoped stage names:

```text
OBT_MC1_RN__RUN_<RUN_SUFFIX>
OBT_MC1_R__RUN_<RUN_SUFFIX>
OBT_MC1_N__RUN_<RUN_SUFFIX>
```

---

# 12. Development/bounded execution

The canonical script must support:

```text
--start-date YYYY-MM-DD
--end-date YYYY-MM-DD
--ssd-limit N
--validate-only
--explain-plan
--keep-staging
```

A run without date or SSD restrictions is a full production build.

A bounded run must be marked internally as non-production and must never silently publish canonical OBT tables.

---

# 13. Deterministic SSD sampling

When:

```text
--ssd-limit N
```

is supplied, determine the sample once.

Select distinct SSDs participating in the requested output interval and choose deterministically, for example:

```text
ORDER BY SSD_KEY
LIMIT N
```

Use that exact SSD population for:

```text
SMART telemetry
failure events
last-seen calculation
temporal features
labels
all three OBT variants
```

Never independently sample each DataFrame.

---

# 14. Bounded-date context

If requested output dates are:

```text
[START_DATE, END_DATE]
```

the feature computation must read SMART history beginning no later than:

```text
START_DATE - 59 days
```

because a 30-day mean shift needs:

```text
current 30-day window
+
previous 30-day window
```

The SMART/follow-up context must extend through at least:

```text
END_DATE + 30 days
```

for target observability.

Feature and label calculations happen against this expanded context.

Only after calculations are complete should output rows be restricted back to:

```text
START_DATE <= OBSERVATION_DATE <= END_DATE
```

---

# 15. Numeric calendar ordering

Spark `rangeBetween()` requires an ordered numeric value for this implementation.

Create a temporary field:

```text
_INTERNAL_OBSERVATION_DAY
```

equivalent to:

```python
F.datediff(
    F.col("OBSERVATION_DATE"),
    F.lit("1970-01-01")
)
```

It exists only during transformation.

It must not appear in the final OBT.

---

# 16. Window definitions

For each SSD:

```python
Window.partitionBy("SSD_KEY").orderBy("_INTERNAL_OBSERVATION_DAY")
```

Define six window specifications.

## Current windows

```text
CURRENT_7D:
rangeBetween(-6, 0)

CURRENT_14D:
rangeBetween(-13, 0)

CURRENT_30D:
rangeBetween(-29, 0)
```

## Previous windows

```text
PREVIOUS_7D:
rangeBetween(-13, -7)

PREVIOUS_14D:
rangeBetween(-27, -14)

PREVIOUS_30D:
rangeBetween(-59, -30)
```

Do not use:

```python
rowsBetween(...)
```

for feature semantics.

Missing dates must reduce coverage, not extend the calendar range backwards.

---

# 17. RN-first feature generation

The authoritative feature computation produces the complete RN DataFrame once.

Do not calculate:

```text
R features
write R

then

N features
write N
```

Instead:

```text
base observations
        │
        ├── all R expressions
        └── all N expressions
                │
                ▼
          RN feature DataFrame
                │
                ▼
        persisted RN staging
             /       \
            /         \
      select R       select N
```

This is the central execution invariant.

---

# 18. Feature family per variable

For each:

```text
V_x
```

where:

```text
V ∈ {R, N}
W ∈ {7, 14, 30}
```

generate exactly 19 final fields.

## Current value

```text
V_x
```

## Means

```text
V_x_MEAN_7D
V_x_MEAN_14D
V_x_MEAN_30D
```

## Current counts

```text
V_x_COUNT_7D
V_x_COUNT_14D
V_x_COUNT_30D
```

## Previous counts

```text
V_x_PREV_COUNT_7D
V_x_PREV_COUNT_14D
V_x_PREV_COUNT_30D
```

## Mean validity

```text
V_x_MEAN_VALID_7D
V_x_MEAN_VALID_14D
V_x_MEAN_VALID_30D
```

## Mean shift

```text
V_x_MEAN_SHIFT_7D
V_x_MEAN_SHIFT_14D
V_x_MEAN_SHIFT_30D
```

## Shift validity

```text
V_x_MEAN_SHIFT_VALID_7D
V_x_MEAN_SHIFT_VALID_14D
V_x_MEAN_SHIFT_VALID_30D
```

Total:

```text
19 fields per representation per SMART ID
```

---

# 19. Mean calculation

For a variable such as:

```text
R_173
```

and 7-day window:

```python
F.avg("R_173").over(current_7d)
```

produces:

```text
R_173_MEAN_7D
```

`AVG` operates only on available non-NULL values.

No SMART value is:

```text
imputed
forward-filled
back-filled
zero-filled
copied from another SSD
```

---

# 20. Coverage calculation

Current count:

```python
F.count("R_173").over(current_7d)
```

Previous count:

```python
F.count("R_173").over(previous_7d)
```

Equivalent logic applies to every representation/window.

Required bounds:

```text
0 <= COUNT_7D <= 7
0 <= COUNT_14D <= 14
0 <= COUNT_30D <= 30
```

and identical bounds for previous counts.

A value above the nominal window size indicates a grain or duplicate-row failure.

---

# 21. Previous means

Previous-window means are required internally to calculate shifts:

```text
_INTERNAL_V_x_PREV_MEAN_7D
_INTERNAL_V_x_PREV_MEAN_14D
_INTERNAL_V_x_PREV_MEAN_30D
```

They must not appear in the final OBT.

The first feature-building DataFrame may therefore contain:

```text
current mean
current count
previous mean
previous count
```

for every variable/window.

A second projection derives validity/shift fields and removes the internal previous means.

---

# 22. Mean validity

For each window:

```text
MEAN_VALID = 1
when COUNT > 0

MEAN_VALID = 0
when COUNT = 0
```

Required consistency:

```text
COUNT = 0
→ MEAN IS NULL
```

```text
COUNT > 0
→ MEAN IS NOT NULL
```

---

# 23. Adjacent-window mean shift

Definition:

```text
V_x_MEAN_SHIFT_W
=
CURRENT_MEAN_W
-
PREVIOUS_MEAN_W
```

Example:

```text
Previous 7 calendar days    Current 7 calendar days
[t-13 ... t-7]              [t-6 ... t]

mean A                      mean B

SHIFT_7D = B - A
```

This is intentionally not:

```text
V(t) - V(t-W)
```

---

# 24. Shift validity

A shift exists only if both windows contain data.

```text
SHIFT_VALID = 1
when CURRENT_COUNT > 0
and PREVIOUS_COUNT > 0
```

Otherwise:

```text
MEAN_SHIFT = NULL
MEAN_SHIFT_VALID = 0
```

Never substitute the current mean as the first available shift.

---

# 25. Efficient expression construction

Do not implement hundreds of chained handwritten `withColumn()` calls.

Use:

```python
SMART_IDS
WINDOW_REGISTRY
REPRESENTATIONS
```

to programmatically build column expressions.

Recommended two-stage pattern:

```text
window_df
    =
    base metadata
    + current SMART values
    + generated current means
    + generated current counts
    + generated previous means
    + generated previous counts

derived_df
    =
    explicit select from window_df
    + generated mean-valid fields
    + generated shifts
    + generated shift-valid fields
```

The final projection must explicitly enumerate its columns.

Do not use a broad:

```text
SELECT *
```

equivalent when materializing the final OBT schema.

---

# 26. Label DataFrame

Build target information once in:

```text
labels_mc1_df
```

Grain:

```text
SMART_DAILY_KEY
```

Use only the columns required for labeling rather than the 847-column feature frame.

---

# 27. Failure summary

From:

```text
FCT_FAILURE_EVENT_MC1
```

group by:

```text
SSD_KEY
```

and derive:

```text
FIRST_FAILURE_DATE = MIN(FAILURE_DATE)
FAILURE_EVENT_COUNT = COUNT(*)
```

`FAILURE_EVENT_COUNT` is internal/audit information.

It does not appear in the final OBT.

If:

```text
FAILURE_EVENT_COUNT > 1
```

record the SSD as a warning-level anomaly.

The first confirmed failure remains authoritative for labeling.

---

# 28. Last observed date

From the complete relevant SMART context calculate:

```text
LAST_SEEN_DATE =
MAX(OBSERVATION_DATE)
by SSD_KEY
```

This must be based on the complete selected SSD population/context, not only the final emitted output-date rows.

---

# 29. Days to failure

After joining first failure by:

```text
SSD_KEY
```

calculate:

```python
F.datediff(
    F.col("FIRST_FAILURE_DATE"),
    F.col("OBSERVATION_DATE"),
)
```

Semantics:

```text
positive → observation precedes failure
0        → same calendar date
negative → observation follows failure
NULL     → no known failure
```

Final field:

```text
DAYS_TO_FAILURE
```

---

# 30. Label ordering

Label rules are evaluated exactly in this order.

## POST_FAILURE

```text
FIRST_FAILURE_DATE IS NOT NULL
AND
OBSERVATION_DATE > FIRST_FAILURE_DATE
```

Result:

```text
LABEL_STATUS = 'POST_FAILURE'
TARGET_30D = NULL
```

## SAME_DAY_FAILURE

```text
OBSERVATION_DATE = FIRST_FAILURE_DATE
```

Result:

```text
LABEL_STATUS = 'SAME_DAY_FAILURE'
DAYS_TO_FAILURE = 0
TARGET_30D = NULL
```

## POSITIVE

```text
1 <= DAYS_TO_FAILURE <= 30
```

Result:

```text
LABEL_STATUS = 'POSITIVE'
TARGET_30D = 1
```

## NEGATIVE

The observation is not covered by a previous rule and:

```text
LAST_SEEN_DATE >= OBSERVATION_DATE + 30 days
```

Result:

```text
LABEL_STATUS = 'NEGATIVE'
TARGET_30D = 0
```

## CENSORED

Everything else:

```text
LABEL_STATUS = 'CENSORED'
TARGET_30D = NULL
```

---

# 31. 2019 labeling limitation

The current failure-label source does not provide equivalent positive-event coverage for 2019.

Therefore:

```text
absence of a failure event
≠ proof that a physical failure never occurred
```

The OBT must not invent positive labels.

It must follow the agreed observability rule exactly.

As a result, 2019 observations can be classified by the current contract as:

```text
NEGATIVE
or
CENSORED
```

when no recorded first failure exists.

This is a documented dataset limitation and must remain visible to downstream model evaluation.

The Spark run should report label counts grouped by:

```text
YEAR(OBSERVATION_DATE)
LABEL_STATUS
```

as informational diagnostics, even though these per-year values do not need additional permanent OBT columns.

---

# 32. Target metadata

Join `labels_mc1_df` to the RN feature DataFrame using:

```text
SMART_DAILY_KEY
```

Every final OBT contains exactly:

```text
FIRST_FAILURE_DATE
LAST_SEEN_DATE
DAYS_TO_FAILURE
LABEL_STATUS
TARGET_30D
```

None except:

```text
TARGET_30D
```

may later enter a supervised modeling interface, and `TARGET_30D` is the response rather than a predictor.

---

# 33. Deterministic final RN schema

Column order must be deterministic.

## First

Six common metadata columns:

```text
SMART_DAILY_KEY
SSD_KEY
DISK_ID
MODEL_CODE
OBSERVATION_DATE_KEY
OBSERVATION_DATE
```

## Then

For every R attribute in `SMART_IDS` order, its complete 19-field feature family.

## Then

For every N attribute in `SMART_IDS` order, its complete 19-field feature family.

## Finally

```text
FIRST_FAILURE_DATE
LAST_SEEN_DATE
DAYS_TO_FAILURE
LABEL_STATUS
TARGET_30D
```

Expected width:

```text
6
+ (22 × 19)
+ (22 × 19)
+ 5
=
847 columns
```

Anything else is a blocking schema failure.

---

# 34. R projection

`OBT_MC1_R` must be projected only from the persisted validated RN staging relation.

It contains:

```text
6 common metadata fields

22 complete R feature families
    = 22 × 19 fields

5 target metadata fields
```

Width:

```text
6 + 418 + 5 = 429
```

It contains no `N_x` predictor/temporal feature.

---

# 35. N projection

`OBT_MC1_N` follows the same rule:

```text
6 common metadata fields

22 complete N feature families
    = 22 × 19 fields

5 target metadata fields
```

Width:

```text
429
```

It contains no `R_x` predictor/temporal feature.

---

# 36. RN staging write

After source/schema validations and construction:

```python
rn_df.write \
    .mode("overwrite") \
    .saveAsTable(rn_stage_fqn)
```

This is the first heavy materialization action.

The transformation itself must be expressed as PySpark DataFrame operations.

Do not generate a CTAS feature query.

---

# 37. RN persisted validation

After writing, immediately reload:

```python
rn_stage_df = spark.table(rn_stage_fqn)
```

All expensive data validations should operate primarily against this persisted relation so Spark does not recompute the rolling-feature graph repeatedly.

Validate:

```text
row count
distinct SMART_DAILY_KEY count
distinct (SSD_KEY, OBSERVATION_DATE) count
MODEL_CODE
schema width
label rules
rolling-count bounds
mean/count consistency
shift/count consistency
no-all-current-R-null
no-all-current-N-null
```

---

# 38. Scalar metric collection

Never collect observation or feature rows to the Python process.

Permitted:

```text
one-row aggregate validation results
small audit metric rows
small schema metadata
small anomaly samples with an explicit LIMIT
```

Forbidden on the complete population:

```python
df.collect()
df.toPandas()
list(df.toLocalIterator())
```

when applied to the observation/feature dataset itself.

Validation metrics should preferably be calculated through aggregate DataFrames and only the final scalar aggregate row returned to Python.

---

# 39. R/N traceability validation

For each SMART ID and each window validate in RN:

```text
R_x_COUNT_W = N_x_COUNT_W
```

and:

```text
R_x_PREV_COUNT_W = N_x_PREV_COUNT_W
```

These are warning-level checks.

A mismatch must:

```text
be counted
be logged
enter R_N_COUNT_MISMATCH_COUNT
```

but must not be silently repaired.

R/N counts remain separate final fields.

---

# 40. R/N staging creation

After RN passes its blocking validations:

```python
rn_stage_df = spark.table(rn_stage_fqn)

r_stage_df = rn_stage_df.select(*R_FINAL_COLUMNS)
n_stage_df = rn_stage_df.select(*N_FINAL_COLUMNS)
```

Write:

```python
r_stage_df.write.mode("overwrite").saveAsTable(r_stage_fqn)
n_stage_df.write.mode("overwrite").saveAsTable(n_stage_fqn)
```

No temporal window expression may appear in either projection operation.

---

# 41. Variant parity

Persisted R, N and RN staging relations must satisfy:

```text
same row count
same SMART_DAILY_KEY population
same common metadata
same target metadata
```

Blocking check:

```text
keys(R) = keys(N) = keys(RN)
```

Since R and N are projections of RN, failure indicates publication/write/schema corruption and must stop the run.

---

# 42. Source blocking validations

Before the feature build begins, stop immediately if any of these conditions holds:

```text
duplicate SMART_DAILY_KEY
duplicate (SSD_KEY, OBSERVATION_DATE)
unexpected MC1 SMART schema
missing expected MC1 SMART column
dimension enrichment changes row count
orphan SSD_KEY
orphan OBSERVATION_DATE_KEY
MODEL_CODE other than MC1
```

Do not start the expensive RN write after source-contract failure.

---

# 43. RN blocking validations

Publication is forbidden if:

```text
row count != Gold source observation count
duplicate SMART_DAILY_KEY exists
duplicate SSD_KEY/date exists
width != 847
rolling count is outside legal range
COUNT=0 with non-NULL mean
COUNT>0 with NULL mean
invalid mean-valid flag
shift exists without both windows
shift-valid flag inconsistent
label violates ordered rules
every R current value is NULL
every N current value is NULL
required feature is absent
unexpected feature is present
```

---

# 44. R/N projection validations

For R:

```text
width = 429
no N predictor feature exists
```

For N:

```text
width = 429
no R predictor feature exists
```

Both:

```text
row count = RN row count
key set = RN key set
metadata = RN metadata
target metadata = RN target metadata
```

---

# 45. Warning-level anomalies

Warnings do not alter source values.

Record:

```text
SSD with >1 failure event
R/N current-count mismatch
R/N previous-count mismatch
```

Warnings appear in logs and audit metrics.

---

# 46. Audit metrics

Each run must record at least:

```text
GOLD_OBSERVATION_COUNT
GOLD_SSD_COUNT

MC1_OBT_ROW_COUNT
MC1_OBT_DISTINCT_SSD_COUNT

POSITIVE_COUNT
NEGATIVE_COUNT
CENSORED_COUNT
SAME_DAY_FAILURE_COUNT
POST_FAILURE_COUNT

DUPLICATE_SMART_DAILY_KEY_COUNT
R_N_COUNT_MISMATCH_COUNT
INVALID_ROLLING_COUNT_COUNT
INVALID_MEAN_COUNT
INVALID_SHIFT_COUNT
MULTIPLE_FAILURE_SSD_COUNT
```

Also log, without changing the fixed audit table contract:

```text
label counts by source year
warehouse name
application name
run-scoped staging table names
```

---

# 47. Audit relation

Final audit relation:

```text
<database>.S_CDATOS_PSET2_OBT.OBT_MC1_RUN_AUDIT
```

Grain:

> One attempted MC1 three-variant OBT build.

Required columns:

```text
RUN_ID
POPULATION_CODE
STARTED_AT_UTC
COMPLETED_AT_UTC
RUN_STATUS

GOLD_OBSERVATION_COUNT
GOLD_SSD_COUNT
MC1_OBT_ROW_COUNT
MC1_OBT_DISTINCT_SSD_COUNT

POSITIVE_COUNT
NEGATIVE_COUNT
CENSORED_COUNT
SAME_DAY_FAILURE_COUNT
POST_FAILURE_COUNT

DUPLICATE_SMART_DAILY_KEY_COUNT
R_N_COUNT_MISMATCH_COUNT
INVALID_ROLLING_COUNT_COUNT
INVALID_MEAN_COUNT
INVALID_SHIFT_COUNT
MULTIPLE_FAILURE_SSD_COUNT

ERROR_MESSAGE
```

`POPULATION_CODE`:

```text
MC1
```

`RUN_STATUS`:

```text
SUCCEEDED
FAILED_VALIDATION
FAILED_EXECUTION
```

Audit writes can use a one-row Spark DataFrame with:

```python
audit_df.write.mode("append").saveAsTable(audit_fqn)
```

---

# 48. Run-scoped staging safety

Never write directly to:

```text
OBT_MC1_RN
OBT_MC1_R
OBT_MC1_N
```

during feature construction.

Always produce:

```text
OBT_MC1_RN__RUN_<suffix>
OBT_MC1_R__RUN_<suffix>
OBT_MC1_N__RUN_<suffix>
```

first.

Existing canonical tables remain untouched until all staging relations pass validation.

---

# 49. Publication mechanism

The recommended publication operation is Snowflake zero-copy cloning of each validated staging table:

```sql
CREATE OR REPLACE TABLE <canonical_RN>
CLONE <staging_RN>
COPY GRANTS
```

followed by equivalent R and N publication.

The feature transformation is still PySpark/Snowpark Connect; this SQL is only metadata-level publication of already-computed Snowflake tables.

Publication order:

```text
RN
R
N
```

Do not delete any staging table until all three canonical publications succeed.

---

# 50. Publication atomicity limitation

Three Snowflake DDL replacements do not constitute one multi-table atomic transaction.

Therefore the implementation must not claim otherwise.

If publication fails partway:

```text
retain all staging tables
record FAILED_EXECUTION
preserve RUN_ID
report which canonical replacements succeeded
```

Because the validated run-scoped tables remain available, publication can be repaired without recomputing the 115-million-row feature set.

A run is considered fully published only after all three canonical tables have been replaced successfully and the audit row records:

```text
SUCCEEDED
```

---

# 51. Staging cleanup

After successful publication:

```text
if --keep-staging:
    retain run-scoped tables
else:
    drop run-scoped staging tables
```

On failed validation/execution:

```text
retain staging by default
```

when they exist, to support diagnosis/recovery.

---

# 52. Explain-plan mode

When:

```text
--explain-plan
```

is enabled, print the Spark/Snowpark Connect execution plan for the RN DataFrame before materialization.

Do not treat plan display as evidence by itself that a query is efficient.

The run should also be identifiable in Snowflake query history through its application name.

---

# 53. Validate-only mode

`--validate-only` performs:

```text
configuration validation
Gold source schema validation
source grain checks
dimension relationship checks
SMART registry validation
bounded transformation schema generation where applicable
```

and does not publish canonical OBTs.

For full-scale use, it should avoid triggering the complete 847-column materialization unless explicitly requested.

---

# 54. Performance contract

The job must not rely on local container memory for the 115-million-row data population.

Snowpark Connect supplies the Spark API while Snowflake's warehouse evaluates the workload.

The implementation should therefore:

```text
project unused Gold columns immediately
construct one RN temporal transformation
avoid full-data local collection
avoid independent R/N recomputation
materialize RN once
validate the persisted RN table
project R/N from persisted RN
```

Do not unconditionally call:

```python
cache()
persist()
```

on the full 847-column RN DataFrame.

Materialization in the Snowflake staging table is the persistence boundary.

---

# 55. Exact RN build flow

The canonical full run is:

```text
START
  │
  ├── load configuration
  ├── generate RUN_ID
  ├── initialize Snowpark Connect
  │
  ├── read Gold source schemas
  ├── validate SMART registry
  ├── validate source grain
  │
  ├── build MC1 base observations
  ├── validate dimension relationships
  │
  ├── build FIRST_FAILURE_DATE summary
  ├── build LAST_SEEN_DATE summary
  ├── build labels_mc1_df
  │
  ├── create numeric observation-day field
  ├── define six WindowSpec objects
  │
  ├── generate all R window expressions
  ├── generate all N window expressions
  │
  ├── calculate current/previous window statistics
  ├── calculate validity fields
  ├── calculate mean shifts
  │
  ├── remove internal helper fields
  ├── join labels by SMART_DAILY_KEY
  ├── explicit 847-column projection
  │
  ├── assert schema width
  ├── optionally explain plan
  │
  ├── WRITE RN STAGING
  │
  ├── reload RN staging
  ├── validate persisted RN
  │
  ├── project R from RN
  ├── project N from RN
  │
  ├── WRITE R STAGING
  ├── WRITE N STAGING
  │
  ├── validate widths/parity/keys/labels
  │
  ├── publish RN
  ├── publish R
  ├── publish N
  │
  ├── write SUCCEEDED audit
  ├── optional staging cleanup
  │
  └── STOP
```

---

# 56. Suggested function boundaries

The single versioned script should remain reviewable rather than placing everything in `main()`.

Recommended functions:

```python
load_config()
parse_args()
generate_run_context()

create_spark_session(config, run_context)

relation_names(config)

validate_source_columns(...)
validate_source_grain(...)
validate_dimension_relationships(...)

resolve_smart_registry(...)

build_base_observations(...)
apply_development_scope(...)

build_failure_summary(...)
build_last_seen(...)
build_labels(...)

build_window_registry()
build_window_statistics(...)
build_derived_temporal_features(...)

build_rn_obt(...)

rn_final_columns()
r_final_columns()
n_final_columns()

validate_rn_stage(...)
validate_variant_stage(...)
validate_variant_parity(...)

write_stage(...)
promote_stage(...)
cleanup_staging(...)

ensure_audit_table(...)
write_audit_row(...)

sanitize_error(...)

main()
```

---

# 57. Error behavior

The top-level job must distinguish:

```text
ValidationError
ExecutionError
```

On validation failure:

```text
RUN_STATUS = FAILED_VALIDATION
exit code != 0
```

On unexpected runtime/Snowflake/write error:

```text
RUN_STATUS = FAILED_EXECUTION
exit code != 0
```

Successful completion:

```text
RUN_STATUS = SUCCEEDED
exit code = 0
```

Always attempt to persist the audit row when the Snowflake session remains usable.

---

# 58. Error sanitization

Before persisting:

```text
ERROR_MESSAGE
```

remove or redact any appearance of:

```text
SNOWFLAKE_PASSWORD
account connection secrets
tokens
connector/session options containing credentials
```

Truncate the stored message to a controlled length.

---

# 59. Current OBT widths

These are blocking contracts:

```text
OBT_MC1_RN = 847 columns
OBT_MC1_R  = 429 columns
OBT_MC1_N  = 429 columns
```

Their derivation is:

```text
22 attributes
×
19 fields per representation
=
418

RN:
6 metadata
+ 418 R
+ 418 N
+ 5 target
= 847

R/N:
6 metadata
+ 418 representation
+ 5 target
= 429
```

---

# 60. Features explicitly excluded

The canonical OBT does not calculate:

```text
standard deviation
rolling min/max
slope
EWMA
Fourier features
PCA
feature selection
correlation filtering
SMOTE
class balancing
training split
validation split
model fitting
probability threshold
```

It also performs no SMART-value imputation.

These remain downstream ML experiments.

---

# 61. Predictor contract for later ML work

The OBT itself contains identifiers and target metadata for traceability.

A future model extraction must exclude:

```text
SMART_DAILY_KEY
SSD_KEY
DISK_ID
MODEL_CODE
OBSERVATION_DATE_KEY

FIRST_FAILURE_DATE
LAST_SEEN_DATE
DAYS_TO_FAILURE
LABEL_STATUS
```

`OBSERVATION_DATE` remains available for temporal splitting but should not automatically be treated as a numeric predictor.

Supervised examples are:

```text
LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')
```

with:

```text
TARGET_30D
```

as the response.

---

# 62. Final implementation invariant

All three persisted datasets must represent exactly the same MC1 SSD-day population.

For every `SMART_DAILY_KEY`:

```text
metadata_R
=
metadata_N
=
metadata_RN
```

and:

```text
target_R
=
target_N
=
target_RN
```

Only the predictor representation differs:

```text
OBT_MC1_R  → raw SMART representation
OBT_MC1_N  → normalized SMART representation
OBT_MC1_RN → both representations
```

This is what permits subsequent model performance differences to be attributed to feature representation rather than inconsistent observations, labels, censoring decisions, or temporal preprocessing.