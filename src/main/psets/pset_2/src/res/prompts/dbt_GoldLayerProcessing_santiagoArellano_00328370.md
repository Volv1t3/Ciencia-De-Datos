# Gold Dimensional Model Specification

## 1. Scope

This specification covers only the dbt models materialized in:

```text
S_CDATOS_PSET2_GOLD
```

The Gold layer must provide the dimensional data model that will later serve as input to the PySpark OBT process.

The Gold layer **does not** perform:

- rolling 7/14/30-day feature generation;
- mean-shift generation;
- ML target construction;
- censoring logic;
- R-only/N-only/R+N experiments;
- feature selection;
- dimensionality reduction;
- training/validation splitting.

Those operations belong to a separate Spark/OBT specification.

---

# 2. Gold model

The Gold layer uses two conformed dimensions and two fact tables:

```text
                         DIM_DATE
                       /          \
                      /            \
                     ▼              ▼
            FCT_SMART_DAILY    FCT_FAILURE_EVENT
                     ▲              ▲
                      \            /
                       \          /
                         DIM_SSD
```

This is a small fact constellation composed of two related stars sharing the same dimensions.

`FCT_SMART_DAILY` is the primary analytical fact for the machine-learning project.

`FCT_FAILURE_EVENT` represents failure events independently from telemetry observations.

The two facts must **not** be joined on equal dates in Gold.

---

# 3. DIM_DATE

## Grain

> One row per calendar date.

## Required columns

```text
DATE_KEY
FULL_DATE
YEAR
QUARTER
MONTH
MONTH_NAME
WEEK_OF_YEAR
DAY_OF_MONTH
DAY_OF_WEEK
DAY_NAME
IS_WEEKEND
```

Recommended key:

```text
DATE_KEY = YYYYMMDD
```

Example:

```text
20180624
```

represents:

```text
2018-06-24
```

The dimension must cover the complete range required by both:

```text
SMART observation dates
+
failure-event dates
```

The same dimension acts as a role-playing date dimension:

```text
FCT_SMART_DAILY.OBSERVATION_DATE_KEY
    → DIM_DATE.DATE_KEY

FCT_FAILURE_EVENT.FAILURE_DATE_KEY
    → DIM_DATE.DATE_KEY
```

---

# 4. DIM_SSD

## Grain

> One row per unique physical SSD identifier.

Unlike the ML dataset, `DIM_SSD` may retain SSDs from every available SSD model.

This allows the dimensional layer to preserve the complete known SSD population even though the current predictive experiment ultimately focuses on:

```text
MODEL = 'MC1'
```

## Required columns

```text
SSD_KEY
DISK_ID
MODEL
```

`SSD_KEY` is the dimensional surrogate key.

`DISK_ID` is the natural identifier provided by the dataset.

Any additional stable SSD metadata may be added if present in the source.

Do not add ML-derived properties such as:

```text
TARGET_30D
DAYS_TO_FAILURE
future failure status
```

to the dimension.

---

# 5. FCT_SMART_DAILY

## Grain

> One MC1 SSD SMART observation on one calendar date.

The logical uniqueness rule is therefore:

```text
SSD_KEY + OBSERVATION_DATE_KEY
```

## Required metadata

```text
SMART_DAILY_KEY
SSD_KEY
OBSERVATION_DATE_KEY
OBSERVATION_DATE
SOURCE_YEAR
```

Recommended deterministic fact key:

```text
SMART_DAILY_KEY =
hash(SSD_KEY, OBSERVATION_DATE)
```

or the equivalent dbt surrogate-key mechanism already used by the project.

## SMART measures

The fact preserves the cleaned MC1 SMART attributes produced by Silver:

```text
R_x
N_x
```

Both representations remain present.

The Gold layer makes **no decision** about whether the raw or normalized representation will later be used by the ML model.

Partial `NULL` values remain `NULL`.

The table must not perform additional SMART imputation.

---

# 6. FCT_FAILURE_EVENT

## Grain

> One confirmed SSD failure event on one calendar date.

## Required columns

```text
FAILURE_EVENT_KEY
SSD_KEY
FAILURE_DATE_KEY
FAILURE_DATE
FAILURE_COUNT
```

where:

```text
FAILURE_COUNT = 1
```

The fact should preserve confirmed failure events independently from whether SMART telemetry exists on the failure date.

Therefore this is valid:

```text
SSD 123

SMART:
2029-01-14
2029-01-15
2029-01-16

FAILURE:
2029-01-20
```

There does **not** need to be a SMART observation for:

```text
2029-01-20
```

for the failure event to exist.

The later Spark pipeline will establish temporal relationships between telemetry observations and future failure events.

---

# 7. Fact-table relationship

Gold must not construct:

```text
FCT_SMART_DAILY
JOIN FCT_FAILURE_EVENT
    ON observation_date = failure_date
```

The facts share conformed dimensions but represent independent business events.

Their future ML relationship will instead be based on:

```text
same SSD
+
relative dates
```

For example, the later Spark process may ask whether:

```text
FAILURE_DATE
```

occurred within a given future horizon after:

```text
OBSERVATION_DATE
```

That logic explicitly remains outside Gold.

---

# 8. dbt dependencies

Gold models must consume Silver models through `ref()`.

Conceptually:

```sql
{{ ref('int_smart_mc1_all_years') }}
```

for SMART data and the appropriate cleaned Silver failure-label relation for failure events.

Gold should not directly query Bronze/raw objects when an equivalent Silver model exists.

`source()` remains responsible for defining external/raw Snowflake inputs earlier in the dbt pipeline, while `ref()` expresses dependencies between Silver and Gold models.

---

# 9. Recommended dbt models

```text
models/
└── gold/
    ├── dimensions/
    │   ├── dim_date.sql
    │   └── dim_ssd.sql
    │
    ├── facts/
    │   ├── fct_smart_daily.sql
    │   └── fct_failure_event.sql
    │
    └── schema.yml
```

All four models materialize into:

```text
S_CDATOS_PSET2_GOLD
```

Recommended physical names:

```text
DIM_DATE
DIM_SSD
FCT_SMART_DAILY
FCT_FAILURE_EVENT
```

---

# 10. DIM_DATE validation

Required dbt tests:

```text
DATE_KEY
    not_null
    unique

FULL_DATE
    not_null
    unique
```

Additional validation should ensure that every fact-table date key resolves to this dimension.

---

# 11. DIM_SSD validation

Required:

```text
SSD_KEY
    not_null
    unique

DISK_ID
    not_null
```

If analysis has proven that `DISK_ID` uniquely identifies a physical SSD globally:

```text
DISK_ID
    unique
```

may also be enforced.

If uniqueness depends on another field, the test must instead use the actual business key.

---

# 12. FCT_SMART_DAILY validation

Required:

```text
SMART_DAILY_KEY
    not_null
    unique

SSD_KEY
    not_null

OBSERVATION_DATE_KEY
    not_null

OBSERVATION_DATE
    not_null
```

Relationships:

```text
SSD_KEY
    → DIM_SSD.SSD_KEY

OBSERVATION_DATE_KEY
    → DIM_DATE.DATE_KEY
```

Business-rule tests:

### MC1 population

Every row must satisfy:

```text
MODEL = 'MC1'
```

whether `MODEL` is physically stored in the fact or resolved through `DIM_SSD`.

### Daily grain

There must be at most one row for:

```text
SSD_KEY + OBSERVATION_DATE
```

### No zero-information records

Every fact row must contain at least one non-NULL retained SMART measurement.

This independently validates the Silver quarantine decision.

### Source year

Allowed values:

```text
2018
2019
```

for the current project dataset.

---

# 13. Paired SMART missingness validation

Previous EDA established that retained:

```text
R_x
N_x
```

pairs have matched missingness.

Gold should validate this property rather than modify the data.

For every retained SMART ID:

```text
R_x IS NULL
iff
N_x IS NULL
```

If the condition fails, the dbt test should report the affected records.

The transformation must not:

```text
copy R_x into N_x
copy N_x into R_x
impute either value
```

to make the rule pass.

A failure means the underlying data assumption must be reviewed.

---

# 14. FCT_FAILURE_EVENT validation

Required:

```text
FAILURE_EVENT_KEY
    not_null
    unique

SSD_KEY
    not_null

FAILURE_DATE_KEY
    not_null

FAILURE_DATE
    not_null
```

Relationships:

```text
SSD_KEY
    → DIM_SSD.SSD_KEY

FAILURE_DATE_KEY
    → DIM_DATE.DATE_KEY
```

Required measure rule:

```text
FAILURE_COUNT = 1
```

A diagnostic test should also report SSDs containing multiple failure events.

Multiple events should not automatically be deleted at the Gold stage because determining first failure, device lifecycle, censoring, or post-failure observations belongs to the later ML/OBT process.

---

# 15. Row-count and grain validation

The Gold transformation must document:

```text
Silver MC1 row count
→ FCT_SMART_DAILY row count
```

After legitimate dimensional-key resolution, the fact should preserve the Silver MC1 observation population one-for-one.

Joining to dimensions must not multiply SMART observations.

Likewise:

```text
clean failure-label event count
→ FCT_FAILURE_EVENT row count
```

must remain explainable.

---

# 16. Gold exclusions

The following fields must **not** be calculated in Gold:

```text
R_x_MEAN_7D
R_x_MEAN_14D
R_x_MEAN_30D

N_x_MEAN_7D
N_x_MEAN_14D
N_x_MEAN_30D

MEAN_SHIFT
rolling counts
coverage ratios

FIRST_FAILURE_DATE
LAST_SEEN_DATE
DAYS_TO_FAILURE

TARGET_30D
LABEL_STATUS
```

Gold provides the normalized dimensional source from which Spark will later derive these values.

---

# 17. Final Gold outputs

The dbt Gold stage therefore ends with exactly these principal analytical relations:

```text
S_CDATOS_PSET2_GOLD.DIM_DATE

S_CDATOS_PSET2_GOLD.DIM_SSD

S_CDATOS_PSET2_GOLD.FCT_SMART_DAILY

S_CDATOS_PSET2_GOLD.FCT_FAILURE_EVENT
```

The next pipeline stage is:

```text
Gold
  ↓
PySpark
  ↓
OBT
  ↓
S_CDATOS_PSET2_OBT
```

Its implementation, including temporal features, R/N/RN variants, censoring and 30-day target generation, must be defined separately.