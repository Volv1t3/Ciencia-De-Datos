# dbt Specification: Secondary Silver SMART Tables

## 1. Purpose

This dbt layer transforms the standardized 2018 and 2019 SMART telemetry tables into datasets suitable for later feature engineering.

This layer is responsible only for:

1. removing SMART columns that contain no information in either year;
2. identifying and isolating records containing no SMART information at all;
3. preserving those rejected records in year-specific audit tables;
4. producing cleaned year-specific SMART datasets;
5. producing MC1-specific datasets;
6. removing SMART attributes that are structurally unavailable for MC1.

This layer **must not** yet perform:

- imputation;
- forward filling;
- temporal feature construction;
- rolling statistics;
- target construction;
- feature scaling;
- training/validation splitting;
- model-specific transformations beyond MC1 schema reduction.

Partial `NULL` values must remain `NULL`.

---

# 2. Input assumptions

The specification assumes two standardized Silver input relations already exist:

```text
smart_2018
smart_2019
```

Their exact dbt `ref()` names can be substituted according to the existing project.

Each table contains:

- record/date identifiers;
- SSD identifier;
- `MODEL`;
- other metadata columns;
- SMART raw attributes `R_x`;
- SMART normalized attributes `N_x`.

All non-SMART metadata columns must be preserved unless explicitly specified otherwise.

---

# 3. Global structurally unavailable SMART attributes

Cross-year analysis established that the following 17 SMART attributes contain no observations in either 2018 or 2019:

```text
2
3
4
6
7
8
10
11
13
189
191
193
200
204
205
207
240
```

For each ID, both the raw and normalized representation are structurally unavailable.

Therefore, the following **34 columns** must be removed from the secondary Silver modeling lineage:

```text
R_2    N_2
R_3    N_3
R_4    N_4
R_6    N_6
R_7    N_7
R_8    N_8
R_10   N_10
R_11   N_11
R_13   N_13
R_189  N_189
R_191  N_191
R_193  N_193
R_200  N_200
R_204  N_204
R_205  N_205
R_207  N_207
R_240  N_240
```

These columns should remain available in the original upstream Silver/source relation for lineage purposes. They are removed only from this secondary analytical layer.

`R_211` and `N_211` must **not** be removed globally.

Although SMART 211 is completely absent in 2018, it contains observations in 2019 and therefore remains part of the common 2018–2019 schema.

---

# 4. Year-specific no-SMART audit tables

Some source records contain metadata but no SMART measurements at all.

Previously identified counts are:

```text
2018: 490,285 rows
2019:     635 rows
```

These rows must not enter the primary modeling lineage.

They must instead be preserved exactly in year-specific audit relations.

Recommended dbt model names:

```text
silver_audit_smart_2018_no_attributes
silver_audit_smart_2019_no_attributes
```

Alternative naming consistent with an `int_` dbt convention:

```text
int_audit_smart_2018_no_attributes
int_audit_smart_2019_no_attributes
```

The audit tables should retain:

- all metadata columns;
- all SMART columns retained by this Silver stage;
- source year;
- optionally an audit reason.

Recommended derived audit metadata:

```text
AUDIT_REASON = 'ALL_SMART_ATTRIBUTES_NULL'
SOURCE_YEAR  = 2018 / 2019
```

No source information should be fabricated or imputed.

---

# 5. Definition of an all-SMART-null record

After excluding the 34 globally useless SMART columns, evaluate all remaining SMART `R_x` and `N_x` columns.

A record belongs to the audit table when:

```text
every retained R_x is NULL
AND
every retained N_x is NULL
```

Conceptually:

```sql
where
    r_1 is null
    and n_1 is null
    ...
    and r_250 is null
    and n_250 is null
```

using only SMART attributes that remain in the common schema.

Because the 34 removed columns are universally `NULL`, checking before or after their removal is logically equivalent. The dbt implementation should nevertheless check the **retained SMART feature set**, because this makes the business rule explicit.

---

# 6. Clean year-specific Silver relations

After:

1. removing the 34 globally unavailable columns; and
2. excluding all-SMART-null records;

produce:

```text
silver_smart_2018_clean
silver_smart_2019_clean
```

Recommended dbt internal names:

```text
int_smart_2018_null_processed
int_smart_2019_null_processed
```

Each relation must contain:

```text
all original metadata columns
+
all SMART columns except the 34 globally unavailable columns
```

Partial missingness must be preserved.

For example:

```text
R_173 = NULL
N_173 = NULL
R_194 = 42
N_194 = 97
```

must remain exactly that way.

The transformation must **not** perform:

```text
NULL → 0
NULL → mean
NULL → median
NULL → mode
NULL → neighboring SSD value
NULL → forward fill
NULL → backward fill
```

The presence of a missing SMART attribute remains part of the observation.

---

# 7. Row-conservation requirement

For each year:

```text
source row count
=
clean Silver row count
+
all-SMART-null audit row count
```

Therefore:

```text
2018 source
= 2018 clean + 490,285 expected audit rows

2019 source
= 2019 clean + 635 expected audit rows
```

These known counts may be used initially as validation checks, although long-term dbt tests should preferably verify conservation dynamically rather than hard-code dataset-specific row counts.

---

# 8. MC1-specific secondary relations

The predictive modeling scope is:

```text
MODEL = 'MC1'
```

The MC1 tables should derive from the already-null-processed yearly tables, not directly from raw data.

Lineage:

```text
smart_2018
    ↓
int_smart_2018_null_processed
    ↓
int_smart_2018_mc1
```

and:

```text
smart_2019
    ↓
int_smart_2019_null_processed
    ↓
int_smart_2019_mc1
```

Filter:

```sql
where MODEL = 'MC1'
```

---

# 9. MC1 structurally unavailable SMART attributes

The MC1-specific analysis identified an additional 12 SMART attributes that are completely unavailable for MC1 in both years:

```text
175
177
181
182
190
192
232
233
241
242
244
245
```

Both raw and normalized representations are unavailable.

Therefore, the MC1 relations additionally remove these **24 columns**:

```text
R_175  N_175
R_177  N_177
R_181  N_181
R_182  N_182
R_190  N_190
R_192  N_192
R_232  N_232
R_233  N_233
R_241  N_241
R_242  N_242
R_244  N_244
R_245  N_245
```

These columns must **not** be removed from the generic year-level Silver tables because other SSD models may report them.

They are removed only after:

```text
MODEL = 'MC1'
```

has established the modeling population.

---

# 10. Resulting dbt lineage

Recommended structure:

```text
                    ┌──────────────────────┐
                    │   SMART 2018 source  │
                    └──────────┬───────────┘
                               │
              remove 34 globally-null columns
                               │
                   classify all-null rows
                      ┌────────┴────────┐
                      │                 │
                      ▼                 ▼
       int_smart_2018_null_processed   audit_2018_no_attributes
                      │
                 MODEL = MC1
                      │
          remove 24 MC1-null columns
                      │
                      ▼
              int_smart_2018_mc1


                    ┌──────────────────────┐
                    │   SMART 2019 source  │
                    └──────────┬───────────┘
                               │
              remove 34 globally-null columns
                               │
                   classify all-null rows
                      ┌────────┴────────┐
                      │                 │
                      ▼                 ▼
       int_smart_2019_null_processed   audit_2019_no_attributes
                      │
                 MODEL = MC1
                      │
          remove 24 MC1-null columns
                      │
                      ▼
              int_smart_2019_mc1
```

---

# 11. Recommended dbt model inventory

A clean dbt directory could contain:

```text
models/
└── silver/
    └── smart/
        ├── intermediate/
        │   ├── int_smart_2018_null_processed.sql
        │   ├── int_smart_2019_null_processed.sql
        │   ├── int_smart_2018_mc1.sql
        │   └── int_smart_2019_mc1.sql
        │
        ├── audit/
        │   ├── int_audit_smart_2018_no_attributes.sql
        │   └── int_audit_smart_2019_no_attributes.sql
        │
        └── schema.yml
```

If audit tables should physically live in a different Snowflake schema, dbt configuration can materialize:

```text
SILVER
SILVER_AUDIT
```

while keeping the same dependency graph.

---

# 12. Recommended materialization

Because the source SMART datasets contain more than 100 million rows per year, these transformations should normally be materialized as tables rather than views if they are repeatedly consumed downstream.

Suggested configuration:

```yaml
materialized: table
```

The precise clustering strategy should be decided using the downstream Snowflake access pattern rather than introduced arbitrarily in this specification.

Likely future filtering dimensions include:

```text
MODEL
date
disk identifier
```

but physical optimization should be handled separately.

---

# 13. dbt validation requirements

## 13.1 All-null audit condition

Every row in:

```text
int_audit_smart_2018_no_attributes
int_audit_smart_2019_no_attributes
```

must have every retained SMART attribute equal to `NULL`.

---

## 13.2 Clean-table exclusion

No row in:

```text
int_smart_2018_null_processed
int_smart_2019_null_processed
```

may have every retained SMART attribute equal to `NULL`.

---

## 13.3 MC1 population test

Every row in:

```text
int_smart_2018_mc1
int_smart_2019_mc1
```

must satisfy:

```text
MODEL = 'MC1'
```

---

## 13.4 Structural-column absence

The 34 globally unavailable columns must not exist in either generic cleaned table.

The additional 24 MC1-unavailable columns must not exist in either MC1-specific table.

---

## 13.5 Partial NULL preservation

The transformation must not reduce the null count of any retained SMART column through imputation.

Any changes in retained-column null counts between input and output should be explainable exclusively by rows being routed into the all-SMART-null audit table or by the `MODEL = 'MC1'` filter.

---

## 13.6 Year isolation

2018 and 2019 must remain separate throughout this stage.

No union should yet occur in the MC1 transformation itself.

This allows year-specific row-count validation and preserves the ability to inspect temporal distribution shifts before constructing the common modeling dataset.

---

# 14. Future union layer

After both MC1 tables have passed validation, a later model may produce:

```text
int_smart_mc1_all_years
```

Conceptually:

```sql
select *, 2018 as source_year
from {{ ref('int_smart_2018_mc1') }}

union all

select *, 2019 as source_year
from {{ ref('int_smart_2019_mc1') }}
```

Because both MC1 relations apply the same structural feature-removal policy, they should expose an identical schema.

`SOURCE_YEAR` should be retained explicitly.

This table becomes the input to the later **feature-construction stage**.

That later stage is intentionally outside the scope of this specification.

---

# 15. Final outputs of this stage

The preprocessing layer therefore produces six principal relations:

```text
1. int_smart_2018_null_processed
2. int_smart_2019_null_processed

3. int_audit_smart_2018_no_attributes
4. int_audit_smart_2019_no_attributes

5. int_smart_2018_mc1
6. int_smart_2019_mc1
```

Optionally, once this stage is validated:

```text
7. int_smart_mc1_all_years
```

may be introduced as the common input to feature engineering.

---

# 16. Design rationale

The transformation deliberately distinguishes three kinds of missing information.

### Globally structurally unavailable

The attribute never exists in either year.

```text
Action: remove from the common Silver analytical schema.
```

### MC1 structurally unavailable

The attribute exists elsewhere but never exists for the selected MC1 population.

```text
Action: retain in generic Silver;
remove only from MC1-specific relations.
```

### Partially unavailable

The attribute is reported for some observations but missing for others.

```text
Action: preserve NULL.
```

No value is invented because the analysis provides no evidence that a statistically imputed value would correctly represent the underlying SMART measurement.

The downstream tree-based model can therefore distinguish:

```text
observed numeric value
```

from:

```text
attribute unavailable for this observation
```

without conflating missingness with a fabricated numeric measurement.

The all-SMART-null records are treated differently because they contain no SMART telemetry from which a failure prediction could be constructed. They are excluded from the principal modeling lineage but retained in audit tables to preserve lineage, reproducibility, and later investigation.