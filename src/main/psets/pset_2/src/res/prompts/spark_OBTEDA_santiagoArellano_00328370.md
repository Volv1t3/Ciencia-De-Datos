# Specification: Exploratory Data Analysis of the MC1 OBT

**Project:** SSD failure prediction using SMART telemetry

**Environment:** Existing PySpark/Jupyter `spark` container, Spark–Snowflake connector, Snowflake virtual warehouse

**Input:** `OBT_MC1_RN`

**Scope:** Initial EDA, before feature selection and model training

## 1. Purpose and scope

The purpose of this analysis is to understand the final, feature-engineered MC1 dataset before using it to train a model that predicts SSD failures within the following 30 days.

The analysis must establish:

1. Whether the dataset satisfies its structural and labeling contracts.
2. How the supervised learning population is distributed.
3. Whether confirmed failures have meaningful preceding sequences of daily SMART observations.
4. Whether selected engineered SMART measurements exhibit observable differences between positive and negative observations.
5. Which findings should influence the later modeling process.

This EDA is **read-only**. It must not alter, clean, overwrite, or produce a replacement for the existing OBT.

### Explicit exclusions

The following analyses are outside this iteration:

- General NULL-frequency and missingness analysis.
- Missingness-pattern analysis or imputation decisions.
- All-pairs correlation matrices.
- Systematic R-versus-N redundancy evaluation.
- PCA, automatic feature selection, and dimensionality reduction.
- Model training, validation, and hyperparameter optimization.
- SMOTE, class balancing, or synthetic observation generation.

These activities may be performed in subsequent stages.

---

## 2. Data source and execution

### Source relation

The notebook reads observation data exclusively from:

```text
<SNOWFLAKE_DATABASE>.S_CDATOS_PSET2_OBT.OBT_MC1_RN
```

The database name must be resolved through the existing connection configuration. Reading `INFORMATION_SCHEMA.COLUMNS` for schema metadata and Snowflake Query History for execution verification is permitted; these are not additional observation-data sources.

Do not hardcode the Snowflake account or credentials in the notebook.

The notebook must follow the existing EDA notebooks in `src/main/python/spark/notebooks/datagrip/`: import `create_spark_session` and `read_snowflake_query` from `src/main/python/spark/lib/snowflake_io.py`, create/reuse the notebook's Spark session, and submit explicit Snowflake `SELECT` queries with `read_snowflake_query(spark, query, schema="S_CDATOS_PSET2_OBT")`. Construct the qualified table name from the configured `SNOWFLAKE_DATABASE`; do not hardcode the database, account, role, warehouse, password, or token.

This is the **classic Spark–Snowflake connector** (`net.snowflake.spark.snowflake`), not the separate Snowpark Connect job used to build the OBT. The Jupyter `spark` service has a `local[*]` Spark master and uses its configured `SNOWFLAKE_WAREHOUSE`; the `snowpark-connect` service's `SNOWFLAKE_WAREHOUSE_HIGH_COMPUTE` setting does not automatically apply to notebooks. If a larger warehouse is desired for this EDA, configure the notebook service's warehouse explicitly before execution.

`SNOWFLAKE_EDA_PAT` (when set) is supplied to the connector instead of the human password. No credential may be printed, stored in the notebook, or embedded in SQL.

### Expected dataset structure

The current contract contains **847 columns**:

| Column family | Count |
|---|---:|
| Observation metadata | 6 |
| Raw SMART engineered fields | 418 |
| Normalized SMART engineered fields | 418 |
| Target and label metadata | 5 |
| **Total** | **847** |

The attached Snowflake DDL confirms 22 SMART IDs, each represented by a raw (`R`) and normalized (`N`) family. Each representation has 19 fields: the original measurement and its rolling means, observation counts, validity flags, and adjacent-window mean shifts. The exact registry, field names, and types are specified in §2.1 below.

The previously validated Gold population contains 115,159,020 MC1 daily observations across 199,655 SSD identifiers. These are reference values, not values to hardcode as assertions: the notebook must report the counts actually found in the current OBT.

### Execution strategy

All full-population scans, filters, joins, window calculations, grouping, percentiles, and row-level sampling must execute in Snowflake. Submit explicit Snowflake SQL through `read_snowflake_query(...)` so warehouse execution is unambiguous; the connector returns a Spark DataFrame containing only the query result. Do **not** read the complete OBT with `read_snowflake_table(...)`, `spark.table(...)`, or `spark.read` and then assume subsequent Spark operations will always push down. `autopushdown=on` is an optimization, not a proof that every Spark operation runs in Snowflake.

Only explicitly bounded aggregates, histogram bins, compact schema metadata, or bounded case-study points may cross into the local Spark driver and then Python/Pandas for display or plotting. Small-result plotting is the only local computation permitted; no full-population analysis or raw-row processing may execute locally. Every query returning detail rows must have a deterministic ordering and a documented hard `LIMIT`. Never use `LIMIT` alone as a substitute for a representative sample.

```text
Jupyter/PySpark helper → connector `query` option → Snowflake warehouse
                                            │
                                            ├─ aggregate by label/year
                                            ├─ aggregate by failure distance
                                            ├─ selected-feature summaries
                                            └─ bounded plot data
                                            │
                                            ▼
                              small Spark result → Pandas/Matplotlib
```

The notebook must never load the full 847-column dataset, an unbounded row-level projection, or a large intermediate result into the local Spark process or Pandas. Use Snowflake Query History or query profiles to verify that expensive SQL ran in the intended warehouse; record the warehouse used and, where practical, query IDs. Do not claim warehouse execution merely because a Spark DataFrame was returned.

### 2.1 Verified `OBT_MC1_RN` schema contract

The attached `CREATE OR REPLACE TABLE S_CDATOS_PSET2.S_CDATOS_PSET2_OBT.OBT_MC1_RN` statement contains exactly 847 distinct columns. It is a **reference schema**, not an instruction to recreate or replace the production table. The notebook must compare the live relation against this contract and report differences rather than silently adapting to them.

**Identity and label columns (11 total):**

| Column | Snowflake type | Role |
|---|---|---|
| `SMART_DAILY_KEY` | `VARCHAR(128)` | Daily observation technical key; integrity check only |
| `SSD_KEY` | `VARCHAR(128)` | SSD surrogate key; grouping and joins only |
| `DISK_ID` | `NUMBER(38,0)` | Source SSD identifier; not unique without model |
| `MODEL_CODE` | `VARCHAR(50331648)` | Expected value `MC1` |
| `OBSERVATION_DATE_KEY` | `NUMBER(8,0)` | Date-dimension key |
| `OBSERVATION_DATE` | `DATE` | Daily observation date |
| `FIRST_FAILURE_DATE` | `DATE` | First recorded failure date; label provenance, never predictor |
| `LAST_SEEN_DATE` | `DATE` | Last observed date; censoring provenance, never predictor |
| `DAYS_TO_FAILURE` | `NUMBER(38,0)` | Signed distance to first failure; never predictor |
| `LABEL_STATUS` | `VARCHAR(134217728)` | `POSITIVE`, `NEGATIVE`, `CENSORED`, `SAME_DAY_FAILURE`, or `POST_FAILURE` |
| `TARGET_30D` | `NUMBER(38,0)` | Binary response (`0`/`1`) or `NULL` |

**SMART registry (22 IDs, in the DDL's order):** `1, 5, 9, 12, 170, 171, 172, 173, 174, 180, 183, 184, 187, 188, 194, 195, 196, 197, 198, 199, 206, 211`. Each ID has both `R_<ID>` and `N_<ID>` families. The same registry appears in `src/main/python/spark/jobs/build_mc1_obt.py`; do not use the broader 2018/2019 Silver SMART menu, which includes attributes not present in this MC1 OBT.

For **each** prefix/ID combination, substitute `<P>_<ID>` (for example `R_5` or `N_5`) into the following exact field template:

| Column template | Per family | Snowflake type | Meaning |
|---|---:|---|---|
| `<P>_<ID>` | 1 | `NUMBER(38,6)` | Daily measurement |
| `<P>_<ID>_MEAN_<W>D` | 3 | `NUMBER(38,10)` | Current-window mean |
| `<P>_<ID>_COUNT_<W>D` | 3 | `NUMBER(38,0)` | Non-null observations in current window |
| `<P>_<ID>_PREV_COUNT_<W>D` | 3 | `NUMBER(38,0)` | Non-null observations in previous window |
| `<P>_<ID>_MEAN_VALID_<W>D` | 3 | `NUMBER(38,0)` | Current-mean validity indicator |
| `<P>_<ID>_MEAN_SHIFT_<W>D` | 3 | `NUMBER(38,9)` | Current minus previous mean |
| `<P>_<ID>_MEAN_SHIFT_VALID_<W>D` | 3 | `NUMBER(38,0)` | Shift validity indicator |
| **Total per family** | **19** | | `W ∈ {7, 14, 30}` |

Thus `22 × 2 × 19 = 836` SMART columns: 418 raw and 418 normalized. Schema inspection must inventory exact names and types from Snowflake metadata, compare them to this generated expected-name template, and report missing, unexpected, and mistyped columns. It must not materialize the 847-column source to inspect the schema.

---

## 3. Analysis A: Dataset health and structural integrity

### Objective

Verify that the delivered OBT is structurally consistent with the dimensional grain and that subsequent EDA results can be trusted.

This section validates the final output rather than repeating the Bronze/Silver data-cleaning analysis.

### A.1 Schema inspection

Record:

- Actual column count.
- Actual row count.
- Column types.
- Presence of all six metadata fields.
- Presence of the five target fields.
- Number of R and N feature columns.
- Observed date range.

Validate that the schema is consistent with the 847-column contract.

Required metadata:

```text
SMART_DAILY_KEY
SSD_KEY
DISK_ID
MODEL_CODE
OBSERVATION_DATE_KEY
OBSERVATION_DATE
```

Required target fields:

```text
FIRST_FAILURE_DATE
LAST_SEEN_DATE
DAYS_TO_FAILURE
LABEL_STATUS
TARGET_30D
```

The schema check should be programmatic rather than manually maintaining a list of 847 columns.

### A.2 Grain validation

The OBT grain is:

> One daily SMART observation per physical MC1 SSD.

Verify these invariants:

| Rule | Expected result |
|---|---|
| `SMART_DAILY_KEY` uniqueness | No duplicates |
| `(SSD_KEY, OBSERVATION_DATE)` uniqueness | No duplicates |
| `MODEL_CODE` | Always `MC1` |
| Date range | Within the known observation period |
| Row count | Explainable against the validated Gold population |

If duplicates exist, report the total number of duplicated keys and a small sample for investigation.

Do not delete duplicates inside the EDA.

### A.3 Required result

Produce a compact dataset summary:

| Metric | Result |
|---|---|
| Total observations | Calculated |
| Total columns | Calculated |
| Distinct SSDs | Calculated |
| Minimum observation date | Calculated |
| Maximum observation date | Calculated |
| Duplicate technical keys | Calculated |
| Duplicate SSD/date combinations | Calculated |
| Unexpected model codes | Calculated |

The result must be compared with the existing Snowflake/Gold health figures.

**Interpretation requirement:** Explain whether any inconsistency would invalidate or limit subsequent statistical analysis.

---

## 4. Analysis B: Target-variable distribution

### Objective

Understand the true composition of the classification dataset, including the difference between failure events, positive daily observations, and distinct failing SSDs.

The target variable is:

```text
TARGET_30D
```

It expresses whether an SSD has a known failure during the next 30 calendar days.

### B.1 Label-status distribution

Calculate the number of observations and distinct SSDs for each:

```text
POSITIVE
NEGATIVE
CENSORED
SAME_DAY_FAILURE
POST_FAILURE
```

Required summary:

| Label status | Observations | Distinct SSDs | % of total |
|---|---:|---:|---:|
| POSITIVE | Calculated | Calculated | Calculated |
| NEGATIVE | Calculated | Calculated | Calculated |
| CENSORED | Calculated | Calculated | Calculated |
| SAME_DAY_FAILURE | Calculated | Calculated | Calculated |
| POST_FAILURE | Calculated | Calculated | Calculated |

The number of observations must be distinguished from the number of physical SSDs.

A single failed SSD may contribute up to 30 positive daily observations for one confirmed failure.

### B.2 Supervised-learning population

Define the current training-eligible population as:

```text
LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')
```

Then calculate:

```text
N_POSITIVE
N_NEGATIVE
N_ELIGIBLE
```

and:

```text
POSITIVE_RATE = N_POSITIVE / N_ELIGIBLE
```

Report the class ratio:

```text
N_NEGATIVE / N_POSITIVE
```

when positive examples exist.

This ratio quantifies imbalance.

It is not yet a reason to resample the dataset.

### B.3 Target distribution by year

Repeat the label summary grouped by:

```text
YEAR(OBSERVATION_DATE)
```

This distinction is particularly important because the current project handoff indicates that confirmed failure-event coverage is concentrated in 2018, without equivalent confirmed labels for 2019.

The notebook must examine whether:

- Positive observations are concentrated in 2018.
- 2019 contains any confirmed positives.
- Negative observations dominate years without equivalent failure-event coverage.
- Censored observations accumulate near observation-period boundaries.

**Important methodological limitation:** Observing telemetry for another 30 days does not independently prove that no failure occurred if the source fails to record some failure events. The EDA must not interpret 2019 `NEGATIVE` as a fully verified survival outcome without confirming failure-label coverage.

Therefore, whole-period statistics and year-specific statistics must be presented separately.

### B.4 Label-consistency checks

Validate:

| Status | Required condition |
|---|---|
| POSITIVE | `TARGET_30D = 1` and `DAYS_TO_FAILURE` from 1 to 30 |
| NEGATIVE | `TARGET_30D = 0`, sufficient future observation horizon, no known failure in horizon |
| SAME_DAY_FAILURE | `DAYS_TO_FAILURE = 0`, target undefined |
| POST_FAILURE | Observation after first failure, target undefined |
| CENSORED | Target undefined |

The count of inconsistent observations must be reported.

### B.5 Visualizations

Produce:

**Figure 1 — Label composition.** A bar chart of the five label statuses, with observations as the measurement.

**Figure 2 — Target distribution by year.** A grouped or stacked bar chart comparing `POSITIVE`, `NEGATIVE`, and excluded/censored observations across 2018 and 2019.

Use a logarithmic scale where necessary to keep minority categories visible; show actual counts in annotations.

### Interpretation

Answer:

- Is the supervised dataset strongly imbalanced?
- How many distinct SSDs contribute positive observations?
- How does target availability differ between years?
- Are there limitations that could bias a subsequent classification model?

---

## 5. Analysis C: Pre-failure observation sequences

### Objective

Validate the central hypothesis underlying the project:

> Multiple SMART observations preceding one failure can represent progressive stages of SSD deterioration, rather than treating each failure event as a single isolated example.

This is the most important project-specific analysis.

### C.1 Positive observations by failure distance

Select:

```text
LABEL_STATUS = 'POSITIVE'
```

Group observations by:

```text
DAYS_TO_FAILURE
```

Valid values:

```text
1, 2, 3, ..., 30
```

Calculate:

- Total positive observations for each failure distance.
- Distinct SSDs represented at each distance.

Required output:

| Days before failure | Positive rows | Distinct SSDs |
|---:|---:|---:|
| 30 | Calculated | Calculated |
| 29 | Calculated | Calculated |
| ... | ... | ... |
| 1 | Calculated | Calculated |

**Figure 3 — Pre-failure observation coverage.**

Plot positive observations against days remaining until failure.

The graph must reveal whether coverage is consistent or sparse throughout the 30-day prediction horizon.

### C.2 Number of positive days per failing SSD

For each SSD with positive observations, calculate:

```text
POSITIVE_DAYS_PER_SSD
=
COUNT(DISTINCT OBSERVATION_DATE)
WHERE LABEL_STATUS = 'POSITIVE'
```

Summarize the distribution using:

```text
minimum
25th percentile
median
mean
75th percentile
maximum
```

The number of positive observations associated with a drive is not necessarily 30. A failing SSD may have fewer daily SMART records, or may only enter the observed dataset shortly before its failure.

**Figure 4 — Distribution of positive days per failing SSD.**

A histogram with:

- X-axis: Number of positive daily observations.
- Y-axis: Number of SSDs.

Include the fraction of positive SSDs that contribute at least 7, 14, and 21 positive days as optional descriptive indicators.

### C.3 Interpretation

Answer:

- How many failing SSDs have sufficiently detailed preceding telemetry to illustrate progression?
- Is the observed 30-day history generally complete or fragmented?
- Are positive observations concentrated in the final few days?
- Does the dataset meaningfully support the proposed temporal feature engineering?

This assessment must distinguish two questions:

1. Does the dataset contain positive records at different failure distances?
2. Do the SMART measurements themselves change in a meaningful way over those distances?

The first is answered here. The second requires the feature exploration below.

---

## 6. Analysis D: Initial relationship between SMART features and failure

### Objective

Satisfy the assignment requirement to investigate a relationship between the target and an explanatory variable, while beginning to evaluate whether the engineered temporal features are useful.

This analysis should remain deliberately narrow.

Rather than analyzing all 836 engineered SMART columns, select one registered SMART **family** (`R` or `N` plus SMART ID) and compare its seven measurement fields with the fixed binary target in independent blocks. A separate field-level selector must allow the user to choose two registered SMART fields for a targeted SMART-versus-SMART interaction. Neither menu may trigger an all-features loop, an all-pairs matrix, or an unbounded projection.

### D.1 Initial feature selection

A reasonable initial candidate is SMART 5, examining these **seven explicit, independently executable analysis blocks**:

```text
R_5
R_5_MEAN_7D
R_5_MEAN_14D
R_5_MEAN_30D
R_5_MEAN_SHIFT_7D
R_5_MEAN_SHIFT_14D
R_5_MEAN_SHIFT_30D
```

These represent three distinct ideas across the 7-, 14-, and 30-day windows:

| Feature | Meaning |
|---|---|
| `R_5` | Current raw measurement |
| `R_5_MEAN_7D` | Average raw measurement over the current seven-day window |
| `R_5_MEAN_14D`, `R_5_MEAN_30D` | Average raw measurement over the current 14- or 30-day window |
| `R_5_MEAN_SHIFT_7D` | Change between the current and preceding seven-day means |
| `R_5_MEAN_SHIFT_14D`, `R_5_MEAN_SHIFT_30D` | Change between the current and preceding 14- or 30-day means |

SMART 5 is selected as an exploratory starting point, not as a proven best predictor. Interpretation of its raw values should respect the source device's SMART definition.

If it proves uninformative, the analyst may select another existing OBT family from a numbered menu, recording the choice and reason. Build the family menu from the verified 22-ID registry and both `R` and `N` representations. For the separate SMART-pair interaction, build a field menu from the measurement-bearing templates (`<P>_<ID>`, `<P>_<ID>_MEAN_<W>D`, `<P>_<ID>_MEAN_SHIFT_<W>D`), intersected with the verified schema. Counts and validity flags are diagnostic fields, not continuous measurements for boxplots or IQR by default. Display number, exact column name or family, representation (`R`/`N`), SMART ID, window, and field type in Spanish as applicable. Validate a typed menu number against the allowlist and quote the resulting SQL identifier; never interpolate arbitrary user text into SQL.

The target comparison uses **all seven fields of one menu-selected family against fixed `TARGET_30D`**, with one independently executable analysis block per field. A separate menu selection may compare **two chosen SMART measurement fields** using paired non-null counts, covariance/correlation, and a bounded plot. This interaction is exploratory and separate from the family-versus-target analysis; it must not replace the required target comparison. Do not select the same feature twice or include target/lineage/label-derived fields in the SMART-pair menu.

### D.2 Positive versus negative comparison

For each selected feature block, compare:

```text
TARGET_30D = 0
```

against:

```text
TARGET_30D = 1
```

using observations considered eligible under the same time-period and label-quality restrictions.

Calculate:

```text
count of usable observations
mean
median
standard deviation
minimum
25th percentile
75th percentile
maximum
```

Do not interpret differences in group means without considering outliers and skew.

Required summary:

| Metric | Negative | Positive |
|---|---:|---:|
| Observations used | Calculated | Calculated |
| Mean | Calculated | Calculated |
| Median | Calculated | Calculated |
| Q1 | Calculated | Calculated |
| Q3 | Calculated | Calculated |
| Maximum | Calculated | Calculated |

All comparisons must disclose the population and dates used. By default, use `LABEL_STATUS IN ('POSITIVE','NEGATIVE')` and report year-specific results before any combined-year figure; do not silently treat unlabeled 2019 outcomes as verified negatives. Apply the same eligibility/date filter to both classes. Cast the selected fixed-point measurement to `DOUBLE` **before** sum-based aggregate functions such as `AVG` or variance to avoid large `NUMBER(38,s)` intermediate overflow; counts and extrema may retain exact numeric types. Disclose that floating-point moments are approximate.

The query must return one small aggregate row per target class. `COUNT(*)` is the eligible class size; `COUNT(feature)` is the usable non-null size. Both are needed to prevent a misleading feature comparison. Approximate quantiles must be labeled as approximate.

### D.3 Distribution plots

**Figure 5 — Feature distribution by target.**

Use a boxplot or comparable distribution visualization for the selected feature.

The plot should answer:

> Do observations followed by a failure have noticeably different feature values from observations not followed by a recorded failure within the horizon?

For a large or highly skewed distribution, compute plot-ready bins or quantile summaries in Snowflake. If individual points are essential, take a representative, reproducible **Snowflake-side** sample with an explicit row cap and show the sample size and method. Never transfer an unbounded sample to Spark/Pandas.

Sampling for visualization must not change the class proportions reported in the full-population summary.

If equal-sized class samples are used to make the distributions comparable, disclose that the graphic is balanced for visualization only.

### D.4 Exploratory outlier analysis

Apply an IQR rule to a selected continuous SMART feature rather than `TARGET_30D`.

Calculate:

```text
IQR = Q3 - Q1

LOWER_BOUND = Q1 - 1.5 * IQR
UPPER_BOUND = Q3 + 1.5 * IQR
```

Record the number of observations outside those bounds.

If Q1 and Q3 are identical, report that the feature's IQR is zero and interpret the resulting outlier count carefully.

Do not automatically remove flagged observations.

For SSD telemetry, extreme measurements could represent the very conditions preceding physical failure.

The analysis should explicitly ask whether outliers are plausible operating measurements, meaningful failure indicators, or evidence of a data-quality problem. Compute IQR bounds and the outlier count in Snowflake, ideally using a compact CTE or a bounded number of chained warehouse queries. Transfer only the resulting summary.

### D.5 One-SSD failure trajectory

Choose one SSD with a confirmed failure and a sufficiently detailed pre-failure history using a deterministic Snowflake-side ranking (for example, most observed positive days, then `SSD_KEY`).

Plot selected SMART features against calendar date, showing the failure date.

Recommended measurements:

```text
R_5
R_5_MEAN_7D
R_5_MEAN_SHIFT_7D
```

Use separate panels or separate figures when units/scales differ.

The intended output is a concrete case study showing the current measurement, recent average, and recent change in the weeks leading up to failure.

Limit the returned trajectory to that one SSD and a documented date window, order by `OBSERVATION_DATE`, and cap the result. This example is illustrative only. It cannot demonstrate that the same pattern generalizes to other failing SSDs.

### D.6 Aggregated feature trajectory

As an optional extension, calculate the median selected feature among positive observations for each:

```text
DAYS_TO_FAILURE = 30 ... 1
```

Plot the median feature value as failure approaches, together with the number of distinct SSDs contributing to each day.

Interpretation must consider that different failure-distance groups may contain different subsets of SSDs.

This figure can provide preliminary evidence of a recurring trend but does not establish causality or predictive performance. It must be a separate, opt-in block so it does not execute during the mandatory path.

### D.7 Notebook interaction and block contract

Use the menu mechanism of `build_eda_smart2018_notebook.py` as the interaction pattern, adapted to the narrower MC1 registry. An explicit selector cell records the chosen family; its seven downstream field blocks execute only when opted into. The notebook must not run a loop over menu entries or all SMART IDs. Provide a separate Markdown heading, SQL/result cell, compact table cell, plot cell, IQR cell, and interpretation for each analysis requested. The seven prebuilt `R_5` fields and the seven fields of a selected alternative family must not be hidden inside one runtime loop: each gets its own named block and can be run independently. Shared helpers may remove SQL boilerplate, but they may not turn the notebook into one monolithic cell or silently execute other families.

The SMART-pair interaction has its own two selector cells and its own query/table/figure/interpretation blocks. Only selected columns are projected. Compute paired statistics in Snowflake, casting numeric values to `DOUBLE` before sum-based aggregates if necessary. Correlation does not establish causality or predictive usefulness. If plotting individual pairs would exceed the bound, use Snowflake-side 2D bins or a reproducible capped sample.

---

## 7. Three required modeling findings

The notebook must conclude with three evidence-based findings, following the assignment's requested structure.

Each finding must contain:

| Component | Requirement |
|---|---|
| Observation | What was discovered |
| Evidence | Concrete statistic, table, or figure |
| Interpretation | Why the behavior matters |
| Modeling consequence | What should change, be evaluated, or remain excluded |

Potential categories to evaluate are:

### Finding category 1: Target imbalance

Investigate whether the eligible dataset is dominated by negative daily observations.

If verified, discuss implications for choosing evaluation metrics and comparing performance against a no-failure baseline.

Do not implement class balancing in this EDA.

### Finding category 2: Temporal coverage of failures

Investigate whether confirmed failures have sufficient pre-failure SMART observations to support seven-, fourteen-, and thirty-day temporal analysis.

If coverage is limited, identify which temporal windows are affected and how that should influence model experimentation.

### Finding category 3: Target information or feature behavior

Investigate a feature–target relationship, an unusual feature distribution, or a year-specific target-label limitation.

If 2019 lacks reliable failure ascertainment, this finding should explicitly evaluate the consequences of treating 2019 observations as negatives.

These are **candidate findings, not predetermined conclusions**. The actual three findings must be selected from the results, with no fabricated numbers or assumed trends.

---

## 8. Statistical and interpretation rules

### Observation-level versus SSD-level statistics

Always distinguish:

- Number of daily observations.
- Number of unique SSDs.
- Number of confirmed failures.

A large number of positive daily observations does not imply an equally large number of independent failure events.

### Binary target treatment

`TARGET_30D` is a binary variable.

Its appropriate initial analyses are class frequencies, proportions, and conditional distributions of explanatory features.

The IQR rule must not be presented as meaningful failure-target outlier detection.

### Correlation and causation

A feature whose distribution differs between positive and negative observations may be useful to investigate.

Such a difference does not prove causation or guarantee good predictive performance.

### Label availability

All class-conditional conclusions must state the analyzed observation period and the completeness limitations of recorded failure events.

A date range without confirmed failure labels cannot automatically be treated as a verified negative population.

### Leakage

The following must never be described as candidate predictors:

```text
FIRST_FAILURE_DATE
LAST_SEEN_DATE
DAYS_TO_FAILURE
LABEL_STATUS
SMART_DAILY_KEY
SSD_KEY
DISK_ID
```

`TARGET_30D` is the response.

`OBSERVATION_DATE` is permitted for grouping and later temporal splitting, but not automatically as a numerical model predictor.

---

## 9. Computational requirements

Given the approximately 115-million-row source and 847-column schema, the EDA implementation must minimize unnecessary warehouse work.

### Required practices

1. Use `create_spark_session(...)` and `read_snowflake_query(spark, sql, schema="S_CDATOS_PSET2_OBT")` from the existing `snowflake_io.py` helper. Do not use `spark.table(...)`: the notebook's Spark session is local and that call does not by itself identify the remote Snowflake relation.
2. Submit read-only `SELECT` statements. Put full-population filters, joins, grouping, window operations, sampling, and aggregation inside Snowflake SQL/CTEs. If an intermediate relation is truly required, use read-only chained CTEs; this EDA must not create/drop tables or stages.
3. Project only the identity, label, date, and selected SMART columns needed for each query. Avoid scanning all 847 fields for a selected-feature chart.
4. Return only analytically bounded results: for example 5 label-status rows, 2 year × status tables, 30 failure-distance rows, 1–30 positive-days histogram bins, and a fixed number of feature-summary or plot-bin rows. Detail-result queries need deterministic ordering and a hard limit declared next to the cell.
5. Call `.toPandas()`, `.collect()`, or `display()` only on those bounded connector results. These operations materialize data in local Spark/Python, so their inputs must already be small; checking a Spark DataFrame's count *after* an unbounded query is not a safe guard.
6. Reuse previously returned compact summaries in later cells. Use deterministic, documented Snowflake-side sampling for plots and single-SSD examples when aggregation is insufficient.
7. Avoid full-table sorts and per-column analysis loops over all 847 fields. The target-comparison menu may issue queries only for the seven fields of the selected family; the SMART-pair menu may issue queries only for its two selected fields.
8. Avoid repeatedly counting or scanning the full OBT for every figure. Share one bounded aggregate result among its associated table and graph blocks where possible. Do not cache the full OBT in local Spark.
9. Record the source table, execution date, configured warehouse name, eligibility/date filters, selected fields, sampling method/seed, row caps, and whether percentiles are approximate. Check Snowflake Query History for expensive queries.
10. Do not overwrite any production OBT table, create a replacement table, or download the full source into the notebook container.

The implementation may use Pandas and Matplotlib for small local summaries and plotting.

If approximate percentiles are used, document them as approximate. Use Snowflake SQL functions supported by the connected warehouse, following the existing SMART EDA query pattern; do not assume Snowpark Connect APIs are available in the Jupyter container. Fixed-point SMART aggregates that internally sum values must cast inputs to `DOUBLE` before aggregation, with the approximate-precision tradeoff noted.

No Spark RDD or local full-dataset processing is required.

---

## 10. Recommended notebook structure

Proposed notebook:

```text
eda_mc1_obt.ipynb
```

With the following sections:

```text
00. Introducción y preguntas de investigación

01. Conexión Spark–Snowflake
    - Confirmación de sesión y almacén de cómputo
    - Relación de origen y configuración de límites/muestreo

02. Salud estructural de la OBT
    - Inventario del esquema
    - Integridad del grano y de las claves
    - Recuentos y tabla de resumen

03. Análisis de la variable objetivo
    - Estados de etiqueta y distribución elegible
    - SSD distintos y distribución por año
    - Validación de reglas y figuras 1–2

04. Secuencias previas a la falla
    - Observaciones positivas por días hasta la falla
    - Días positivos por SSD
    - Cobertura y figuras 3–4

05. Relación entre SMART y la variable objetivo
    - Menú de una familia SMART y selector de dos características
    - Bloque independiente: R_5
    - Bloque independiente: R_5_MEAN_7D
    - Bloque independiente: R_5_MEAN_14D
    - Bloque independiente: R_5_MEAN_30D
    - Bloque independiente: R_5_MEAN_SHIFT_7D
    - Bloque independiente: R_5_MEAN_SHIFT_14D
    - Bloque independiente: R_5_MEAN_SHIFT_30D
    - Siete bloques independientes para la familia SMART alternativa elegida
    - Estadísticos, IQR, comparación de clases y figuras por bloque
    - Trayectoria de un SSD en su propio bloque
    - Interacción SMART–SMART elegida, en su propio bloque

06. Resultados e interpretación
    - Tres hallazgos documentados
    - Implicaciones para el modelado y limitaciones

07. Conclusión
    - Qué permite analizar el conjunto de datos
    - Qué permanece incierto
    - Preguntas para la siguiente etapa de aprendizaje automático
```

**Language requirement:** All notebook Markdown (including headings, captions, table descriptions, interpretations, warnings, and final findings) and every graph title, axis label, legend, and annotation must be in natural Spanish with correct accents and `ñ`, saved as UTF-8. Code identifiers and SQL column names may remain in English. Do not strip diacritics or force ASCII-only text. This language rule applies to the deliverable notebook; the implementation specification itself may remain in English.

**Block requirement:** Each section and internal step must have its own Markdown heading and executable cell(s). Repeated feature analyses must be separate, visible blocks rather than a `for` loop that runs all features or plots in one cell. A block may call shared helpers, but it must state its selected feature, population, query cost/size bound, result, and short Spanish interpretation. Optional blocks must be opt-in and must not execute during a normal top-to-bottom run.

---

## 11. Suggested code organization

The eventual repository can adapt the following logical modules to its existing structure:

```text
eda/
├── notebooks/
│   └── eda_mc1_obt.ipynb
│
├── analysis/
│   ├── health.py
│   ├── targets.py
│   ├── failure_sequences.py
│   └── feature_relationships.py
│
├── figures/
│
└── reports/
    └── mc1_obt_eda_findings.md
```

The notebook may call reusable functions for connection setup, menu validation, SQL quoting, and bounded result conversion. This does not override the requirement for independently executable feature and figure blocks. Code duplication between user-visible blocks is acceptable here.

The final location of these files must follow the existing repository's conventions.

---

## 12. Required deliverables

| Deliverable | Contents |
|---|---|
| Jupyter notebook | Versionable, reproducible Spanish-language EDA using the existing Spark/Snowflake connector |
| Dataset summary | OBT structure, counts, and grain validation |
| Target summary | Labels, class distribution, and year breakdown |
| Temporal summary | Positive days before failure and coverage by SSD |
| Feature comparison | Seven independently executable SMART 5 example blocks, seven opt-in blocks for a menu-selected family versus target, and opt-in selected SMART pair |
| Figures | Label, failure-window, and selected-feature visualizations |
| Findings document | Three evidence-backed findings and their modeling implications |

Figures may be saved in PNG or SVG format. Summary tables may be exported as small CSV files where useful.

No model artifacts are required.

---

## 13. Completion criteria

The initial EDA is complete when:

- The OBT schema and grain have been verified.
- The actual supervised population and class imbalance have been quantified.
- The number of independent failing SSDs has been distinguished from positive observation count.
- The 30-day pre-failure sequence has been analyzed.
- At least one engineered SMART feature has been compared against the target.
- A meaningful numeric-feature outlier analysis has been performed without automatic deletion.
- Three findings have been recorded with evidence and modeling implications.
- The notebook runs reproducibly without downloading the full OBT.
- Snowflake Query History confirms full-population processing ran in the configured warehouse; only bounded results reached local Spark/Pandas.
- The live 847-column OBT schema matches the attached DDL's exact name/type template, or discrepancies are reported.
- Notebook prose and figure text are in Spanish with UTF-8 accents; feature analyses are separate executable blocks and no all-feature loop runs.
- Known failure-label coverage limitations are explicitly documented.

The result must provide a reasoned basis for the next stage of the project, without prematurely selecting features or training models.
