# Spark notebooks

[Project home](../../../../../README.md) · [Silver models](../../../dbt/ssd_failure_prediction/models/silver/README.md) · **Notebooks and EDA** · [Null-analysis book](ssd_null_analysis_book/README.md) · [MC1 OBT jobs](../jobs/README.md)

JupyterLab serves this directory at `http://127.0.0.1:${JUPYTER_PORT}`. Use the
token stored in the private `src/res/env/.env` file. The Spark application UI is
available at `http://127.0.0.1:${SPARK_UI_PORT}` while a notebook Spark session
is active.

When connecting from DataGrip, configure the remote Jupyter server with the
host URL and token from `JUPYTER_PORT` and `JUPYTER_TOKEN`. Select the
`PySpark 4 + Snowflake` kernel for this project. Do not select a local Python
interpreter or the generic `Python 3 (ipykernel)` kernel: those environments do
not receive Spark's Python, Py4J, connector JAR, or `/opt/spark/lib` paths.

The experiment notebook under `datagrip/` sends MLflow requests from this
kernel to `http://mlflow:5000` via `MLFLOW_TRACKING_URI`. Open the tracking UI
at `http://127.0.0.1:${MLFLOW_PORT}` on the host (default port 5000). Compose
keeps its SQLite database and artifacts in `src/main/mlflow`; generated data in
that directory is ignored by Git. Rebuild the `spark` image after changing
`src/res/config/spark/requirements.txt`, then restart the notebook kernel.

The custom kernel is defined in
`src/res/config/spark/kernels/pyspark-snowflake/kernel.json`. A quick kernel
check is:

```python
import os
import sys

import pyspark
import snowflake_io

print(sys.executable)          # /usr/bin/python3
print(pyspark.__version__)     # 4.0.4
print(os.environ["SPARK_HOME"])  # /opt/spark
```

Notebook code can then import the shared connector helpers directly. For a
large source, submit an aggregate SQL query to Snowflake and retrieve only its
small result; a lazy `read_snowflake_table` DataFrame does not guarantee that
every later transformation will be pushed down:

```python
from snowflake_io import create_spark_session, read_snowflake_query

spark = create_spark_session("smart-quality-analysis")
summary = read_snowflake_query(
    spark,
    "SELECT COUNT(*) AS ROW_COUNT FROM SMART_2018",
    schema="S_CDATOS_PSET2_SILVER",
).toPandas()
```

For accounts requiring MFA on password sign-ins, create a Snowflake
programmatic access token (PAT) for the same `SNOWFLAKE_USER` and place its
secret in `SNOWFLAKE_EDA_PAT` in the private `src/res/env/.env`. The Spark
container passes that token to the connector's `sfPassword` field; the human
password is not passed to this container. A PAT replaces the password, not the
username, and it is separate from `JUPYTER_TOKEN`. Choose a role with access to
the EDA tables and warehouse, and ensure the PAT's role restriction matches
`SNOWFLAKE_ROLE`. Snowflake may also require a network policy and an
authentication policy that allows PATs. Copy the token when Snowflake displays
it; the secret cannot be retrieved later. Do not use a TOTP code or enrollment
key here.

After editing `.env`, recreate only the Spark service and restart the notebook
kernel before running a small Snowflake query:

```bash
docker compose --env-file src/res/env/.env up -d --no-deps --force-recreate spark
```

No image rebuild or notebook edit is needed for this credential change. PATs
expire and must be rotated before expiration. The other Snowflake clients in
Compose have independent credentials and are not switched to the EDA PAT.

The Snowflake connector uses the configured virtual warehouse and enables
query pushdown. Operations that cannot be pushed down still execute with the
container's `local[*]` Spark master. Never call `toPandas()` on a complete SMART
table; aggregate or sample in Spark first.

```mermaid
flowchart LR
    DG[DataGrip or browser] --> J[JupyterLab container]
    J --> K[PySpark 4 + Snowflake kernel]
    K --> H[snowflake_io helper]
    H --> C[Snowflake Spark connector]
    C --> W[Snowflake virtual warehouse]
    W -->|aggregated result| K
    K -->|small pandas frame| V[Tables and plots]
    K -->|CSV evidence| E[Versioned or interactive exports]
```

Filtering, projection, and aggregation should remain in the connector path so
Snowflake performs the large scan. Only bounded aggregate results belong in
pandas or plotting libraries inside the local container.

The SMART 2018 and 2019 notebooks are generated from
`build_eda_smart2018_notebook.py`. Despite its historical filename, the
generator accepts either supported year and creates numbered menus for selecting
one univariate variable or two interaction variables. Regenerate them from the
project root with:

```bash
python3 src/main/python/spark/notebooks/build_eda_smart2018_notebook.py --year 2018
python3 src/main/python/spark/notebooks/build_eda_smart2018_notebook.py --year 2019
```

The MC1 OBT notebook is generated independently from
[`build_eda_mc1_obt_notebook.py`](build_eda_mc1_obt_notebook.py), producing
[`EDA_MC1_OBT_CloudComputingEnabled.ipynb`](datagrip/EDA_MC1_OBT_CloudComputingEnabled.ipynb).
It uses read-only Snowflake SQL for the full-population analysis and keeps
the seven SMART 5 measurements (daily value, 7/14/30-day means, and
7/14/30-day mean shifts) in separate Spanish-language blocks. An optional
numbered family menu selects one `R`/`N` SMART attribute and activates the
same seven independent target-comparison blocks for it; a separate field menu
selects a SMART pair. Neither menu scans every feature. Generate and verify it
from the project root with:

```bash
python3 src/main/python/spark/notebooks/build_eda_mc1_obt_notebook.py
python3 src/main/python/spark/notebooks/build_eda_mc1_obt_notebook.py --check
```

Regeneration overwrites that notebook and clears its saved cell outputs. Keep
execution evidence separately before regenerating.

The labels notebook is purpose-built for its event grain and is generated with:

```bash
python3 src/main/python/spark/notebooks/build_eda_failure_labels_notebook.py
```

## Packaged null-structure analysis

[`ssd_null_analysis_book/`](ssd_null_analysis_book/) is the versioned,
self-contained analysis package used to document the SMART missingness findings
that informed the Silver and MC1 modeling policies. It contains:

- five small pandas-based notebooks under `notebooks/`;
- seven interpretation and methodology chapters under `docs/`;
- the exact CSV evidence consumed by those notebooks under `exports/`;
- an export manifest with file sizes and SHA-256 hashes; and
- a minimal standalone `requirements.txt`.

These packaged notebooks analyze already aggregated CSV evidence and therefore
use a normal Python kernel. The DataGrip notebooks under `datagrip/` are the
ones that connect to Snowflake through the project Spark kernel.

Directories named `*-pycharm-support-libs/` are IDE-downloaded debugger and
table-rendering runtimes. They are intentionally ignored because they are not
authored notebook source and PyCharm recreates them when required.

Menu item `1` maps to `R_1`. Pairwise covariance/correlation is exact by
default, while its relationship graph uses a bounded Snowflake sample. Null
ranking, null-pattern, and disk/model summaries remain exact warehouse
aggregations. The two null tables export independently to
`src/main/python/spark/notebooks/exports` on the host.

A Spanish walkthrough of both notebook families (every EDA section with its
Snowflake query and finding, and every null-analysis notebook with its result)
is in [`docs/10_notebooks.md`](../../../../../docs/10_notebooks.md).

For the conclusions derived from those exports, continue with the
[SSD SMART null-structure analysis book](ssd_null_analysis_book/README.md).
