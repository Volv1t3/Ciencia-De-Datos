"""Generate the menu-driven SSD failure-label EDA notebook."""

#? Generador de notebook de EDA (Analisis Exploratorio). Este script NO analiza datos: escribe un
#? archivo .ipynb en notebooks/datagrip/ con celdas de markdown y codigo. El notebook generado se
#? abre en JupyterLab (contenedor spark) y consulta Silver en Snowflake para medir completitud
#? (nulos por columna y patrones de nulos), validez (rangos, distribuciones), duplicados y riesgo
#? de falla. Sus resultados (CSV en exports/) son la evidencia de la politica de limpieza de dbt.
#? Ojo: el contenido de las celdas esta dentro de cadenas de texto; cambiarlo cambia el notebook.
from __future__ import annotations

import json
from pathlib import Path


TARGET = (
    Path(__file__).parent
    / "datagrip"
    / "EDA_SSD_FAILURE_LABELS_CloudComputingEnabled.ipynb"
)


#? Crea una celda markdown en formato nbformat.
def markdown(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": source}


#? Crea una celda de codigo vacia de resultados (outputs = []).
def code(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source,
    }


#? Lista de celdas del notebook, en orden. Todo lo que sigue entre comillas es contenido de celdas.
cells = [
    markdown(
        """# SSD failure-label exploratory data analysis

This notebook analyzes
`S_CDATOS_PSET2.S_CDATOS_PSET2_SILVER.SSD_FAILURE_LABELS` through the
Snowflake Spark connector.

The label relation has a different analytical grain from the annual SMART
tables: each row represents a labeled failure event. Its menu therefore covers
disk, model, failure date, and failure-hour distributions. Pairwise interaction
is expressed as co-occurrence counts rather than covariance between SMART
measurements.
"""
    ),
    markdown("## 0. Runtime, imports, and configuration"),
    code(
        """import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import pyspark
import seaborn as sns

from snowflake_io import create_spark_session, read_snowflake_query

print("Python:", sys.executable)
print("PySpark:", pyspark.__version__)
print("Spark home:", os.environ.get("SPARK_HOME"))

spark = create_spark_session("ssd-failure-labels-cloud-eda")
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

LABEL_TABLE = "S_CDATOS_PSET2.S_CDATOS_PSET2_SILVER.SSD_FAILURE_LABELS"
LABEL_SCHEMA = "S_CDATOS_PSET2_SILVER"
LABEL_VARIABLES = ["DISK_ID", "MODEL_CODE", "FAILURE_DATE", "FAILURE_AT"]
VARIABLE_MENU = pd.DataFrame({
    "NUMBER": range(1, len(LABEL_VARIABLES) + 1),
    "VARIABLE": LABEL_VARIABLES,
})

NULL_PATTERN_DISPLAY_LIMIT = 10_000
INTERACTION_DISPLAY_LIMIT = 200
EXPORT_DIR = Path("/opt/spark/notebooks/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

sns.set_theme(style="whitegrid")
pd.set_option("display.max_rows", 500)
"""
    ),
    code(
        """def quote_identifier(value: str) -> str:
    if value not in LABEL_VARIABLES:
        raise ValueError(f"Unrecognized label variable: {value}")
    return '"' + value.replace('"', '""') + '"'


def snowflake_query(query: str):
    return read_snowflake_query(spark, query, schema=LABEL_SCHEMA)


def aggregate_to_pandas(query: str) -> pd.DataFrame:
    return snowflake_query(query).toPandas()


def show_variable_menu() -> None:
    display(VARIABLE_MENU)


def select_variable(prompt: str, *, excluded: set[str] | None = None) -> str:
    excluded = excluded or set()
    while True:
        raw_value = input(f"{prompt} [1-{len(LABEL_VARIABLES)}]: ").strip()
        try:
            number = int(raw_value)
        except ValueError:
            print("Enter an integer from the NUMBER column.")
            continue
        if not 1 <= number <= len(LABEL_VARIABLES):
            print(f"Enter a number between 1 and {len(LABEL_VARIABLES)}.")
            continue
        variable = LABEL_VARIABLES[number - 1]
        if variable in excluded:
            print(f"{variable} is already selected; choose another variable.")
            continue
        print(f"Selected {number} -> {variable}")
        return variable


def category_expression(variable: str) -> str:
    quote_identifier(variable)
    if variable == "FAILURE_AT":
        return "TO_VARCHAR(DATE_TRUNC('hour', FAILURE_AT))"
    if variable == "FAILURE_DATE":
        return "TO_VARCHAR(FAILURE_DATE)"
    if variable == "DISK_ID":
        return "TO_VARCHAR(DISK_ID)"
    return "MODEL_CODE"


def distribution_for(variable: str) -> pd.DataFrame:
    expression = category_expression(variable)
    return aggregate_to_pandas(f'''
        SELECT
            {expression} AS CATEGORY,
            COUNT(*) AS RECORD_COUNT,
            ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 6) AS RECORD_PERCENT
        FROM {LABEL_TABLE}
        WHERE {quote_identifier(variable)} IS NOT NULL
        GROUP BY 1
        ORDER BY RECORD_COUNT DESC, CATEGORY
    ''')
"""
    ),
    markdown(
        """## 1. Menu-driven univariate distribution

Select one label variable. The next cell returns its complete aggregated
distribution table; the following cell plots its most informative categories.
"""
    ),
    code(
        """show_variable_menu()
selected_label_variable = select_variable("Select the label variable")
"""
    ),
    code(
        """selected_distribution = distribution_for(selected_label_variable)
display(selected_distribution)
"""
    ),
    code(
        """plot_distribution = selected_distribution.head(40).copy()
if selected_label_variable in {"FAILURE_DATE", "FAILURE_AT"}:
    plot_distribution = plot_distribution.sort_values("CATEGORY")
    _, axis = plt.subplots(figsize=(12, 5))
    axis.plot(
        plot_distribution["CATEGORY"],
        plot_distribution["RECORD_COUNT"].astype(float),
        marker="o",
    )
    axis.tick_params(axis="x", rotation=70)
else:
    plot_distribution = plot_distribution.sort_values("RECORD_COUNT")
    _, axis = plt.subplots(figsize=(11, max(4, 0.3 * len(plot_distribution))))
    colors = [
        "#d62728" if selected_label_variable == "MODEL_CODE" and str(value).upper() == "MC1"
        else "#4c78a8"
        for value in plot_distribution["CATEGORY"]
    ]
    axis.barh(
        plot_distribution["CATEGORY"].astype(str),
        plot_distribution["RECORD_COUNT"].astype(float),
        color=colors,
    )
axis.set_title(f"SSD failure labels by {selected_label_variable}")
axis.set_xlabel(selected_label_variable)
axis.set_ylabel("Failure-label records")
plt.tight_layout()
plt.show()
"""
    ),
    markdown(
        """## 2. Interaction between two selected label variables

Select two distinct variables. Snowflake groups the observed pairs and returns
their record counts and share of all non-null selected pairs. This is a
co-occurrence analysis; `DISK_ID` is an identifier, so treating it as a
continuous measurement for covariance would not be meaningful.
"""
    ),
    code(
        """show_variable_menu()
selected_interaction_a = select_variable("Select the first label variable")
selected_interaction_b = select_variable(
    "Select the second label variable",
    excluded={selected_interaction_a},
)
"""
    ),
    code(
        """expression_a = category_expression(selected_interaction_a)
expression_b = category_expression(selected_interaction_b)
label_interactions = aggregate_to_pandas(f'''
    SELECT
        {expression_a} AS VARIABLE_A_VALUE,
        {expression_b} AS VARIABLE_B_VALUE,
        COUNT(*) AS RECORD_COUNT,
        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 6) AS RECORD_PERCENT
    FROM {LABEL_TABLE}
    WHERE {quote_identifier(selected_interaction_a)} IS NOT NULL
      AND {quote_identifier(selected_interaction_b)} IS NOT NULL
    GROUP BY 1, 2
    ORDER BY RECORD_COUNT DESC, VARIABLE_A_VALUE, VARIABLE_B_VALUE
    LIMIT {INTERACTION_DISPLAY_LIMIT}
''')
display(label_interactions)
"""
    ),
    code(
        """interaction_plot = label_interactions.head(30).copy()
interaction_plot["PAIR"] = (
    interaction_plot["VARIABLE_A_VALUE"].astype(str)
    + " | "
    + interaction_plot["VARIABLE_B_VALUE"].astype(str)
)
interaction_plot = interaction_plot.sort_values("RECORD_COUNT")

_, axis = plt.subplots(figsize=(12, max(5, 0.32 * len(interaction_plot))))
axis.barh(
    interaction_plot["PAIR"],
    interaction_plot["RECORD_COUNT"].astype(float),
    color="#4c78a8",
)
axis.set_title(f"{selected_interaction_a} × {selected_interaction_b}")
axis.set_xlabel("Failure-label records")
axis.set_ylabel("Observed pair")
plt.tight_layout()
plt.show()
"""
    ),
    markdown(
        """## 3. Null-pattern analysis and CSV exports

The first cell ranks nullable business columns and exports the ranking. The
second cell groups exact simultaneous-null combinations and exports those
patterns independently.
"""
    ),
    code(
        """null_count_expressions = [
    f'COUNT_IF({quote_identifier(column)} IS NULL) AS "M{index}"'
    for index, column in enumerate(LABEL_VARIABLES)
]
null_count_row = aggregate_to_pandas(
    "SELECT COUNT(*) AS TOTAL_ROWS,\\n" + ",\\n".join(null_count_expressions)
    + f"\\nFROM {LABEL_TABLE}"
).iloc[0]

total_rows = int(null_count_row["TOTAL_ROWS"])
null_column_summary = pd.DataFrame.from_records([
    {
        "VARIABLE": column,
        "NULL_ROWS": int(null_count_row[f"M{index}"]),
        "NULL_PERCENT": 100 * int(null_count_row[f"M{index}"]) / total_rows,
    }
    for index, column in enumerate(LABEL_VARIABLES)
    if int(null_count_row[f"M{index}"]) > 0
], columns=["VARIABLE", "NULL_ROWS", "NULL_PERCENT"]).sort_values(
    ["NULL_ROWS", "VARIABLE"], ascending=[False, True]
).reset_index(drop=True)

nullable_columns = null_column_summary["VARIABLE"].tolist()
display(null_column_summary)
null_column_export = EXPORT_DIR / "ssd_failure_labels_null_counts_by_column.csv"
null_column_summary.to_csv(null_column_export, index=False)
print(f"Exported null-column ranking to {null_column_export}")
"""
    ),
    code(
        """if not nullable_columns:
    null_pattern_details = pd.DataFrame([{
        "NULL_COLUMN_COUNT": 0,
        "NULL_COLUMNS": "[]",
        "RECORD_COUNT": total_rows,
    }])
else:
    null_array_items = [
        f"IFF({quote_identifier(column)} IS NULL, '{column}', NULL)"
        for column in nullable_columns
    ]
    null_pattern_details = aggregate_to_pandas(f'''
        WITH row_patterns AS (
            SELECT ARRAY_CONSTRUCT_COMPACT(
                {', '.join(null_array_items)}
            ) AS NULL_COLUMN_ARRAY
            FROM {LABEL_TABLE}
        ),
        pattern_counts AS (
            SELECT
                ARRAY_SIZE(NULL_COLUMN_ARRAY) AS NULL_COLUMN_COUNT,
                TO_JSON(NULL_COLUMN_ARRAY) AS NULL_COLUMNS,
                COUNT(*) AS RECORD_COUNT
            FROM row_patterns
            GROUP BY 1, 2
        )
        SELECT *
        FROM pattern_counts
        ORDER BY NULL_COLUMN_COUNT DESC, RECORD_COUNT DESC, NULL_COLUMNS
        LIMIT {NULL_PATTERN_DISPLAY_LIMIT}
    ''')

display(null_pattern_details)
null_pattern_export = EXPORT_DIR / "ssd_failure_labels_null_combinations.csv"
null_pattern_details.to_csv(null_pattern_export, index=False)
print(f"Exported null combinations to {null_pattern_export}")
"""
    ),
    markdown("## 4. Disk, model, and failure-date distributions"),
    markdown("### 4.1 Disk failure-label table"),
    code(
        """disk_distribution = aggregate_to_pandas(f'''
    SELECT
        DISK_ID,
        COUNT(*) AS FAILURE_LABEL_COUNT,
        COUNT(DISTINCT MODEL_CODE) AS MODEL_COUNT,
        MIN(FAILURE_AT) AS FIRST_FAILURE_AT,
        MAX(FAILURE_AT) AS LAST_FAILURE_AT
    FROM {LABEL_TABLE}
    GROUP BY DISK_ID
    ORDER BY FAILURE_LABEL_COUNT DESC, DISK_ID
    LIMIT 100
''')
display(disk_distribution)
"""
    ),
    markdown("### 4.2 Model table with MC1 emphasis"),
    code(
        """model_distribution = aggregate_to_pandas(f'''
    SELECT
        MODEL_CODE,
        IFF(UPPER(MODEL_CODE) = 'MC1', 'TARGET_MC1', 'OTHER_MODEL') AS MODEL_GROUP,
        COUNT(*) AS FAILURE_LABEL_COUNT,
        COUNT(DISTINCT DISK_ID) AS FAILED_DISK_COUNT,
        MIN(FAILURE_DATE) AS FIRST_FAILURE_DATE,
        MAX(FAILURE_DATE) AS LAST_FAILURE_DATE,
        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 6) AS RECORD_PERCENT
    FROM {LABEL_TABLE}
    GROUP BY MODEL_CODE
    ORDER BY IFF(UPPER(MODEL_CODE) = 'MC1', 0, 1), FAILURE_LABEL_COUNT DESC
''')
display(model_distribution)

mc1_labels = model_distribution[
    model_distribution["MODEL_CODE"].astype(str).str.upper() == "MC1"
]
if mc1_labels.empty:
    print("MC1 is not present in SSD_FAILURE_LABELS.")
else:
    display(mc1_labels)
"""
    ),
    markdown("### 4.3 Model failure-label graph"),
    code(
        """model_plot = model_distribution.head(30).sort_values("FAILURE_LABEL_COUNT")
colors = [
    "#d62728" if str(model).upper() == "MC1" else "#4c78a8"
    for model in model_plot["MODEL_CODE"]
]
_, axis = plt.subplots(figsize=(11, max(5, 0.32 * len(model_plot))))
axis.barh(
    model_plot["MODEL_CODE"].astype(str),
    model_plot["FAILURE_LABEL_COUNT"].astype(float),
    color=colors,
)
axis.set_title("SSD failure labels by model code (MC1 highlighted)")
axis.set_xlabel("Failure-label records")
axis.set_ylabel("Model code")
plt.tight_layout()
plt.show()
"""
    ),
    markdown("### 4.4 Failure timeline table and graph"),
    code(
        """failure_timeline = aggregate_to_pandas(f'''
    SELECT FAILURE_DATE, COUNT(*) AS FAILURE_LABEL_COUNT
    FROM {LABEL_TABLE}
    GROUP BY FAILURE_DATE
    ORDER BY FAILURE_DATE
''')
display(failure_timeline)

_, axis = plt.subplots(figsize=(13, 5))
axis.plot(
    pd.to_datetime(failure_timeline["FAILURE_DATE"]),
    failure_timeline["FAILURE_LABEL_COUNT"].astype(float),
    color="#d62728",
)
axis.set_title("SSD failure labels over time")
axis.set_xlabel("Failure date")
axis.set_ylabel("Failure-label records")
plt.tight_layout()
plt.show()
"""
    ),
    markdown("## 5. Session cleanup"),
    code("spark.stop()"),
]

#? Estructura final del .ipynb: celdas + kernel "pyspark-snowflake" (ver src/res/config/spark/kernels).
notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "PySpark 4 + Snowflake",
            "language": "python",
            "name": "pyspark-snowflake",
        },
        "language_info": {"name": "python", "version": "3.10"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

#? Escribe el notebook en disco (se sobreescribe en cada ejecucion del generador).
TARGET.parent.mkdir(parents=True, exist_ok=True)
TARGET.write_text(json.dumps(notebook, indent=1) + "\n", encoding="utf-8")
print(f"Wrote {TARGET} with {len(cells)} cells")
