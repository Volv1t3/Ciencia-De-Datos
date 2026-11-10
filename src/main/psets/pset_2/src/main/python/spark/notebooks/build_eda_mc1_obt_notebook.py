"""Generate the Spanish, Snowflake-backed MC1 OBT EDA notebook.

The generated notebook submits full-population work as Snowflake SQL through
the existing Spark connector. It never reads the complete OBT into local Spark.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from textwrap import dedent


TARGET = Path(__file__).resolve().parent / "datagrip" / "EDA_MC1_OBT_CloudComputingEnabled.ipynb"


def markdown(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": dedent(source).strip() + "\n"}


def code(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": dedent(source).strip() + "\n",
    }


cells: list[dict] = [
    markdown(
        """\
        # Análisis exploratorio de la OBT MC1

        Esta libreta analiza `OBT_MC1_RN` antes de seleccionar características o
        entrenar un clasificador de fallas en los próximos 30 días.

        Las consultas sobre la población completa se ejecutan **en Snowflake**
        mediante el conector Spark–Snowflake. El proceso local de Spark solo
        recibe resultados pequeños y acotados para tablas y figuras. No se
        descarga la OBT ni se usan transformaciones locales sobre sus filas.

        Hay eventos de falla registrados tanto en 2018 como en 2019. Se analiza
        su distribución por año; contar con eventos en ambos períodos no garantiza
        por sí solo que el registro de fallas sea exhaustivo.
        """
    ),
    markdown("## 0. Conexión, alcance y límites de ejecución"),
    code(
        '''\
        import os
        import re
        import sys
        from pathlib import Path

        import matplotlib.pyplot as plt
        import pandas as pd
        import pyspark

        from snowflake_io import create_spark_session, read_snowflake_query

        spark = create_spark_session("mc1-obt-eda")
        OBT_SCHEMA = "S_CDATOS_PSET2_OBT"
        OBT_NAME = "OBT_MC1_RN"
        DATABASE = os.environ["SNOWFLAKE_DATABASE"].strip()
        IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
        if not IDENTIFIER.fullmatch(DATABASE):
            raise ValueError("Nombre de base de datos no admitido por esta libreta")
        OBT_TABLE = f'"{DATABASE}"."{OBT_SCHEMA}"."{OBT_NAME}"'
        COLUMNS_TABLE = f'"{DATABASE}".INFORMATION_SCHEMA.COLUMNS'

        SMART_IDS = (
            1, 5, 9, 12, 170, 171, 172, 173, 174, 180, 183, 184,
            187, 188, 194, 195, 196, 197, 198, 199, 206, 211,
        )
        WINDOWS = (7, 14, 30)
        IDENTITY_COLUMNS = (
            "SMART_DAILY_KEY", "SSD_KEY", "DISK_ID", "MODEL_CODE",
            "OBSERVATION_DATE_KEY", "OBSERVATION_DATE",
        )
        LABEL_COLUMNS = (
            "FIRST_FAILURE_DATE", "LAST_SEEN_DATE", "DAYS_TO_FAILURE",
            "LABEL_STATUS", "TARGET_30D",
        )

        # Son límites de resultados, no tamaños de partición de la OBT.
        MAX_METADATA_ROWS = 1200
        MAX_SUMMARY_ROWS = 60
        MAX_PLOT_ROWS = 2000
        # Ejecución económica por defecto: solo R_5 y sin IQR masivo.
        # Active una familia individual o RUN_IN_DEPTH_ANALYSIS de forma deliberada.
        RUN_IN_DEPTH_ANALYSIS = False
        RUN_IQR_ANALYSIS = False
        RUN_R5_FEATURES = {
            "R_5": True,
            "R_5_MEAN_7D": False,
            "R_5_MEAN_14D": False,
            "R_5_MEAN_30D": False,
            "R_5_MEAN_SHIFT_7D": False,
            "R_5_MEAN_SHIFT_14D": False,
            "R_5_MEAN_SHIFT_30D": False,
        }
        RUN_SELECTED_FAMILY = False
        RUN_SELECTED_FAMILY_FIELDS = {
            "": True,
            "_MEAN_7D": False,
            "_MEAN_14D": False,
            "_MEAN_30D": False,
            "_MEAN_SHIFT_7D": False,
            "_MEAN_SHIFT_14D": False,
            "_MEAN_SHIFT_30D": False,
        }
        RUN_PAIR_ANALYSIS = False
        RUN_TRAJECTORY = False
        RUN_AGGREGATE_TRAJECTORY = False

        print("Versión de PySpark:", pyspark.__version__)
        print("Tabla de origen:", OBT_TABLE)
        print("Almacén configurado:", os.environ.get("SNOWFLAKE_WAREHOUSE", "[sin definir]"))
        print("La sesión Spark es local; las consultas de la OBT se envían a Snowflake.")
        '''
    ),
    markdown(
        """\
        ### 0.1 Contrato para recibir solo resultados pequeños

        `query_small` añade un límite exterior de seguridad y rechaza cualquier
        resultado que exceda el máximo declarado. Cada consulta de detalle
        incluye además su propio orden estable y límite. El límite **no**
        sustituye la agregación: todas las operaciones sobre millones de filas
        deben estar dentro del SQL enviado a Snowflake.
        """
    ),
    code(
        '''\
        def query_small(sql: str, *, max_rows: int = MAX_SUMMARY_ROWS) -> pd.DataFrame:
            if max_rows < 1 or max_rows > MAX_PLOT_ROWS:
                raise ValueError("Límite de resultados fuera del rango permitido")
            statement = sql.strip().rstrip(";")
            if not statement.upper().startswith(("SELECT", "WITH")) or ";" in statement:
                raise ValueError("Solo se admiten consultas SELECT de una sentencia")
            guarded = f"SELECT * FROM ({statement}) AS RESULTADO LIMIT {max_rows + 1}"
            result = read_snowflake_query(spark, guarded, schema=OBT_SCHEMA).toPandas()
            if len(result) > max_rows:
                raise RuntimeError(
                    f"Resultado demasiado grande: más de {max_rows} filas. "
                    "Agregue o reduzca los datos en Snowflake."
                )
            return result


        def quote_feature(column: str) -> str:
            if column not in FEATURE_NAMES:
                raise ValueError("La característica no pertenece al registro MC1")
            return '"' + column + '"'


        def show_table(frame: pd.DataFrame, *, empty_message: str) -> None:
            if frame.empty:
                print(empty_message)
            else:
                display(frame)


        def assert_zero_counts(frame: pd.DataFrame, *names: str) -> None:
            """Bloquea resultados si una validación indica fallos o devuelve NULL."""
            if len(frame) != 1:
                raise AssertionError(f"La validación esperaba una fila; recibió {len(frame)}")
            violations = {}
            for name in names:
                value = frame.iloc[0][name]
                if pd.isna(value):
                    violations[name] = "NULL (recuento ambiguo)"
                elif int(value) != 0:
                    violations[name] = int(value)
            if violations:
                raise AssertionError(f"Validaciones incumplidas: {violations}")
            print("Validaciones correctas:", ", ".join(names))
        '''
    ),
    markdown("### 0.2 Confirmación del almacén de cómputo de Snowflake"),
    code(
        '''\
        runtime = query_small("""
            SELECT CURRENT_DATABASE() AS BASE_DE_DATOS,
                   CURRENT_WAREHOUSE() AS ALMACEN_DE_COMPUTO,
                   CURRENT_ROLE() AS ROL,
                   CURRENT_TIMESTAMP() AS FECHA_DE_EJECUCION
        """, max_rows=1)
        display(runtime)
        print("Compruebe en Query History que las consultas extensas usen este almacén.")
        '''
    ),
    markdown("## 1. Salud estructural de la OBT"),
    markdown(
        """\
        ### 1.1 Inventario de columnas y tipos

        El contrato esperado tiene 847 columnas: seis de identidad, cinco de
        etiqueta y 836 características SMART (`22 × 2 × 19`). La consulta a
        `INFORMATION_SCHEMA` lee metadatos, no filas de telemetría.
        """
    ),
    code(
        '''\
        column_inventory = query_small(f"""
            SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE,
                   CHARACTER_MAXIMUM_LENGTH, ORDINAL_POSITION
            FROM {COLUMNS_TABLE}
            WHERE TABLE_SCHEMA = '{OBT_SCHEMA}' AND TABLE_NAME = '{OBT_NAME}'
            ORDER BY ORDINAL_POSITION
        """, max_rows=MAX_METADATA_ROWS)

        def expected_schema() -> dict[str, str]:
            expected = {
                "SMART_DAILY_KEY": "VARCHAR(128)",
                "SSD_KEY": "VARCHAR(128)",
                "DISK_ID": "NUMBER(38,0)",
                "MODEL_CODE": "VARCHAR(50331648)",
                "OBSERVATION_DATE_KEY": "NUMBER(8,0)",
                "OBSERVATION_DATE": "DATE",
                "FIRST_FAILURE_DATE": "DATE",
                "LAST_SEEN_DATE": "DATE",
                "DAYS_TO_FAILURE": "NUMBER(38,0)",
                "LABEL_STATUS": "VARCHAR(134217728)",
                "TARGET_30D": "NUMBER(38,0)",
            }
            for prefix in ("R", "N"):
                for smart_id in SMART_IDS:
                    root = f"{prefix}_{smart_id}"
                    expected[root] = "NUMBER(38,6)"
                    for window in WINDOWS:
                        suffix = f"{window}D"
                        expected[f"{root}_MEAN_{suffix}"] = "NUMBER(38,10)"
                        expected[f"{root}_COUNT_{suffix}"] = "NUMBER(38,0)"
                        expected[f"{root}_PREV_COUNT_{suffix}"] = "NUMBER(38,0)"
                        expected[f"{root}_MEAN_VALID_{suffix}"] = "NUMBER(38,0)"
                        expected[f"{root}_MEAN_SHIFT_{suffix}"] = "NUMBER(38,9)"
                        expected[f"{root}_MEAN_SHIFT_VALID_{suffix}"] = "NUMBER(38,0)"
            return expected


        def observed_type(row) -> str:
            kind = str(row.DATA_TYPE).upper()
            if kind in {"TEXT", "VARCHAR"}:
                return f"VARCHAR({int(row.CHARACTER_MAXIMUM_LENGTH)})"
            if kind in {"NUMBER", "DECIMAL", "NUMERIC"}:
                return f"NUMBER({int(row.NUMERIC_PRECISION)},{int(row.NUMERIC_SCALE)})"
            return kind


        EXPECTED_SCHEMA = expected_schema()
        observed = {row.COLUMN_NAME: observed_type(row) for row in column_inventory.itertuples()}
        missing = sorted(set(EXPECTED_SCHEMA) - set(observed))
        unexpected = sorted(set(observed) - set(EXPECTED_SCHEMA))
        mistyped = [
            (name, EXPECTED_SCHEMA[name], observed[name])
            for name in sorted(set(EXPECTED_SCHEMA) & set(observed))
            if EXPECTED_SCHEMA[name] != observed[name]
        ]
        schema_summary = pd.DataFrame([{
            "COLUMNAS_ESPERADAS": len(EXPECTED_SCHEMA),
            "COLUMNAS_OBSERVADAS": len(column_inventory),
            "COLUMNAS_FALTANTES": len(missing),
            "COLUMNAS_INESPERADAS": len(unexpected),
            "TIPOS_DIFERENTES": len(mistyped),
            "COLUMNAS_R": sum(name.startswith("R_") for name in observed),
            "COLUMNAS_N": sum(name.startswith("N_") for name in observed),
        }])
        display(schema_summary)
        if missing or unexpected or mistyped:
            display(pd.DataFrame(mistyped, columns=["COLUMNA", "TIPO_ESPERADO", "TIPO_OBSERVADO"]))
            print("Faltantes:", missing)
            print("Inesperadas:", unexpected)
            raise RuntimeError("El esquema vivo no coincide con el contrato de la OBT MC1")
        FEATURE_NAMES = set(observed) - set(IDENTITY_COLUMNS) - set(LABEL_COLUMNS)
        print("El esquema coincide con las 847 columnas esperadas.")
        '''
    ),
    markdown("### 1.2 Población, fechas y modelo"),
    code(
        '''\
        population = query_small(f"""
            SELECT COUNT(*) AS OBSERVACIONES,
                   COUNT(DISTINCT SSD_KEY) AS SSD_DISTINTOS,
                   MIN(OBSERVATION_DATE) AS FECHA_MINIMA,
                   MAX(OBSERVATION_DATE) AS FECHA_MAXIMA,
                   COALESCE(COUNT_IF(MODEL_CODE IS NULL OR MODEL_CODE <> 'MC1'), 0)
                       AS MODELOS_INESPERADOS,
                   COALESCE(COUNT_IF(OBSERVATION_DATE < DATE '2018-01-01'
                            OR OBSERVATION_DATE > DATE '2019-12-31'), 0)
                       AS FECHAS_FUERA_DE_RANGO,
                   COALESCE(COUNT_IF(SMART_DAILY_KEY IS NULL OR SSD_KEY IS NULL
                            OR OBSERVATION_DATE IS NULL), 0) AS CLAVES_INCOMPLETAS
            FROM {OBT_TABLE}
        """, max_rows=1)
        display(population)
        print("Referencia Gold histórica: 115 159 020 observaciones y 199 655 SSD.")
        print("La comparación es informativa; no sustituye el recuento de la OBT actual.")
        '''
    ),
    markdown("### 1.3 Unicidad de la clave técnica"),
    code(
        '''\
        technical_duplicates = query_small(f"""
            WITH repeated AS (
                SELECT SMART_DAILY_KEY, COUNT(*) AS ROWS_PER_KEY
                FROM {OBT_TABLE}
                GROUP BY SMART_DAILY_KEY
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) AS CLAVES_REPETIDAS,
                   COALESCE(SUM(ROWS_PER_KEY - 1), 0) AS FILAS_EXCEDENTES
            FROM repeated
        """, max_rows=1)
        display(technical_duplicates)
        '''
    ),
    markdown("### 1.4 Unicidad de SSD y fecha"),
    code(
        '''\
        daily_duplicates = query_small(f"""
            WITH repeated AS (
                SELECT SSD_KEY, OBSERVATION_DATE, COUNT(*) AS ROWS_PER_DAY
                FROM {OBT_TABLE}
                GROUP BY SSD_KEY, OBSERVATION_DATE
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) AS PARES_REPETIDOS,
                   COALESCE(SUM(ROWS_PER_DAY - 1), 0) AS FILAS_EXCEDENTES
            FROM repeated
        """, max_rows=1)
        display(daily_duplicates)
        '''
    ),
    markdown("### 1.5 Muestras acotadas de duplicados, solo si existen"),
    code(
        '''\
        if int(technical_duplicates.iloc[0]["CLAVES_REPETIDAS"]) > 0:
            display(query_small(f"""
                SELECT SMART_DAILY_KEY, COUNT(*) AS REPETICIONES
                FROM {OBT_TABLE}
                GROUP BY SMART_DAILY_KEY HAVING COUNT(*) > 1
                ORDER BY REPETICIONES DESC, SMART_DAILY_KEY LIMIT 20
            """, max_rows=20))
        if int(daily_duplicates.iloc[0]["PARES_REPETIDOS"]) > 0:
            display(query_small(f"""
                SELECT SSD_KEY, OBSERVATION_DATE, COUNT(*) AS REPETICIONES
                FROM {OBT_TABLE}
                GROUP BY SSD_KEY, OBSERVATION_DATE HAVING COUNT(*) > 1
                ORDER BY REPETICIONES DESC, SSD_KEY, OBSERVATION_DATE LIMIT 20
            """, max_rows=20))
        if not int(technical_duplicates.iloc[0]["CLAVES_REPETIDAS"]) and not int(daily_duplicates.iloc[0]["PARES_REPETIDOS"]):
            print("No se encontraron claves diarias duplicadas.")
        '''
    ),
    markdown("### 1.6 Compuerta de validación estructural"),
    code(
        '''\
        if population.empty or int(population.iloc[0]["OBSERVACIONES"]) <= 0:
            raise AssertionError("La OBT no tiene observaciones para analizar")
        assert_zero_counts(
            population, "MODELOS_INESPERADOS", "FECHAS_FUERA_DE_RANGO", "CLAVES_INCOMPLETAS"
        )
        assert_zero_counts(technical_duplicates, "CLAVES_REPETIDAS", "FILAS_EXCEDENTES")
        assert_zero_counts(daily_duplicates, "PARES_REPETIDOS", "FILAS_EXCEDENTES")
        print("Integridad estructural validada; se permite continuar con las etiquetas.")
        '''
    ),
    markdown(
        """\
        **Interpretación de la sección 1.** Si aparecen claves duplicadas, tipos
        inesperados, fechas fuera del período o modelos diferentes de MC1, se debe
        investigar el origen antes de interpretar tasas o distribuciones. Esta
        libreta no elimina ni corrige filas.
        """
    ),
]

cells.extend([
    markdown("## 2. Distribución y consistencia de la variable objetivo"),
    markdown("### 2.1 Estados de etiqueta y SSD distintos"),
    code(
        '''\
        label_distribution = query_small(f"""
            WITH grouped AS (
                SELECT LABEL_STATUS,
                       COUNT(*) AS OBSERVACIONES,
                       COUNT(DISTINCT SSD_KEY) AS SSD_DISTINTOS
                FROM {OBT_TABLE}
                GROUP BY LABEL_STATUS
            )
            SELECT LABEL_STATUS, OBSERVACIONES, SSD_DISTINTOS,
                   ROUND(100.0 * OBSERVACIONES / NULLIF(SUM(OBSERVACIONES) OVER (), 0), 6)
                       AS PORCENTAJE_OBSERVACIONES
            FROM grouped
            ORDER BY OBSERVACIONES DESC, LABEL_STATUS
        """, max_rows=20)
        display(label_distribution)
        print("Un SSD puede aparecer en varios estados; no sume SSD_DISTINTOS entre estados.")
        allowed_statuses = {
            "POSITIVE", "NEGATIVE", "CENSORED", "SAME_DAY_FAILURE", "POST_FAILURE"
        }
        found_statuses = set(label_distribution["LABEL_STATUS"].dropna().tolist())
        if label_distribution.empty or label_distribution["LABEL_STATUS"].isna().any() or not found_statuses <= allowed_statuses:
            raise AssertionError(
                f"Estados de etiqueta no válidos: {found_statuses - allowed_statuses}"
            )
        '''
    ),
    markdown("### 2.2 Figura 1: composición de las etiquetas"),
    code(
        '''\
        if not label_distribution.empty:
            figure, axis = plt.subplots(figsize=(10, 4))
            axis.bar(label_distribution["LABEL_STATUS"].fillna("SIN_ESTADO"),
                     label_distribution["OBSERVACIONES"].astype(float))
            axis.set_title("Composición de los estados de etiqueta")
            axis.set_xlabel("Estado de etiqueta")
            axis.set_ylabel("Observaciones diarias")
            axis.set_yscale("symlog", linthresh=1)
            for bar in axis.patches:
                axis.annotate(f"{int(bar.get_height()):,}",
                              (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                              ha="center", va="bottom", fontsize=8, rotation=30)
            axis.tick_params(axis="x", rotation=25)
            plt.tight_layout()
            plt.show()
        '''
    ),
    markdown("### 2.3 Población elegible para aprendizaje supervisado"),
    code(
        '''\
        training_population = query_small(f"""
            SELECT COALESCE(COUNT_IF(LABEL_STATUS = 'POSITIVE'), 0) AS POSITIVAS,
                   COALESCE(COUNT_IF(LABEL_STATUS = 'NEGATIVE'), 0) AS NEGATIVAS,
                   COALESCE(COUNT_IF(LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')), 0) AS ELEGIBLES,
                   COUNT(DISTINCT IFF(LABEL_STATUS = 'POSITIVE', SSD_KEY, NULL))
                       AS SSD_CON_POSITIVOS,
                   COUNT(DISTINCT IFF(FIRST_FAILURE_DATE IS NOT NULL, SSD_KEY, NULL))
                       AS SSD_CON_PRIMERA_FALLA_REGISTRADA,
                   ROUND(
                       COALESCE(COUNT_IF(LABEL_STATUS = 'POSITIVE'), 0) /
                       NULLIF(COALESCE(COUNT_IF(LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')), 0), 0), 8
                   ) AS TASA_POSITIVA,
                   ROUND(
                       COALESCE(COUNT_IF(LABEL_STATUS = 'NEGATIVE'), 0) /
                       NULLIF(COALESCE(COUNT_IF(LABEL_STATUS = 'POSITIVE'), 0), 0), 4
                   ) AS RAZON_NEGATIVAS_POR_POSITIVA
            FROM {OBT_TABLE}
        """, max_rows=1)
        display(training_population)
        print("La tasa usa observaciones, no eventos independientes de falla.")
        print("La OBT conserva la primera falla por SSD; no cuenta eventos posteriores.")
        '''
    ),
    markdown("### 2.4 Estados de etiqueta por año"),
    code(
        '''\
        labels_by_year = query_small(f"""
            SELECT YEAR(OBSERVATION_DATE) AS ANIO, LABEL_STATUS,
                   COUNT(*) AS OBSERVACIONES,
                   COUNT(DISTINCT SSD_KEY) AS SSD_DISTINTOS
            FROM {OBT_TABLE}
            GROUP BY YEAR(OBSERVATION_DATE), LABEL_STATUS
            ORDER BY ANIO, LABEL_STATUS
        """, max_rows=50)
        display(labels_by_year)
        print("Hay registros de falla para 2018 y 2019 según la fuente del proyecto.")
        print("Revise los positivos y los SSD con fallas por año; la presencia de eventos no implica exhaustividad.")
        '''
    ),
    markdown("### 2.5 Figura 2: distribución anual de las etiquetas"),
    code(
        '''\
        if not labels_by_year.empty:
            chart_rows = labels_by_year.copy()
            chart_rows["OBSERVACIONES"] = pd.to_numeric(chart_rows["OBSERVACIONES"])
            plot_data = chart_rows.pivot_table(
                index="ANIO", columns="LABEL_STATUS", values="OBSERVACIONES", fill_value=0
            )
            axis = plot_data.plot(kind="bar", figsize=(10, 4), logy=True)
            axis.set_title("Distribución anual de los estados de etiqueta")
            axis.set_xlabel("Año de observación")
            axis.set_ylabel("Observaciones diarias; escala logarítmica")
            axis.legend(title="Estado de etiqueta")
            for bar in axis.patches:
                if bar.get_height() > 0:
                    axis.annotate(f"{int(bar.get_height()):,}",
                                  (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                                  ha="center", va="bottom", fontsize=7, rotation=90)
            plt.tight_layout()
            plt.show()
        '''
    ),
    markdown(
        """\
        ### 2.6 Verificación de las reglas de etiquetado

        Se recomputan el estado, el objetivo y la distancia a la primera falla
        con la prioridad usada para construir la OBT. Esta verificación comprueba
        la coherencia interna, **no** la completitud externa del archivo de
        fallas. No se transfieren filas individuales.
        """
    ),
    code(
        '''\
        label_consistency = query_small(f"""
            WITH expected AS (
                SELECT LABEL_STATUS, TARGET_30D, DAYS_TO_FAILURE,
                       DATEDIFF('day', OBSERVATION_DATE, FIRST_FAILURE_DATE)
                           AS EXPECTED_DAYS,
                       CASE
                           WHEN FIRST_FAILURE_DATE IS NOT NULL
                                AND OBSERVATION_DATE > FIRST_FAILURE_DATE
                               THEN 'POST_FAILURE'
                           WHEN FIRST_FAILURE_DATE IS NOT NULL
                                AND OBSERVATION_DATE = FIRST_FAILURE_DATE
                               THEN 'SAME_DAY_FAILURE'
                           WHEN DATEDIFF('day', OBSERVATION_DATE, FIRST_FAILURE_DATE)
                                BETWEEN 1 AND 30 THEN 'POSITIVE'
                           WHEN LAST_SEEN_DATE >= DATEADD('day', 30, OBSERVATION_DATE)
                               THEN 'NEGATIVE'
                           ELSE 'CENSORED'
                       END AS EXPECTED_STATUS
                FROM {OBT_TABLE}
            )
            SELECT COALESCE(COUNT_IF(LABEL_STATUS IS DISTINCT FROM EXPECTED_STATUS), 0)
                       AS ESTADOS_INCONSISTENTES,
                   COALESCE(COUNT_IF(TARGET_30D IS DISTINCT FROM
                       CASE EXPECTED_STATUS WHEN 'POSITIVE' THEN 1
                            WHEN 'NEGATIVE' THEN 0 ELSE NULL END), 0)
                       AS OBJETIVOS_INCONSISTENTES,
                   COALESCE(COUNT_IF(DAYS_TO_FAILURE IS DISTINCT FROM EXPECTED_DAYS), 0)
                       AS DISTANCIAS_INCONSISTENTES
            FROM expected
        """, max_rows=1)
        display(label_consistency)
        assert_zero_counts(
            label_consistency,
            "ESTADOS_INCONSISTENTES", "OBJETIVOS_INCONSISTENTES", "DISTANCIAS_INCONSISTENTES"
        )
        '''
    ),
    markdown(
        """\
        **Interpretación de la sección 2.** Distinga observaciones positivas,
        SSD con positivos y eventos de falla. Una razón elevada de negativas por
        positiva indica desbalance, no autoriza remuestreo automático. La
        presencia de eventos en 2018 y 2019 no demuestra que todas las fallas
        estén registradas; reporte los resultados por año.
        """
    ),
    markdown("## 3. Secuencias previas a la falla"),
    markdown("### 3.1 Positivos por distancia temporal a la falla"),
    code(
        '''\
        positives_by_distance = query_small(f"""
            SELECT DAYS_TO_FAILURE AS DIAS_HASTA_LA_FALLA,
                   COUNT(*) AS OBSERVACIONES_POSITIVAS,
                   COUNT(DISTINCT SSD_KEY) AS SSD_DISTINTOS
            FROM {OBT_TABLE}
            WHERE LABEL_STATUS = 'POSITIVE' AND DAYS_TO_FAILURE BETWEEN 1 AND 30
            GROUP BY DAYS_TO_FAILURE
            ORDER BY DIAS_HASTA_LA_FALLA DESC
        """, max_rows=30)
        display(positives_by_distance)
        '''
    ),
    markdown("### 3.2 Figura 3: cobertura de los treinta días previos"),
    code(
        '''\
        if not positives_by_distance.empty:
            figure, axis = plt.subplots(figsize=(10, 4))
            axis.plot(positives_by_distance["DIAS_HASTA_LA_FALLA"].astype(int),
                      positives_by_distance["OBSERVACIONES_POSITIVAS"].astype(float),
                      marker="o")
            axis.invert_xaxis()
            axis.set_title("Cobertura de observaciones antes de la primera falla")
            axis.set_xlabel("Días restantes hasta la falla")
            axis.set_ylabel("Observaciones positivas")
            plt.tight_layout()
            plt.show()
        '''
    ),
    markdown("### 3.3 Días positivos disponibles por SSD"),
    code(
        '''\
        positive_days_histogram = query_small(f"""
            WITH per_ssd AS (
                SELECT SSD_KEY,
                       COUNT(DISTINCT OBSERVATION_DATE) AS POSITIVE_DAYS
                FROM {OBT_TABLE}
                WHERE LABEL_STATUS = 'POSITIVE'
                GROUP BY SSD_KEY
            )
            SELECT POSITIVE_DAYS AS DIAS_POSITIVOS, COUNT(*) AS SSD
            FROM per_ssd
            GROUP BY POSITIVE_DAYS
            ORDER BY DIAS_POSITIVOS
        """, max_rows=31)
        display(positive_days_histogram)
        '''
    ),
    markdown("### 3.4 Resumen estadístico de días positivos"),
    code(
        '''\
        positive_days_summary = query_small(f"""
            WITH per_ssd AS (
                SELECT SSD_KEY,
                       COUNT(DISTINCT OBSERVATION_DATE) AS POSITIVE_DAYS
                FROM {OBT_TABLE}
                WHERE LABEL_STATUS = 'POSITIVE'
                GROUP BY SSD_KEY
            )
            SELECT COUNT(*) AS SSD_CON_POSITIVOS,
                   MIN(POSITIVE_DAYS) AS MINIMO,
                   APPROX_PERCENTILE(POSITIVE_DAYS, 0.25) AS CUARTIL_1_APROX,
                   APPROX_PERCENTILE(POSITIVE_DAYS, 0.50) AS MEDIANA_APROX,
                   AVG(POSITIVE_DAYS::DOUBLE) AS MEDIA,
                   APPROX_PERCENTILE(POSITIVE_DAYS, 0.75) AS CUARTIL_3_APROX,
                   MAX(POSITIVE_DAYS) AS MAXIMO,
                   COALESCE(COUNT_IF(POSITIVE_DAYS >= 7), 0) AS SSD_CON_7_DIAS,
                   COALESCE(COUNT_IF(POSITIVE_DAYS >= 14), 0) AS SSD_CON_14_DIAS,
                   COALESCE(COUNT_IF(POSITIVE_DAYS >= 21), 0) AS SSD_CON_21_DIAS
            FROM per_ssd
        """, max_rows=1)
        display(positive_days_summary)
        '''
    ),
    markdown("### 3.5 Figura 4: historial positivo por SSD"),
    code(
        '''\
        if not positive_days_histogram.empty:
            figure, axis = plt.subplots(figsize=(10, 4))
            axis.bar(positive_days_histogram["DIAS_POSITIVOS"].astype(int),
                     positive_days_histogram["SSD"].astype(float))
            axis.set_title("Días con observaciones positivas por SSD")
            axis.set_xlabel("Número de días positivos")
            axis.set_ylabel("Número de SSD")
            plt.tight_layout()
            plt.show()
        '''
    ),
    markdown(
        """\
        **Interpretación de la sección 3.** La presencia de registros a varias
        distancias confirma cobertura temporal, pero no demuestra por sí sola
        que los valores SMART cambien antes de fallar. La completitud de las
        trayectorias debe evaluarse por SSD, no solo por el total de filas.
        """
    ),
])


cells.extend([
    markdown("## 4. Comparación inicial entre SMART y la falla"),
    markdown(
        """\
        **Control de costo.** En ejecución normal solo se analiza `R_5`.
        El resto de las características están separadas por celda y desactivadas
        por defecto. Configure `RUN_R5_FEATURES`, `RUN_SELECTED_FAMILY_FIELDS`,
        `RUN_IN_DEPTH_ANALYSIS` y `RUN_IQR_ANALYSIS` al inicio de la libreta.
        Cada resumen/IQR habilitado puede recorrer millones de observaciones;
        revise el costo en Query History antes de aumentar el alcance.
        """
    ),
    markdown(
        """\
        ### 4.1 Registro MC1 y selector numérico

        El primer menú contiene únicamente mediciones, medias y desplazamientos
        presentes en la OBT. Sirve para elegir dos campos en la comparación
        SMART–SMART. El segundo menú elige una familia completa (`R` o `N` más
        un ID SMART) para compararla con `TARGET_30D`. Los conteos y banderas de
        validez quedan fuera de los diagramas de caja e IQR.
        """
    ),
    code(
        '''\
        menu_rows = []
        for prefix in ("R", "N"):
            for smart_id in SMART_IDS:
                root = f"{prefix}_{smart_id}"
                menu_rows.append((root, prefix, smart_id, "valor diario", "—"))
                for window in WINDOWS:
                    menu_rows.append((f"{root}_MEAN_{window}D", prefix, smart_id,
                                      "media móvil", window))
                    menu_rows.append((f"{root}_MEAN_SHIFT_{window}D", prefix, smart_id,
                                      "cambio de medias", window))
        menu_rows = [row for row in menu_rows if row[0] in FEATURE_NAMES]
        feature_menu = pd.DataFrame(
            menu_rows,
            columns=["COLUMNA", "REPRESENTACIÓN", "ATRIBUTO_SMART", "TIPO", "VENTANA_DÍAS"],
        )
        feature_menu.insert(0, "NÚMERO", range(1, len(feature_menu) + 1))
        display(feature_menu)
        print("El menú se construye a partir del esquema validado, sin leer filas de la OBT.")

        def feature_from_number(number: int) -> str:
            if not isinstance(number, int) or not 1 <= number <= len(feature_menu):
                raise ValueError(f"Seleccione un número entre 1 y {len(feature_menu)}")
            return str(feature_menu.iloc[number - 1]["COLUMNA"])

        def ask_feature_number(prompt: str) -> int:
            while True:
                try:
                    number = int(input(prompt).strip())
                    feature_from_number(number)
                    return number
                except ValueError as error:
                    print(error)
        '''
    ),
    markdown("### 4.1.1 Menú de familias SMART para comparar con el objetivo"),
    code(
        '''\
        family_menu = pd.DataFrame([
            (prefix, smart_id, f"{prefix}_{smart_id}")
            for prefix in ("R", "N") for smart_id in SMART_IDS
        ], columns=["REPRESENTACIÓN", "ATRIBUTO_SMART", "FAMILIA"])
        family_menu.insert(0, "NÚMERO", range(1, len(family_menu) + 1))
        display(family_menu)

        def family_from_number(number: int) -> str:
            if not isinstance(number, int) or not 1 <= number <= len(family_menu):
                raise ValueError(f"Seleccione un número entre 1 y {len(family_menu)}")
            return str(family_menu.iloc[number - 1]["FAMILIA"])

        def ask_family_number() -> int:
            while True:
                try:
                    number = int(input("Número de familia SMART para comparar con la falla: ").strip())
                    family_from_number(number)
                    return number
                except ValueError as error:
                    print(error)
        '''
    ),
    markdown("### 4.2 Funciones compartidas para consultas acotadas"),
    code(
        '''\
        def feature_target_summary(feature: str) -> pd.DataFrame:
            identifier = quote_feature(feature)
            return query_small(f"""
                SELECT YEAR(OBSERVATION_DATE) AS ANIO,
                       TARGET_30D AS OBJETIVO,
                       COUNT(*) AS OBSERVACIONES_ELEGIBLES,
                       COUNT({identifier}) AS OBSERVACIONES_UTILIZABLES,
                       COALESCE(COUNT_IF({identifier} IS NULL), 0) AS VALORES_NULOS,
                       MIN({identifier}) AS MINIMO,
                       APPROX_PERCENTILE({identifier}::DOUBLE, 0.25) AS Q1_APROX,
                       AVG({identifier}::DOUBLE) AS MEDIA_APROX,
                       APPROX_PERCENTILE({identifier}::DOUBLE, 0.50) AS MEDIANA_APROX,
                       APPROX_PERCENTILE({identifier}::DOUBLE, 0.75) AS Q3_APROX,
                       MAX({identifier}) AS MAXIMO,
                       STDDEV_SAMP({identifier}::DOUBLE) AS DESVIACION_APROX
                FROM {OBT_TABLE}
                WHERE LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')
                GROUP BY YEAR(OBSERVATION_DATE), TARGET_30D
                ORDER BY ANIO, OBJETIVO
            """, max_rows=10)


        def feature_iqr_summary(feature: str) -> pd.DataFrame:
            identifier = quote_feature(feature)
            return query_small(f"""
                WITH bounds AS (
                    SELECT YEAR(OBSERVATION_DATE) AS ANIO,
                           APPROX_PERCENTILE({identifier}::DOUBLE, 0.25) AS Q1,
                           APPROX_PERCENTILE({identifier}::DOUBLE, 0.75) AS Q3
                    FROM {OBT_TABLE}
                    WHERE LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')
                    GROUP BY YEAR(OBSERVATION_DATE)
                )
                SELECT b.ANIO, b.Q1 AS Q1_APROX, b.Q3 AS Q3_APROX,
                       b.Q3 - b.Q1 AS IQR_APROX,
                       b.Q1 - 1.5 * (b.Q3 - b.Q1) AS LIMITE_INFERIOR,
                       b.Q3 + 1.5 * (b.Q3 - b.Q1) AS LIMITE_SUPERIOR,
                       COUNT({identifier}) AS VALORES_UTILIZABLES,
                       COALESCE(COUNT_IF({identifier}::DOUBLE < b.Q1 - 1.5 * (b.Q3 - b.Q1)
                                OR {identifier}::DOUBLE > b.Q3 + 1.5 * (b.Q3 - b.Q1)), 0)
                           AS VALORES_EXTREMOS
                FROM {OBT_TABLE} AS s JOIN bounds AS b
                  ON YEAR(s.OBSERVATION_DATE) = b.ANIO
                WHERE s.LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')
                GROUP BY b.ANIO, b.Q1, b.Q3
                ORDER BY b.ANIO
            """, max_rows=5)


        def draw_feature_boxplot(summary: pd.DataFrame, feature: str) -> None:
            if summary.empty:
                print("No hay grupos elegibles para esta característica.")
                return
            boxes = []
            for row in summary.itertuples():
                if int(row.OBSERVACIONES_UTILIZABLES) == 0:
                    continue
                boxes.append({
                    "label": f"{int(row.ANIO)}: " + (
                        "falla en 30 días" if int(row.OBJETIVO) == 1
                        else "sin falla registrada"
                    ),
                    "whislo": float(row.MINIMO),
                    "q1": float(row.Q1_APROX),
                    "med": float(row.MEDIANA_APROX),
                    "q3": float(row.Q3_APROX),
                    "whishi": float(row.MAXIMO),
                    "fliers": [],
                })
            if not boxes:
                print("No hay valores no nulos para dibujar.")
                return
            figure, axis = plt.subplots(figsize=(9, 3))
            axis.bxp(boxes, vert=False, showfliers=False)
            axis.set_title(f"Distribución aproximada de {feature} según la falla")
            axis.set_xlabel(feature)
            plt.tight_layout()
            plt.show()


        print("Medias, desviaciones y cuantiles son aproximados; mínimos y máximos son exactos.")
        '''
    ),
])


def add_feature_block(feature: str, ordinal: str, result_name: str) -> None:
    """Generate independent notebook cells, never a runtime feature loop."""
    cells.extend([
        markdown(
            f"""\
            ### {ordinal} {feature}: comparación con el objetivo

            Este bloque analiza exclusivamente `{feature}` entre observaciones
            `POSITIVE` y `NEGATIVE`. El recuento elegible y el recuento no nulo
            se presentan por separado; no se eliminan datos de la OBT.
            Se ejecuta solamente si se activa su entrada en `RUN_R5_FEATURES`
            o si `RUN_IN_DEPTH_ANALYSIS=True`. Su IQR necesita además `RUN_IQR_ANALYSIS=True`.
            """
        ),
        code(
            '''\
            if RUN_IN_DEPTH_ANALYSIS or RUN_R5_FEATURES["__FEATURE__"]:
                __RESULT__ = feature_target_summary("__FEATURE__")
                display(__RESULT__)
                print("Población: estados positivo/negativo, resumidos por año.")
            else:
                __RESULT__ = pd.DataFrame()
                print("Análisis desactivado: habilite RUN_R5_FEATURES o RUN_IN_DEPTH_ANALYSIS.")
            '''.replace("__RESULT__", result_name).replace("__FEATURE__", feature)
        ),
        markdown(f"#### {ordinal}.1 Figura: distribución de {feature} por objetivo"),
        code(
            '''\
            if not __RESULT__.empty:
                draw_feature_boxplot(__RESULT__, "__FEATURE__")
            else:
                print("Figura omitida: el análisis de esta característica está desactivado.")
            '''.replace("__RESULT__", result_name).replace("__FEATURE__", feature)
        ),
        markdown(f"#### {ordinal}.2 Valores extremos de {feature} mediante IQR"),
        code(
            '''\
            if RUN_IQR_ANALYSIS and (RUN_IN_DEPTH_ANALYSIS or RUN_R5_FEATURES["__FEATURE__"]):
                display(feature_iqr_summary("__FEATURE__"))
                print("Si IQR = 0, interprete el número de extremos con cautela.")
                print("Los valores extremos no se borran: pueden anticipar una falla real.")
            else:
                print("IQR omitido: active RUN_IQR_ANALYSIS y la característica elegida.")
            '''.replace("__FEATURE__", feature)
        ),
        markdown(
            f"""\
            **Interpretación de `{feature}`.** Después de ejecutar el bloque,
            compare medianas, cuartiles, tamaños de grupo y valores utilizables.
            Una diferencia visual no demuestra causalidad ni capacidad predictiva.
            """
        ),
    ])


add_feature_block("R_5", "4.3", "r5_summary")
add_feature_block("R_5_MEAN_7D", "4.4", "r5_mean7_summary")
add_feature_block("R_5_MEAN_14D", "4.5", "r5_mean14_summary")
add_feature_block("R_5_MEAN_30D", "4.6", "r5_mean30_summary")
add_feature_block("R_5_MEAN_SHIFT_7D", "4.7", "r5_shift7_summary")
add_feature_block("R_5_MEAN_SHIFT_14D", "4.8", "r5_shift14_summary")
add_feature_block("R_5_MEAN_SHIFT_30D", "4.9", "r5_shift30_summary")

cells.extend([
    markdown(
        """\
        ### 4.10 Familia SMART alternativa elegida con el menú

        Active `RUN_SELECTED_FAMILY` en la configuración e introduzca el número
        del menú de **familias**. Por defecto se ejecuta solo su valor diario;
        active entradas individuales en `RUN_SELECTED_FAMILY_FIELDS` para las
        medias/cambios adicionales, o use `RUN_IN_DEPTH_ANALYSIS` para todos.
        Los IQR de esa familia requieren además `RUN_IQR_ANALYSIS=True`.
        Cada campo tiene su propia tabla, figura e IQR.
        """
    ),
    code(
        '''\
        default_family_number = int(
            family_menu.loc[family_menu["FAMILIA"] == "R_5", "NÚMERO"].iloc[0]
        )
        SELECTED_FAMILY_NUMBER = (
            ask_family_number() if RUN_SELECTED_FAMILY else default_family_number
        )
        selected_family = family_from_number(SELECTED_FAMILY_NUMBER)

        def selected_family_feature(suffix: str) -> str:
            if suffix not in ("", "_MEAN_7D", "_MEAN_14D", "_MEAN_30D",
                              "_MEAN_SHIFT_7D", "_MEAN_SHIFT_14D", "_MEAN_SHIFT_30D"):
                raise ValueError("Sufijo SMART no admitido")
            feature = selected_family + suffix
            if feature not in FEATURE_NAMES:
                raise ValueError(f"La OBT no contiene {feature}")
            return feature

        print("Familia elegida:", selected_family)
        print("RUN_SELECTED_FAMILY ejecuta el campo diario; active otros campos por separado.")
        '''
    ),
])


def add_selected_family_block(suffix: str, ordinal: str, title: str, result_name: str) -> None:
    """Emit separate notebook cells for one selected-family field."""
    cells.extend([
        markdown(f"### {ordinal} Familia elegida: {title}"),
        code(
            '''\
            if RUN_SELECTED_FAMILY and (RUN_IN_DEPTH_ANALYSIS or RUN_SELECTED_FAMILY_FIELDS["__SUFFIX__"]):
                __FEATURE_NAME__ = selected_family_feature("__SUFFIX__")
                __RESULT__ = feature_target_summary(__FEATURE_NAME__)
                display(__RESULT__)
                print("Año y objetivo se mantienen separados en la tabla.")
            else:
                print("Bloque de familia optativo sin ejecutar.")
            '''.replace("__SUFFIX__", suffix)
               .replace("__FEATURE_NAME__", result_name + "_feature")
               .replace("__RESULT__", result_name)
        ),
        markdown(f"#### {ordinal}.1 Figura: {title}"),
        code(
            '''\
            if RUN_SELECTED_FAMILY and (RUN_IN_DEPTH_ANALYSIS or RUN_SELECTED_FAMILY_FIELDS["__SUFFIX__"]):
                draw_feature_boxplot(__RESULT__, __FEATURE_NAME__)
            else:
                print("Bloque de familia optativo sin ejecutar.")
            '''.replace("__RESULT__", result_name)
               .replace("__FEATURE_NAME__", result_name + "_feature").replace("__SUFFIX__", suffix)
        ),
        markdown(f"#### {ordinal}.2 Valores extremos: {title}"),
        code(
            '''\
            if RUN_SELECTED_FAMILY and (RUN_IN_DEPTH_ANALYSIS or RUN_SELECTED_FAMILY_FIELDS["__SUFFIX__"]):
                if RUN_IQR_ANALYSIS:
                    display(feature_iqr_summary(__FEATURE_NAME__))
                else:
                    print("IQR optativo desactivado (RUN_IQR_ANALYSIS=False).")
            else:
                print("Bloque de familia optativo sin ejecutar.")
            '''.replace("__FEATURE_NAME__", result_name + "_feature").replace("__SUFFIX__", suffix)
        ),
        markdown(
            f"**Interpretación.** Examine por separado `{title}`: compare las "
            "clases y los años, observe la cobertura no nula y no atribuya "
            "causalidad a una diferencia descriptiva."
        ),
    ])


add_selected_family_block("", "4.10.1", "valor diario", "family_current_summary")
add_selected_family_block("_MEAN_7D", "4.10.2", "media de 7 días", "family_mean7_summary")
add_selected_family_block("_MEAN_14D", "4.10.3", "media de 14 días", "family_mean14_summary")
add_selected_family_block("_MEAN_30D", "4.10.4", "media de 30 días", "family_mean30_summary")
add_selected_family_block("_MEAN_SHIFT_7D", "4.10.5", "cambio de media de 7 días", "family_shift7_summary")
add_selected_family_block("_MEAN_SHIFT_14D", "4.10.6", "cambio de media de 14 días", "family_shift14_summary")
add_selected_family_block("_MEAN_SHIFT_30D", "4.10.7", "cambio de media de 30 días", "family_shift30_summary")

cells.extend([
    markdown(
        """\
        ### 4.11 Comparación optativa de dos características SMART

        Elija **dos números diferentes** del menú anterior. Los resultados
        cuantifican asociación lineal entre mediciones observadas en la misma
        fila; no evalúan causalidad ni sustituyen la comparación con el objetivo.
        Active `RUN_PAIR_ANALYSIS` para ejecutar los bloques de consulta y figura.
        """
    ),
    code(
        '''\
        default_a_number = int(
            feature_menu.loc[feature_menu["COLUMNA"] == "R_5", "NÚMERO"].iloc[0]
        )
        default_b_number = int(
            feature_menu.loc[feature_menu["COLUMNA"] == "R_5_MEAN_7D", "NÚMERO"].iloc[0]
        )
        FEATURE_A_NUMBER = (
            ask_feature_number("Número de la primera característica SMART: ")
            if RUN_PAIR_ANALYSIS else default_a_number
        )
        FEATURE_B_NUMBER = (
            ask_feature_number("Número de la segunda característica SMART: ")
            if RUN_PAIR_ANALYSIS else default_b_number
        )
        feature_a = feature_from_number(FEATURE_A_NUMBER)
        feature_b = feature_from_number(FEATURE_B_NUMBER)
        while feature_a == feature_b and RUN_PAIR_ANALYSIS:
            print("Seleccione dos características distintas.")
            FEATURE_B_NUMBER = ask_feature_number("Otro número para la segunda característica: ")
            feature_b = feature_from_number(FEATURE_B_NUMBER)
        print("Pareja elegida:", feature_a, "y", feature_b)
        '''
    ),
    markdown("#### 4.11.1 Tabla de covarianza y correlación"),
    code(
        '''\
        if RUN_PAIR_ANALYSIS:
            a = quote_feature(feature_a)
            b = quote_feature(feature_b)
            pair_summary = query_small(f"""
                SELECT COALESCE(COUNT_IF({a} IS NOT NULL AND {b} IS NOT NULL), 0)
                           AS PARES_UTILIZABLES,
                       COVAR_SAMP({a}::DOUBLE, {b}::DOUBLE) AS COVARIANZA,
                       CORR({a}::DOUBLE, {b}::DOUBLE) AS CORRELACION,
                       REGR_SLOPE({b}::DOUBLE, {a}::DOUBLE) AS PENDIENTE_B_SOBRE_A
                FROM {OBT_TABLE}
                WHERE LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')
            """, max_rows=1)
            display(pair_summary)
        else:
            print("Bloque optativo sin ejecutar.")
        '''
    ),
    markdown("#### 4.11.2 Figura acotada de la pareja elegida"),
    code(
        '''\
        if RUN_PAIR_ANALYSIS:
            pair_sample = query_small(f"""
                SELECT {a} AS CARACTERISTICA_A, {b} AS CARACTERISTICA_B
                FROM {OBT_TABLE}
                WHERE LABEL_STATUS IN ('POSITIVE', 'NEGATIVE')
                      AND {a} IS NOT NULL AND {b} IS NOT NULL
                      AND MOD(HASH(SMART_DAILY_KEY), 1000) = 0
                ORDER BY HASH(SMART_DAILY_KEY), SMART_DAILY_KEY
                LIMIT 1000
            """, max_rows=1000)
            if not pair_sample.empty:
                figure, axis = plt.subplots(figsize=(7, 5))
                axis.scatter(pair_sample["CARACTERISTICA_A"].astype(float),
                             pair_sample["CARACTERISTICA_B"].astype(float),
                             alpha=0.25, s=8)
                axis.set_title(f"Relación exploratoria entre {feature_a} y {feature_b}")
                axis.set_xlabel(feature_a)
                axis.set_ylabel(feature_b)
                plt.tight_layout()
                plt.show()
            else:
                print("La muestra reproducible no devolvió pares observados.")
            print("Figura: submuestra estable por hash (~0,1 %), máximo 1000 filas.")
            print("La tabla anterior usa toda la población elegible; la figura no.")
        else:
            print("Bloque optativo sin ejecutar.")
        '''
    ),
    markdown(
        """\
        **Interpretación de la pareja.** Una correlación cercana a cero no
        excluye relaciones no lineales. La muestra de puntos solo permite
        visualizar; la covarianza y correlación se calculan en Snowflake sobre
        toda la población elegible con valores no nulos en ambas columnas.
        """
    ),
    markdown("### 4.12 Trayectoria optativa de un SSD antes de la falla"),
    code(
        '''\
        if RUN_TRAJECTORY:
            trajectory_candidate = query_small(f"""
                SELECT SSD_KEY, COUNT(DISTINCT OBSERVATION_DATE) AS DIAS_POSITIVOS
                FROM {OBT_TABLE}
                WHERE LABEL_STATUS = 'POSITIVE'
                GROUP BY SSD_KEY
                ORDER BY DIAS_POSITIVOS DESC, SSD_KEY
                LIMIT 1
            """, max_rows=1)
            display(trajectory_candidate)
        else:
            print("Bloque optativo sin ejecutar.")
        '''
    ),
    markdown("#### 4.12.1 Puntos diarios acotados del SSD elegido"),
    code(
        '''\
        if RUN_TRAJECTORY and not trajectory_candidate.empty:
            ssd_key = str(trajectory_candidate.iloc[0]["SSD_KEY"])
            ssd_key_literal = "'" + ssd_key.replace("'", "''") + "'"
            trajectory = query_small(f"""
                SELECT OBSERVATION_DATE, FIRST_FAILURE_DATE,
                       R_5, R_5_MEAN_7D, R_5_MEAN_SHIFT_7D
                FROM {OBT_TABLE}
                WHERE SSD_KEY = {ssd_key_literal}
                      AND FIRST_FAILURE_DATE IS NOT NULL
                      AND OBSERVATION_DATE BETWEEN
                          DATEADD('day', -30, FIRST_FAILURE_DATE) AND FIRST_FAILURE_DATE
                ORDER BY OBSERVATION_DATE, SMART_DAILY_KEY
                LIMIT 35
            """, max_rows=35)
            display(trajectory)
        elif RUN_TRAJECTORY:
            print("No hay un SSD con falla confirmada para esta trayectoria.")
        else:
            print("Bloque optativo sin ejecutar.")
        '''
    ),
    markdown("#### 4.12.2 Figura de la trayectoria de un SSD"),
    code(
        '''\
        if RUN_TRAJECTORY and not trajectory_candidate.empty and not trajectory.empty:
            figure, axes = plt.subplots(3, 1, figsize=(11, 8), sharex=True)
            for axis, feature in zip(axes, ("R_5", "R_5_MEAN_7D", "R_5_MEAN_SHIFT_7D")):
                axis.plot(pd.to_datetime(trajectory["OBSERVATION_DATE"]),
                          trajectory[feature].astype(float), marker="o")
                axis.axvline(pd.to_datetime(trajectory["FIRST_FAILURE_DATE"].iloc[0]),
                             color="red", linestyle="--", label="Primera falla")
                axis.set_ylabel(feature)
                axis.legend()
            axes[0].set_title("Trayectoria ilustrativa de un SSD antes de la falla")
            axes[-1].set_xlabel("Fecha de observación")
            plt.tight_layout()
            plt.show()
        elif RUN_TRAJECTORY:
            print("No hay puntos de trayectoria para representar.")
        else:
            print("Bloque optativo sin ejecutar.")
        '''
    ),
    markdown(
        """\
        **Interpretación de la trayectoria.** Este SSD se eligió
        determinísticamente por mayor número de días positivos y luego por
        `SSD_KEY`. El ejemplo ilustra una secuencia; no generaliza a toda la
        población de fallas.
        """
    ),
    markdown("### 4.13 Trayectoria agregada optativa por días restantes"),
    code(
        '''\
        if RUN_AGGREGATE_TRAJECTORY:
            aggregate_trajectory = query_small(f"""
                SELECT DAYS_TO_FAILURE AS DIAS_HASTA_LA_FALLA,
                       APPROX_PERCENTILE(R_5::DOUBLE, 0.50) AS MEDIANA_R5_APROX,
                       COUNT(DISTINCT SSD_KEY) AS SSD_DISTINTOS
                FROM {OBT_TABLE}
                WHERE LABEL_STATUS = 'POSITIVE' AND DAYS_TO_FAILURE BETWEEN 1 AND 30
                GROUP BY DAYS_TO_FAILURE
                ORDER BY DIAS_HASTA_LA_FALLA DESC
            """, max_rows=30)
            display(aggregate_trajectory)
        else:
            print("Bloque optativo sin ejecutar.")
        '''
    ),
    markdown("#### 4.13.1 Figura de la trayectoria agregada"),
    code(
        '''\
        if RUN_AGGREGATE_TRAJECTORY and not aggregate_trajectory.empty:
            figure, first_axis = plt.subplots(figsize=(10, 4))
            second_axis = first_axis.twinx()
            x = aggregate_trajectory["DIAS_HASTA_LA_FALLA"].astype(int)
            first_axis.plot(x, aggregate_trajectory["MEDIANA_R5_APROX"].astype(float),
                            marker="o", color="tab:blue")
            second_axis.bar(x, aggregate_trajectory["SSD_DISTINTOS"].astype(float),
                            alpha=0.2, color="tab:orange")
            first_axis.invert_xaxis()
            first_axis.set_title("Mediana aproximada de R_5 antes de la falla")
            first_axis.set_xlabel("Días restantes hasta la falla")
            first_axis.set_ylabel("Mediana aproximada de R_5")
            second_axis.set_ylabel("SSD contribuyentes")
            plt.tight_layout()
            plt.show()
        elif RUN_AGGREGATE_TRAJECTORY:
            print("No hay observaciones positivas para representar.")
        else:
            print("Bloque optativo sin ejecutar.")
        '''
    ),
    markdown(
        """\
        **Interpretación.** Los SSD que contribuyen pueden variar según el día
        previo a la falla; una mediana cambiante no demuestra deterioro causal
        ni rendimiento predictivo.
        """
    ),
    markdown("## 5. Tres hallazgos y consecuencias para el modelado"),
    markdown(
        """\
        Complete estos hallazgos **después de ejecutar** las consultas. No
        convierta los ejemplos en conclusiones predeterminadas. Cada hallazgo
        debe citar una tabla o figura y el período analizado.

        ### Hallazgo 1: equilibrio de clases

        - **Observación:** completar con la tasa positiva y la razón de clases.
        - **Evidencia:** tabla 2.3 y figura 1 o 2.
        - **Interpretación:** distinguir observaciones de SSD y eventos de falla.
        - **Consecuencia:** definir métricas adecuadas; no aplicar remuestreo aquí.

        ### Hallazgo 2: cobertura temporal

        - **Observación:** completar con días positivos por SSD y distancias cubiertas.
        - **Evidencia:** tablas 3.1, 3.3, 3.4 y figuras 3–4.
        - **Interpretación:** indicar si hay trayectorias completas o fragmentadas.
        - **Consecuencia:** decidir qué ventanas temporales merecen evaluación.

        ### Hallazgo 3: característica y calidad de la etiqueta

        - **Observación:** completar según un bloque SMART y la cobertura anual.
        - **Evidencia:** tabla/figura de sección 4 y tabla 2.4.
        - **Interpretación:** considerar sesgo, valores extremos y cobertura de etiquetas en ambos años.
        - **Consecuencia:** proponer un experimento posterior sin afirmar causalidad.
        """
    ),
    markdown("## 6. Límites y cierre"),
    markdown(
        """\
        Esta libreta es exploratoria y de solo lectura sobre Snowflake. No
        entrena modelos, no elimina extremos y no imputa nulos. La validez de
        los negativos depende de la cobertura real de eventos de falla en
        ambos años. Las comparaciones por observación tampoco equivalen a
        eventos de falla independientes.

        Revise Query History para confirmar el almacén y el costo de las
        consultas. Conserve en control de versiones la libreta generada y
        registre por separado las cifras y conclusiones de la ejecución real.
        """
    ),
])


def build_notebook() -> dict:
    return {
        "cells": [{**cell, "id": f"mc1-eda-{index:03d}"} for index, cell in enumerate(cells)],
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true",
        help="Verify that the generated notebook matches the committed file",
    )
    args = parser.parse_args()
    rendered = json.dumps(build_notebook(), ensure_ascii=False, indent=1) + "\n"
    if args.check:
        if not TARGET.exists() or TARGET.read_text(encoding="utf-8") != rendered:
            parser.error(f"Notebook is missing or out of date: {TARGET}")
        print(f"Verified {TARGET} ({len(cells)} cells)")
        return
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(rendered, encoding="utf-8")
    print(f"Wrote {TARGET} ({len(cells)} cells)")


if __name__ == "__main__":
    main()
