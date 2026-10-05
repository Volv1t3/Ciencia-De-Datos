"""Esqueleto de conectividad Spark en modo local para verificar el conector de Snowflake.

Este script actua como una prueba de humo (smoke test) de diagnostico para el runtime del
contenedor clasico de Spark. Inicializa una SparkSession local, confirma las versiones de Spark
y Python, y consulta el registro DataSource de la JVM de Spark para asegurar que el archivo JAR
del conector net.snowflake.spark.snowflake este cargado en el classpath. No ejecuta transformaciones
pesadas de datos (el generador canonico de la OBT es build_mc1_obt.py).
"""


#? ==========================================================================================
#? SCRIPT DE HUMO / CONECTIVIDAD: build_obt.py
#? ------------------------------------------------------------------------------------------
#? Proposito:
#?   Verificar que el contenedor 'spark' tiene una JVM funcional de Apache Spark 4.0.4 y que
#?   el conector 'net.snowflake.spark.snowflake' se encuentra correctamente registrado en el
#?   classpath (/opt/spark/jars/).
#?
#? Diferencia con build_mc1_obt.py:
#?   - build_obt.py: Smoke test local ligero para el conector clasico spark-snowflake.
#?   - build_mc1_obt.py: Job productivo de generacion de OBT que usa Snowpark Connect.
#?
#? Como se ejecuta:
#?   docker compose --env-file src/res/env/.env exec spark \
#?     /opt/spark/bin/spark-submit /opt/spark/jobs/build_obt.py
#? ==========================================================================================
import os
import platform

from pyspark.sql import SparkSession



def main() -> None:
    spark = SparkSession.builder.appName("ssd-failure-obt-skeleton").getOrCreate()
    try:
        print(f"Spark version: {spark.version}")
        print(f"Python version: {platform.python_version()}")
        print(f"Spark master: {spark.sparkContext.master}")
        #? Pregunta a la JVM de Spark si existe la fuente de datos 'net.snowflake.spark.snowflake'.
        connector_class = (
            spark._jvm.org.apache.spark.sql.execution.datasources.DataSource
            .lookupDataSource(
                "net.snowflake.spark.snowflake",
                spark._jsparkSession.sessionState().conf(),
            )
        )
        print(f"Snowflake connector class: {connector_class.getName()}")

        if os.getenv("SNOWFLAKE_ACCOUNT"):
            print("Snowflake credentials are present; no remote query is run by this skeleton.")
        else:
            print("Snowflake credentials are absent; connector presence was checked locally only.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
