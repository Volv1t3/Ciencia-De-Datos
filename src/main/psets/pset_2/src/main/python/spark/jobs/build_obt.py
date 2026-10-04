"""Local-mode Spark connectivity skeleton for the future Snowflake OBT job."""

#? Script de humo (skeleton) del contenedor 'spark': solo verifica que Spark arranca y que el
#? conector spark-snowflake esta instalado. NO construye la OBT; la OBT real es build_mc1_obt.py.
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
