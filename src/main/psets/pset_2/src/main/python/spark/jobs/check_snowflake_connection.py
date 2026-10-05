"""Verifica el acceso y ejecucion de consultas en vivo del conector Spark a Snowflake.

Este script ejecuta una consulta de metadatos en vivo a traves del conector clasico
spark-snowflake utilizando las funciones auxiliares de snowflake_io.py. Comprueba la
autenticacion, asignacion de rol, acceso al warehouse activo y contexto de base/esquema.
"""


#? ==========================================================================================
#? VERIFICACION DE CONEXION EN VIVO: check_snowflake_connection.py
#? ------------------------------------------------------------------------------------------
#? Proposito:
#?   Probar la conectividad real de red y autenticacion entre Apache Spark (contenedor 'spark')
#?   y la cuenta de Snowflake en la nube usando las credenciales del archivo .env.
#?
#? Que hace:
#?   Ejecuta una consulta SQL ligera (SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), ...) a traves
#?   de net.snowflake.spark.snowflake y muestra la tabla por consola con .show(truncate=False).
#?
#? Como se ejecuta:
#?   docker compose --env-file src/res/env/.env exec spark \
#?     /opt/spark/bin/spark-submit /opt/spark/jobs/check_snowflake_connection.py
#? ==========================================================================================
from snowflake_io import create_spark_session, read_snowflake_query



def main() -> None:
    spark = create_spark_session("ssd-failure-snowflake-connection-check")
    try:
        result = read_snowflake_query(
            spark,
            """
            SELECT CURRENT_ACCOUNT() AS ACCOUNT,
                   CURRENT_ROLE() AS ROLE,
                   CURRENT_WAREHOUSE() AS WAREHOUSE,
                   CURRENT_DATABASE() AS DATABASE,
                   CURRENT_SCHEMA() AS SCHEMA
            """,
        )
        result.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
