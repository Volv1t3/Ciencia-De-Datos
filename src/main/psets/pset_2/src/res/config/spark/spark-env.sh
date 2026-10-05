#? ==========================================================================================
#? CONFIGURACION DE ENTORNO SPARK (spark-env.sh)
#? ------------------------------------------------------------------------------------------
#? Este script es ejecutado por los launchers de Apache Spark al arrancar la JVM en el
#? contenedor 'spark'. Docker Compose inyecta estos valores desde el archivo .env, lo cual
#? permite ajustar los limites de memoria de la JVM sin reconstruir la imagen de Docker.
#?
#? Memoria asignada al proceso driver y a los ejecutores locales (modo local[*]):
#?   SPARK_DRIVER_MEMORY: Memoria heap para el driver (2g por defecto para desarrollo local).
#?   SPARK_EXECUTOR_MEMORY: Memoria heap para ejecutores (2g por defecto para desarrollo local).
#? ==========================================================================================
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
