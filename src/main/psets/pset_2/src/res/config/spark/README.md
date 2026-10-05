# Configuración del entorno Spark

[Inicio del proyecto](../../../../README.md) · [Imágenes Docker](../../docker/custom-images/README.md) · **Configuración de Spark** · [Plataforma Spark](../../../main/python/spark/README.md)

Este directorio contiene los archivos de configuración, propiedades del motor y definiciones de kernels aplicados al entorno de Apache Spark 4.0.4 dentro del contenedor `spark`.

---

## Inventario de configuración

| Archivo | Ruta montada en el contenedor | Propósito |
| --- | --- | --- |
| [`spark-defaults.conf`](spark-defaults.conf) | `/opt/spark/conf/spark-defaults.conf` | Configuración base del motor Spark (modo local, AQE, zona horaria, particiones de shuffle y registro de eventos). |
| [`spark-env.sh`](spark-env.sh) | `/opt/spark/conf/spark-env.sh` | Script launcher que asigna la memoria heap de la JVM para el driver y ejecutores desde variables de entorno. |
| [`log4j2.properties`](log4j2.properties) | `/opt/spark/conf/log4j2.properties` | Configuración de Apache Log4j 2 para suprimir logs verbosos e informativos en las celdas de Jupyter. |
| [`requirements.txt`](requirements.txt) | Compilado en la imagen `/tmp/requirements.txt` | Paquetes de Python para análisis estadístico y visualización instalados en la imagen interactiva. |
| [`kernels/pyspark-snowflake/kernel.json`](kernels/pyspark-snowflake/kernel.json) | `/usr/local/share/jupyter/kernels/pyspark-snowflake/kernel.json` | Definición del kernel personalizado de Jupyter con rutas a Spark, Py4J y librerías compartidas. |

---

## Detalle de las configuraciones

### 1. `spark-defaults.conf`

```properties
spark.master                              local[*]
spark.ui.port                             4040
spark.sql.adaptive.enabled                true
spark.sql.session.timeZone                UTC
spark.ui.showConsoleProgress              false
spark.sql.shuffle.partitions              8
spark.eventLog.enabled                    true
spark.eventLog.dir                        file:/opt/spark/logs
```

- **`spark.master local[*]`**: Configura Spark para ejecutarse en modo mononodo local utilizando todos los núcleos de CPU asignados al contenedor Docker.
- **`spark.sql.adaptive.enabled true`**: Activa la ejecución adaptativa de consultas (AQE), permitiendo a Spark optimizar dinámicamente uniones y reducir particiones vacías en tiempo de ejecución.
- **`spark.sql.session.timeZone UTC`**: Fija la interpretación horaria a UTC, garantizando operaciones de calendario consistentes sin desvíos por el huso horario local de la máquina anfitriona.
- **`spark.sql.shuffle.partitions 8`**: Reduce el valor por defecto de clusters (200 particiones), el cual genera sobrecarga innecesaria al procesar conjuntos pequeños o agregaciones locales en un único contenedor.
- **`spark.eventLog.enabled true`**: Guarda el historial de ejecución de cada aplicación en `/opt/spark/logs` (montado en el host en `src/res/logs/spark`), permitiendo auditar la ejecución de trabajos con el Spark History Server.

### 2. `spark-env.sh`

```bash
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
```

Los scripts de inicio de Spark ejecutan este archivo antes de arrancar la JVM. Docker Compose inyecta las variables `SPARK_DRIVER_MEMORY` y `SPARK_EXECUTOR_MEMORY` desde el archivo `.env`, permitiendo ajustar los límites de RAM de acuerdo con la capacidad de la máquina del desarrollador sin tener que reconstruir la imagen Docker.

### 3. `log4j2.properties`

Establece el nivel de registro de los loggers de Spark y Hadoop en `WARN`:
```properties
rootLogger.level = warn
logger.spark.level = warn
logger.hadoop.level = warn
```
Por defecto, Spark emite miles de mensajes de nivel `INFO` durante la planificación y resolución de consultas. Al elevar el umbral a `WARN`, se evita que la consola y los notebooks de Jupyter se saturen de mensajes irrelevantes.

### 4. `kernels/pyspark-snowflake/kernel.json`

```json
{
  "argv": ["/usr/bin/python3", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
  "display_name": "PySpark 4 + Snowflake",
  "language": "python",
  "metadata": { "debugger": true },
  "env": {
    "SPARK_HOME": "/opt/spark",
    "PYSPARK_PYTHON": "/usr/bin/python3",
    "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/opt/spark/lib"
  }
}
```

#### Por qué es indispensable este kernel personalizado

Cuando DataGrip o un navegador web se conecta a JupyterLab, iniciar un kernel estándar de `Python 3 (ipykernel)` ejecuta el intérprete de Python regular sin la inicialización propia de Spark. Como consecuencia:
- `import pyspark` falla con `ModuleNotFoundError`.
- Faltan las rutas al puente Java de Py4J.
- La ruta `/opt/spark/lib` (donde reside `snowflake_io.py`) no forma parte de `sys.path`.

El kernel `PySpark 4 + Snowflake` inicializa de antemano `SPARK_HOME`, `PYSPARK_PYTHON` y `PYTHONPATH` incluyendo los paquetes internos de Spark, el archivo comprimido de Py4J y el directorio `/opt/spark/lib`. Siempre se debe elegir este kernel al trabajar con los notebooks del proyecto.
