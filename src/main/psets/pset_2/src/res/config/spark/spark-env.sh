# This file is sourced by Spark's launch scripts. Compose supplies these values
# from .env, allowing local resource limits to change without rebuilding.
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
