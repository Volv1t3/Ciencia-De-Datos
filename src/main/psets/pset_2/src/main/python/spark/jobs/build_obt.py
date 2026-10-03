"""Local-mode Spark connectivity skeleton for the future Snowflake OBT job."""

import os
import platform

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("ssd-failure-obt-skeleton").getOrCreate()
    try:
        print(f"Spark version: {spark.version}")
        print(f"Python version: {platform.python_version()}")
        print(f"Spark master: {spark.sparkContext.master}")
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
