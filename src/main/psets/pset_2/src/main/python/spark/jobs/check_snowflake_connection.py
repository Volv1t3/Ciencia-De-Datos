"""Verify Spark connector access to the configured Snowflake warehouse."""

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
