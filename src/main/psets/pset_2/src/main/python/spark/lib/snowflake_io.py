"""Shared Spark/Snowflake connector configuration for jobs and notebooks."""

from __future__ import annotations

import os
from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def snowflake_options(schema: str | None = None) -> dict[str, str]:
    """Build connector options without logging or persisting credentials."""
    account = _required_env("SNOWFLAKE_ACCOUNT")
    host = account if account.endswith(".snowflakecomputing.com") else f"{account}.snowflakecomputing.com"
    options = {
        "sfURL": host,
        "sfUser": _required_env("SNOWFLAKE_USER"),
        "sfPassword": _required_env("SNOWFLAKE_PASSWORD"),
        "sfWarehouse": _required_env("SNOWFLAKE_WAREHOUSE"),
        "sfDatabase": _required_env("SNOWFLAKE_DATABASE"),
        "sfSchema": schema or _required_env("SNOWFLAKE_SCHEMA"),
        "autopushdown": "on",
        "sfTimezone": "spark",
    }
    role = os.getenv("SNOWFLAKE_ROLE", "").strip()
    if role:
        options["sfRole"] = role
    return options


def create_spark_session(app_name: str = "ssd-failure-notebook") -> SparkSession:
    """Create the local Spark driver used by the notebook kernel."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_snowflake_table(
    spark: SparkSession,
    table: str,
    *,
    schema: str | None = None,
    extra_options: Mapping[str, str] | None = None,
) -> DataFrame:
    """Return a lazy DataFrame backed by a Snowflake table."""
    options = snowflake_options(schema)
    if extra_options:
        options.update(extra_options)
    return (
        spark.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(**options)
        .option("dbtable", table)
        .load()
    )


def read_snowflake_query(
    spark: SparkSession,
    query: str,
    *,
    schema: str | None = None,
    extra_options: Mapping[str, str] | None = None,
) -> DataFrame:
    """Execute a SELECT in Snowflake and expose its result as a DataFrame."""
    options = snowflake_options(schema)
    if extra_options:
        options.update(extra_options)
    return (
        spark.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(**options)
        .option("query", query)
        .load()
    )
