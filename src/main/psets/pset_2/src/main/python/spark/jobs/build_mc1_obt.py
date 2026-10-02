#!/usr/bin/env python3
"""Build the three MC1 one-big-table variants with Snowpark Connect for Spark.

The heavy transformation is expressed exclusively with the PySpark DataFrame
API and evaluated by Snowflake through Snowpark Connect.  Snowflake SQL is used
only for schema/audit DDL and zero-copy publication of already validated stage
tables.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import secrets
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from functools import reduce
from typing import Iterable, Mapping, Sequence

from pyspark import SparkConf
from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import (
    LongType,
    NumericType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from snowflake import snowpark_connect
from snowflake.snowpark_connect.snowflake_session import SnowflakeSession


LOGGER = logging.getLogger("pset2.mc1_obt")

SMART_IDS: tuple[int, ...] = (
    1,
    5,
    9,
    12,
    170,
    171,
    172,
    173,
    174,
    180,
    183,
    184,
    187,
    188,
    194,
    195,
    196,
    197,
    198,
    199,
    206,
    211,
)
REPRESENTATIONS: tuple[str, ...] = ("R", "N")
WINDOW_DAYS: tuple[int, ...] = (7, 14, 30)

COMMON_COLUMNS: tuple[str, ...] = (
    "SMART_DAILY_KEY",
    "SSD_KEY",
    "DISK_ID",
    "MODEL_CODE",
    "OBSERVATION_DATE_KEY",
    "OBSERVATION_DATE",
)
TARGET_COLUMNS: tuple[str, ...] = (
    "FIRST_FAILURE_DATE",
    "LAST_SEEN_DATE",
    "DAYS_TO_FAILURE",
    "LABEL_STATUS",
    "TARGET_30D",
)

EXPECTED_RN_WIDTH = 847
EXPECTED_VARIANT_WIDTH = 429
INTERNAL_DAY_COLUMN = "_INTERNAL_OBSERVATION_DAY"
SMART_COLUMN_PATTERN = re.compile(r"^[RN]_[0-9]+$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

AUDIT_METRIC_NAMES: tuple[str, ...] = (
    "GOLD_OBSERVATION_COUNT",
    "GOLD_SSD_COUNT",
    "MC1_OBT_ROW_COUNT",
    "MC1_OBT_DISTINCT_SSD_COUNT",
    "POSITIVE_COUNT",
    "NEGATIVE_COUNT",
    "CENSORED_COUNT",
    "SAME_DAY_FAILURE_COUNT",
    "POST_FAILURE_COUNT",
    "DUPLICATE_SMART_DAILY_KEY_COUNT",
    "R_N_COUNT_MISMATCH_COUNT",
    "INVALID_ROLLING_COUNT_COUNT",
    "INVALID_MEAN_COUNT",
    "INVALID_SHIFT_COUNT",
    "MULTIPLE_FAILURE_SSD_COUNT",
)


class ValidationError(RuntimeError):
    """A blocking contract violation in source data or generated output."""


class ExecutionError(RuntimeError):
    """A Snowflake, Snowpark Connect, write, or publication failure."""


@dataclass(frozen=True)
class WindowDefinition:
    days: int
    current_start: int
    current_end: int
    previous_start: int
    previous_end: int


WINDOW_REGISTRY: tuple[WindowDefinition, ...] = (
    WindowDefinition(7, -6, 0, -13, -7),
    WindowDefinition(14, -13, 0, -27, -14),
    WindowDefinition(30, -29, 0, -59, -30),
)


@dataclass(frozen=True)
class JobConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    gold_schema: str
    obt_schema: str
    role: str | None = None

    @property
    def sensitive_values(self) -> tuple[str, ...]:
        return tuple(
            value for value in (self.password, self.account, self.user) if value
        )


@dataclass(frozen=True)
class JobArguments:
    start_date: date | None
    end_date: date | None
    ssd_limit: int | None
    validate_only: bool
    explain_plan: bool
    keep_staging: bool

    @property
    def is_bounded(self) -> bool:
        return any((self.start_date, self.end_date, self.ssd_limit))

    @property
    def is_production(self) -> bool:
        return not self.is_bounded and not self.validate_only


@dataclass(frozen=True)
class RunContext:
    run_id: str
    run_suffix: str
    started_at_utc: datetime
    app_name: str


@dataclass(frozen=True)
class RelationNames:
    dim_ssd: str
    dim_date: str
    smart_fact: str
    failure_fact: str
    rn_stage: str
    r_stage: str
    n_stage: str
    rn_canonical: str
    r_canonical: str
    n_canonical: str
    audit: str


@dataclass
class AuditMetrics:
    values: dict[str, int | None] = field(
        default_factory=lambda: {name: None for name in AUDIT_METRIC_NAMES}
    )

    def set(self, **metrics: int | None) -> None:
        unknown = set(metrics).difference(self.values)
        if unknown:
            raise ValueError(f"Unknown audit metrics: {sorted(unknown)}")
        self.values.update(metrics)


def parse_iso_date(raw_value: str) -> date:
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD, received {raw_value!r}."
        ) from exc


def parse_args(argv: Sequence[str] | None = None) -> JobArguments:
    parser = argparse.ArgumentParser(
        description="Build validated MC1 RN, R, and N OBT tables in Snowflake."
    )
    parser.add_argument("--start-date", type=parse_iso_date)
    parser.add_argument("--end-date", type=parse_iso_date)
    parser.add_argument("--ssd-limit", type=int)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--explain-plan", action="store_true")
    parser.add_argument("--keep-staging", action="store_true")
    parsed = parser.parse_args(argv)

    if parsed.start_date and parsed.end_date and parsed.start_date > parsed.end_date:
        parser.error("--start-date must be less than or equal to --end-date.")
    if parsed.ssd_limit is not None and parsed.ssd_limit <= 0:
        parser.error("--ssd-limit must be greater than zero.")

    return JobArguments(
        start_date=parsed.start_date,
        end_date=parsed.end_date,
        ssd_limit=parsed.ssd_limit,
        validate_only=parsed.validate_only,
        explain_plan=parsed.explain_plan,
        keep_staging=parsed.keep_staging,
    )


def _required_environment(name: str, fallback: str | None = None) -> str:
    value = os.getenv(name) or (os.getenv(fallback) if fallback else None)
    if not value or not value.strip():
        suffix = f" or {fallback}" if fallback else ""
        raise ValidationError(f"Required environment variable {name}{suffix} is absent.")
    return value.strip()


def _validated_identifier(value: str, variable_name: str) -> str:
    candidate = value.strip()
    if not IDENTIFIER_PATTERN.fullmatch(candidate):
        raise ValidationError(
            f"{variable_name} must be an unquoted Snowflake identifier containing "
            "only letters, digits, underscores, or dollar signs."
        )
    return candidate.upper()


def load_config() -> JobConfig:
    database = _validated_identifier(
        _required_environment("SNOWFLAKE_DATABASE", "DBT_SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_DATABASE",
    )
    gold_schema = _validated_identifier(
        os.getenv("SNOWFLAKE_GOLD_SCHEMA", "S_CDATOS_PSET2_GOLD"),
        "SNOWFLAKE_GOLD_SCHEMA",
    )
    obt_schema = _validated_identifier(
        os.getenv("SNOWFLAKE_OBT_SCHEMA", "S_CDATOS_PSET2_OBT"),
        "SNOWFLAKE_OBT_SCHEMA",
    )
    role = os.getenv("SNOWFLAKE_ROLE")
    return JobConfig(
        account=_required_environment("SNOWFLAKE_ACCOUNT"),
        user=_required_environment("SNOWFLAKE_USER"),
        password=_required_environment("SNOWFLAKE_PASSWORD"),
        warehouse=_required_environment("SNOWFLAKE_WAREHOUSE"),
        database=database,
        gold_schema=gold_schema,
        obt_schema=obt_schema,
        role=role.strip() if role and role.strip() else None,
    )


def generate_run_context() -> RunContext:
    started_at = datetime.now(timezone.utc)
    nonce = secrets.token_hex(16)
    payload = f"MC1{started_at.isoformat()}{nonce}".encode("utf-8")
    run_id = hashlib.sha256(payload).hexdigest()
    suffix = run_id[:16].upper()
    return RunContext(
        run_id=run_id,
        run_suffix=suffix,
        started_at_utc=started_at,
        app_name=f"pset2-mc1-obt-{suffix.lower()}",
    )


def relation_names(config: JobConfig, run: RunContext) -> RelationNames:
    gold = f"{config.database}.{config.gold_schema}"
    obt = f"{config.database}.{config.obt_schema}"
    return RelationNames(
        dim_ssd=f"{gold}.DIM_SSD",
        dim_date=f"{gold}.DIM_DATE",
        smart_fact=f"{gold}.FCT_SMART_DAILY_MC1",
        failure_fact=f"{gold}.FCT_FAILURE_EVENT_MC1",
        rn_stage=f"{obt}.OBT_MC1_RN__RUN_{run.run_suffix}",
        r_stage=f"{obt}.OBT_MC1_R__RUN_{run.run_suffix}",
        n_stage=f"{obt}.OBT_MC1_N__RUN_{run.run_suffix}",
        rn_canonical=f"{obt}.OBT_MC1_RN",
        r_canonical=f"{obt}.OBT_MC1_R",
        n_canonical=f"{obt}.OBT_MC1_N",
        audit=f"{obt}.OBT_MC1_RUN_AUDIT",
    )


def create_spark_session(
    config: JobConfig, run: RunContext
) -> tuple[SparkSession, SnowflakeSession]:
    conf = (
        SparkConf()
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.sql.ansi.enabled", "true")
    )
    connection_parameters = {
        "account": config.account,
        "user": config.user,
        "password": config.password,
        "warehouse": config.warehouse,
        "database": config.database,
        "schema": config.gold_schema,
    }
    if config.role:
        connection_parameters["role"] = config.role

    spark = snowpark_connect.init_spark_session(
        conf=conf,
        connection_parameters=connection_parameters,
        app_name=run.app_name,
    )
    return spark, SnowflakeSession(spark)


def execute_snowflake_sql(session: SnowflakeSession, statement: str) -> None:
    """Execute metadata SQL and force completion without collecting data rows."""

    session.sql(statement).collect()


def ensure_output_schema(
    session: SnowflakeSession, config: JobConfig
) -> None:
    execute_snowflake_sql(
        session,
        f"CREATE SCHEMA IF NOT EXISTS {config.database}.{config.obt_schema}",
    )


def ensure_audit_table(session: SnowflakeSession, audit_fqn: str) -> None:
    execute_snowflake_sql(
        session,
        f"""
        CREATE TABLE IF NOT EXISTS {audit_fqn} (
            RUN_ID VARCHAR(64) NOT NULL,
            POPULATION_CODE VARCHAR(16) NOT NULL,
            STARTED_AT_UTC TIMESTAMP_LTZ NOT NULL,
            COMPLETED_AT_UTC TIMESTAMP_LTZ NOT NULL,
            RUN_STATUS VARCHAR(32) NOT NULL,
            GOLD_OBSERVATION_COUNT NUMBER(38, 0),
            GOLD_SSD_COUNT NUMBER(38, 0),
            MC1_OBT_ROW_COUNT NUMBER(38, 0),
            MC1_OBT_DISTINCT_SSD_COUNT NUMBER(38, 0),
            POSITIVE_COUNT NUMBER(38, 0),
            NEGATIVE_COUNT NUMBER(38, 0),
            CENSORED_COUNT NUMBER(38, 0),
            SAME_DAY_FAILURE_COUNT NUMBER(38, 0),
            POST_FAILURE_COUNT NUMBER(38, 0),
            DUPLICATE_SMART_DAILY_KEY_COUNT NUMBER(38, 0),
            R_N_COUNT_MISMATCH_COUNT NUMBER(38, 0),
            INVALID_ROLLING_COUNT_COUNT NUMBER(38, 0),
            INVALID_MEAN_COUNT NUMBER(38, 0),
            INVALID_SHIFT_COUNT NUMBER(38, 0),
            MULTIPLE_FAILURE_SSD_COUNT NUMBER(38, 0),
            ERROR_MESSAGE VARCHAR(4000)
        )
        """.strip(),
    )


def smart_columns(representation: str | None = None) -> list[str]:
    representations = (representation,) if representation else REPRESENTATIONS
    return [f"{rep}_{smart_id}" for rep in representations for smart_id in SMART_IDS]


def feature_family_columns(representation: str, smart_id: int) -> list[str]:
    base = f"{representation}_{smart_id}"
    columns = [base]
    columns.extend(f"{base}_MEAN_{days}D" for days in WINDOW_DAYS)
    columns.extend(f"{base}_COUNT_{days}D" for days in WINDOW_DAYS)
    columns.extend(f"{base}_PREV_COUNT_{days}D" for days in WINDOW_DAYS)
    columns.extend(f"{base}_MEAN_VALID_{days}D" for days in WINDOW_DAYS)
    columns.extend(f"{base}_MEAN_SHIFT_{days}D" for days in WINDOW_DAYS)
    columns.extend(f"{base}_MEAN_SHIFT_VALID_{days}D" for days in WINDOW_DAYS)
    return columns


def representation_feature_columns(representation: str) -> list[str]:
    return [
        column
        for smart_id in SMART_IDS
        for column in feature_family_columns(representation, smart_id)
    ]


def rn_final_columns() -> list[str]:
    return [
        *COMMON_COLUMNS,
        *representation_feature_columns("R"),
        *representation_feature_columns("N"),
        *TARGET_COLUMNS,
    ]


def r_final_columns() -> list[str]:
    return [
        *COMMON_COLUMNS,
        *representation_feature_columns("R"),
        *TARGET_COLUMNS,
    ]


def n_final_columns() -> list[str]:
    return [
        *COMMON_COLUMNS,
        *representation_feature_columns("N"),
        *TARGET_COLUMNS,
    ]


def _case_insensitive_columns(df: DataFrame) -> dict[str, str]:
    return {name.upper(): name for name in df.columns}


def normalize_required_columns(
    df: DataFrame, required: Iterable[str], relation_name: str
) -> DataFrame:
    lookup = _case_insensitive_columns(df)
    required_list = list(required)
    missing = sorted(set(required_list).difference(lookup))
    if missing:
        raise ValidationError(
            f"{relation_name} is missing required columns: {', '.join(missing)}"
        )
    return df.select(*(F.col(lookup[name]).alias(name) for name in required_list))


def validate_smart_registry(smart_fact: DataFrame, relation_name: str) -> None:
    lookup = _case_insensitive_columns(smart_fact)
    discovered = {name for name in lookup if SMART_COLUMN_PATTERN.fullmatch(name)}
    expected = set(smart_columns())
    missing = sorted(expected.difference(discovered))
    unexpected = sorted(discovered.difference(expected))
    if missing or unexpected:
        details: list[str] = []
        if missing:
            details.append(f"missing={missing}")
        if unexpected:
            details.append(f"unexpected={unexpected}")
        raise ValidationError(
            f"{relation_name} violates the exact MC1 SMART registry: "
            + "; ".join(details)
        )

    type_by_name = {field.name.upper(): field.dataType for field in smart_fact.schema.fields}
    non_numeric = sorted(
        name for name in expected if not isinstance(type_by_name[name], NumericType)
    )
    if non_numeric:
        raise ValidationError(
            f"{relation_name} contains non-numeric SMART predictors: {non_numeric}"
        )


def _date_literal(value: date) -> Column:
    return F.lit(value.isoformat()).cast("date")


def apply_output_date_scope(df: DataFrame, args: JobArguments) -> DataFrame:
    scoped = df
    if args.start_date:
        scoped = scoped.where(F.col("OBSERVATION_DATE") >= _date_literal(args.start_date))
    if args.end_date:
        scoped = scoped.where(F.col("OBSERVATION_DATE") <= _date_literal(args.end_date))
    return scoped


def apply_context_date_scope(df: DataFrame, args: JobArguments) -> DataFrame:
    scoped = df
    if args.start_date:
        context_start = args.start_date - timedelta(days=59)
        scoped = scoped.where(F.col("OBSERVATION_DATE") >= _date_literal(context_start))
    if args.end_date:
        context_end = args.end_date + timedelta(days=30)
        scoped = scoped.where(F.col("OBSERVATION_DATE") <= _date_literal(context_end))
    return scoped


def apply_development_scope(
    smart_fact: DataFrame, args: JobArguments
) -> tuple[DataFrame, DataFrame, DataFrame | None]:
    """Return context rows, output rows, and the one deterministic SSD sample."""

    output_candidates = apply_output_date_scope(smart_fact, args)
    selected_ssds: DataFrame | None = None
    if args.is_bounded:
        selected_ssds = output_candidates.select("SSD_KEY").distinct().orderBy("SSD_KEY")
        if args.ssd_limit:
            selected_ssds = selected_ssds.limit(args.ssd_limit)

    context = apply_context_date_scope(smart_fact, args)
    output = output_candidates
    if selected_ssds is not None:
        context = context.join(selected_ssds, on="SSD_KEY", how="inner")
        output = output.join(selected_ssds, on="SSD_KEY", how="inner")
    return context, output, selected_ssds


def _normalized_scalar_mapping(row: Row) -> dict[str, int | str | None]:
    return {str(key).upper(): value for key, value in row.asDict().items()}


def collect_scalar_aggregate(df: DataFrame, expressions: Sequence[Column]) -> dict[str, int | str | None]:
    row = df.agg(*expressions).first()
    if row is None:
        raise ExecutionError("A scalar validation aggregate returned no row.")
    return _normalized_scalar_mapping(row)


def grain_metrics(df: DataFrame) -> dict[str, int]:
    metrics = collect_scalar_aggregate(
        df,
        (
            F.count(F.lit(1)).alias("ROW_COUNT"),
            F.countDistinct("SMART_DAILY_KEY").alias("DISTINCT_KEY_COUNT"),
            F.countDistinct("SSD_KEY", "OBSERVATION_DATE").alias(
                "DISTINCT_SSD_DATE_COUNT"
            ),
            F.countDistinct("SSD_KEY").alias("DISTINCT_SSD_COUNT"),
        ),
    )
    return {name: int(value or 0) for name, value in metrics.items()}


def validate_source_grain(df: DataFrame, scope_name: str) -> dict[str, int]:
    metrics = grain_metrics(df)
    if metrics["ROW_COUNT"] == 0:
        raise ValidationError(f"The {scope_name} SMART population is empty.")
    if metrics["ROW_COUNT"] != metrics["DISTINCT_KEY_COUNT"]:
        raise ValidationError(
            f"The {scope_name} SMART population contains duplicate SMART_DAILY_KEY values."
        )
    if metrics["ROW_COUNT"] != metrics["DISTINCT_SSD_DATE_COUNT"]:
        raise ValidationError(
            f"The {scope_name} SMART population contains duplicate SSD/date values."
        )
    return metrics


def _duplicate_key_count(df: DataFrame, key_columns: Sequence[str]) -> int:
    duplicate_groups = (
        df.groupBy(*key_columns)
        .count()
        .where(F.col("count") > F.lit(1))
    )
    value = collect_scalar_aggregate(
        duplicate_groups,
        (F.count(F.lit(1)).alias("DUPLICATE_GROUP_COUNT"),),
    )["DUPLICATE_GROUP_COUNT"]
    return int(value or 0)


def validate_dimension_relationships(
    context_fact: DataFrame,
    dim_ssd: DataFrame,
    dim_date: DataFrame,
) -> None:
    if _duplicate_key_count(dim_ssd, ("SSD_KEY",)):
        raise ValidationError("DIM_SSD contains duplicate SSD_KEY values.")
    if _duplicate_key_count(dim_date, ("DATE_KEY",)):
        raise ValidationError("DIM_DATE contains duplicate DATE_KEY values.")

    orphan_ssds = (
        context_fact.select("SSD_KEY")
        .join(dim_ssd.select("SSD_KEY"), on="SSD_KEY", how="left_anti")
        .count()
    )
    if orphan_ssds:
        raise ValidationError(
            f"The scoped SMART fact contains {orphan_ssds} orphan SSD_KEY rows."
        )

    date_lookup = dim_date.select(
        F.col("DATE_KEY").alias("_DIM_DATE_KEY"),
        F.col("FULL_DATE").alias("_DIM_FULL_DATE"),
    )
    date_checks = context_fact.join(
        date_lookup,
        context_fact["OBSERVATION_DATE_KEY"] == date_lookup["_DIM_DATE_KEY"],
        "left",
    )
    invalid_dates = collect_scalar_aggregate(
        date_checks,
        (
            F.sum(
                F.when(F.col("_DIM_DATE_KEY").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("ORPHAN_DATE_KEYS"),
            F.sum(
                F.when(
                    F.col("_DIM_DATE_KEY").isNotNull()
                    & (F.col("OBSERVATION_DATE") != F.col("_DIM_FULL_DATE")),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("DATE_VALUE_MISMATCHES"),
        ),
    )
    if int(invalid_dates["ORPHAN_DATE_KEYS"] or 0):
        raise ValidationError("The SMART fact contains orphan OBSERVATION_DATE_KEY values.")
    if int(invalid_dates["DATE_VALUE_MISMATCHES"] or 0):
        raise ValidationError(
            "OBSERVATION_DATE does not match DIM_DATE.FULL_DATE for one or more rows."
        )


def build_base_observations(
    scoped_fact: DataFrame,
    dim_ssd: DataFrame,
    dim_date: DataFrame,
    expected_count: int,
) -> DataFrame:
    ssd_lookup = dim_ssd.select(
        F.col("SSD_KEY").alias("_DIM_SSD_KEY"),
        "DISK_ID",
        "MODEL_CODE",
    )
    date_lookup = dim_date.select(
        F.col("DATE_KEY").alias("_DIM_DATE_KEY"),
        F.col("FULL_DATE").alias("_DIM_FULL_DATE"),
    )
    fact = scoped_fact.alias("fact")
    enriched = (
        fact.join(
            ssd_lookup,
            fact["SSD_KEY"] == ssd_lookup["_DIM_SSD_KEY"],
            "inner",
        )
        .join(
            date_lookup,
            (fact["OBSERVATION_DATE_KEY"] == date_lookup["_DIM_DATE_KEY"])
            & (fact["OBSERVATION_DATE"] == date_lookup["_DIM_FULL_DATE"]),
            "inner",
        )
        .select(
            *(fact[column].alias(column) for column in (
                "SMART_DAILY_KEY",
                "SSD_KEY",
                "OBSERVATION_DATE_KEY",
                "OBSERVATION_DATE",
                *smart_columns(),
            )),
            F.col("DISK_ID"),
            F.upper(F.col("MODEL_CODE")).alias("MODEL_CODE"),
        )
        .select(*COMMON_COLUMNS, *smart_columns())
    )

    actual_count = enriched.count()
    if actual_count != expected_count:
        raise ValidationError(
            "Dimension enrichment changed the scoped SMART observation count "
            f"from {expected_count} to {actual_count}."
        )
    invalid_models = enriched.where(F.col("MODEL_CODE") != F.lit("MC1")).count()
    if invalid_models:
        raise ValidationError(
            f"The enriched MC1 source contains {invalid_models} non-MC1 rows."
        )
    return enriched


def validate_failure_relationships(
    failures: DataFrame, dim_ssd: DataFrame, dim_date: DataFrame
) -> None:
    orphan_ssds = (
        failures.select("SSD_KEY")
        .join(dim_ssd.select("SSD_KEY"), on="SSD_KEY", how="left_anti")
        .count()
    )
    if orphan_ssds:
        raise ValidationError(
            f"FCT_FAILURE_EVENT_MC1 contains {orphan_ssds} orphan SSD_KEY rows."
        )

    date_lookup = dim_date.select(
        F.col("DATE_KEY").alias("_DIM_FAILURE_DATE_KEY"),
        F.col("FULL_DATE").alias("_DIM_FAILURE_DATE"),
    )
    invalid = failures.join(
        date_lookup,
        failures["FAILURE_DATE_KEY"] == date_lookup["_DIM_FAILURE_DATE_KEY"],
        "left",
    )
    metrics = collect_scalar_aggregate(
        invalid,
        (
            F.sum(
                F.when(
                    F.col("_DIM_FAILURE_DATE_KEY").isNull(), F.lit(1)
                ).otherwise(F.lit(0))
            ).alias("ORPHAN_DATE_KEYS"),
            F.sum(
                F.when(
                    F.col("_DIM_FAILURE_DATE_KEY").isNotNull()
                    & (F.col("FAILURE_DATE") != F.col("_DIM_FAILURE_DATE")),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("DATE_VALUE_MISMATCHES"),
        ),
    )
    if int(metrics["ORPHAN_DATE_KEYS"] or 0):
        raise ValidationError("FCT_FAILURE_EVENT_MC1 has orphan FAILURE_DATE_KEY values.")
    if int(metrics["DATE_VALUE_MISMATCHES"] or 0):
        raise ValidationError(
            "FAILURE_DATE does not match DIM_DATE.FULL_DATE for one or more events."
        )


def build_failure_summary(failures: DataFrame) -> DataFrame:
    return failures.groupBy("SSD_KEY").agg(
        F.min("FAILURE_DATE").alias("FIRST_FAILURE_DATE"),
        F.count(F.lit(1)).alias("FAILURE_EVENT_COUNT"),
    )


def build_last_seen(context_observations: DataFrame) -> DataFrame:
    return context_observations.groupBy("SSD_KEY").agg(
        F.max("OBSERVATION_DATE").alias("LAST_SEEN_DATE")
    )


def _expected_label_status() -> Column:
    days = F.datediff(F.col("FIRST_FAILURE_DATE"), F.col("OBSERVATION_DATE"))
    return (
        F.when(
            F.col("FIRST_FAILURE_DATE").isNotNull()
            & (F.col("OBSERVATION_DATE") > F.col("FIRST_FAILURE_DATE")),
            F.lit("POST_FAILURE"),
        )
        .when(
            F.col("FIRST_FAILURE_DATE").isNotNull()
            & (F.col("OBSERVATION_DATE") == F.col("FIRST_FAILURE_DATE")),
            F.lit("SAME_DAY_FAILURE"),
        )
        .when(days.between(1, 30), F.lit("POSITIVE"))
        .when(
            F.col("LAST_SEEN_DATE") >= F.date_add(F.col("OBSERVATION_DATE"), 30),
            F.lit("NEGATIVE"),
        )
        .otherwise(F.lit("CENSORED"))
    )


def _expected_target() -> Column:
    status = _expected_label_status()
    return (
        F.when(status == F.lit("POSITIVE"), F.lit(1))
        .when(status == F.lit("NEGATIVE"), F.lit(0))
        .otherwise(F.lit(None).cast("integer"))
    )


def build_labels(
    output_observations: DataFrame,
    failure_summary: DataFrame,
    last_seen: DataFrame,
) -> DataFrame:
    labels = (
        output_observations.select(
            "SMART_DAILY_KEY", "SSD_KEY", "OBSERVATION_DATE"
        )
        .join(failure_summary, on="SSD_KEY", how="left")
        .join(last_seen, on="SSD_KEY", how="left")
        .withColumn(
            "DAYS_TO_FAILURE",
            F.datediff(F.col("FIRST_FAILURE_DATE"), F.col("OBSERVATION_DATE")),
        )
        .withColumn("LABEL_STATUS", _expected_label_status())
        .withColumn("TARGET_30D", _expected_target())
    )
    return labels.select("SMART_DAILY_KEY", *TARGET_COLUMNS)


def build_window_registry() -> dict[int, tuple[Window, Window]]:
    ordered = Window.partitionBy("SSD_KEY").orderBy(INTERNAL_DAY_COLUMN)
    return {
        definition.days: (
            ordered.rangeBetween(definition.current_start, definition.current_end),
            ordered.rangeBetween(definition.previous_start, definition.previous_end),
        )
        for definition in WINDOW_REGISTRY
    }


def build_window_statistics(context_observations: DataFrame) -> DataFrame:
    window_registry = build_window_registry()
    base = context_observations.withColumn(
        INTERNAL_DAY_COLUMN,
        F.datediff(F.col("OBSERVATION_DATE"), F.lit("1970-01-01")),
    )
    expressions: list[Column] = [
        *(F.col(column) for column in COMMON_COLUMNS),
        *(F.col(column) for column in smart_columns()),
    ]
    for representation in REPRESENTATIONS:
        for smart_id in SMART_IDS:
            base_name = f"{representation}_{smart_id}"
            for days in WINDOW_DAYS:
                current_window, previous_window = window_registry[days]
                expressions.extend(
                    (
                        F.avg(base_name)
                        .over(current_window)
                        .alias(f"{base_name}_MEAN_{days}D"),
                        F.count(base_name)
                        .over(current_window)
                        .alias(f"{base_name}_COUNT_{days}D"),
                        F.avg(base_name)
                        .over(previous_window)
                        .alias(f"_INTERNAL_{base_name}_PREV_MEAN_{days}D"),
                        F.count(base_name)
                        .over(previous_window)
                        .alias(f"{base_name}_PREV_COUNT_{days}D"),
                    )
                )
    return base.select(*expressions)


def build_derived_temporal_features(window_df: DataFrame) -> DataFrame:
    expressions: list[Column] = [F.col(column) for column in COMMON_COLUMNS]
    for representation in REPRESENTATIONS:
        for smart_id in SMART_IDS:
            base_name = f"{representation}_{smart_id}"
            expressions.append(F.col(base_name))
            expressions.extend(
                F.col(f"{base_name}_MEAN_{days}D") for days in WINDOW_DAYS
            )
            expressions.extend(
                F.col(f"{base_name}_COUNT_{days}D") for days in WINDOW_DAYS
            )
            expressions.extend(
                F.col(f"{base_name}_PREV_COUNT_{days}D") for days in WINDOW_DAYS
            )
            for days in WINDOW_DAYS:
                count_column = F.col(f"{base_name}_COUNT_{days}D")
                expressions.append(
                    F.when(count_column > F.lit(0), F.lit(1))
                    .otherwise(F.lit(0))
                    .cast("integer")
                    .alias(f"{base_name}_MEAN_VALID_{days}D")
                )
            for days in WINDOW_DAYS:
                current_count = F.col(f"{base_name}_COUNT_{days}D")
                previous_count = F.col(f"{base_name}_PREV_COUNT_{days}D")
                current_mean = F.col(f"{base_name}_MEAN_{days}D")
                previous_mean = F.col(f"_INTERNAL_{base_name}_PREV_MEAN_{days}D")
                expressions.append(
                    F.when(
                        (current_count > F.lit(0)) & (previous_count > F.lit(0)),
                        current_mean - previous_mean,
                    )
                    .otherwise(F.lit(None))
                    .alias(f"{base_name}_MEAN_SHIFT_{days}D")
                )
            for days in WINDOW_DAYS:
                current_count = F.col(f"{base_name}_COUNT_{days}D")
                previous_count = F.col(f"{base_name}_PREV_COUNT_{days}D")
                expressions.append(
                    F.when(
                        (current_count > F.lit(0)) & (previous_count > F.lit(0)),
                        F.lit(1),
                    )
                    .otherwise(F.lit(0))
                    .cast("integer")
                    .alias(f"{base_name}_MEAN_SHIFT_VALID_{days}D")
                )
    return window_df.select(*expressions)


def assert_exact_schema(
    df: DataFrame, expected_columns: Sequence[str], relation_name: str
) -> None:
    actual = [column.upper() for column in df.columns]
    expected = list(expected_columns)
    if actual != expected:
        missing = sorted(set(expected).difference(actual))
        unexpected = sorted(set(actual).difference(expected))
        raise ValidationError(
            f"{relation_name} schema/order mismatch: width={len(actual)}, "
            f"expected_width={len(expected)}, missing={missing}, unexpected={unexpected}"
        )


def build_rn_obt(
    context_observations: DataFrame,
    labels: DataFrame,
    args: JobArguments,
) -> DataFrame:
    window_df = build_window_statistics(context_observations)
    derived = build_derived_temporal_features(window_df)
    output_features = apply_output_date_scope(derived, args)
    rn_df = output_features.join(labels, on="SMART_DAILY_KEY", how="inner").select(
        *rn_final_columns()
    )
    assert_exact_schema(rn_df, rn_final_columns(), "generated OBT_MC1_RN")
    if len(rn_df.columns) != EXPECTED_RN_WIDTH:
        raise ValidationError(
            f"Generated RN width is {len(rn_df.columns)}, expected {EXPECTED_RN_WIDTH}."
        )
    return rn_df


def _any_of(expressions: Sequence[Column]) -> Column:
    if not expressions:
        return F.lit(False)
    return reduce(lambda left, right: left | right, expressions)


def _all_of(expressions: Sequence[Column]) -> Column:
    if not expressions:
        return F.lit(True)
    return reduce(lambda left, right: left & right, expressions)


def _count_where(df: DataFrame, predicate: Column, alias: str) -> int:
    value = collect_scalar_aggregate(
        df,
        (
            F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))).alias(alias),
        ),
    )[alias]
    return int(value or 0)


def _rolling_count_invalid_predicate() -> Column:
    invalid: list[Column] = []
    for representation in REPRESENTATIONS:
        for smart_id in SMART_IDS:
            base = f"{representation}_{smart_id}"
            for days in WINDOW_DAYS:
                for count_name in (
                    f"{base}_COUNT_{days}D",
                    f"{base}_PREV_COUNT_{days}D",
                ):
                    count_column = F.col(count_name)
                    invalid.append(
                        count_column.isNull()
                        | (count_column < F.lit(0))
                        | (count_column > F.lit(days))
                    )
    return _any_of(invalid)


def _mean_invalid_predicate() -> Column:
    invalid: list[Column] = []
    for representation in REPRESENTATIONS:
        for smart_id in SMART_IDS:
            base = f"{representation}_{smart_id}"
            for days in WINDOW_DAYS:
                count_column = F.col(f"{base}_COUNT_{days}D")
                mean_column = F.col(f"{base}_MEAN_{days}D")
                valid_column = F.col(f"{base}_MEAN_VALID_{days}D")
                invalid.append(
                    ((count_column == F.lit(0)) & mean_column.isNotNull())
                    | ((count_column > F.lit(0)) & mean_column.isNull())
                    | valid_column.isNull()
                    | (
                        valid_column
                        != F.when(count_column > F.lit(0), F.lit(1)).otherwise(F.lit(0))
                    )
                )
    return _any_of(invalid)


def _shift_invalid_predicate() -> Column:
    invalid: list[Column] = []
    for representation in REPRESENTATIONS:
        for smart_id in SMART_IDS:
            base = f"{representation}_{smart_id}"
            for days in WINDOW_DAYS:
                current_count = F.col(f"{base}_COUNT_{days}D")
                previous_count = F.col(f"{base}_PREV_COUNT_{days}D")
                shift = F.col(f"{base}_MEAN_SHIFT_{days}D")
                valid = F.col(f"{base}_MEAN_SHIFT_VALID_{days}D")
                should_exist = (current_count > F.lit(0)) & (
                    previous_count > F.lit(0)
                )
                invalid.append(
                    (should_exist & shift.isNull())
                    | ((~should_exist) & shift.isNotNull())
                    | valid.isNull()
                    | (
                        valid
                        != F.when(should_exist, F.lit(1)).otherwise(F.lit(0))
                    )
                )
    return _any_of(invalid)


def _rn_count_mismatch_predicate() -> Column:
    mismatches: list[Column] = []
    for smart_id in SMART_IDS:
        for days in WINDOW_DAYS:
            mismatches.extend(
                (
                    F.col(f"R_{smart_id}_COUNT_{days}D")
                    != F.col(f"N_{smart_id}_COUNT_{days}D"),
                    F.col(f"R_{smart_id}_PREV_COUNT_{days}D")
                    != F.col(f"N_{smart_id}_PREV_COUNT_{days}D"),
                )
            )
    return _any_of(mismatches)


def _label_invalid_predicate() -> Column:
    expected_days = F.datediff(
        F.col("FIRST_FAILURE_DATE"), F.col("OBSERVATION_DATE")
    )
    return (
        ~F.col("DAYS_TO_FAILURE").eqNullSafe(expected_days)
        | ~F.col("LABEL_STATUS").eqNullSafe(_expected_label_status())
        | ~F.col("TARGET_30D").eqNullSafe(_expected_target())
    )


def validate_rn_stage(
    rn_stage: DataFrame,
    expected_row_count: int,
    metrics: AuditMetrics,
) -> None:
    assert_exact_schema(rn_stage, rn_final_columns(), "persisted OBT_MC1_RN stage")
    if len(rn_stage.columns) != EXPECTED_RN_WIDTH:
        raise ValidationError(
            f"Persisted RN width is {len(rn_stage.columns)}, expected {EXPECTED_RN_WIDTH}."
        )

    validation_metrics = collect_scalar_aggregate(
        rn_stage,
        (
            F.count(F.lit(1)).alias("ROW_COUNT"),
            F.countDistinct("SMART_DAILY_KEY").alias("DISTINCT_KEY_COUNT"),
            F.countDistinct("SSD_KEY", "OBSERVATION_DATE").alias(
                "DISTINCT_SSD_DATE_COUNT"
            ),
            F.countDistinct("SSD_KEY").alias("DISTINCT_SSD_COUNT"),
            *(
                F.sum(
                    F.when(
                        F.col("LABEL_STATUS") == F.lit(status), F.lit(1)
                    ).otherwise(F.lit(0))
                ).alias(f"{status}_COUNT")
                for status in (
                    "POSITIVE",
                    "NEGATIVE",
                    "CENSORED",
                    "SAME_DAY_FAILURE",
                    "POST_FAILURE",
                )
            ),
            F.sum(
                F.when(_rolling_count_invalid_predicate(), F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("INVALID_ROLLING"),
            F.sum(
                F.when(_mean_invalid_predicate(), F.lit(1)).otherwise(F.lit(0))
            ).alias("INVALID_MEAN"),
            F.sum(
                F.when(_shift_invalid_predicate(), F.lit(1)).otherwise(F.lit(0))
            ).alias("INVALID_SHIFT"),
            F.sum(
                F.when(_rn_count_mismatch_predicate(), F.lit(1)).otherwise(F.lit(0))
            ).alias("RN_COUNT_MISMATCH"),
            F.sum(
                F.when(_label_invalid_predicate(), F.lit(1)).otherwise(F.lit(0))
            ).alias("INVALID_LABEL"),
            F.sum(
                F.when(
                    F.col("MODEL_CODE").isNull()
                    | (F.col("MODEL_CODE") != F.lit("MC1")),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("INVALID_MODEL"),
            F.sum(
                F.when(F.col("DISK_ID").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("MISSING_DISK_ID"),
            F.sum(
                F.when(F.col("LAST_SEEN_DATE").isNull(), F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("MISSING_LAST_SEEN"),
            F.sum(
                F.when(
                    _all_of(
                        [F.col(column).isNull() for column in smart_columns("R")]
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("ALL_R_NULL"),
            F.sum(
                F.when(
                    _all_of(
                        [F.col(column).isNull() for column in smart_columns("N")]
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("ALL_N_NULL"),
        ),
    )
    identity = {
        name: int(validation_metrics[name] or 0)
        for name in (
            "ROW_COUNT",
            "DISTINCT_KEY_COUNT",
            "DISTINCT_SSD_DATE_COUNT",
            "DISTINCT_SSD_COUNT",
        )
    }
    duplicate_keys = identity["ROW_COUNT"] - identity["DISTINCT_KEY_COUNT"]
    duplicate_ssd_dates = identity["ROW_COUNT"] - identity["DISTINCT_SSD_DATE_COUNT"]
    invalid_rolling = int(validation_metrics["INVALID_ROLLING"] or 0)
    invalid_mean = int(validation_metrics["INVALID_MEAN"] or 0)
    invalid_shift = int(validation_metrics["INVALID_SHIFT"] or 0)
    mismatch_count = int(validation_metrics["RN_COUNT_MISMATCH"] or 0)
    invalid_labels = int(validation_metrics["INVALID_LABEL"] or 0)
    invalid_models = int(validation_metrics["INVALID_MODEL"] or 0)
    missing_disk_id = int(validation_metrics["MISSING_DISK_ID"] or 0)
    missing_last_seen = int(validation_metrics["MISSING_LAST_SEEN"] or 0)
    all_r_null = int(validation_metrics["ALL_R_NULL"] or 0)
    all_n_null = int(validation_metrics["ALL_N_NULL"] or 0)

    metrics.set(
        MC1_OBT_ROW_COUNT=identity["ROW_COUNT"],
        MC1_OBT_DISTINCT_SSD_COUNT=identity["DISTINCT_SSD_COUNT"],
        POSITIVE_COUNT=int(validation_metrics["POSITIVE_COUNT"] or 0),
        NEGATIVE_COUNT=int(validation_metrics["NEGATIVE_COUNT"] or 0),
        CENSORED_COUNT=int(validation_metrics["CENSORED_COUNT"] or 0),
        SAME_DAY_FAILURE_COUNT=int(
            validation_metrics["SAME_DAY_FAILURE_COUNT"] or 0
        ),
        POST_FAILURE_COUNT=int(validation_metrics["POST_FAILURE_COUNT"] or 0),
        DUPLICATE_SMART_DAILY_KEY_COUNT=duplicate_keys,
        R_N_COUNT_MISMATCH_COUNT=mismatch_count,
        INVALID_ROLLING_COUNT_COUNT=invalid_rolling,
        INVALID_MEAN_COUNT=invalid_mean,
        INVALID_SHIFT_COUNT=invalid_shift,
    )

    blocking = {
        "row count mismatch": identity["ROW_COUNT"] != expected_row_count,
        "duplicate SMART_DAILY_KEY": duplicate_keys != 0,
        "duplicate SSD/date": duplicate_ssd_dates != 0,
        "invalid rolling count": invalid_rolling != 0,
        "invalid mean/count consistency": invalid_mean != 0,
        "invalid shift consistency": invalid_shift != 0,
        "invalid labels": invalid_labels != 0,
        "non-MC1 model": invalid_models != 0,
        "missing DISK_ID": missing_disk_id != 0,
        "missing LAST_SEEN_DATE": missing_last_seen != 0,
        "all-current-R-null row": all_r_null != 0,
        "all-current-N-null row": all_n_null != 0,
    }
    failures = [name for name, failed in blocking.items() if failed]
    if failures:
        raise ValidationError(
            "Persisted RN stage failed blocking validations: " + ", ".join(failures)
        )
    if mismatch_count:
        LOGGER.warning(
            "RN contains %s rows with at least one R/N current or previous count mismatch.",
            mismatch_count,
        )


def write_stage(df: DataFrame, stage_fqn: str) -> None:
    LOGGER.info("Writing run-scoped stage %s", stage_fqn)
    df.write.mode("overwrite").saveAsTable(stage_fqn)


def validate_variant_stage(
    variant: DataFrame,
    representation: str,
    expected_row_count: int,
) -> None:
    expected_columns = r_final_columns() if representation == "R" else n_final_columns()
    assert_exact_schema(
        variant, expected_columns, f"persisted OBT_MC1_{representation} stage"
    )
    if len(variant.columns) != EXPECTED_VARIANT_WIDTH:
        raise ValidationError(
            f"Persisted {representation} width is {len(variant.columns)}, "
            f"expected {EXPECTED_VARIANT_WIDTH}."
        )
    forbidden_prefix = "N_" if representation == "R" else "R_"
    forbidden = [
        column for column in variant.columns if column.upper().startswith(forbidden_prefix)
    ]
    if forbidden:
        raise ValidationError(
            f"{representation} stage contains forbidden representation columns: {forbidden}"
        )
    if variant.count() != expected_row_count:
        raise ValidationError(f"{representation} stage row count does not match RN.")


def validate_variant_parity(
    rn_stage: DataFrame, r_stage: DataFrame, n_stage: DataFrame
) -> None:
    trace_columns = [*COMMON_COLUMNS, *TARGET_COLUMNS]
    rn_trace = rn_stage.select(*trace_columns)
    for name, variant in (("R", r_stage), ("N", n_stage)):
        variant_trace = variant.select(*trace_columns)
        missing = rn_trace.exceptAll(variant_trace).limit(1).count()
        unexpected = variant_trace.exceptAll(rn_trace).limit(1).count()
        if missing or unexpected:
            raise ValidationError(
                f"{name} stage does not preserve the RN key/metadata/target population."
            )


def log_year_label_diagnostics(rn_stage: DataFrame) -> None:
    diagnostics = (
        rn_stage.groupBy(
            F.year("OBSERVATION_DATE").alias("SOURCE_YEAR"), "LABEL_STATUS"
        )
        .count()
        .orderBy("SOURCE_YEAR", "LABEL_STATUS")
        .collect()
    )
    for row in diagnostics:
        LOGGER.info(
            "Label diagnostic: year=%s status=%s count=%s",
            row["SOURCE_YEAR"],
            row["LABEL_STATUS"],
            row["count"],
        )


def promote_stage(
    session: SnowflakeSession,
    stage_fqn: str,
    canonical_fqn: str,
) -> None:
    execute_snowflake_sql(
        session,
        f"CREATE OR REPLACE TABLE {canonical_fqn} CLONE {stage_fqn} COPY GRANTS",
    )


def cleanup_staging(session: SnowflakeSession, relations: RelationNames) -> None:
    for stage_fqn in (relations.rn_stage, relations.r_stage, relations.n_stage):
        execute_snowflake_sql(session, f"DROP TABLE IF EXISTS {stage_fqn}")


def sanitize_error(error: BaseException, config: JobConfig | None) -> str:
    message = f"{type(error).__name__}: {error}"
    if config:
        for sensitive_value in config.sensitive_values:
            message = message.replace(sensitive_value, "<redacted>")
    message = re.sub(
        r"(?i)(password|token|secret)\s*[=:]\s*[^,;\s]+",
        r"\1=<redacted>",
        message,
    )
    return message[:4000]


def _audit_schema() -> StructType:
    fields = [
        StructField("RUN_ID", StringType(), False),
        StructField("POPULATION_CODE", StringType(), False),
        StructField("STARTED_AT_UTC", TimestampType(), False),
        StructField("COMPLETED_AT_UTC", TimestampType(), False),
        StructField("RUN_STATUS", StringType(), False),
    ]
    fields.extend(StructField(name, LongType(), True) for name in AUDIT_METRIC_NAMES)
    fields.append(StructField("ERROR_MESSAGE", StringType(), True))
    return StructType(fields)


def write_audit_row(
    spark: SparkSession,
    audit_fqn: str,
    run: RunContext,
    run_status: str,
    metrics: AuditMetrics,
    error_message: str | None,
) -> None:
    completed_at = datetime.now(timezone.utc)
    values: list[object] = [
        run.run_id,
        "MC1",
        run.started_at_utc,
        completed_at,
        run_status,
    ]
    values.extend(metrics.values[name] for name in AUDIT_METRIC_NAMES)
    values.append(error_message)
    audit_df = spark.createDataFrame([tuple(values)], schema=_audit_schema())
    audit_df.write.mode("append").saveAsTable(audit_fqn)


def stop_spark_session(spark: SparkSession | None) -> None:
    if spark is None:
        return
    try:
        spark.stop()
    except Exception:
        LOGGER.warning("Unable to stop the Snowpark Connect session cleanly.")


def _load_sources(
    spark: SparkSession, relations: RelationNames
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    raw_smart = spark.table(relations.smart_fact)
    validate_smart_registry(raw_smart, relations.smart_fact)
    smart_fact = normalize_required_columns(
        raw_smart,
        (
            "SMART_DAILY_KEY",
            "SSD_KEY",
            "OBSERVATION_DATE_KEY",
            "OBSERVATION_DATE",
            *smart_columns(),
        ),
        relations.smart_fact,
    )
    dim_ssd = normalize_required_columns(
        spark.table(relations.dim_ssd),
        ("SSD_KEY", "DISK_ID", "MODEL_CODE"),
        relations.dim_ssd,
    )
    dim_date = normalize_required_columns(
        spark.table(relations.dim_date),
        ("DATE_KEY", "FULL_DATE"),
        relations.dim_date,
    )
    failures = normalize_required_columns(
        spark.table(relations.failure_fact),
        (
            "FAILURE_EVENT_KEY",
            "SSD_KEY",
            "FAILURE_DATE_KEY",
            "FAILURE_DATE",
            "FAILURE_AT",
            "FAILURE_COUNT",
        ),
        relations.failure_fact,
    )
    return smart_fact, dim_ssd, dim_date, failures


def run_job(args: JobArguments) -> int:
    config: JobConfig | None = None
    run = generate_run_context()
    spark: SparkSession | None = None
    snowflake_session: SnowflakeSession | None = None
    relations: RelationNames | None = None
    metrics = AuditMetrics()
    audit_ready = False
    published: list[str] = []

    try:
        config = load_config()
        relations = relation_names(config, run)
        spark, snowflake_session = create_spark_session(config, run)
        ensure_output_schema(snowflake_session, config)
        ensure_audit_table(snowflake_session, relations.audit)
        audit_ready = True

        LOGGER.info(
            "Starting MC1 OBT run_id=%s mode=%s application=%s warehouse=%s",
            run.run_id,
            "PRODUCTION" if args.is_production else "NON_PRODUCTION",
            run.app_name,
            config.warehouse,
        )
        LOGGER.info(
            "Run-scoped stages: RN=%s R=%s N=%s",
            relations.rn_stage,
            relations.r_stage,
            relations.n_stage,
        )

        smart_fact, dim_ssd, dim_date, failures = _load_sources(spark, relations)
        context_fact, output_fact, selected_ssds = apply_development_scope(
            smart_fact, args
        )
        context_grain = validate_source_grain(context_fact, "feature-context")
        output_grain = (
            context_grain
            if context_fact is output_fact
            else validate_source_grain(output_fact, "requested-output")
        )
        metrics.set(
            GOLD_OBSERVATION_COUNT=output_grain["ROW_COUNT"],
            GOLD_SSD_COUNT=output_grain["DISTINCT_SSD_COUNT"],
        )

        validate_dimension_relationships(context_fact, dim_ssd, dim_date)
        context_base = build_base_observations(
            context_fact, dim_ssd, dim_date, context_grain["ROW_COUNT"]
        )
        output_base = apply_output_date_scope(context_base, args)
        if output_base.count() != output_grain["ROW_COUNT"]:
            raise ValidationError(
                "Dimension-enriched output count does not match the scoped Gold fact."
            )

        scoped_failures = failures
        if selected_ssds is not None:
            scoped_failures = failures.join(selected_ssds, on="SSD_KEY", how="inner")
        validate_failure_relationships(scoped_failures, dim_ssd, dim_date)
        failure_summary = build_failure_summary(scoped_failures)
        multiple_failure_ssds = _count_where(
            failure_summary,
            F.col("FAILURE_EVENT_COUNT") > F.lit(1),
            "MULTIPLE_FAILURE_SSDS",
        )
        metrics.set(MULTIPLE_FAILURE_SSD_COUNT=multiple_failure_ssds)
        if multiple_failure_ssds:
            LOGGER.warning(
                "%s SSDs have more than one recorded failure; the first failure is authoritative.",
                multiple_failure_ssds,
            )

        labels = build_labels(
            output_base,
            failure_summary,
            build_last_seen(context_base),
        )
        rn_df = build_rn_obt(context_base, labels, args)
        if args.explain_plan:
            rn_df.explain(extended=True)

        if args.validate_only:
            LOGGER.info(
                "Validate-only completed: source contracts and generated %s-column RN schema are valid; no stages or audit row were written.",
                len(rn_df.columns),
            )
            stop_spark_session(spark)
            return 0

        write_stage(rn_df, relations.rn_stage)
        rn_stage = spark.table(relations.rn_stage)
        validate_rn_stage(rn_stage, output_grain["ROW_COUNT"], metrics)

        r_projection = rn_stage.select(*r_final_columns())
        n_projection = rn_stage.select(*n_final_columns())
        write_stage(r_projection, relations.r_stage)
        write_stage(n_projection, relations.n_stage)

        r_stage = spark.table(relations.r_stage)
        n_stage = spark.table(relations.n_stage)
        expected_rows = int(metrics.values["MC1_OBT_ROW_COUNT"] or 0)
        validate_variant_stage(r_stage, "R", expected_rows)
        validate_variant_stage(n_stage, "N", expected_rows)
        validate_variant_parity(rn_stage, r_stage, n_stage)
        log_year_label_diagnostics(rn_stage)

        success_note: str | None = None
        if args.is_bounded:
            success_note = (
                "NON_PRODUCTION: canonical publication skipped; validated run-scoped "
                "staging tables retained."
            )
            LOGGER.info(success_note)
        else:
            for stage_fqn, canonical_fqn in (
                (relations.rn_stage, relations.rn_canonical),
                (relations.r_stage, relations.r_canonical),
                (relations.n_stage, relations.n_canonical),
            ):
                promote_stage(snowflake_session, stage_fqn, canonical_fqn)
                published.append(canonical_fqn)
                LOGGER.info("Published %s from %s", canonical_fqn, stage_fqn)

        write_audit_row(
            spark,
            relations.audit,
            run,
            "SUCCEEDED",
            metrics,
            success_note,
        )

        if args.is_production and not args.keep_staging:
            try:
                cleanup_staging(snowflake_session, relations)
            except Exception:
                LOGGER.warning(
                    "Canonical publication and audit succeeded, but staging cleanup failed; retaining the run-scoped tables."
                )
        elif args.keep_staging:
            LOGGER.info("Retaining staging tables because --keep-staging was supplied.")

        LOGGER.info("MC1 OBT run %s completed successfully.", run.run_id)
        stop_spark_session(spark)
        return 0

    except ValidationError as error:
        status = "FAILED_VALIDATION"
        safe_error = sanitize_error(error, config)
        LOGGER.error("%s", safe_error)
    except Exception as error:  # Snowpark/connector failures are runtime failures.
        status = "FAILED_EXECUTION"
        safe_error = sanitize_error(error, config)
        publication_note = (
            f"; canonical tables published before failure={published}" if published else ""
        )
        safe_error = (safe_error + publication_note)[:4000]
        LOGGER.error("%s", safe_error)

    if (
        spark is not None
        and relations is not None
        and audit_ready
        and not args.validate_only
    ):
        try:
            write_audit_row(
                spark,
                relations.audit,
                run,
                status,
                metrics,
                safe_error,
            )
        except Exception as audit_error:
            LOGGER.error(
                "Unable to persist failure audit row: %s",
                sanitize_error(audit_error, config),
            )
    LOGGER.error("Run-scoped staging tables are retained when they exist.")
    stop_spark_session(spark)
    return 2 if status == "FAILED_VALIDATION" else 3


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args(argv)
    return run_job(args)


if __name__ == "__main__":
    sys.exit(main())
