"""Build PostgreSQL staging tables from Bronze using the design CSV metadata."""

from __future__ import annotations

import re
from collections import defaultdict

from prefect import flow
from pyspark.sql import DataFrame, SparkSession, functions as F
from prefect.task_runners import SequentialTaskRunner

from cli.flows.metadata import PROJECT_ROOT, load_config, load_design_metadata, load_stack_config
from log.config.logger_setup import logger_manager
from platforms.processing.config.delta_utils import write_jdbc

logger = logger_manager.get_logger(__name__)
DERIVED = {"derived", "system timestamp", "system flag"}


def _bronze_path(stack: dict, source: str) -> str:
    minio = stack["minio"]
    return f"s3a://{minio['bucket']}/{minio['bronze_prefix']}/{source}"


def _source_refs(rows: list[dict[str, str]]) -> dict[str, set[str]]:
    refs: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        value = row.get("Source_Table.Column", "").strip()
        if not value or value.lower() in DERIVED:
            continue
        for item in re.split(r",|\s+/\s+", value):
            source, separator, column = item.strip().partition(".")
            if separator:
                column = {
                    "SK_BUREAU_ID": "SK_ID_BUREAU",
                }.get(column.strip(), column.strip())
                refs[source.lower()].add(column)
    return refs


def _read_sources(
    spark: SparkSession, stack: dict, refs: dict[str, set[str]]
) -> DataFrame:
    if not refs:
        raise ValueError("Target has no source mapping")
    source_names = list(refs)
    base_name = source_names[0]
    frame = spark.read.parquet(_bronze_path(stack, base_name))
    frame = frame.select(*[F.col(column) for column in refs[base_name]])

    for source_name in source_names[1:]:
        right = spark.read.parquet(_bronze_path(stack, source_name))
        right = right.select(*[F.col(column) for column in refs[source_name]])
        common = sorted(set(frame.columns).intersection(right.columns))
        if not common:
            raise ValueError(
                f"Cannot infer join between {base_name} and {source_name}; "
                "define an explicit join in the design contract."
            )
        join_key = "SK_ID_CURR" if "SK_ID_CURR" in common else common[0]
        right = right.select(
            *[
                F.col(column).alias(f"{source_name}__{column}")
                if column != join_key else F.col(column)
                for column in right.columns
            ]
        )
        frame = frame.join(right, on=join_key, how="left")
    return frame


def _expression(
    row: dict[str, str], source_columns: dict[str, set[str]], base_source: str
) -> F.Column:
    source = row.get("Source_Table.Column", "").strip()
    logic = row.get("Transformation_Logic", "").strip()
    column = row["Column_Name"].strip()
    if source.lower() in DERIVED:
        if source.lower() == "system timestamp":
            return F.current_timestamp().alias(column)
        if source.lower() == "system flag":
            return F.lit("Y").alias(column)
        return F.monotonically_increasing_id().alias(column)
    source_name, separator, source_column = source.partition(".")
    if separator and source_name.lower() in source_columns:
        source_column = {
            "SK_BUREAU_ID": "SK_ID_BUREAU",
        }.get(source_column, source_column)
        source_column = (
            source_column
            if source_name.lower() == base_source
            else f"{source_name.lower()}__{source_column}"
        )
    expression = logic or source_column
    if re.search(r"lookup|join|nối|nếu|tên chuẩn hóa|identity", expression, re.I):
        expression = source_column
    for source_name, columns in source_columns.items():
        if source_name == base_source:
            continue
        for source_column_name in columns:
            expression = re.sub(
                rf"\b{re.escape(source_column_name)}\b",
                f"{source_name}__{source_column_name}",
                expression,
                flags=re.I,
            )
    expression = re.sub(
        r"DENSE_RANK\(\).*?(?:hoặc|ou).*?Sequence",
        "monotonically_increasing_id()",
        expression,
        flags=re.I,
    )
    if expression.lower().startswith("distinct trim("):
        expression = "trim(NAME_CONTRACT_TYPE)"
    result = F.expr(expression)
    target_type = row.get("Column_Type", "").strip().upper()
    if target_type.startswith("DECIMAL"):
        result = result.cast(target_type.replace("DECIMAL", "decimal", 1))
    elif target_type in {"INT", "BIGINT", "SMALLINT", "TINYINT", "BOOLEAN", "DATE", "TIMESTAMP"}:
        result = result.cast(target_type.lower())
    elif target_type.startswith("VARCHAR") or target_type.startswith("CHAR"):
        result = result.cast("string")
    return result.alias(column)


def build_target(spark: SparkSession, stack: dict, rows: list[dict[str, str]]) -> DataFrame:
    refs = _source_refs(rows)
    frame = _read_sources(spark, stack, refs)
    base_source = next(iter(refs))
    return frame.select(
        *[_expression(row, refs, base_source) for row in rows]
    )


def _build_date_dimension(spark: SparkSession, config: dict) -> DataFrame:
    date_config = config["staging"]["date_dimension"]
    dates = spark.range(1).select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(date_config["start_date"])),
                F.to_date(F.lit(date_config["end_date"])),
            )
        ).alias("Full_Date")
    )
    return dates.select(
        F.date_format("Full_Date", "yyyyMMdd").cast("int").alias("Date_SK"),
        "Full_Date",
        F.dayofmonth("Full_Date").cast("tinyint").alias("Day_of_Month"),
        F.month("Full_Date").cast("tinyint").alias("Month_Number"),
        F.date_format("Full_Date", "MMMM").alias("Month_Name"),
        F.quarter("Full_Date").cast("tinyint").alias("Quarter_Number"),
        F.year("Full_Date").cast("smallint").alias("Year_Number"),
        (F.dayofweek("Full_Date").isin([1, 7])).alias("Is_Weekend"),
    )


def _postgres_config(config: dict) -> dict:
    postgres = dict(config["postgres"])
    postgres["dbname"] = postgres["database"]
    postgres["schema"] = config["postgres"]["schema"]
    return postgres


def _ensure_schema(config: dict) -> None:
    import psycopg2

    pg = config["postgres"]
    with psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["user"], password=pg["password"],
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg["schema"]}"')


@flow(
    name="Bronze To PostgreSQL Staging",
    log_prints=False,
    task_runner=SequentialTaskRunner(),
    description="Transform MinIO Bronze into PostgreSQL tables from design metadata.",
)
def staging_transform_flow() -> list[str]:
    config = load_config()["project_params"]
    stack = load_stack_config()
    rows_by_target: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in load_design_metadata():
        target = row.get("Target_Table_Name", "").strip()
        if target and row.get("Column_Name", "").strip():
            rows_by_target[target].append(row)

    _ensure_schema(config)
    spark = _create_spark(stack)
    outputs = []
    for target, rows in rows_by_target.items():
        if not _source_refs(rows):
            if target == "Dim_Date":
                frame = _build_date_dimension(spark, config)
                write_jdbc(
                    frame,
                    _postgres_config(config),
                    target,
                    config["postgres"]["write_mode"],
                )
                outputs.append(target)
                logger.info("staging_date_dimension_completed target=%s", target)
                continue
            logger.warning(
                "staging_target_skipped target=%s reason=no_source_mapping",
                target,
            )
            continue
        logger.info("staging_transform_started target=%s", target)
        frame = build_target(spark, stack, rows)
        write_jdbc(frame, _postgres_config(config), target, config["postgres"]["write_mode"])
        outputs.append(target)
        logger.info("staging_transform_completed target=%s", target)
    spark.stop()
    return outputs


def _create_spark(stack: dict) -> SparkSession:
    from platforms.processing.spark_stack.spark_session import get_spark_session

    return get_spark_session(stack)


if __name__ == "__main__":
    staging_transform_flow()
