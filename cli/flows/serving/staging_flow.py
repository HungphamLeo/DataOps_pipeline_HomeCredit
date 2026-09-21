"""Config-driven Bronze to Staging transformations.

The flow contains only generic dataframe operations. Source-to-target mappings,
recipes, formulas and constants live in ``homecredit_config.yaml`` and are
validated against ``Design modeling_doc.csv``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

from cli.flows.metadata import (
    PROJECT_ROOT,
    load_config,
    load_design_metadata,
    load_source_columns,
    validate_recipe,
)
from cli.flows.serving.ge_validator import run_ge_checkpoint
from log.config.logger_setup import logger_manager
from platforms.processing.config.delta_utils import write_jdbc
from platforms.processing.spark_stack.spark_session import get_spark_session

logger = logger_manager.get_logger(__name__)


def _constant(value: object, config: dict) -> object:
    if isinstance(value, dict) and "constant" in value:
        return config["constants"][value["constant"]]
    return value


def _latest_bronze(config: dict, table_name: str) -> pd.DataFrame:
    bronze = config["bronze"]
    table_dir = PROJECT_ROOT / config["paths"]["bronze_dir"] / f"{bronze['output_prefix']}{table_name}"
    partitions = sorted(
        path for path in table_dir.glob(f"{bronze['partition_column']}=*") if path.is_dir()
    )
    if not partitions:
        raise FileNotFoundError(f"No Bronze partition found for source '{table_name}'")
    path = partitions[-1] / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Bronze data file does not exist: {path}")
    logger.info("staging_source_read_started table=%s path=%s", table_name, path)
    frame = pd.read_parquet(path)
    logger.info("staging_source_read_completed table=%s rows=%d", table_name, len(frame))
    return frame


def _apply_transforms(frame: pd.DataFrame, recipe: dict, config: dict) -> pd.DataFrame:
    frame = frame.copy()
    for transform in recipe.get("transforms", []):
        operation = transform["op"]
        if operation == "replace":
            source = _constant(transform["from"], config)
            frame[transform["column"]] = frame[transform["column"]].replace(
                source, _constant(transform.get("to"), config)
            )
        elif operation == "flag_equals":
            frame[transform["output"]] = (
                frame[transform["column"]] == _constant(transform["value"], config)
            ).astype("int8")
        elif operation == "divide":
            denominator = _constant(transform["denominator"], config)
            frame[transform["output"]] = (
                frame[transform["numerator"]] * transform.get("multiplier", 1)
                / denominator
            )
        elif operation == "ratio":
            denominator = frame[transform["denominator"]].replace(0, np.nan)
            frame[transform["output"]] = frame[transform["numerator"]] / denominator
        elif operation == "subtract":
            frame[transform["output"]] = (
                frame[transform["left"]] - frame[transform["right"]]
            )
        elif operation == "greater_than_flag":
            frame[transform["output"]] = (
                frame[transform["column"]] > _constant(transform["value"], config)
            ).astype("int8")
        elif operation == "clip_subtract":
            frame[transform["output"]] = (
                frame[transform["left"]] - frame[transform["right"]]
            ).clip(lower=_constant(transform["lower"], config))
        else:
            raise ValueError(f"Unsupported configured transform operation: {operation}")
    return frame.replace([np.inf, -np.inf], np.nan)


def _apply_pre_aggregations(
    frame: pd.DataFrame, recipe: dict, join_key: str
) -> pd.DataFrame:
    if not recipe.get("pre_aggregations"):
        return frame
    results = frame[[join_key]].drop_duplicates().set_index(join_key)
    for aggregation in recipe["pre_aggregations"]:
        series = frame[aggregation["source"]].astype(str)
        if aggregation["operation"] != "mean_in":
            raise ValueError(f"Unsupported pre-aggregation: {aggregation['operation']}")
        values = set(aggregation["values"])
        result = series.isin(values).groupby(frame[join_key]).mean()
        results[aggregation["output"]] = result
    return results.reset_index()


def _aggregate(frame: pd.DataFrame, recipe: dict) -> pd.DataFrame:
    group_by = recipe["group_by"]
    grouped = frame.groupby(group_by, dropna=False)
    output = frame[group_by].drop_duplicates().set_index(group_by)
    for name, specification in recipe.get("aggregations", {}).items():
        column = specification["column"]
        operation = specification["operation"]
        series = frame[column]
        if operation == "count":
            result = grouped[column].count()
        elif operation == "sum":
            result = grouped[column].sum()
        elif operation == "mean":
            result = grouped[column].mean()
        elif operation == "max":
            result = grouped[column].max()
        elif operation == "nunique":
            result = grouped[column].nunique()
        elif operation == "count_equals":
            result = series.eq(specification["value"]).groupby(
                [frame[key] for key in group_by]
            ).sum()
        elif operation == "count_greater_than":
            result = series.gt(specification["value"]).groupby(
                [frame[key] for key in group_by]
            ).sum()
        elif operation == "mean_positive":
            positive = series.where(series > 0)
            result = positive.groupby(
                [frame[key] for key in group_by]
            ).mean()
        else:
            raise ValueError(f"Unsupported configured aggregation: {operation}")
        output[name] = result
    return output.reset_index()


def build_recipe(recipe: dict, config: dict) -> pd.DataFrame:
    inputs = {name: _latest_bronze(config, name) for name in recipe["inputs"]}
    frame = inputs[recipe["inputs"][0]]

    join = recipe.get("join")
    if join:
        if len(recipe["inputs"]) != 2:
            raise ValueError("Configured join recipes must have exactly two inputs")
        right_name = recipe["inputs"][1]
        right = _apply_pre_aggregations(
            inputs[right_name], recipe, join["right_on"]
        )
        frame = frame.merge(
            right,
            how=join["how"],
            left_on=join["left_on"],
            right_on=join["right_on"],
            suffixes=("", f"_{right_name}"),
        )

    frame = _apply_transforms(frame, recipe, config)
    return _aggregate(frame, recipe) if recipe.get("aggregations") else frame


def _spark_config(config: dict) -> dict:
    """Build the technology configuration required by the shared Spark factory."""
    stack_path = PROJECT_ROOT / "platforms" / "config" / "stack.yaml"
    import yaml

    with stack_path.open(encoding="utf-8") as file:
        stack_config = yaml.safe_load(file) or {}
    return stack_config


def _postgres_config(config: dict) -> dict:
    postgres = dict(config["postgres"])
    postgres["dbname"] = postgres["database"]
    postgres["schema"] = config["staging"]["output_schema"]
    return postgres


def _ensure_postgres_schema(config: dict) -> None:
    import psycopg2

    postgres = config["postgres"]
    schema = config["staging"]["output_schema"]
    logger.info("postgres_schema_check_started schema=%s", schema)
    with psycopg2.connect(
        host=postgres["host"],
        port=postgres["port"],
        dbname=postgres["database"],
        user=postgres["user"],
        password=postgres["password"],
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    logger.info("postgres_schema_check_completed schema=%s", schema)


@task(log_prints=False)
def transform_and_save(table_name: str, recipe: dict, config: dict) -> str:
    frame = build_recipe(recipe, config)
    staging = config["staging"]
    _ensure_postgres_schema(config)
    output_path = (
        PROJECT_ROOT / config["paths"]["staging_dir"] / table_name / "data.parquet"
    )
    if staging["write_intermediate_parquet"]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(
            output_path,
            engine=config["runtime"]["parquet_engine"],
            index=False,
        )

    spark = get_spark_session(_spark_config(config))
    spark_frame = spark.createDataFrame(frame)
    write_jdbc(
        spark_frame,
        _postgres_config(config),
        table_name,
        mode=config["postgres"]["write_mode"],
    )
    logger.info(
        "staging_table_saved table=%s rows=%d columns=%d schema=%s intermediate_parquet=%s",
        table_name,
        len(frame),
        len(frame.columns),
        config["staging"]["output_schema"],
        staging["write_intermediate_parquet"],
    )
    return table_name


@flow(
    name="Staging Transform Flow",
    log_prints=False,
    task_runner=SequentialTaskRunner(),
    description="Execute metadata-driven Bronze to PostgreSQL Staging recipes.",
)
def staging_transform_flow() -> list[str]:
    config = load_config()["project_params"]
    metadata = load_design_metadata()
    source_columns = load_source_columns(config)
    recipes = config["staging"]["recipes"]
    if not recipes:
        raise ValueError("No Staging recipes configured")

    for recipe in recipes.values():
        validate_recipe(recipe, metadata, source_columns)

    _ensure_postgres_schema(config)
    logger.info("staging_flow_started recipe_count=%d", len(recipes))
    task_options = transform_and_save.with_options(
        retries=config["runtime"]["retries"],
        retry_delay_seconds=config["runtime"]["retry_delay_seconds"],
    )
    outputs = [
        task_options.submit(table_name, recipe, config).result()
        for table_name, recipe in recipes.items()
    ]

    ge = config["great_expectations"]
    if ge["enabled"]:
        run_ge_checkpoint.submit(
            checkpoint_name=ge["staging_checkpoint"],
            ge_root_dir=str(PROJECT_ROOT / config["paths"]["ge_root_dir"]),
            enabled=True,
        )
    logger.info("staging_flow_completed table_count=%d", len(outputs))
    return outputs


if __name__ == "__main__":
    staging_transform_flow()
