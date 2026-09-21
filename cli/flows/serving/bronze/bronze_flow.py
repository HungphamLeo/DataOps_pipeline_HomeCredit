"""Load every supported source CSV into MinIO Bronze without business transforms."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from prefect import flow, task
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from prefect.task_runners import SequentialTaskRunner

from cli.flows.metadata import PROJECT_ROOT, load_config, load_stack_config
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def _source_files(config: dict) -> list[Path]:
    source_dir = PROJECT_ROOT / config["paths"]["source_dir"]
    excluded = set(config["bronze"]["excluded_files"])
    return sorted(
        path for path in source_dir.glob("*.csv")
        if path.name not in excluded
    )


def _table_name(path: Path) -> str:
    return path.stem.lower()


def _bronze_path(stack: dict, table_name: str) -> str:
    minio = stack["minio"]
    return f"s3a://{minio['bucket']}/{minio['bronze_prefix']}/{table_name}"


@task(log_prints=False)
def ingest_source_to_bronze(source_path: str, config: dict, stack: dict) -> str:
    spark: SparkSession = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active Spark session is required for Bronze ingestion")

    source = Path(source_path)
    table_name = _table_name(source)
    load_date = datetime.utcnow().strftime(config["runtime"]["load_date_format"])
    logger.info("bronze_load_started source=%s table=%s", source, table_name)

    frame = (
        spark.read
        .option("header", True)
        .option("inferSchema", config["bronze"]["infer_schema"])
        .option("encoding", config["runtime"].get("csv_encoding", "UTF-8"))
        .csv(str(source))
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(source.name))
        .withColumn("_load_date", F.lit(load_date))
    )
    output = _bronze_path(stack, table_name)
    (
        frame.write
        .mode("overwrite")
        .partitionBy("_load_date")
        .parquet(output)
    )
    logger.info(
        "bronze_load_completed table=%s path=%s columns=%d",
        table_name, output, len(frame.columns),
    )
    return output


@flow(
    name="Bronze Source To MinIO",
    log_prints=False,
    task_runner=SequentialTaskRunner(),
    description="Persist every source table as raw Parquet in MinIO Bronze.",
)
def bronze_ingest_flow() -> list[str]:
    config = load_config()["project_params"]
    stack = load_stack_config()
    files = _source_files(config)
    if not files:
        raise FileNotFoundError("No source CSV files found for Bronze ingestion")

    logger.info("bronze_flow_started source_count=%d", len(files))
    spark = _create_spark(stack)
    outputs = [
        ingest_source_to_bronze.submit(str(path), config, stack).result()
        for path in files
    ]
    logger.info("bronze_flow_completed source_count=%d", len(outputs))
    spark.stop()
    return outputs


def _create_spark(stack: dict) -> SparkSession:
    from platforms.processing.spark_stack.spark_session import get_spark_session

    return get_spark_session(stack)


if __name__ == "__main__":
    bronze_ingest_flow()
