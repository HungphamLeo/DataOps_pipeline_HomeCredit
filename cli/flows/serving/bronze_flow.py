"""Load every supported source CSV into MinIO Bronze without business transforms."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from prefect import flow, task
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from prefect.task_runners import SequentialTaskRunner

from cli.flows.serving.metadata import PROJECT_ROOT, load_config, load_stack_config
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def _source_files(config: dict) -> list[Path]:
    try:
        source_dir = PROJECT_ROOT / config["paths"]["source_dir"]
        excluded = set(config["bronze"]["excluded_files"])
        return sorted(
            path for path in source_dir.glob("*.csv")
            if path.name not in excluded
        )
    except Exception as e:
        logger.exception(f"bronze_source_discovery_failed {e}")
        raise


def _table_name(path: Path) -> str:
    try:
        return path.stem.lower()
    except Exception as e:
        logger.exception("bronze_table_name_failed path=%s error=%s", path, e)
        raise


def _bronze_path(stack: dict, table_name: str) -> str:
    try:
        minio = stack["minio"]
        return f"s3a://{minio['bucket']}/{minio['bronze_prefix']}/{table_name}"
    except Exception as e:
        logger.exception("bronze_path_build_failed table=%s error=%s", table_name, e)
        raise

@task(log_prints=False)
def ingest_source_to_bronze(source_path: str, config: dict, stack: dict) -> str:
    source = Path(source_path)
    table_name = _table_name(source)
    try:
        spark: SparkSession = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("An active Spark session is required for Bronze ingestion")
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
        frame.write.mode("overwrite").partitionBy("_load_date").parquet(output)
        logger.info(
            "bronze_load_completed table=%s path=%s columns=%d",
            table_name, output, len(frame.columns),
        )
        return output
    except Exception as e:
        logger.exception("bronze_load_failed source=%s table=%s error=%s", source, table_name, e)
        raise


@flow(
    name="Bronze Source To MinIO",
    log_prints=False,
    task_runner=SequentialTaskRunner(),
    description="Persist every source table as raw Parquet in MinIO Bronze.",
)
def bronze_ingest_flow() -> list[str]:
    spark = None
    try:
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
        return outputs
    except Exception as e:
        logger.exception("bronze_flow_failed error=%s", e)
        raise
    finally:
        if spark is not None:
            spark.stop()


def _create_spark(stack: dict) -> SparkSession:
    try:
        from platforms.processing.spark_stack.spark_session import get_spark_session
        return get_spark_session(stack)
    except Exception as e:
        logger.exception("bronze_spark_creation_failed error=%s", e)
        raise


if __name__ == "__main__":
    bronze_ingest_flow()
