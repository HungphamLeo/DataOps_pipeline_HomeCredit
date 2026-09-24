"""
Bronze ingestion: CSV nguồn → Delta Lake trên MinIO.

Mỗi source file có 1 hàm riêng biệt để dễ debug khi stuck.
SparkSession tạo một lần ở @flow level và truyền trực tiếp vào từng hàm
(SparkSession không serializable qua Prefect task boundary — Spark Definitive Guide).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from prefect import flow
from pyspark.sql import DataFrame, SparkSession, functions as F

from cli.flows.serving.metadata import load_config, load_stack_config
from config import get_source_dir
from platforms.prefect_orchestra.prefect_main import PrefectETLPipelineConfig
from platforms.processing.config.delta_utils import add_bronze_metadata, write_delta_partitioned
from platforms.processing.spark_stack.spark_session import get_spark_session

_pipeline_cfg = PrefectETLPipelineConfig()
logger = _pipeline_cfg.bronze_logger

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _bronze_path(stack: dict, table_name: str) -> str:
    minio = stack["minio"]
    return f"s3a://{minio['bucket']}/{minio['bronze_prefix']}/{table_name}"


def _load_csv(spark: SparkSession, source: Path, config: dict) -> DataFrame:
    """Read a CSV with standard Bronze options."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", config["bronze"]["infer_schema"])
        .option("encoding", config["runtime"].get("csv_encoding", "UTF-8"))
        .csv(str(source))
    )


def _write_bronze(df: DataFrame, stack: dict, table_name: str, load_date: str) -> str:
    """Add metadata columns, write Delta partitioned by _load_date, return output path."""
    df = add_bronze_metadata(df, table_name)
    df = df.withColumn("_load_date", F.lit(load_date))
    path = _bronze_path(stack, table_name)
    write_delta_partitioned(df, path, partition_col="_load_date")
    return path


# ---------------------------------------------------------------------------
# 1 hàm per source file — độc lập, dễ debug
# ---------------------------------------------------------------------------

def load_application(spark: SparkSession, config: dict, stack: dict, load_date: str) -> str:
    src = get_source_dir() / "application.csv"
    logger.info("bronze_load_started table=application")
    df = _load_csv(spark, src, config)
    out = _write_bronze(df, stack, "application", load_date)
    logger.info("bronze_load_completed table=application path=%s rows=%d", out, df.count())
    return out


def load_bureau(spark: SparkSession, config: dict, stack: dict, load_date: str) -> str:
    src = get_source_dir() / "bureau.csv"
    logger.info("bronze_load_started table=bureau")
    df = _load_csv(spark, src, config)
    out = _write_bronze(df, stack, "bureau", load_date)
    logger.info("bronze_load_completed table=bureau path=%s", out)
    return out


def load_bureau_balance(spark: SparkSession, config: dict, stack: dict, load_date: str) -> str:
    src = get_source_dir() / "bureau_balance.csv"
    logger.info("bronze_load_started table=bureau_balance")
    df = _load_csv(spark, src, config)
    out = _write_bronze(df, stack, "bureau_balance", load_date)
    logger.info("bronze_load_completed table=bureau_balance path=%s", out)
    return out


def load_previous_application(spark: SparkSession, config: dict, stack: dict, load_date: str) -> str:
    src = get_source_dir() / "previous_application.csv"
    logger.info("bronze_load_started table=previous_application")
    df = _load_csv(spark, src, config)
    out = _write_bronze(df, stack, "previous_application", load_date)
    logger.info("bronze_load_completed table=previous_application path=%s", out)
    return out


def load_installments_payments(spark: SparkSession, config: dict, stack: dict, load_date: str) -> str:
    src = get_source_dir() / "installments_payments.csv"
    logger.info("bronze_load_started table=installments_payments")
    df = _load_csv(spark, src, config)
    out = _write_bronze(df, stack, "installments_payments", load_date)
    logger.info("bronze_load_completed table=installments_payments path=%s", out)
    return out


def load_pos_cash_balance(spark: SparkSession, config: dict, stack: dict, load_date: str) -> str:
    src = get_source_dir() / "POS_CASH_balance.csv"
    logger.info("bronze_load_started table=pos_cash_balance")
    df = _load_csv(spark, src, config)
    out = _write_bronze(df, stack, "pos_cash_balance", load_date)
    logger.info("bronze_load_completed table=pos_cash_balance path=%s", out)
    return out


def load_credit_card_balance(spark: SparkSession, config: dict, stack: dict, load_date: str) -> str:
    src = get_source_dir() / "credit_card_balance.csv"
    logger.info("bronze_load_started table=credit_card_balance")
    df = _load_csv(spark, src, config)
    out = _write_bronze(df, stack, "credit_card_balance", load_date)
    logger.info("bronze_load_completed table=credit_card_balance path=%s", out)
    return out


# ---------------------------------------------------------------------------
# @flow — điều phối tuần tự, Spark session tạo 1 lần
# ---------------------------------------------------------------------------

_LOADERS = [
    load_application,
    load_bureau,
    load_bureau_balance,
    load_previous_application,
    load_installments_payments,
    load_pos_cash_balance,
    load_credit_card_balance,
]


@flow(
    name="Bronze Source To MinIO",
    log_prints=False,
    description="Ingest tất cả CSV nguồn vào Bronze Delta Lake trên MinIO. 1 hàm per file.",
)
def bronze_ingest_flow() -> list[str]:
    """
    Tạo Spark session một lần, gọi lần lượt từng loader function độc lập.
    Khi debug, log sẽ chỉ rõ hàm nào bị stuck (bronze_load_started table=...).
    """
    spark = None
    try:
        config = load_config()["project_params"]
        stack = load_stack_config()
        load_date = datetime.now(timezone.utc).strftime(config["runtime"]["load_date_format"])
        logger.info("bronze_flow_started load_date=%s", load_date)

        spark = get_spark_session(stack)
        outputs = []
        for loader in _LOADERS:
            try:
                out = loader(spark, config, stack, load_date)
                outputs.append(out)
            except Exception as e:
                logger.exception("bronze_loader_failed loader=%s error=%s", loader.__name__, e)
                raise

        logger.info("bronze_flow_completed tables=%d", len(outputs))
        return outputs
    except Exception as e:
        logger.exception("bronze_flow_failed error=%s", e)
        raise
    finally:
        if spark is not None:
            spark.stop()


# if __name__ == "__main__":
#     bronze_ingest_flow()
