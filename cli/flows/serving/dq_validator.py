# cli/flows/serving/dq_validator.py
"""
Data Quality Validator — Pandera + Circuit Breaker + DLQ.

Thay thế ge_validator.py. Không cần GE project folder hay checkpoint YAML.

Flow:
  1. Validate CRITICAL rules → nếu fail → raise DataQualityError (halt pipeline)
  2. Validate WARNING rules  → nếu fail → ghi bad rows vào quarantine DLQ
  3. Ghi audit log vào PostgreSQL audit.dq_results
"""
from __future__ import annotations

import uuid
from prefect import task
from pyspark.sql import DataFrame, SparkSession

from platforms.config.delta_utils import write_delta
from platforms.prefect_orchestra.prefect_main import PrefectETLPipelineConfig
from cli.flows.serving.metadata import load_stack_config
from cli.data_contract.application_contract import (
    APPLICATION_CRITICAL,
    APPLICATION_WARNING,
)

_pipeline_cfg = PrefectETLPipelineConfig()
logger = _pipeline_cfg.bronze_logger


class DataQualityError(Exception):
    """Raised khi CRITICAL contract bị vi phạm — triggers Circuit Breaker."""


def _quarantine_path(stack: dict, table_name: str) -> str:
    bucket = stack["minio"]["bucket"]
    return f"s3a://{bucket}/quarantine/{table_name}"


# @task(name="dq_validate", log_prints=False)
def validate_bronze(
    df: DataFrame,
    table_name: str,
    spark: SparkSession,
    enabled: bool = True,
) -> DataFrame:
    """
    Validate Bronze DataFrame theo Data Contract.

    Parameters
    ----------
    df : DataFrame       DataFrame sau khi load từ CSV (trước khi write Delta)
    table_name : str     Tên bảng — dùng để chọn contract và đường dẫn DLQ
    spark : SparkSession Cần thiết để write DLQ
    enabled : bool       False → skip validation (dev mode)

    Returns
    -------
    DataFrame            Clean rows (CRITICAL rows đã bị filter nếu có WARNING)

    Raises
    ------
    DataQualityError     Nếu CRITICAL rule bị vi phạm
    """
    if not enabled:
        logger.info("dq_validation_skipped table=%s", table_name)
        return df

    run_id = str(uuid.uuid4())[:8]
    logger.info("dq_validation_started table=%s run_id=%s rows=%d",
                table_name, run_id, df.count())

    # Chỉ áp dụng contract cho application hiện tại
    # Mở rộng: map table_name → contract tương ứng
    if table_name != "application":
        logger.info("dq_no_contract_defined table=%s — skip", table_name)
        return df

    # ------------------------------------------------------------------
    # 1. CRITICAL check — lazy=True để collect tất cả vi phạm trước
    # ------------------------------------------------------------------
    try:
        APPLICATION_CRITICAL.validate(df, lazy=True)
        logger.info("dq_critical_passed table=%s run_id=%s", table_name, run_id)
    except Exception as e:
        logger.error("dq_critical_failed table=%s run_id=%s error=%s",
                     table_name, run_id, e)
        raise DataQualityError(
            f"CRITICAL contract violated for table '{table_name}': {e}"
        ) from e

    # ------------------------------------------------------------------
    # 2. WARNING check — tách bad rows → DLQ, trả về clean rows
    # ------------------------------------------------------------------
    try:
        APPLICATION_WARNING.validate(df, lazy=True)
        logger.info("dq_warning_passed table=%s run_id=%s", table_name, run_id)
        return df
    except Exception as e:
        logger.warning("dq_warning_violated table=%s run_id=%s — routing to DLQ",
                       table_name, run_id)
        stack = load_stack_config()
        dlq_path = _quarantine_path(stack, table_name)

        # Lấy invalid rows từ pandera failure cases
        try:
            failure_df = e.failure_cases   # pandera SchemaErrors.failure_cases
            # Join ngược để lấy original rows
            bad_ids = failure_df.select("index").distinct()
            bad_rows = df.join(bad_ids, df["SK_ID_CURR"] == bad_ids["index"], "inner")
            clean_rows = df.join(bad_ids, df["SK_ID_CURR"] == bad_ids["index"], "left_anti")

            write_delta(bad_rows, dlq_path)
            logger.warning("dq_quarantine_written table=%s path=%s rows=%d",
                           table_name, dlq_path, bad_rows.count())
            return clean_rows
        except Exception as dlq_err:
            # DLQ write fail không được block pipeline
            logger.error("dq_dlq_write_failed table=%s error=%s — returning full df",
                         table_name, dlq_err)
            return df