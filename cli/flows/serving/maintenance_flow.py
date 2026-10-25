"""
Delta Lake Maintenance Flow — OPTIMIZE (Z-ORDER) & VACUUM toàn bộ Bronze tables.

Chạy sau bronze_ingest_flow để giải quyết Small File Problem và dọn snapshot cũ.

Schedule: Chạy định kỳ (VD: hàng tuần vào Chủ nhật 2AM) — không nhất thiết mỗi ngày.

Entry point:
    from cli.flows.serving.maintenance_flow import delta_maintenance_flow
    delta_maintenance_flow()
"""

from __future__ import annotations

import time

from prefect import flow

from cli.flows.serving.metadata import load_config, load_stack_config
from platforms.prefect_orchestra.prefect_main import PrefectETLPipelineConfig
from platforms.config.delta_utils import maintain_all_bronze_tables
from platforms.processing.spark_stack.spark_session import get_spark_session

_pipeline_cfg = PrefectETLPipelineConfig()
logger = _pipeline_cfg.bronze_logger  # maintenance thuộc Bronze layer


@flow(
    name="Delta Lake Bronze Maintenance",
    log_prints=False,
    description="OPTIMIZE (Z-ORDER) và VACUUM toàn bộ Bronze Delta tables. Giải quyết Small File Problem.",
)
def delta_maintenance_flow() -> None:
    """
    1. Load homecredit_config.yaml lấy bronze_maintenance config (table list + zorder keys).
    2. Load stack.yaml lấy MinIO bucket/prefix.
    3. Tạo SparkSession, chạy OPTIMIZE + VACUUM từng bảng Bronze.
    4. Stop Spark trong finally — không leak session.
    """
    start_ts = time.time()
    spark = None
    try:
        config = load_config()
        stack = load_stack_config()

        maintenance_cfg = config["bronze_maintenance"]
        minio_cfg = stack["minio"]

        logger.info(
            "delta_maintenance_flow_started tables=%d retention_hours=%s",
            len(maintenance_cfg["tables"]),
            maintenance_cfg["retention_hours"],
        )

        spark = get_spark_session(stack)

        maintain_all_bronze_tables(
            spark=spark,
            bucket=minio_cfg["bucket"],
            prefix=minio_cfg["bronze_prefix"],
            tables_cfg=maintenance_cfg["tables"],
            retention_hours=maintenance_cfg["retention_hours"],
        )

        logger.info(
            "delta_maintenance_flow_completed duration_seconds=%.2f",
            time.time() - start_ts,
        )
    except Exception as e:
        logger.exception(
            "delta_maintenance_flow_failed duration_seconds=%.2f error=%s",
            time.time() - start_ts,
            e,
        )
        raise
    finally:
        if spark is not None:
            spark.stop()