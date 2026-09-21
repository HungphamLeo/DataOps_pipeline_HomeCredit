"""
Master Pipeline Flow — điều phối toàn bộ pipeline DataOps:
Bronze Ingest → Staging Transform → Mart Build

Entry point: python cli/main.py
"""
import time
from pathlib import Path
from prefect import flow

from cli.flows.serving.bronze.bronze_flow import bronze_ingest_flow
from cli.flows.serving.staging.staging_flow import staging_transform_flow
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)
BASE_DIR = Path(__file__).resolve().parents[2]


@flow(
    name="Master Home Credit Pipeline",
    log_prints=True,
    description=(
        "Master flow điều phối toàn bộ pipeline DataOps: "
        "Bronze → Staging → Mart, bao gồm kiểm tra chất lượng dữ liệu GE ở mỗi tầng."
    ),
)
def master_pipeline_flow():
    """
    Master flow điều phối toàn bộ pipeline DataOps:
    1. Bronze: Ingest CSV thô → Parquet, phân vùng theo _load_date.
    2. Staging: Transform, aggregate Bronze → 6 staging tables.
    3. PostgreSQL schema stg contains the design-model target tables.
    """
    start_ts = time.time()
    logger.info("legacy_master_pipeline_started")
    bronze_ingest_flow()
    staging_transform_flow()
    # mart_build_flow()
    elapsed = time.time() - start_ts
    logger.info("legacy_master_pipeline_completed duration_seconds=%.2f", elapsed)


if __name__ == "__main__":
    # Chạy pipeline một lần ngay lập tức để test
    master_pipeline_flow()

    # --- Scheduling ---
    # Để schedule pipeline chạy lúc 1 giờ sáng hàng ngày, deploy với Prefect CLI:
    #
    #   prefect deploy \
    #     --name "home-credit-daily" \
    #     --flow prefect_orchestra/flow/daily_pipeline.py:master_pipeline_flow \
    #     --cron "0 1 * * *" \
    #     --timezone "UTC"
    #
    # Hoặc dùng .serve() cho local agent (không recommended cho production):
    #   master_pipeline_flow.serve(
    #       name="home-credit-daily-deployment",
    #       cron="0 1 * * *",
    #   )
