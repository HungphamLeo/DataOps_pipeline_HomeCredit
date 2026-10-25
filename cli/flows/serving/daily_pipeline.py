"""
Master Pipeline Flow — điều phối toàn bộ pipeline DataOps:
Bronze Ingest → Staging Transform

Entry point: python -m cli.main

Quan hệ với PrefectETLPipelineConfig:
  - File này chứa @flow definition (business orchestration).
  - PrefectETLPipelineConfig (platforms/prefect_orchestra/prefect_main.py) là
    platform-layer accessor dùng khi cần lấy config bên trong Prefect worker/deployment.
  - Không dùng song song 2 cách load config — mọi config đều qua config.py.
"""
import time

from prefect import flow
import uuid
from cli.flows.serving.bronze_flow import bronze_ingest_flow
from cli.flows.serving.silver_flow import staging_transform_flow
from platforms.prefect_orchestra.prefect_main import PrefectETLPipelineConfig

_pipeline_cfg = PrefectETLPipelineConfig()
logger = _pipeline_cfg.serving_logger

import httpx, os

def _notify_failure(flow, flow_run, state):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL") or os.getenv("TELEGRAM_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("notify_failure_skipped no webhook URL configured")
        return
    try:
        httpx.post(webhook_url, json={
            "text": (
                f"❌ *Pipeline FAILED*\n"
                f"flow=`{flow.name}`\n"
                f"run_id=`{flow_run.id}`\n"
                f"error=`{state.message}`"
            )
        }, timeout=5)
    except Exception as notify_err:
        logger.warning("notify_failure_error error=%s", notify_err)

@flow(
    name="Master Home Credit Pipeline",
    on_failure=[_notify_failure],
    log_prints=True,
    description=(
        "Master flow điều phối toàn bộ pipeline DataOps: "
        "Bronze → Staging, bao gồm kiểm tra chất lượng dữ liệu GE ở mỗi tầng."
    ),
)
def master_pipeline_flow() -> None:
    """
    Master flow:
    1. Bronze: Ingest CSV thô → Delta Lake (MinIO), phân vùng theo _load_date.
    2. Staging: Transform Bronze → target tables trong PostgreSQL schema stg.
    """

    run_id = str(uuid.uuid4())[:8]
    start_ts = time.time()
    try:
        logger.info("master_pipeline_started")
        bronze_ingest_flow()
        staging_transform_flow()
        logger.info(
            "master_pipeline_completed duration_seconds=%.2f",
            time.time() - start_ts,
        )
    except Exception as e:
        logger.exception(
            "master_pipeline_failed duration_seconds=%.2f error=%s",
            time.time() - start_ts,
            e,
        )
        raise


# ---------------------------------------------------------------------------
# Scheduling — KHÔNG chạy ephemeral server, dùng Prefect deployment hoặc .serve()
#
# Option A — Prefect deployment (production, Prefect server + worker):
#   prefect deploy \
#     --name "home-credit-daily" \
#     --flow cli/flows/serving/daily_pipeline.py:master_pipeline_flow \
#     --cron "0 1 * * *" \
#     --timezone "Asia/Ho_Chi_Minh"
#
# Option B — local .serve() (dev/staging, không cần worker):
#   if __name__ == "__main__":
#       master_pipeline_flow.serve(
#           name="home-credit-daily-local",
#           cron="0 1 * * *",
#       )
# ---------------------------------------------------------------------------
