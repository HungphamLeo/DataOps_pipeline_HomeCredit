"""
CLI entry point cho Home Credit DataOps Pipeline.

Chạy:
    python cli/main.py
"""
import sys
from pathlib import Path
from log.config.logger_setup import logger_manager

# Đảm bảo project root có trong sys.path cho các package nội bộ.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger_manager.configure(str(PROJECT_ROOT / "log" / "config" / "logger_config.yaml"))
logger = logger_manager.get_logger(__name__)
from cli.flows.serving.bronze.bronze_flow import bronze_ingest_flow
from cli.flows.serving.staging.staging_flow import staging_transform_flow

if __name__ == "__main__":
    logger.info("cli_pipeline_start")
    try:
        bronze_ingest_flow()
        staging_transform_flow()
    except Exception:
        logger.exception("cli_pipeline_failed")
        raise
    logger.info("cli_pipeline_completed")
