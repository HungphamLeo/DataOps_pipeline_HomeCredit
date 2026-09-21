"""
CLI entry point cho Home Credit DataOps Pipeline.

Chạy:
    python cli/main.py
"""
import sys
from pathlib import Path
from log.config.logger_setup import logger_manager

# Đảm bảo project root có trong sys.path để import prefect_orchestra
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger_manager.configure(str(PROJECT_ROOT / "log" / "config" / "logger_config.yaml"))
logger = logger_manager.get_logger(__name__)
from cli.flows.bronze_silver_pipeline import bronze_to_silver_flow

if __name__ == "__main__":
    logger.info("cli_pipeline_start")
    try:
        bronze_to_silver_flow()
    except Exception:
        logger.exception("cli_pipeline_failed")
        raise
    logger.info("cli_pipeline_completed")
