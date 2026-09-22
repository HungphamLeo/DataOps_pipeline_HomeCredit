"""
CLI entry point cho Home Credit DataOps Pipeline.

Chạy:
    python cli/main.py
"""
import sys
from pathlib import Path
from log.config.logger_setup import logger_manager
from cli.flows import master_pipeline_flow
# Đảm bảo project root có trong sys.path cho các package nội bộ.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger_manager.configure(str(PROJECT_ROOT / "log" / "config" / "logger_config.yaml"))
logger = logger_manager.get_logger(__name__)


if __name__ == "__main__":
    logger.info("cli_pipeline_start")
    try:
        master_pipeline_flow()
    except Exception as e:
        logger.exception("cli_pipeline_failed error=%s", e)
        raise
    logger.info("cli_pipeline_completed")
