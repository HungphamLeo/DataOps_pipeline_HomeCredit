"""
CLI entry point cho Home Credit DataOps Pipeline.

Chạy:
    python -m cli.main          # từ project root
    python cli/main.py          # từ project root
"""
import sys
from pathlib import Path

# Đảm bảo project root có trong sys.path trước mọi import nội bộ.
# Anchor theo file này — không dùng parents[n] dễ lỗi theo depth.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config import get_logger_config_path          # noqa: E402
from log.config.logger_setup import logger_manager # noqa: E402
from cli.flows import master_pipeline_flow          # noqa: E402

logger_manager.configure(str(get_logger_config_path()))
logger = logger_manager.get_logger(__name__)


if __name__ == "__main__":
    logger.info("cli_pipeline_start")
    try:
        master_pipeline_flow()
    except Exception as e:
        logger.exception("cli_pipeline_failed error=%s", e)
        raise
    logger.info("cli_pipeline_completed")
