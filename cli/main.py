"""
CLI entry point cho Home Credit DataOps Pipeline.

Chạy:
    python cli/main.py
"""
import sys
from pathlib import Path

# Đảm bảo project root có trong sys.path để import prefect_orchestra
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prefect_orchestra.flow.daily_pipeline import master_pipeline_flow

if __name__ == "__main__":
    master_pipeline_flow()
