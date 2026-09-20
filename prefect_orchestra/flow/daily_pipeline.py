"""
Master Pipeline Flow — điều phối toàn bộ pipeline DataOps:
Bronze Ingest → Staging Transform → Mart Build

Entry point: python cli/main.py
"""
import time
from pathlib import Path
from prefect import flow

from prefect_orchestra.flow.bronze_flow import bronze_ingest_flow
from prefect_orchestra.flow.staging_flow import staging_transform_flow
from prefect_orchestra.flow.mart_flow import mart_build_flow

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
    3. Mart: Join Staging → 3 mart tables (ML features, report, default cohort).
    """
    start_ts = time.time()
    print("=" * 60)
    print("=== MASTER PIPELINE: BẮT ĐẦU ===")
    print("=" * 60)

    # 1 — Bronze
    print("\n[1/3] Kích hoạt Bronze Ingest Flow...")
    bronze_ingest_flow()
    print("[1/3] ✓ Bronze Ingest Flow hoàn thành.")

    # 2 — Staging
    print("\n[2/3] Kích hoạt Staging Transform Flow...")
    staging_transform_flow()
    print("[2/3] ✓ Staging Transform Flow hoàn thành.")

    # 3 — Mart
    print("\n[3/3] Kích hoạt Mart Build Flow...")
    mart_build_flow()
    print("[3/3] ✓ Mart Build Flow hoàn thành.")

    elapsed = time.time() - start_ts
    print("\n" + "=" * 60)
    print(f"=== MASTER PIPELINE: HOÀN THÀNH — {elapsed:.1f}s ===")
    print("=" * 60)


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
