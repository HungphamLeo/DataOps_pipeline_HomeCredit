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
        "Bronze -> Staging -> Mart, bao gồm kiểm tra chất lượng dữ liệu GE ở mỗi tầng."
    ),
)
def master_pipeline_flow():
    """
    Master flow điều phối toàn bộ pipeline DataOps:
    1. Tầng Bronze: Ingest dữ liệu thô và kiểm tra chất lượng.
    2. Tầng Staging: Transform, aggregate dữ liệu và kiểm tra chất lượng.
    3. Tầng Mart: Xây dựng các data mart cuối cùng và kiểm tra chất lượng.
    """
    print("--- Bắt đầu Master Pipeline Flow ---")

    print("Kích hoạt Bronze Ingest Flow...")
    bronze_ingest_flow()
    print("Bronze Ingest Flow đã hoàn thành.")

    print("Kích hoạt Staging Transform Flow...")
    staging_transform_flow()
    print("Staging Transform Flow đã hoàn thành.")

    print("Kích hoạt Mart Build Flow...")
    mart_build_flow()
    print("Mart Build Flow đã hoàn thành.")

    print("--- Master Pipeline Flow đã kết thúc. ---")


if __name__ == "__main__":
    # Chạy pipeline một lần ngay lập tức
    master_pipeline_flow()

    # Để schedule pipeline (1 AM daily), dùng Prefect CLI:
    # prefect deploy --name "home-credit-daily" \
    #   --flow prefect_orchestra/flow/daily_pipeline.py:master_pipeline_flow \
    #   --cron "0 1 * * *" --timezone "UTC"
