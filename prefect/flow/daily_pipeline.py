from pathlib import Path
from prefect import flow
from prefect.schedules import CronSchedule

# Import sub-flow mới đã được module hóa
# Sử dụng relative import để đảm bảo hoạt động trong một package
from .bronze_flow import bronze_ingest_flow
from .staging_flow import staging_transform_flow
from .mart_flow import mart_build_flow

BASE_DIR = Path(__file__).resolve().parents[2]


@flow(
    name="Master Home Credit Pipeline", 
    log_prints=True,
    description="Master flow điều phối toàn bộ pipeline DataOps: Bronze -> Staging -> Mart, bao gồm cả kiểm tra chất lượng dữ liệu."
)
def master_pipeline_flow():
    """
    Master flow điều phối toàn bộ pipeline DataOps:
    1. Tầng Bronze: Ingest dữ liệu thô và kiểm tra chất lượng.
    2. Tầng Staging: Transform, aggregate dữ liệu và kiểm tra chất lượng.
    3. Tầng Mart: Xây dựng các data mart cuối cùng và kiểm tra chất lượng.
    """
    print("--- Bắt đầu Master Pipeline Flow ---")

    # --- Tầng Bronze ---
    print("Kích hoạt Bronze Ingest Flow...")
    bronze_ingest_flow() # Master flow sẽ đợi flow này hoàn thành
    print("Bronze Ingest Flow đã hoàn thành.")

    # --- Tầng Staging ---
    print("Kích hoạt Staging Transform Flow...")
    staging_transform_flow() # Chạy sau khi bronze flow hoàn thành
    print("Staging Transform Flow đã hoàn thành.")

    # --- Tầng Mart ---
    print("Kích hoạt Mart Build Flow...")
    mart_build_flow() # Chạy sau khi staging flow hoàn thành
    print("Mart Build Flow đã hoàn thành.")

    print("--- Master Pipeline Flow đã kết thúc. ---")


if __name__ == "__main__":
    # Chạy pipeline một lần ngay lập tức để test
    master_pipeline_flow()

    # Để chạy pipeline này theo lịch (ví dụ: 1 giờ sáng hàng ngày),
    # bạn nên tạo một deployment bằng Prefect CLI:
    # prefect deploy --name "home-credit-pipeline" --flow prefect/flow/daily_pipeline.py:master_pipeline_flow --cron "0 1 * * *" --timezone "UTC"
    #
    # Hoặc dùng .serve() để chạy agent cục bộ (ít phổ biến hơn cho production):
    # master_pipeline_flow.serve(name="home-credit-daily-deployment", schedule=CronSchedule(cron="0 1 * * *", timezone="UTC"))
