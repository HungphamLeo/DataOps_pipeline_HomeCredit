import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash

# Import GE task
from tasks.ge_validator import run_ge_checkpoint

BASE_DIR = Path(__file__).resolve().parents[2]


@task(
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
    log_prints=True,
)
def ingest_source_to_bronze(source_config: dict, config: dict):
    """
    Đọc một file CSV nguồn, thêm metadata, và lưu dưới dạng Parquet
    đã phân vùng vào tầng Bronze.
    """
    raw_data_dir = BASE_DIR / config["paths"]["raw_data_dir"]
    bronze_dir = BASE_DIR / config["paths"]["output_bronze_dir"]

    table_name = source_config["name"]
    file_name = source_config["file"]
    source_path = raw_data_dir / file_name

    print(f"Đang xử lý {table_name} từ {source_path}...")

    if not source_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file nguồn: {source_path}")

    df = pd.read_csv(source_path)

    # Thêm metadata
    load_ts = datetime.utcnow()
    df["_load_ts"] = load_ts
    df["_source_file"] = file_name
    df["_load_date"] = load_ts.date()

    # Lưu vào tầng Bronze (đã phân vùng)
    output_path = bronze_dir / f"bronze_{table_name}"
    print(f"Đang lưu vào {output_path}...")
    df.to_parquet(output_path, partition_cols=["_load_date"], engine="pyarrow", index=False)
    print(f"Đã ingest thành công {table_name} vào tầng Bronze.")
    return output_path


@flow(name="Bronze Ingest Flow", log_prints=True)
def bronze_ingest_flow():
    """Điều phối việc ingest tất cả các nguồn CSV vào tầng Bronze."""
    config_path = BASE_DIR / "prefect" / "config" / "pipeline_config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    sources = config["sources"]
    for source in sources:
        ingest_source_to_bronze.submit(source_config=source, config=config)
        
    # Sau khi tất cả các task ingest được gửi đi, chạy GE checkpoint.
    # Flow sẽ đợi các task ingest hoàn thành trước khi chạy bước này.
    run_ge_checkpoint.submit(checkpoint_name="bronze_checkpoint.yml", 
                             ge_root_dir=str(BASE_DIR / "great_expectations"))


if __name__ == "__main__":
    bronze_ingest_flow()