import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash

from prefect_orchestra.flow.ge_validator import run_ge_checkpoint

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "prefect_orchestra" / "config" / "pipeline_config.yaml"


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

    # Lưu vào tầng Bronze (đã phân vùng theo _load_date)
    output_path = bronze_dir / f"bronze_{table_name}"
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"Đang lưu vào {output_path}...")
    df.to_parquet(output_path, partition_cols=["_load_date"], engine="pyarrow", index=False)
    print(f"Đã ingest thành công {table_name} ({len(df)} rows) vào tầng Bronze.")
    return str(output_path)


@flow(name="Bronze Ingest Flow", log_prints=True)
def bronze_ingest_flow():
    """Điều phối việc ingest tất cả các nguồn CSV vào tầng Bronze."""
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    sources = config["sources"]
    ingest_futures = []
    for source in sources:
        future = ingest_source_to_bronze.submit(source_config=source, config=config)
        ingest_futures.append(future)

    # Chạy GE checkpoint sau khi tất cả các task ingest hoàn thành
    run_ge_checkpoint.submit(
        checkpoint_name="bronze_checkpoint",
        ge_root_dir=str(BASE_DIR / "great_expectations"),
        wait_for=ingest_futures,
    )


if __name__ == "__main__":
    bronze_ingest_flow()
