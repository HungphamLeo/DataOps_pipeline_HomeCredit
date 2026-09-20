"""
Bronze Ingest Flow — CSV → Parquet (Bronze Layer)

Đọc 7 file CSV nguồn, thêm metadata, và lưu dạng Parquet phân vùng
theo _load_date vào tầng Bronze.

Root cause fix: sử dụng ConcurrentTaskRunner thay vì ThreadPoolTaskRunner
mặc định để tránh lỗi GatherTaskGroup với anyio 4.x. Với anyio 3.x
(pin trong requirements.txt) thì ThreadPoolTaskRunner cũng hoạt động.
"""
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.tasks import task_input_hash

from prefect_orchestra.flow.ge_validator import run_ge_checkpoint

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "prefect_orchestra" / "config" / "pipeline_config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@task(
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
    log_prints=True,
)
def ingest_source_to_bronze(source_config: dict, config: dict) -> str:
    """
    Đọc một file CSV nguồn, thêm metadata, và lưu dưới dạng Parquet
    đã phân vùng vào tầng Bronze.

    Idempotency: mỗi lần chạy ghi vào partition _load_date=<ngày hôm nay>.
    Chạy lại cùng ngày sẽ overwrite đúng thư mục partition đó.
    """
    raw_data_dir = BASE_DIR / config["paths"]["raw_data_dir"]
    bronze_dir = BASE_DIR / config["paths"]["output_bronze_dir"]

    table_name = source_config["name"]
    file_name = source_config["file"]
    source_path = raw_data_dir / file_name

    print(f"[Bronze] Đang xử lý '{table_name}' từ {source_path}...")

    if not source_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file nguồn: {source_path}\n"
            "Hãy đặt các file HC_*.csv vào thư mục sql/dev/source_data/"
        )

    df = pd.read_csv(source_path, low_memory=False)

    # --- Metadata columns ---
    load_ts = datetime.utcnow()
    df["_load_ts"] = load_ts.isoformat()          # string: safe cho parquet partition
    df["_source_file"] = file_name
    df["_load_date"] = load_ts.strftime("%Y-%m-%d")  # partition key as string

    # --- Idempotent write: overwrite chỉ partition của ngày hôm nay ---
    output_dir = bronze_dir / f"bronze_{table_name}"
    partition_dir = output_dir / f"_load_date={load_ts.strftime('%Y-%m-%d')}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = partition_dir / "data.parquet"
    df.drop(columns=["_load_date"]).to_parquet(
        parquet_path,
        engine="pyarrow",
        index=False,
    )
    print(
        f"[Bronze] ✓ '{table_name}': {len(df):,} rows → {parquet_path}"
    )
    return str(partition_dir)


@flow(
    name="Bronze Ingest Flow",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(),
    description="Ingest 7 raw CSV sources vào Bronze layer (Parquet, partitioned by _load_date).",
)
def bronze_ingest_flow():
    """
    Điều phối việc ingest tất cả các nguồn CSV vào tầng Bronze.

    Tất cả 7 table được ingest song song (ConcurrentTaskRunner).
    GE checkpoint chỉ chạy sau khi tất cả ingest task hoàn thành.
    """
    config = _load_config()
    ge_enabled = config.get("great_expectations", {}).get("enabled", True)
    ge_root_dir = str(BASE_DIR / config["paths"]["ge_root_dir"])

    sources = config["sources"]

    # Submit tất cả ingest tasks song song
    ingest_futures = [
        ingest_source_to_bronze.submit(source_config=source, config=config)
        for source in sources
    ]

    # GE checkpoint chạy sau khi toàn bộ ingest hoàn thành
    run_ge_checkpoint.submit(
        checkpoint_name=config["great_expectations"]["bronze_checkpoint"],
        ge_root_dir=ge_root_dir,
        enabled=ge_enabled,
        wait_for=ingest_futures,
    )


if __name__ == "__main__":
    bronze_ingest_flow()
