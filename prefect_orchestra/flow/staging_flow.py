"""
Staging Transform Flow — Bronze → Staging Layer

Đọc dữ liệu từ tầng Bronze, áp dụng transform và aggregate,
lưu kết quả vào tầng Staging.
"""
import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from prefect_orchestra.flow.ge_validator import run_ge_checkpoint

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "prefect_orchestra" / "config" / "pipeline_config.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Helper tasks
# ---------------------------------------------------------------------------

@task(log_prints=True, retries=2, retry_delay_seconds=10)
def read_latest_bronze_partition(table_name: str, config: dict) -> pd.DataFrame:
    """Đọc partition mới nhất từ một bảng trong tầng Bronze."""
    bronze_dir = BASE_DIR / config["paths"]["output_bronze_dir"]
    table_path = bronze_dir / f"bronze_{table_name}"

    if not table_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy đường dẫn bảng Bronze: {table_path}\n"
            "Hãy chạy bronze_ingest_flow() trước."
        )

    partitions = sorted(
        [p for p in table_path.iterdir() if p.is_dir() and p.name.startswith("_load_date=")],
        reverse=True,
    )
    if not partitions:
        raise FileNotFoundError(f"Không tìm thấy partition nào cho bảng Bronze: '{table_name}'")

    latest_partition = partitions[0]
    parquet_files = list(latest_partition.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Không có file parquet trong partition: {latest_partition}")

    print(f"[Staging] Đọc partition '{latest_partition.name}' cho bảng '{table_name}'")
    df = pd.read_parquet(latest_partition)
    print(f"[Staging] '{table_name}': {len(df):,} rows loaded.")
    return df


@task(log_prints=True, retries=1, retry_delay_seconds=10)
def save_staging_table(df: pd.DataFrame, table_name: str, config: dict) -> str:
    """Lưu DataFrame vào tầng Staging dưới dạng một file Parquet."""
    staging_dir = BASE_DIR / config["paths"]["output_staging_dir"]
    output_path = staging_dir / table_name
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "data.parquet"
    df.to_parquet(file_path, index=False, engine="pyarrow")
    print(f"[Staging] ✓ '{table_name}': {len(df):,} rows → {file_path}")
    return str(file_path)


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

@task(log_prints=True)
def build_stg_application(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_application với các feature phái sinh."""
    print("[Staging] Đang xây dựng stg_application...")

    df = df.copy()
    # DAYS_EMPLOYED == 365243 là mã hoá "không làm việc" / "chưa bao giờ đi làm"
    df["DAYS_EMPLOYED_ANOMALY"] = (df["DAYS_EMPLOYED"] == 365243).astype(int)
    df["DAYS_EMPLOYED"] = df["DAYS_EMPLOYED"].replace({365243: np.nan})

    df["AGE_YEARS"] = df["DAYS_BIRTH"] / -365.0
    df["EMPLOYED_YEARS"] = df["DAYS_EMPLOYED"] / -365.0
    df["INCOME_CREDIT_RATIO"] = df["AMT_INCOME_TOTAL"] / df["AMT_CREDIT"].replace(0, np.nan)
    df["ANNUITY_INCOME_RATIO"] = df["AMT_ANNUITY"] / df["AMT_INCOME_TOTAL"].replace(0, np.nan)
    df["CREDIT_GOODS_RATIO"] = df["AMT_CREDIT"] / df["AMT_GOODS_PRICE"].replace(0, np.nan)

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    print(f"[Staging] stg_application: {len(df):,} rows, {len(df.columns)} cols.")
    return df


@task(log_prints=True)
def build_stg_bureau_summary(bureau_df: pd.DataFrame, bureau_balance_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_bureau_summary — aggregate lịch sử credit bureau theo SK_ID_CURR."""
    print("[Staging] Đang xây dựng stg_bureau_summary...")

    bb_agg = bureau_balance_df.groupby("SK_ID_BUREAU").agg(
        BUREAU_BALANCE_MONTHS_COUNT=("MONTHS_BALANCE", "size"),
        BUREAU_BAD_STATUS_RATE=("STATUS", lambda x: np.mean(x.isin(["1", "2", "3", "4", "5"]))),
    ).reset_index()

    bureau_full = bureau_df.merge(bb_agg, how="left", on="SK_ID_BUREAU")

    bureau_agg = bureau_full.groupby("SK_ID_CURR").agg(
        BUREAU_LOAN_COUNT=("SK_ID_BUREAU", "nunique"),
        BUREAU_ACTIVE_COUNT=("CREDIT_ACTIVE", lambda x: (x == "Active").sum()),
        BUREAU_CLOSED_COUNT=("CREDIT_ACTIVE", lambda x: (x == "Closed").sum()),
        BUREAU_MAX_OVERDUE=("AMT_CREDIT_MAX_OVERDUE", "max"),
        BUREAU_TOTAL_DEBT=("AMT_CREDIT_SUM_DEBT", "sum"),
        BUREAU_TOTAL_CREDIT=("AMT_CREDIT_SUM", "sum"),
        BUREAU_AVG_DPD=("CREDIT_DAY_OVERDUE", "mean"),
        BUREAU_AVG_BAD_STATUS_RATE=("BUREAU_BAD_STATUS_RATE", "mean"),
    ).reset_index()

    print(f"[Staging] stg_bureau_summary: {len(bureau_agg):,} rows.")
    return bureau_agg


@task(log_prints=True)
def build_stg_prev_application_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_prev_application_summary — aggregate lịch sử đơn vay trước."""
    print("[Staging] Đang xây dựng stg_prev_application_summary...")

    agg = df.groupby("SK_ID_CURR").agg(
        PREV_APP_COUNT=("SK_ID_PREV", "count"),
        PREV_APPROVED_COUNT=("NAME_CONTRACT_STATUS", lambda x: (x == "Approved").sum()),
        PREV_REFUSED_COUNT=("NAME_CONTRACT_STATUS", lambda x: (x == "Refused").sum()),
        PREV_AVG_CREDIT=("AMT_CREDIT", "mean"),
        PREV_MAX_CREDIT=("AMT_CREDIT", "max"),
        PREV_AVG_ANNUITY=("AMT_ANNUITY", "mean"),
    ).reset_index()

    agg["PREV_APPROVAL_RATE"] = agg["PREV_APPROVED_COUNT"] / agg["PREV_APP_COUNT"].replace(0, np.nan)
    agg.replace([np.inf, -np.inf], np.nan, inplace=True)

    print(f"[Staging] stg_prev_application_summary: {len(agg):,} rows.")
    return agg


@task(log_prints=True)
def build_stg_installment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_installment_summary — aggregate hành vi thanh toán."""
    print("[Staging] Đang xây dựng stg_installment_summary...")

    df = df.copy()
    df["PAYMENT_DELAY_DAYS"] = df["DAYS_ENTRY_PAYMENT"] - df["DAYS_INSTALMENT"]
    df["PAYMENT_IS_LATE"] = (df["PAYMENT_DELAY_DAYS"] > 0).astype(int)
    df["PAYMENT_AMT_SHORTFALL"] = (df["AMT_INSTALMENT"] - df["AMT_PAYMENT"]).clip(lower=0)

    agg = df.groupby("SK_ID_CURR").agg(
        INSTALMENT_COUNT=("NUM_INSTALMENT_NUMBER", "count"),
        INSTALMENT_LATE_COUNT=("PAYMENT_IS_LATE", "sum"),
        INSTALMENT_LATE_RATE=("PAYMENT_IS_LATE", "mean"),
        INSTALMENT_AVG_DELAY_DAYS=("PAYMENT_DELAY_DAYS", lambda x: x[x > 0].mean()),
        INSTALMENT_AMT_SHORTFALL=("PAYMENT_AMT_SHORTFALL", "sum"),
    ).reset_index()

    agg.replace([np.inf, -np.inf], np.nan, inplace=True)
    print(f"[Staging] stg_installment_summary: {len(agg):,} rows.")
    return agg


@task(log_prints=True)
def build_stg_pos_cash_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_pos_cash_summary — aggregate trạng thái POS/CASH hàng tháng."""
    print("[Staging] Đang xây dựng stg_pos_cash_summary...")

    agg = df.groupby("SK_ID_CURR").agg(
        POS_MONTHS_COUNT=("MONTHS_BALANCE", "size"),
        POS_MAX_DPD=("SK_DPD", "max"),
        POS_AVG_DPD=("SK_DPD", "mean"),
        POS_OVERDUE_MONTHS=("SK_DPD", lambda x: (x > 0).sum()),
    ).reset_index()

    agg["POS_OVERDUE_RATE"] = agg["POS_OVERDUE_MONTHS"] / agg["POS_MONTHS_COUNT"].replace(0, np.nan)
    agg.replace([np.inf, -np.inf], np.nan, inplace=True)

    print(f"[Staging] stg_pos_cash_summary: {len(agg):,} rows.")
    return agg


@task(log_prints=True)
def build_stg_credit_card_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_credit_card_summary — aggregate hành vi thẻ tín dụng hàng tháng."""
    print("[Staging] Đang xây dựng stg_credit_card_summary...")

    df = df.copy()
    df["CC_UTILIZATION"] = df["AMT_BALANCE"] / df["AMT_CREDIT_LIMIT_ACTUAL"].replace(0, np.nan)
    df["CC_PAYMENT_RATIO"] = df["AMT_PAYMENT_TOTAL_CURRENT"] / df["AMT_INST_MIN_REGULARITY"].replace(0, np.nan)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    agg = df.groupby("SK_ID_CURR").agg(
        CC_MONTHS_COUNT=("MONTHS_BALANCE", "size"),
        CC_AVG_BALANCE=("AMT_BALANCE", "mean"),
        CC_MAX_BALANCE=("AMT_BALANCE", "max"),
        CC_AVG_UTILIZATION=("CC_UTILIZATION", "mean"),
        CC_AVG_PAYMENT_RATIO=("CC_PAYMENT_RATIO", "mean"),
        CC_MAX_DPD=("SK_DPD", "max"),
    ).reset_index()

    print(f"[Staging] stg_credit_card_summary: {len(agg):,} rows.")
    return agg


# ---------------------------------------------------------------------------
# Staging Flow
# ---------------------------------------------------------------------------

@flow(
    name="Staging Transform Flow",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(),
    description="Bronze → Staging: transform + aggregate 6 staging tables.",
)
def staging_transform_flow():
    """
    Đọc dữ liệu từ tầng Bronze, áp dụng các phép biến đổi và tổng hợp,
    và lưu kết quả vào tầng Staging.

    Lưu ý: các bước read bronze → build staging → save staging được thực hiện
    tuần tự theo đúng thứ tự phụ thuộc dữ liệu. Các table độc lập
    được submit song song qua ConcurrentTaskRunner.
    """
    print("[Staging] --- Bắt đầu Staging Transform Flow ---")
    config = _load_config()
    ge_enabled = config.get("great_expectations", {}).get("enabled", True)
    ge_root_dir = str(BASE_DIR / config["paths"]["ge_root_dir"])

    # --- Đọc tất cả Bronze tables ---
    app_f = read_latest_bronze_partition.submit("application", config)
    bureau_f = read_latest_bronze_partition.submit("bureau", config)
    bureau_balance_f = read_latest_bronze_partition.submit("bureau_balance", config)
    prev_app_f = read_latest_bronze_partition.submit("previous_application", config)
    installments_f = read_latest_bronze_partition.submit("installments_payments", config)
    pos_cash_f = read_latest_bronze_partition.submit("pos_cash_balance", config)
    credit_card_f = read_latest_bronze_partition.submit("credit_card_balance", config)

    # --- Build staging tables (các bước phụ thuộc vào kết quả đọc bronze) ---
    stg_app_f = build_stg_application.submit(app_f)
    stg_bureau_f = build_stg_bureau_summary.submit(bureau_f, bureau_balance_f)
    stg_prev_f = build_stg_prev_application_summary.submit(prev_app_f)
    stg_inst_f = build_stg_installment_summary.submit(installments_f)
    stg_pos_f = build_stg_pos_cash_summary.submit(pos_cash_f)
    stg_cc_f = build_stg_credit_card_summary.submit(credit_card_f)

    # --- Lưu các staging tables ---
    save_app_f = save_staging_table.submit(stg_app_f, "stg_application", config)
    save_bureau_f = save_staging_table.submit(stg_bureau_f, "stg_bureau_summary", config)
    save_prev_f = save_staging_table.submit(stg_prev_f, "stg_prev_application_summary", config)
    save_inst_f = save_staging_table.submit(stg_inst_f, "stg_installment_summary", config)
    save_pos_f = save_staging_table.submit(stg_pos_f, "stg_pos_cash_summary", config)
    save_cc_f = save_staging_table.submit(stg_cc_f, "stg_credit_card_summary", config)

    # --- GE checkpoint sau khi tất cả staging tables đã được lưu ---
    run_ge_checkpoint.submit(
        checkpoint_name=config["great_expectations"]["staging_checkpoint"],
        ge_root_dir=ge_root_dir,
        enabled=ge_enabled,
        wait_for=[save_app_f, save_bureau_f, save_prev_f, save_inst_f, save_pos_f, save_cc_f],
    )

    print("[Staging] --- Staging Transform Flow đã kết thúc ---")


if __name__ == "__main__":
    staging_transform_flow()
