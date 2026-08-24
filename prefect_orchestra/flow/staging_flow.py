import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from prefect import flow, task

from prefect_orchestra.flow.ge_validator import run_ge_checkpoint

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "prefect_orchestra" / "config" / "pipeline_config.yaml"


# ---------------------------------------------------------------------------
# Helper tasks
# ---------------------------------------------------------------------------

@task(log_prints=True, retries=2, retry_delay_seconds=10)
def read_latest_bronze_partition(table_name: str, config: dict) -> pd.DataFrame:
    """Đọc partition mới nhất từ một bảng trong tầng Bronze."""
    bronze_dir = BASE_DIR / config["paths"]["output_bronze_dir"]
    table_path = bronze_dir / f"bronze_{table_name}"

    if not table_path.exists():
        raise FileNotFoundError(f"Không tìm thấy đường dẫn bảng Bronze: {table_path}")

    partitions = [p for p in table_path.iterdir() if p.is_dir() and p.name.startswith("_load_date=")]
    if not partitions:
        raise FileNotFoundError(f"Không tìm thấy partition nào cho bảng Bronze: {table_name}")

    latest_partition = max(partitions)
    print(f"Đang đọc partition mới nhất '{latest_partition.name}' cho bảng '{table_name}'")

    df = pd.read_parquet(latest_partition)
    return df


@task(log_prints=True)
def save_staging_table(df: pd.DataFrame, table_name: str, config: dict):
    """Lưu DataFrame vào tầng Staging dưới dạng một file Parquet."""
    staging_dir = BASE_DIR / config["paths"]["output_staging_dir"]
    output_path = staging_dir / table_name
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "data.parquet"
    print(f"Đang lưu bảng staging '{table_name}' ({len(df)} rows) vào {file_path}")
    df.to_parquet(file_path, index=False, engine="pyarrow")
    print(f"Đã lưu thành công.")
    return str(file_path)


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

@task(log_prints=True)
def build_stg_application(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_application với các feature phái sinh."""
    print("Đang xây dựng stg_application...")

    # DAYS_EMPLOYED == 365243 là mã hoá "không làm việc"
    df = df.copy()
    df["DAYS_EMPLOYED_ANOMALY"] = (df["DAYS_EMPLOYED"] == 365243).astype(int)
    df["DAYS_EMPLOYED"] = df["DAYS_EMPLOYED"].replace({365243: np.nan})

    df["AGE_YEARS"] = df["DAYS_BIRTH"] / -365.0
    df["EMPLOYED_YEARS"] = df["DAYS_EMPLOYED"] / -365.0
    df["INCOME_CREDIT_RATIO"] = df["AMT_INCOME_TOTAL"] / df["AMT_CREDIT"].replace(0, np.nan)
    df["ANNUITY_INCOME_RATIO"] = df["AMT_ANNUITY"] / df["AMT_INCOME_TOTAL"].replace(0, np.nan)
    df["CREDIT_GOODS_RATIO"] = df["AMT_CREDIT"] / df["AMT_GOODS_PRICE"].replace(0, np.nan)

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    print(f"stg_application xây dựng xong: {len(df)} rows, {len(df.columns)} cols.")
    return df


@task(log_prints=True)
def build_stg_bureau_summary(bureau_df: pd.DataFrame, bureau_balance_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_bureau_summary — aggregate lịch sử credit bureau theo SK_ID_CURR."""
    print("Đang xây dựng stg_bureau_summary...")

    # Tổng hợp bureau_balance theo SK_ID_BUREAU
    bb_agg = bureau_balance_df.groupby("SK_ID_BUREAU").agg(
        BUREAU_BALANCE_MONTHS_COUNT=("MONTHS_BALANCE", "size"),
        BUREAU_BAD_STATUS_RATE=("STATUS", lambda x: np.mean(x.isin(["1", "2", "3", "4", "5"]))),
    ).reset_index()

    # Join bureau + bureau_balance aggregates
    bureau_full = bureau_df.merge(bb_agg, how="left", on="SK_ID_BUREAU")

    # Tổng hợp theo SK_ID_CURR
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

    print(f"stg_bureau_summary xây dựng xong: {len(bureau_agg)} rows.")
    return bureau_agg


@task(log_prints=True)
def build_stg_prev_application_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_prev_application_summary — aggregate lịch sử đơn vay trước."""
    print("Đang xây dựng stg_prev_application_summary...")

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

    print(f"stg_prev_application_summary xây dựng xong: {len(agg)} rows.")
    return agg


@task(log_prints=True)
def build_stg_installment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_installment_summary — aggregate hành vi thanh toán."""
    print("Đang xây dựng stg_installment_summary...")

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
    print(f"stg_installment_summary xây dựng xong: {len(agg)} rows.")
    return agg


@task(log_prints=True)
def build_stg_pos_cash_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_pos_cash_summary — aggregate trạng thái POS/CASH hàng tháng."""
    print("Đang xây dựng stg_pos_cash_summary...")

    agg = df.groupby("SK_ID_CURR").agg(
        POS_MONTHS_COUNT=("MONTHS_BALANCE", "size"),
        POS_MAX_DPD=("SK_DPD", "max"),
        POS_AVG_DPD=("SK_DPD", "mean"),
        POS_OVERDUE_MONTHS=("SK_DPD", lambda x: (x > 0).sum()),
    ).reset_index()

    agg["POS_OVERDUE_RATE"] = agg["POS_OVERDUE_MONTHS"] / agg["POS_MONTHS_COUNT"].replace(0, np.nan)
    agg.replace([np.inf, -np.inf], np.nan, inplace=True)

    print(f"stg_pos_cash_summary xây dựng xong: {len(agg)} rows.")
    return agg


@task(log_prints=True)
def build_stg_credit_card_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng stg_credit_card_summary — aggregate hành vi thẻ tín dụng hàng tháng."""
    print("Đang xây dựng stg_credit_card_summary...")

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

    print(f"stg_credit_card_summary xây dựng xong: {len(agg)} rows.")
    return agg


# ---------------------------------------------------------------------------
# Staging Flow
# ---------------------------------------------------------------------------

@flow(name="Staging Transform Flow", log_prints=True)
def staging_transform_flow():
    """
    Đọc dữ liệu từ tầng Bronze, áp dụng các phép biến đổi và tổng hợp,
    và lưu kết quả vào tầng Staging.
    """
    print("--- Bắt đầu Staging Transform Flow ---")

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    # --- Tải tất cả các bảng bronze ---
    app_df = read_latest_bronze_partition("application", config)
    bureau_df = read_latest_bronze_partition("bureau", config)
    bureau_balance_df = read_latest_bronze_partition("bureau_balance", config)
    prev_app_df = read_latest_bronze_partition("previous_application", config)
    installments_df = read_latest_bronze_partition("installments_payments", config)
    pos_cash_df = read_latest_bronze_partition("pos_cash_balance", config)
    credit_card_df = read_latest_bronze_partition("credit_card_balance", config)

    # --- Build và lưu từng bảng staging ---
    stg_app_df = build_stg_application(app_df)
    save_app_future = save_staging_table.submit(stg_app_df, "stg_application", config)

    stg_bureau_df = build_stg_bureau_summary(bureau_df, bureau_balance_df)
    save_staging_table(stg_bureau_df, "stg_bureau_summary", config)

    stg_prev_df = build_stg_prev_application_summary(prev_app_df)
    save_staging_table(stg_prev_df, "stg_prev_application_summary", config)

    stg_inst_df = build_stg_installment_summary(installments_df)
    save_staging_table(stg_inst_df, "stg_installment_summary", config)

    stg_pos_df = build_stg_pos_cash_summary(pos_cash_df)
    save_staging_table(stg_pos_df, "stg_pos_cash_summary", config)

    stg_cc_df = build_stg_credit_card_summary(credit_card_df)
    save_staging_table(stg_cc_df, "stg_credit_card_summary", config)

    # Chạy GE checkpoint sau khi bảng staging chính đã được lưu
    run_ge_checkpoint.submit(
        checkpoint_name="staging_checkpoint",
        ge_root_dir=str(BASE_DIR / "great_expectations"),
        wait_for=[save_app_future],
    )

    print("--- Staging Transform Flow đã kết thúc ---")


if __name__ == "__main__":
    staging_transform_flow()
