import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from functools import reduce
from prefect import flow, task

from prefect_orchestra.flow.ge_validator import run_ge_checkpoint

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "prefect_orchestra" / "config" / "pipeline_config.yaml"


# ---------------------------------------------------------------------------
# Helper tasks
# ---------------------------------------------------------------------------

@task(log_prints=True, retries=2, retry_delay_seconds=10)
def read_staging_table(table_name: str, config: dict) -> pd.DataFrame:
    """Đọc một bảng từ tầng Staging."""
    staging_dir = BASE_DIR / config["paths"]["output_staging_dir"]
    table_path = staging_dir / table_name / "data.parquet"

    if not table_path.exists():
        raise FileNotFoundError(f"Không tìm thấy bảng Staging: {table_path}")

    print(f"Đang đọc bảng staging '{table_name}' từ {table_path}")
    df = pd.read_parquet(table_path)
    return df


@task(log_prints=True)
def save_mart_table(df: pd.DataFrame, table_name: str, config: dict):
    """Lưu DataFrame vào tầng Mart dưới dạng một file Parquet."""
    mart_dir = BASE_DIR / config["paths"]["output_mart_dir"]
    output_path = mart_dir / table_name
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "data.parquet"
    print(f"Đang lưu bảng mart '{table_name}' ({len(df)} rows) vào {file_path}")
    df.to_parquet(file_path, index=False, engine="pyarrow")
    print(f"Đã lưu thành công.")
    return str(file_path)


# ---------------------------------------------------------------------------
# Mart build tasks
# ---------------------------------------------------------------------------

@task(log_prints=True)
def build_mart_risk_model_features(staging_dfs: dict) -> pd.DataFrame:
    """Xây dựng bảng wide-table cho ML bằng cách LEFT JOIN tất cả các bảng staging."""
    print("Đang xây dựng mart_risk_model_features...")

    stg_app = staging_dfs["stg_application"]

    summary_tables = [
        staging_dfs["stg_bureau_summary"],
        staging_dfs["stg_prev_application_summary"],
        staging_dfs["stg_installment_summary"],
        staging_dfs["stg_pos_cash_summary"],
        staging_dfs["stg_credit_card_summary"],
    ]

    df_final = reduce(
        lambda left, right: pd.merge(left, right, on="SK_ID_CURR", how="left"),
        summary_tables,
        stg_app,
    )

    print(f"mart_risk_model_features xây dựng xong: shape {df_final.shape}")
    return df_final


@task(log_prints=True)
def build_mart_risk_report_summary(model_features_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng tóm tắt cho mục đích báo cáo với RISK_TIER bucket."""
    print("Đang xây dựng mart_risk_report_summary...")

    df = model_features_df.copy()

    # RISK_TIER logic (aligned với plan):
    # High   : ext_source_2 < 0.3  AND bureau_max_overdue > 0
    # Low    : ext_source_2 > 0.5  AND bureau_avg_bad_status_rate < 0.1
    # Medium : everything else
    def assign_risk_tier(row):
        ext2 = row.get("EXT_SOURCE_2", np.nan)
        max_overdue = row.get("BUREAU_MAX_OVERDUE", np.nan)
        bad_rate = row.get("BUREAU_AVG_BAD_STATUS_RATE", np.nan)

        if pd.notna(ext2) and pd.notna(max_overdue) and ext2 < 0.3 and max_overdue > 0:
            return "High"
        elif pd.notna(ext2) and pd.notna(bad_rate) and ext2 > 0.5 and bad_rate < 0.1:
            return "Low"
        return "Medium"

    df["RISK_TIER"] = df.apply(assign_risk_tier, axis=1)

    report_cols = [
        "SK_ID_CURR", "TARGET",
        "AGE_YEARS", "NAME_INCOME_TYPE", "NAME_EDUCATION_TYPE",
        "NAME_FAMILY_STATUS", "NAME_HOUSING_TYPE",
        "AMT_INCOME_TOTAL", "AMT_CREDIT", "INCOME_CREDIT_RATIO",
        "EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3",
        "BUREAU_LOAN_COUNT", "BUREAU_MAX_OVERDUE", "BUREAU_AVG_BAD_STATUS_RATE",
        "INSTALMENT_LATE_RATE", "POS_MAX_DPD", "CC_AVG_UTILIZATION",
        "RISK_TIER",
    ]

    existing_cols = [c for c in report_cols if c in df.columns]
    result = df[existing_cols]
    print(f"mart_risk_report_summary xây dựng xong: shape {result.shape}")
    return result


@task(log_prints=True)
def build_mart_default_cohort(app_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng tỉ lệ nợ xấu theo các nhóm cohort."""
    print("Đang xây dựng mart_default_cohort...")

    cohort_dims = [
        "NAME_INCOME_TYPE",
        "NAME_EDUCATION_TYPE",
        "NAME_FAMILY_STATUS",
        "NAME_HOUSING_TYPE",
        "REGION_RATING_CLIENT",
        "NAME_CONTRACT_TYPE",
    ]
    all_cohorts = []

    for dim in cohort_dims:
        if dim not in app_df.columns:
            print(f"Cột '{dim}' không tồn tại trong stg_application, bỏ qua.")
            continue
        cohort_agg = (
            app_df.groupby(dim)["TARGET"]
            .agg(["count", "sum"])
            .reset_index()
            .rename(columns={"count": "TOTAL_COUNT", "sum": "DEFAULT_COUNT", dim: "COHORT_VALUE"})
        )
        cohort_agg["COHORT_DIM"] = dim
        cohort_agg["DEFAULT_RATE"] = cohort_agg["DEFAULT_COUNT"] / cohort_agg["TOTAL_COUNT"]
        all_cohorts.append(
            cohort_agg[["COHORT_DIM", "COHORT_VALUE", "TOTAL_COUNT", "DEFAULT_COUNT", "DEFAULT_RATE"]]
        )

    result = pd.concat(all_cohorts, ignore_index=True)
    print(f"mart_default_cohort xây dựng xong: {len(result)} rows.")
    return result


# ---------------------------------------------------------------------------
# Mart Flow
# ---------------------------------------------------------------------------

@flow(name="Mart Build Flow", log_prints=True)
def mart_build_flow():
    """
    Đọc dữ liệu từ tầng Staging, join chúng thành các bảng phân tích,
    và lưu kết quả vào tầng Mart.
    """
    print("--- Bắt đầu Mart Build Flow ---")

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    staging_tables = [
        "stg_application",
        "stg_bureau_summary",
        "stg_prev_application_summary",
        "stg_installment_summary",
        "stg_pos_cash_summary",
        "stg_credit_card_summary",
    ]
    staging_dfs = {tbl: read_staging_table(tbl, config) for tbl in staging_tables}

    # --- Build mart tables ---
    model_features_df = build_mart_risk_model_features(staging_dfs)
    save_model_future = save_mart_table.submit(model_features_df, "mart_risk_model_features", config)

    report_summary_df = build_mart_risk_report_summary(model_features_df)
    save_mart_table(report_summary_df, "mart_risk_report_summary", config)

    default_cohort_df = build_mart_default_cohort(staging_dfs["stg_application"])
    save_mart_table(default_cohort_df, "mart_default_cohort", config)

    # GE checkpoint sau khi mart chính đã được lưu
    run_ge_checkpoint.submit(
        checkpoint_name="mart_checkpoint",
        ge_root_dir=str(BASE_DIR / "great_expectations"),
        wait_for=[save_model_future],
    )

    print("--- Mart Build Flow đã kết thúc ---")


if __name__ == "__main__":
    mart_build_flow()
