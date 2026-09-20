"""
Mart Build Flow — Staging → Mart Layer

Đọc dữ liệu từ tầng Staging, join thành các bảng analytical,
và lưu kết quả vào tầng Mart.
"""
import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from functools import reduce
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
def read_staging_table(table_name: str, config: dict) -> pd.DataFrame:
    """Đọc một bảng từ tầng Staging."""
    staging_dir = BASE_DIR / config["paths"]["output_staging_dir"]
    table_path = staging_dir / table_name / "data.parquet"

    if not table_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy bảng Staging: {table_path}\n"
            "Hãy chạy staging_transform_flow() trước."
        )

    print(f"[Mart] Đọc bảng staging '{table_name}' từ {table_path}")
    df = pd.read_parquet(table_path)
    print(f"[Mart] '{table_name}': {len(df):,} rows loaded.")
    return df


@task(log_prints=True, retries=1, retry_delay_seconds=10)
def save_mart_table(df: pd.DataFrame, table_name: str, config: dict) -> str:
    """Lưu DataFrame vào tầng Mart dưới dạng một file Parquet."""
    mart_dir = BASE_DIR / config["paths"]["output_mart_dir"]
    output_path = mart_dir / table_name
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "data.parquet"
    df.to_parquet(file_path, index=False, engine="pyarrow")
    print(f"[Mart] ✓ '{table_name}': {len(df):,} rows → {file_path}")
    return str(file_path)


# ---------------------------------------------------------------------------
# Mart build tasks
# ---------------------------------------------------------------------------

@task(log_prints=True)
def build_mart_risk_model_features(staging_dfs: dict) -> pd.DataFrame:
    """Xây dựng bảng wide-table cho ML bằng cách LEFT JOIN tất cả các bảng staging."""
    print("[Mart] Đang xây dựng mart_risk_model_features...")

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

    print(f"[Mart] mart_risk_model_features: shape {df_final.shape}")
    return df_final


@task(log_prints=True)
def build_mart_risk_report_summary(model_features_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng tóm tắt cho mục đích báo cáo với RISK_TIER bucket."""
    print("[Mart] Đang xây dựng mart_risk_report_summary...")

    df = model_features_df.copy()

    # RISK_TIER logic (aligned với governance plan):
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
    print(f"[Mart] mart_risk_report_summary: shape {result.shape}")
    return result


@task(log_prints=True)
def build_mart_default_cohort(app_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng tỉ lệ nợ xấu theo các nhóm cohort."""
    print("[Mart] Đang xây dựng mart_default_cohort...")

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
            print(f"[Mart] Cột '{dim}' không tồn tại trong stg_application, bỏ qua.")
            continue
        cohort_agg = (
            app_df.groupby(dim)["TARGET"]
            .agg(["count", "sum"])
            .reset_index()
            .rename(columns={"count": "TOTAL_COUNT", "sum": "DEFAULT_COUNT", dim: "COHORT_VALUE"})
        )
        cohort_agg["COHORT_DIM"] = dim
        cohort_agg["DEFAULT_RATE"] = (
            cohort_agg["DEFAULT_COUNT"] / cohort_agg["TOTAL_COUNT"].replace(0, np.nan)
        )
        all_cohorts.append(
            cohort_agg[["COHORT_DIM", "COHORT_VALUE", "TOTAL_COUNT", "DEFAULT_COUNT", "DEFAULT_RATE"]]
        )

    if not all_cohorts:
        raise ValueError("Không có cohort dimension nào được tìm thấy trong stg_application.")

    result = pd.concat(all_cohorts, ignore_index=True)
    print(f"[Mart] mart_default_cohort: {len(result):,} rows.")
    return result


# ---------------------------------------------------------------------------
# Mart Flow
# ---------------------------------------------------------------------------

@flow(
    name="Mart Build Flow",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(),
    description="Staging → Mart: build 3 analytical tables (ML features, report, default cohort).",
)
def mart_build_flow():
    """
    Đọc dữ liệu từ tầng Staging, join chúng thành các bảng phân tích,
    và lưu kết quả vào tầng Mart.

    mart_risk_report_summary và mart_default_cohort được build song song
    sau khi mart_risk_model_features đã sẵn sàng.
    """
    print("[Mart] --- Bắt đầu Mart Build Flow ---")
    config = _load_config()
    ge_enabled = config.get("great_expectations", {}).get("enabled", True)
    ge_root_dir = str(BASE_DIR / config["paths"]["ge_root_dir"])

    # --- Đọc tất cả staging tables song song ---
    staging_table_names = [
        "stg_application",
        "stg_bureau_summary",
        "stg_prev_application_summary",
        "stg_installment_summary",
        "stg_pos_cash_summary",
        "stg_credit_card_summary",
    ]
    staging_futures = {
        tbl: read_staging_table.submit(tbl, config)
        for tbl in staging_table_names
    }

    # Resolve futures thành dict của DataFrames
    staging_dfs = {tbl: f.result() for tbl, f in staging_futures.items()}

    # --- Build mart tables ---
    model_features_df = build_mart_risk_model_features(staging_dfs)
    save_model_f = save_mart_table.submit(model_features_df, "mart_risk_model_features", config)

    # Report summary và default cohort có thể build song song
    report_summary_f = build_mart_risk_report_summary.submit(model_features_df)
    default_cohort_f = build_mart_default_cohort.submit(staging_dfs["stg_application"])

    save_report_f = save_mart_table.submit(
        report_summary_f, "mart_risk_report_summary", config,
        wait_for=[report_summary_f],
    )
    save_cohort_f = save_mart_table.submit(
        default_cohort_f, "mart_default_cohort", config,
        wait_for=[default_cohort_f],
    )

    # --- GE checkpoint sau khi tất cả mart tables đã được lưu ---
    run_ge_checkpoint.submit(
        checkpoint_name=config["great_expectations"]["mart_checkpoint"],
        ge_root_dir=ge_root_dir,
        enabled=ge_enabled,
        wait_for=[save_model_f, save_report_f, save_cohort_f],
    )

    print("[Mart] --- Mart Build Flow đã kết thúc ---")


if __name__ == "__main__":
    mart_build_flow()
