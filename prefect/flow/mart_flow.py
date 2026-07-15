import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from functools import reduce
from prefect import flow, task

# Import GE task
from tasks.ge_validator import run_ge_checkpoint

# --- Cấu hình và đường dẫn ---
BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "prefect" / "config" / "pipeline_config.yaml"

# --- Các Task trợ giúp ---

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
    print(f"Đang lưu bảng mart '{table_name}' vào {file_path}")
    df.to_parquet(file_path, index=False, engine="pyarrow")
    print(f"Đã lưu thành công {len(df)} dòng vào {file_path}")
    return file_path

# --- Các Task xây dựng Mart ---

@task(log_prints=True)
def build_mart_risk_model_features(staging_dfs: dict) -> pd.DataFrame:
    """Xây dựng bảng wide-table cho ML bằng cách join tất cả các bảng staging."""
    print("Đang xây dựng mart_risk_model_features...")
    
    stg_app = staging_dfs['stg_application']
    
    summary_tables = [
        staging_dfs['stg_bureau_summary'],
        staging_dfs['stg_prev_application_summary'],
        staging_dfs['stg_installment_summary'],
        staging_dfs['stg_pos_cash_summary'],
        staging_dfs['stg_credit_card_summary']
    ]
    
    df_final = reduce(lambda left, right: pd.merge(left, right, on='SK_ID_CURR', how='left'), summary_tables, stg_app)
    
    print(f"Bảng join cuối cùng có shape: {df_final.shape}")
    return df_final

@task(log_prints=True)
def build_mart_risk_report_summary(model_features_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng tóm tắt cho mục đích báo cáo."""
    print("Đang xây dựng mart_risk_report_summary...")
    
    df = model_features_df.copy()
    
    def assign_risk_tier(row):
        ext_source_2 = row.get('EXT_SOURCE_2', np.nan)
        max_overdue = row.get('BUREAU_MAX_OVERDUE', np.nan)
        bad_status_rate = row.get('BUREAU_AVG_BAD_STATUS_RATE', np.nan)
        
        if pd.notna(ext_source_2) and pd.notna(max_overdue) and ext_source_2 < 0.3 and max_overdue > 0:
            return "High"
        elif pd.notna(ext_source_2) and pd.notna(bad_status_rate) and ext_source_2 > 0.5 and bad_status_rate < 0.1:
            return "Low"
        else:
            return "Medium"

    df['RISK_TIER'] = df.apply(assign_risk_tier, axis=1)
    
    report_cols = [
        'SK_ID_CURR', 'TARGET', 'AGE_YEARS', 'NAME_INCOME_TYPE', 'NAME_EDUCATION_TYPE', 
        'NAME_FAMILY_STATUS', 'AMT_INCOME_TOTAL', 'AMT_CREDIT', 'INCOME_CREDIT_RATIO',
        'EXT_SOURCE_2', 'BUREAU_LOAN_COUNT', 'BUREAU_MAX_OVERDUE', 'BUREAU_AVG_BAD_STATUS_RATE', 
        'INSTALMENT_LATE_RATE', 'POS_MAX_DPD', 'CC_AVG_UTILIZATION', 'RISK_TIER'
    ]
    
    existing_report_cols = [col for col in report_cols if col in df.columns]
    return df[existing_report_cols]

@task(log_prints=True)
def build_mart_default_cohort(app_df: pd.DataFrame) -> pd.DataFrame:
    """Xây dựng bảng tỉ lệ nợ xấu theo các nhóm cohort."""
    print("Đang xây dựng mart_default_cohort...")
    
    cohort_dims = ['NAME_INCOME_TYPE', 'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS', 'NAME_HOUSING_TYPE', 'REGION_RATING_CLIENT']
    all_cohorts = []
    
    for dim in cohort_dims:
        cohort_agg = app_df.groupby(dim)['TARGET'].agg(['count', 'sum']).reset_index()
        cohort_agg.rename(columns={'count': 'TOTAL_COUNT', 'sum': 'DEFAULT_COUNT', dim: 'COHORT_VALUE'}, inplace=True)
        cohort_agg['COHORT_DIM'] = dim
        cohort_agg['DEFAULT_RATE'] = cohort_agg['DEFAULT_COUNT'] / cohort_agg['TOTAL_COUNT']
        all_cohorts.append(cohort_agg[['COHORT_DIM', 'COHORT_VALUE', 'TOTAL_COUNT', 'DEFAULT_COUNT', 'DEFAULT_RATE']])
        
    return pd.concat(all_cohorts, ignore_index=True)

# --- Mart Flow chính ---

@flow(name="Mart Build Flow", log_prints=True)
def mart_build_flow():
    """
    Đọc dữ liệu từ tầng Staging, join chúng thành các bảng phân tích,
    và lưu kết quả vào tầng Mart.
    """
    print("--- Bắt đầu Mart Build Flow ---")
    
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
        
    staging_tables = ['stg_application', 'stg_bureau_summary', 'stg_prev_application_summary', 'stg_installment_summary', 'stg_pos_cash_summary', 'stg_credit_card_summary']
    staging_dfs = {tbl: read_staging_table(tbl, config) for tbl in staging_tables}
    
    model_features_df = build_mart_risk_model_features(staging_dfs)
    save_model_features_future = save_mart_table.submit(model_features_df, "mart_risk_model_features", config)
    
    report_summary_df = build_mart_risk_report_summary(model_features_df)
    save_mart_table(report_summary_df, "mart_risk_report_summary", config)
    
    default_cohort_df = build_mart_default_cohort(staging_dfs['stg_application'])
    save_mart_table(default_cohort_df, "mart_default_cohort", config)
    
    # Chạy GE checkpoint sau khi bảng mart chính cho model đã được lưu
    run_ge_checkpoint.submit(checkpoint_name="mart_checkpoint.yml",
                             ge_root_dir=str(BASE_DIR / "great_expectations"),
                             wait_for=[save_model_features_future])
    
    print("--- Mart Build Flow đã kết thúc ---")

if __name__ == "__main__":
    mart_build_flow()