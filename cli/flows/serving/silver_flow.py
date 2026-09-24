"""
Silver (Staging) transformation: Bronze Delta → PostgreSQL schema stg.

Pattern:
  1. Đọc Bronze tables từ MinIO → register Spark temp views (bronze_<table>).
  2. Mỗi target table có 1 hàm build_<table>() riêng biệt.
  3. Mỗi hàm đọc SQL file tương ứng trong cli/flows/sql/staging/ và chạy spark.sql().
  4. Kết quả write ra PostgreSQL qua JDBC.

Khi debug: log sẽ chỉ rõ hàm nào bị stuck (silver_build_started target=...).
SparkSession truyền trực tiếp — không serialize qua Prefect task boundary.
"""

from __future__ import annotations

from pathlib import Path

import psycopg2
from prefect import flow
from pyspark.sql import SparkSession, functions as F

from cli.flows.serving.metadata import load_config, load_stack_config
from platforms.prefect_orchestra.prefect_main import PrefectETLPipelineConfig
from platforms.processing.config.delta_utils import write_jdbc
from platforms.processing.spark_stack.spark_session import get_spark_session

_pipeline_cfg = PrefectETLPipelineConfig()
logger = _pipeline_cfg.silver_logger

# Path tới thư mục SQL queries
# silver_flow.py nằm ở cli/flows/serving/ → parents[1] = cli/flows
_SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "staging"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bronze_path(stack: dict, table: str) -> str:
    m = stack["minio"]
    return f"s3a://{m['bucket']}/{m['bronze_prefix']}/{table}"


def _read_sql(name: str) -> str:
    """Đọc SQL transformation query từ cli/flows/sql/staging/<name>.sql."""
    path = _SQL_DIR / f"{name}.sql"
    with path.open(encoding="utf-8") as f:
        return f.read()


def _register_bronze_view(spark: SparkSession, stack: dict, table: str) -> None:
    """Đọc Bronze Delta và register temp view tên bronze_<table>."""
    spark.read.format("delta").load(_bronze_path(stack, table)) \
        .createOrReplaceTempView(f"bronze_{table}")
    logger.info("bronze_view_registered view=bronze_%s", table)


def _build_and_write(
    spark: SparkSession,
    sql_name: str,
    target_table: str,
    pg_cfg: dict,
) -> None:
    """
    Chạy SQL transformation, write kết quả vào PostgreSQL.
    sql_name    : tên file SQL (không có .sql), ví dụ 'dim_date'
    target_table: tên bảng PostgreSQL trong schema stg
    """
    logger.info("silver_build_started target=%s", target_table)
    df = spark.sql(_read_sql(sql_name))
    write_jdbc(df, pg_cfg, target_table, mode=pg_cfg.get("write_mode", "overwrite"))
    logger.info("silver_build_completed target=%s", target_table)


def _pg_cfg(config: dict) -> dict:
    pg = dict(config["postgres"])
    pg["dbname"] = pg["database"]
    return pg


def _ensure_schema(config: dict) -> None:
    pg = config["postgres"]
    with psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["user"], password=pg["password"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg["schema"]}"')
    logger.info("silver_schema_ready schema=%s", pg["schema"])


# ---------------------------------------------------------------------------
# Dim_Date — generated, không cần Bronze view
# ---------------------------------------------------------------------------

def build_dim_date(spark: SparkSession, config: dict, pg_cfg: dict) -> None:
    date_cfg = config["staging"]["date_dimension"]
    # Tạo temp view date_range như SQL query dim_date.sql expects
    spark.range(1).select(
        F.explode(F.sequence(
            F.to_date(F.lit(date_cfg["start_date"])),
            F.to_date(F.lit(date_cfg["end_date"])),
        )).alias("Full_Date")
    ).createOrReplaceTempView("date_range")
    _build_and_write(spark, "dim_date", "Dim_Date", pg_cfg)


# ---------------------------------------------------------------------------
# Dim tables (dims phải build trước facts vì facts FK vào dims)
# ---------------------------------------------------------------------------

def build_dim_application_status(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "previous_application")
    _build_and_write(spark, "dim_application_status", "Dim_Application_Status", pg_cfg)


def build_dim_contract_type(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "application")
    _build_and_write(spark, "dim_contract_type", "Dim_Contract_Type", pg_cfg)


def build_dim_customer(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "application")
    _build_and_write(spark, "dim_customer", "Dim_Customer", pg_cfg)


# ---------------------------------------------------------------------------
# Fact tables
# ---------------------------------------------------------------------------

def build_fact_loan_repayment(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "installments_payments")
    _register_bronze_view(spark, stack, "previous_application")
    _build_and_write(spark, "fact_loan_repayment", "Fact_Loan_Repayment", pg_cfg)


def build_fact_loan_application(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "application")
    _register_bronze_view(spark, stack, "previous_application")
    _build_and_write(spark, "fact_loan_application", "Fact_Loan_Application", pg_cfg)


def build_fact_bureau_credit(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "bureau")
    _build_and_write(spark, "fact_bureau_credit", "Fact_Bureau_Credit", pg_cfg)


def build_fact_bureau_monthly_snapshot(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "bureau_balance")
    _build_and_write(spark, "fact_bureau_monthly_snapshot", "Fact_Bureau_Monthly_Snapshot", pg_cfg)


def build_fact_credit_balance(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "credit_card_balance")
    _register_bronze_view(spark, stack, "application")   # needed for dim_contract CTE
    _build_and_write(spark, "fact_credit_balance", "Fact_Credit_Balance", pg_cfg)


def build_fact_pos_cash_balance(spark: SparkSession, stack: dict, pg_cfg: dict) -> None:
    _register_bronze_view(spark, stack, "pos_cash_balance")
    _register_bronze_view(spark, stack, "application")   # needed for dim_contract CTE
    _build_and_write(spark, "fact_pos_cash_balance", "Fact_POS_CASH_balance", pg_cfg)


# ---------------------------------------------------------------------------
# @flow — đúng thứ tự: dims → facts
# ---------------------------------------------------------------------------

@flow(
    name="Bronze To PostgreSQL Staging",
    log_prints=False,
    description="Transform Bronze Delta → PostgreSQL stg. 1 hàm per bảng, Spark SQL.",
)
def staging_transform_flow() -> list[str]:
    """
    Thứ tự build: Dim_Date → dims → facts.
    Mỗi hàm build_<table>() độc lập — khi fail log chỉ rõ target nào bị stuck.
    """
    spark = None
    try:
        config = load_config()["project_params"]
        stack = load_stack_config()
        pg_cfg = _pg_cfg(config)

        _ensure_schema(config)
        spark = get_spark_session(stack)
        logger.info("silver_flow_started")

        completed: list[str] = []

        # --- Dimensions (không phụ thuộc nhau) ---
        for fn, label in [
            (lambda: build_dim_date(spark, config, pg_cfg),               "Dim_Date"),
            (lambda: build_dim_application_status(spark, stack, pg_cfg),  "Dim_Application_Status"),
            (lambda: build_dim_contract_type(spark, stack, pg_cfg),        "Dim_Contract_Type"),
            (lambda: build_dim_customer(spark, stack, pg_cfg),             "Dim_Customer"),
        ]:
            try:
                fn()
                completed.append(label)
            except Exception as e:
                logger.exception("silver_dim_failed target=%s error=%s", label, e)
                raise

        # --- Facts (phụ thuộc dims đã có trong PostgreSQL) ---
        for fn, label in [
            (lambda: build_fact_loan_repayment(spark, stack, pg_cfg),        "Fact_Loan_Repayment"),
            (lambda: build_fact_loan_application(spark, stack, pg_cfg),      "Fact_Loan_Application"),
            (lambda: build_fact_bureau_credit(spark, stack, pg_cfg),         "Fact_Bureau_Credit"),
            (lambda: build_fact_bureau_monthly_snapshot(spark, stack, pg_cfg), "Fact_Bureau_Monthly_Snapshot"),
            (lambda: build_fact_credit_balance(spark, stack, pg_cfg),        "Fact_Credit_Balance"),
            (lambda: build_fact_pos_cash_balance(spark, stack, pg_cfg),      "Fact_POS_CASH_balance"),
        ]:
            try:
                fn()
                completed.append(label)
            except Exception as e:
                logger.exception("silver_fact_failed target=%s error=%s", label, e)
                raise

        logger.info("silver_flow_completed tables=%d", len(completed))
        return completed
    except Exception as e:
        logger.exception("silver_flow_failed error=%s", e)
        raise
    finally:
        if spark is not None:
            spark.stop()


# if __name__ == "__main__":
#     staging_transform_flow()
