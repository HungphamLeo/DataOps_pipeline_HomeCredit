"""Spark pipeline: source CSV -> MinIO Bronze Delta -> PostgreSQL Silver."""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import yaml
from prefect import flow, task
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from platforms.processing.spark_stack.spark_session import get_spark_session
from log.config.logger_setup import logger_manager

LOGGER = logger_manager.get_logger(__name__)
CONFIG_PATH = PROJECT_ROOT / "platforms" / "config" / "stack.yaml"
PIPELINE_CONFIG_PATH = PROJECT_ROOT / "cli" / "config" / "homecredit_config.yaml"

SOURCE_TABLES = (
    "application", "bureau", "bureau_balance", "previous_application",
    "installments_payments", "pos_cash_balance", "credit_card_balance",
)


def _expand(value: object) -> object:
    if not isinstance(value, str) or not value.startswith("${"):
        return value
    expression = value[2:-1]
    name, _, default = expression.partition(":-")
    return os.environ.get(name, default)


def _expand_tree(value: object) -> object:
    if isinstance(value, dict):
        return {key: _expand_tree(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_tree(item) for item in value]
    return _expand(value)


def load_config() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as file:
        config = _expand_tree(yaml.safe_load(file) or {})
    with PIPELINE_CONFIG_PATH.open(encoding="utf-8") as file:
        project_config = _expand_tree(yaml.safe_load(file) or {})
    config["project"] = project_config.get("project_params", {})
    logger_manager.configure(str(PROJECT_ROOT / config["project"]["logger_config"]))
    LOGGER.info(
        "pipeline_config_loaded environment=%s source_dir=%s",
        config.get("environment"),
        config["pipeline"]["source_dir"],
    )
    return config


def _source_path(config: dict, table_name: str) -> str:
    return str(PROJECT_ROOT / config["pipeline"]["source_dir"] /
               config["pipeline"]["source_files"][table_name])


def _bronze_path(config: dict, table_name: str) -> str:
    minio = config["minio"]
    return f"s3a://{minio['bucket']}/{minio['bronze_prefix']}/{table_name}"


def _jdbc(config: dict) -> tuple[str, dict, str]:
    pg = config["postgres"]
    return (
        f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}",
        {"user": pg["user"], "password": pg["password"],
         "driver": "org.postgresql.Driver"},
        pg["silver_schema"],
    )


def _ensure_silver_schema(config: dict) -> None:
    import psycopg2
    pg = config["postgres"]
    with psycopg2.connect(
        host=pg["host"], port=pg["port"], database=pg["database"],
        user=pg["user"], password=pg["password"],
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg["silver_schema"]}"')


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def ingest_to_bronze(table_name: str, config: dict) -> str:
    started = time.monotonic()
    spark = get_spark_session(config)
    source = _source_path(config, table_name)
    LOGGER.info("bronze_ingest_started table=%s source=%s", table_name, source)
    if not Path(source).exists():
        LOGGER.error("bronze_source_missing table=%s source=%s", table_name, source)
        raise FileNotFoundError(f"Source file does not exist: {source}")
    load_date = datetime.utcnow().strftime("%Y-%m-%d")
    frame = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .option("mode", "FAILFAST").csv(source)
        .withColumn("_load_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(Path(source).name))
        .withColumn("_load_date", F.lit(load_date).cast("date"))
    )
    (frame.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"_load_date = '{load_date}'")
     .partitionBy("_load_date").save(_bronze_path(config, table_name)))
    row_count = frame.count()
    LOGGER.info(
        "bronze_ingest_completed table=%s rows=%s duration_seconds=%.2f path=%s",
        table_name, row_count, time.monotonic() - started,
        _bronze_path(config, table_name),
    )
    return _bronze_path(config, table_name)


def _read_bronze(spark: SparkSession, config: dict, table_name: str) -> DataFrame:
    return spark.read.format("delta").load(_bronze_path(config, table_name))


def _date_key(column: object) -> object:
    return F.date_format(
        F.date_add(F.current_date(), column.cast("int")), "yyyyMMdd"
    ).cast("int")


def build_silver_tables(spark: SparkSession, config: dict) -> Dict[str, DataFrame]:
    app = _read_bronze(spark, config, "application")
    bureau = _read_bronze(spark, config, "bureau")
    bureau_balance = _read_bronze(spark, config, "bureau_balance")
    previous = _read_bronze(spark, config, "previous_application")
    installments = _read_bronze(spark, config, "installments_payments")
    pos = _read_bronze(spark, config, "pos_cash_balance")
    card = _read_bronze(spark, config, "credit_card_balance")

    customer = (app.select(
        F.col("SK_ID_CURR").cast("long").alias("customer_bk"),
        "DAYS_BIRTH", "CODE_GENDER", "NAME_FAMILY_STATUS",
        "NAME_EDUCATION_TYPE", "REGION_RATING_CLIENT",
        "AMT_INCOME_TOTAL", "DAYS_EMPLOYED",
    ).withColumn("effective_date", F.current_timestamp())
     .withColumn("expiry_date", F.to_timestamp(F.lit("9999-12-31")))
     .withColumn("is_current_flag", F.lit("Y")))

    contract = (app.select(F.col("NAME_CONTRACT_TYPE").alias("contract_type_code"))
        .unionByName(previous.select(
            F.col("NAME_CONTRACT_TYPE").alias("contract_type_code")))
        .distinct().withColumn("contract_type_sk", F.monotonically_increasing_id())
        .withColumn("contract_type_name", F.col("contract_type_code")))

    decision_days = F.col("DAYS_DECISION") if "DAYS_DECISION" in app.columns else F.lit(0)
    loan_application = app.select(
        F.monotonically_increasing_id().alias("application_sk"),
        F.col("SK_ID_CURR").cast("long").alias("customer_bk"),
        _date_key(decision_days).alias("decision_date_sk"),
        "NAME_CONTRACT_TYPE", "AMT_INCOME_TOTAL", "AMT_CREDIT",
        "AMT_ANNUITY", "AMT_GOODS_PRICE", "TARGET",
    )

    repayment = installments.select(
        F.monotonically_increasing_id().alias("repayment_sk"),
        F.col("SK_ID_CURR").cast("long").alias("customer_bk"),
        _date_key(F.col("DAYS_INSTALMENT")).alias("due_date_sk"),
        _date_key(F.col("DAYS_ENTRY_PAYMENT")).alias("payment_date_sk"),
        F.col("SK_ID_PREV").cast("string").alias("loan_id"),
        F.col("NUM_INSTALMENT_NUMBER").cast("int").alias("instalment_number"),
        "AMT_INSTALMENT", "AMT_PAYMENT",
        (F.col("AMT_INSTALMENT") - F.col("AMT_PAYMENT")).alias("underpaid_amount"),
        (F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT")).alias("days_past_due"),
    )
    bureau_credit = bureau.select(
        F.monotonically_increasing_id().alias("bureau_credit_sk"),
        F.col("SK_ID_CURR").cast("long").alias("customer_bk"),
        _date_key(F.col("DAYS_CREDIT")).alias("credit_date_sk"),
        F.col("SK_ID_BUREAU").cast("string").alias("bureau_id"),
        "CREDIT_ACTIVE", "CREDIT_TYPE", "AMT_CREDIT_SUM",
        "AMT_CREDIT_MAX_OVERDUE",
    )
    bureau_snapshot = bureau_balance.select(
        F.monotonically_increasing_id().alias("bureau_snapshot_sk"),
        F.col("SK_ID_BUREAU").cast("string").alias("bureau_id"),
        _date_key(F.col("MONTHS_BALANCE") * F.lit(30)).alias("month_date_sk"),
        F.col("STATUS").cast("string").alias("status_code"),
        F.col("MONTHS_BALANCE").cast("short").alias("months_balance"),
    )
    credit_balance = card.select(
        F.monotonically_increasing_id().alias("credit_balance_sk"),
        F.col("SK_ID_CURR").cast("long").alias("customer_bk"),
        _date_key(F.col("MONTHS_BALANCE") * F.lit(30)).alias("credit_date_sk"),
        F.col("SK_ID_PREV").cast("string").alias("bureau_id"),
        "AMT_BALANCE", "AMT_CREDIT_LIMIT_ACTUAL", "AMT_PAYMENT_TOTAL_CURRENT",
    )
    pos_balance = pos.select(
        F.monotonically_increasing_id().alias("pos_cash_sk"),
        F.col("SK_ID_CURR").cast("long").alias("customer_bk"),
        _date_key(F.col("MONTHS_BALANCE") * F.lit(30)).alias("month_date_sk"),
        F.col("SK_ID_PREV").cast("string").alias("loan_id"),
        "CNT_INSTALMENT", "CNT_INSTALMENT_FUTURE", "SK_DPD", "SK_DPD_DEF",
    )
    dates = spark.sql(
        "SELECT explode(sequence(to_date('2000-01-01'), current_date(), "
        "interval 1 day)) AS full_date"
    ).select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_sk"),
        "full_date", F.dayofmonth("full_date").alias("day_of_month"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.quarter("full_date").alias("quarter_number"),
        F.year("full_date").alias("year_number"),
        F.dayofweek("full_date").isin([1, 7]).alias("is_weekend"),
    )
    return {
        "dim_date": dates, "dim_customer": customer,
        "dim_contract_type": contract, "fact_loan_application": loan_application,
        "fact_loan_repayment": repayment, "fact_bureau_credit": bureau_credit,
        "fact_bureau_monthly_snapshot": bureau_snapshot,
        "fact_credit_balance": credit_balance, "fact_pos_cash_balance": pos_balance,
    }


@task(retries=1, retry_delay_seconds=30, log_prints=True)
def transform_to_silver(config: dict) -> list[str]:
    started = time.monotonic()
    LOGGER.info("silver_transform_started schema=%s", config["postgres"]["silver_schema"])
    spark = get_spark_session(config)
    _ensure_silver_schema(config)
    url, properties, schema = _jdbc(config)
    tables = build_silver_tables(spark, config)
    for table_name, frame in tables.items():
        frame.write.jdbc(
            url=url, table=f"{schema}.{table_name}", mode="overwrite",
            properties=properties,
        )
        LOGGER.info(
            "silver_table_written table=%s columns=%s duration_seconds=%.2f",
            table_name, len(frame.columns), time.monotonic() - started,
        )
    LOGGER.info(
        "silver_transform_completed tables=%d duration_seconds=%.2f",
        len(tables), time.monotonic() - started,
    )
    return list(tables)


@flow(name="Home Credit Bronze to Silver", log_prints=True)
def bronze_to_silver_flow() -> list[str]:
    started = time.monotonic()
    LOGGER.info("bronze_to_silver_flow_started tables=%d", len(SOURCE_TABLES))
    config = load_config()
    futures = [ingest_to_bronze.submit(table_name, config) for table_name in SOURCE_TABLES]
    for future in futures:
        future.result()
    tables = transform_to_silver(config)
    LOGGER.info(
        "bronze_to_silver_flow_completed tables=%d duration_seconds=%.2f",
        len(tables), time.monotonic() - started,
    )
    return tables


if __name__ == "__main__":
    bronze_to_silver_flow()
