"""
Spark session factory for Delta Lake + MinIO (S3A) workloads.

Usage:
    from platforms.processing.spark_stack.spark_session import get_spark_session
    from config import load_stack_config

    spark = get_spark_session(load_stack_config())
"""
import os

from pyspark.sql import SparkSession

from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def get_spark_session(config: dict) -> SparkSession:
    """
    Cấu hình và trả về một SparkSession cho Delta Lake + MinIO.

    Lưu ý quan trọng:
    - enableHiveSupport() bị loại bỏ: không tương thích với Delta Lake
      catalog trong chế độ local và yêu cầu Hive metastore riêng.
    - Credentials MinIO đọc từ config dict (không hard-code).
    - Env vars MINIO_ACCESS_KEY / MINIO_SECRET_KEY override config nếu tồn tại.
    """
    try:
        spark_cfg = config["spark"]
        minio_cfg = config["minio"]
        logger.info(
            "spark_session_start app=%s master=%s",
            spark_cfg.get("app_name"),
            spark_cfg.get("master", "local[*]"),
        )
        access_key = os.environ.get("MINIO_ACCESS_KEY", minio_cfg["access_key"])
        secret_key = os.environ.get("MINIO_SECRET_KEY", minio_cfg["secret_key"])
        endpoint_url = os.environ.get(
            "MINIO_ENDPOINT",
            minio_cfg.get("endpoint", minio_cfg.get("endpoint_url")),
        )
        if not endpoint_url:
            raise ValueError("MinIO endpoint is required in stack configuration")
        if not endpoint_url.startswith(("http://", "https://")):
            endpoint_url = f"http://{endpoint_url}"
        packages = spark_cfg.get("packages") or spark_cfg.get("jars_packages")
        if isinstance(packages, list):
            packages = ",".join(packages)

        builder = (
            SparkSession.builder
            .appName(spark_cfg["app_name"])
            .master(spark_cfg.get("master", "local[*]"))
            .config("spark.jars.packages", packages)
            .config(
                "spark.sql.extensions",
                spark_cfg.get(
                    "sql_extensions",
                    "io.delta.sql.DeltaSparkSessionExtension",
                ),
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config(
                "spark.delta.logStore.class",
                "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
            )
            .config("spark.driver.memory", spark_cfg.get("driver_memory", "4g"))
            .config("spark.executor.memory", spark_cfg.get("executor_memory", "4g"))
        )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info(
            "spark_session_ready app=%s minio_endpoint=%s packages=%s",
            spark_cfg.get("app_name"),
            endpoint_url,
            packages,
        )
        return spark
    except Exception as e:
        logger.exception("spark_session_failed error=%s", e)
        raise
