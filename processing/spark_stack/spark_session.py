"""
Spark session factory for Delta Lake + MinIO (S3A) workloads.

Usage:
    from processing.spark_stack.spark_session import get_spark_session
    import yaml

    with open("prefect_orchestra/config/pipeline_config.yaml") as f:
        config = yaml.safe_load(f)

    spark = get_spark_session(config)
"""
from pyspark.sql import SparkSession
import os


def get_spark_session(config: dict) -> SparkSession:
    """
    Cấu hình và trả về một SparkSession cho Delta Lake + MinIO.

    Lưu ý quan trọng:
    - enableHiveSupport() bị loại bỏ: không tương thích với Delta Lake
      catalog trong chế độ local và yêu cầu Hive metastore riêng.
    - Credentials MinIO đọc từ config dict (không hard-code).
    - Env vars MINIO_ACCESS_KEY / MINIO_SECRET_KEY override config nếu tồn tại.
    """
    spark_cfg = config["spark"]
    minio_cfg = config["minio"]

    # Allow env vars to override config file credentials
    access_key = os.environ.get("MINIO_ACCESS_KEY", minio_cfg["access_key"])
    secret_key = os.environ.get("MINIO_SECRET_KEY", minio_cfg["secret_key"])
    endpoint_url = os.environ.get("MINIO_ENDPOINT", minio_cfg["endpoint_url"])

    builder = (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .master(spark_cfg.get("master", "local[*]"))
        # Delta Lake packages
        .config("spark.jars.packages", spark_cfg["jars_packages"])
        .config("spark.sql.extensions", spark_cfg["sql_extensions"])
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # MinIO / S3A configuration
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # Delta Lake runtime settings
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        # Memory tuning (overridable via config)
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "4g"))
        .config("spark.executor.memory", spark_cfg.get("executor_memory", "4g"))
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(
        f"[Spark] Session created — app: {spark_cfg['app_name']}, "
        f"master: {spark_cfg.get('master', 'local[*]')}"
    )
    return spark
