from pyspark.sql import SparkSession
import yaml
from pathlib import Path

def get_spark_session(config: dict):
    """
    Initializes and returns a SparkSession configured for Delta Lake and MinIO.
    """
    spark_config = config['spark']
    minio_config = config['minio']

    builder = (
        SparkSession.builder.appName(spark_config['app_name'])
        .config("spark.jars.packages", spark_config['jars_packages'])
        .config("spark.sql.extensions", spark_config['sql_extensions'])
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", minio_config['endpoint_url'])
        .config("spark.hadoop.fs.s3a.access.key", minio_config['access_key'])
        .config("spark.hadoop.fs.s3a.secret.key", minio_config['secret_key'])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .enableHiveSupport()
    )
    
    print("SparkSession configured for Delta Lake and MinIO.")
    return builder.getOrCreate()