"""
Delta Lake utility functions for Bronze, Staging, and Mart layers.

Provides:
- add_bronze_metadata       — adds _load_ts, _source_file, _load_date
- get_delta_path            — builds the S3A path for a Delta table
- write_delta_partitioned   — partition-overwrite write (Bronze)
- write_delta               — full-overwrite write (Staging)
- write_jdbc                — write to PostgreSQL via JDBC (Mart)
- run_sql_transform         — execute a SQL file with Spark
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def add_bronze_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Bronze metadata: _load_ts, _source_file, _load_date (partition key)."""
    load_ts = current_timestamp()
    return (
        df
        .withColumn("_load_ts", load_ts)
        .withColumn("_source_file", lit(source_file))
        .withColumn("_load_date", to_date(load_ts))
    )


# ---------------------------------------------------------------------------
# Path builders
# ---------------------------------------------------------------------------

def get_delta_path(config: dict, layer: str, table_name: str) -> str:
    """
    Constructs the S3A path for a Delta table.

    Parameters
    ----------
    config : dict
        Pipeline config dict (from pipeline_config.yaml).
    layer : str
        One of "bronze" or "staging".
    table_name : str
        Table name as stored in the config (e.g. "stg_application").
        The "stg_" prefix is preserved — stripping was a bug in the original code.

    Returns
    -------
    str
        Full S3A path, e.g. "s3a://lakehouse/staging/stg_application".
    """
    bucket = config["minio"]["bucket"]
    layer_path = config["minio"][f"{layer}_path"]
    # Do NOT strip any prefix from table_name — preserve stg_ / bronze_ etc.
    return f"s3a://{bucket}/{layer_path}/{table_name}"


# ---------------------------------------------------------------------------
# Write utilities
# ---------------------------------------------------------------------------

def write_delta_partitioned(df: DataFrame, path: str, partition_col: str = "_load_date") -> None:
    """
    Bronze write strategy: partition overwrite.
    Chạy lại cùng ngày sẽ overwrite đúng partition đó, không nhân đôi data.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(partition_col)
        .save(path)
    )


def write_delta(df: DataFrame, path: str) -> None:
    """
    Staging write strategy: full overwrite.
    Staging là derived data, recompute từ Bronze mỗi lần.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
    )


def write_jdbc(df: DataFrame, pg_config: dict, table_name: str, mode: str = "overwrite") -> None:
    """
    Write to PostgreSQL via JDBC.

    mode="overwrite" causes Spark to DROP and CREATE the table — appropriate
    for full-refresh mart tables. Use mode="append" for incremental loads.

    Parameters
    ----------
    df : DataFrame
    pg_config : dict
        Sub-dict from pipeline_config.yaml under "postgres".
    table_name : str
        Bare table name (without schema prefix); the schema is taken from pg_config.
    mode : str
        Spark write mode — "overwrite" | "append".
    """
    host = pg_config["host"]
    port = pg_config["port"]
    dbname = pg_config["dbname"]
    schema = pg_config.get("schema", "mart")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    properties = {
        "user": pg_config["user"],
        "password": pg_config["password"],
        "driver": "org.postgresql.Driver",
        "currentSchema": schema,
    }
    full_table_name = f"{schema}.{table_name}"

    (
        df.write
        .jdbc(
            url=jdbc_url,
            table=full_table_name,
            mode=mode,
            properties=properties,
        )
    )


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

def run_sql_transform(spark: SparkSession, sql_path: str) -> DataFrame:
    """
    Read a .sql file and execute it using spark.sql().

    The SQL file may reference temp views that must be registered before calling
    this function:
        bronze_df.createOrReplaceTempView("bronze_application")
        result_df = run_sql_transform(spark, "sql/staging/stg_application.sql")
    """
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_query = f.read()
    return spark.sql(sql_query)
