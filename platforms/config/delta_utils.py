"""
Delta Lake utility functions for Bronze, Staging, and Mart layers.

Provides:
- add_bronze_metadata       — adds _load_ts, _source_file, _load_date
- get_delta_path            — builds the S3A path for a Delta table
- write_delta_partitioned   — partition-overwrite write (Bronze)
- write_delta               — full-overwrite write (Staging)
- write_jdbc                — write to PostgreSQL via JDBC (Silver/Mart)
- run_sql_transform         — execute a SQL file with Spark
- optimize_delta_table      — inline fire-and-forget OPTIMIZE after large writes
- optimize_and_vacuum_table — full OPTIMIZE + VACUUM for scheduled maintenance jobs
- maintain_all_bronze_tables — iterate all Bronze tables from config and maintain each
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date
from typing import Optional
from delta.tables import DeltaTable
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


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
    try:
        logger.info("delta_partitioned_write_started path=%s partition=%s", path, partition_col)
        (df.write.format("delta").mode("overwrite")
         .option("partitionOverwriteMode", "dynamic")
         .partitionBy(partition_col).save(path))
        logger.info("delta_partitioned_write_completed path=%s", path)
    except Exception as e:
        logger.exception("delta_partitioned_write_failed path=%s error=%s", path, e)
        raise


def write_delta(df: DataFrame, path: str) -> None:
    """
    Staging write strategy: full overwrite.
    Staging là derived data, recompute từ Bronze mỗi lần.
    """
    try:
        logger.info("delta_write_started path=%s", path)
        df.write.format("delta").mode("overwrite").save(path)
        logger.info("delta_write_completed path=%s", path)
    except Exception as e:
        logger.exception("delta_write_failed path=%s error=%s", path, e)
        raise


def write_jdbc(df: DataFrame, pg_config: dict, table_name: str, mode: str = "overwrite") -> None:
    """
    Write to PostgreSQL via JDBC.

    mode="overwrite" causes Spark to DROP and CREATE the table — appropriate
    for full-refresh mart tables. Use mode="append" for incremental loads.

    Parameters
    ----------
    df : DataFrame
    pg_config : dict
        PostgreSQL config from the active pipeline configuration.
    table_name : str
        Bare table name (without schema prefix); the schema is taken from pg_config.
    mode : str
        Spark write mode — "overwrite" | "append".
    """
    host = pg_config["host"]
    port = pg_config["port"]
    dbname = pg_config["dbname"]
    schema = pg_config.get("schema", "stg")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    properties = {
        "user": pg_config["user"],
        "password": pg_config["password"],
        "driver": "org.postgresql.Driver",
        "currentSchema": schema,
    }
    full_table_name = f"{schema}.{table_name}"

    try:
        logger.info(
            "jdbc_write_started table=%s schema=%s mode=%s",
            table_name, schema, mode,
        )
        df.write.jdbc(
            url=jdbc_url, table=full_table_name, mode=mode, properties=properties,
        )
        logger.info("jdbc_write_completed table=%s schema=%s", table_name, schema)
    except Exception as e:
        logger.exception(
            "jdbc_write_failed table=%s schema=%s mode=%s error=%s",
            table_name, schema, mode, e,
        )
        raise


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
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_query = f.read()
        return spark.sql(sql_query)
    except Exception as e:
        logger.exception("spark_sql_transform_failed sql_path=%s error=%s", sql_path, e)
        raise


# ---------------------------------------------------------------------------
# Inline optimization helper (fire-and-forget — does NOT raise on failure)
# ---------------------------------------------------------------------------

def optimize_delta_table(spark: SparkSession, path: str, zorder_col: Optional[str] = None) -> None:
    """
    Inline OPTIMIZE sau khi ghi dữ liệu lớn.

    Dùng ngay sau write_delta_partitioned() / write_delta() để compact small files.
    Không raise exception — pipeline tiếp tục dù OPTIMIZE fail (best-effort).
    Khác với optimize_and_vacuum_table() bên dưới: không chạy VACUUM và không raise.
    """
    try:
        delta_table = DeltaTable.forPath(spark, path)
        if zorder_col:
            delta_table.optimize().executeZOrderBy(zorder_col)
        else:
            delta_table.optimize().executeCompaction()
    except Exception as e:
        logger.warning("optimize_delta_table_skipped path=%s error=%s", path, e)


# ---------------------------------------------------------------------------
# Scheduled maintenance — OPTIMIZE + VACUUM (raises on failure)
# ---------------------------------------------------------------------------

def optimize_and_vacuum_table(
    spark: SparkSession,
    table_path: str,
    zorder_col: Optional[str] = None,
    retention_hours: float = 168.0,
) -> None:
    """
    Chạy OPTIMIZE (Z-Order Compaction) và VACUUM cho một bảng Delta.
    
    Parameters
    ----------
    table_path : str
        Đường dẫn S3A tới Delta table (e.g. s3a://lakehouse/bronze/bureau_balance)
    zorder_col : Optional[str]
        Cột dùng để sắp xếp đa chiều Z-ORDER (tăng tốc độ lọc Predicate Pushdown)
    retention_hours : float
        Số giờ lưu lại snapshot cũ (mặc định 168 giờ = 7 ngày)
    """
    try:
        logger.info("delta_maintenance_start path=%s zorder=%s", table_path, zorder_col)
        delta_table = DeltaTable.forPath(spark, table_path)

        # 1. OPTIMIZE
        if zorder_col:
            logger.info("running_zorder_optimize col=%s", zorder_col)
            delta_table.optimize().executeZOrderBy(zorder_col)
        else:
            logger.info("running_standard_compaction")
            delta_table.optimize().executeCompaction()

        # 2. VACUUM
        logger.info("running_vacuum retention_hours=%s", retention_hours)
        delta_table.vacuum(retention_hours)

        logger.info("delta_maintenance_success path=%s", table_path)
    except Exception as e:
        logger.exception("delta_maintenance_failed path=%s error=%s", table_path, e)
        raise


def maintain_all_bronze_tables(
    spark: SparkSession,
    bucket: str,
    prefix: str,
    tables_cfg: dict,
    retention_hours: float = 168.0,
) -> None:
    """
    Bảo trì toàn bộ Bronze Delta tables: OPTIMIZE (Z-ORDER) + VACUUM.

    tables_cfg được truyền từ caller (cli/flows/config/homecredit_config.yaml).
    Mỗi bảng chạy độc lập — 1 bảng fail không block các bảng còn lại (best-effort).

    Parameters
    ----------
    bucket : str
        MinIO/S3 bucket name (từ stack["minio"]["bucket"]).
    prefix : str
        Bronze prefix path (từ stack["minio"]["bronze_prefix"]).
    tables_cfg : dict
        Dict dạng {table_name: {zorder_col: ...}} — đọc từ homecredit_config.yaml.
    retention_hours : float
        Số giờ lưu snapshot cũ trước khi VACUUM dọn (mặc định 168h = 7 ngày).
    """
    for table_name, opts in tables_cfg.items():
        try:
            zorder_col = opts.get("zorder_col")
            table_path = f"s3a://{bucket}/{prefix}/{table_name}"
            optimize_and_vacuum_table(spark, table_path, zorder_col, retention_hours)
        except Exception as e:
            logger.warning(
                "maintenance_skipped table=%s error=%s", table_name, e
            )