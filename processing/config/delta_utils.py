from pyspark.sql.functions import current_timestamp, lit, to_date
from pyspark.sql import DataFrame

def add_bronze_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Adds metadata columns to a Bronze DataFrame."""
    load_ts = current_timestamp()
    return df.withColumn("_load_ts", load_ts) \
             .withColumn("_source_file", lit(source_file)) \
             .withColumn("_load_date", to_date(load_ts))

def get_delta_path(config: dict, layer: str, table_name: str) -> str:
    """Constructs the S3A path for a Delta table."""
    bucket = config['minio']['bucket']
    # Adjust for stg_ table names
    clean_table_name = table_name.replace('stg_', '')
    layer_path = config['minio'][f'{layer}_path']
    return f"s3a://{bucket}/{layer_path}/{clean_table_name}"

def write_delta_partitioned(df: DataFrame, path: str, partition_col: str):
    """Writes a DataFrame to a partitioned Delta table with partition overwrite."""
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("partitionOverwriteMode", "dynamic") \
      .partitionBy(partition_col) \
      .save(path)

def write_delta(df: DataFrame, path: str):
    """Writes a DataFrame to a non-partitioned Delta table with full overwrite."""
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save(path)

def write_jdbc(df: DataFrame, pg_config: dict, table_name: str):
    """Writes a DataFrame to a PostgreSQL table using JDBC with overwrite mode."""
    jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['dbname']}"
    properties = {
        "user": pg_config['user'],
        "password": pg_config['password'],
        "driver": "org.postgresql.Driver"
    }
    full_table_name = f"{pg_config['schema']}.{table_name}"

    df.write \
      .jdbc(url=jdbc_url,
            table=full_table_name,
            mode="overwrite", # This will DROP and CREATE the table
            properties=properties)

def run_sql_transform(spark, sql_path: str) -> DataFrame:
    """Reads a SQL file and executes it using Spark."""
    with open(sql_path, 'r') as f:
        sql_query = f.read()
    return spark.sql(sql_query)