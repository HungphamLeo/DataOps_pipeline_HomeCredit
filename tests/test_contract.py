import pytest
import pandera.errors
from pyspark.sql import SparkSession
from cli.data_contract.application_contract import APPLICATION_CRITICAL, APPLICATION_WARNING

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test-contracts").getOrCreate()

def test_application_critical_valid(spark):
    data = [(100001, 0, 500000.0), (100002, 1, 700000.0)]
    df = spark.createDataFrame(data, ["SK_ID_CURR", "TARGET", "AMT_CREDIT"])
    # Validate không ném lỗi
    validated_df = APPLICATION_CRITICAL.validate(df)
    assert validated_df.count() == 2

def test_application_critical_invalid_target(spark):
    data = [(100001, 5, 500000.0)]  # TARGET = 5 là sai
    df = spark.createDataFrame(data, ["SK_ID_CURR", "TARGET", "AMT_CREDIT"])
    with pytest.raises(Exception):
        APPLICATION_CRITICAL.validate(df, lazy=True)