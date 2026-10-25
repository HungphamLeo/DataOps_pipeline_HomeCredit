from platforms.config.delta_utils import get_delta_path

def test_get_delta_path_bronze():
    mock_config = {
        "minio": {
            "bucket": "homecredit-lakehouse",
            "bronze_path": "bronze"
        }
    }
    path = get_delta_path(mock_config, layer="bronze", table_name="application_train")
    assert path == "s3a://homecredit-lakehouse/bronze/application_train"

def test_get_delta_path_staging():
    mock_config = {
        "minio": {
            "bucket": "homecredit-lakehouse",
            "staging_path": "staging"
        }
    }
    path = get_delta_path(mock_config, layer="staging", table_name="stg_application")
    assert path == "s3a://homecredit-lakehouse/staging/stg_application"