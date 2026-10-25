from cli.flows.serving.metadata import load_config, load_stack_config

def test_load_config_structure():
    cfg = load_config()
    assert "bronze_maintenance" in cfg
    assert "tables" in cfg["bronze_maintenance"]
    assert "application_train" in cfg["bronze_maintenance"]["tables"]

def test_load_stack_config_structure():
    stack = load_stack_config()
    assert "minio" in stack
    assert "spark" in stack
    assert "bucket" in stack["minio"]