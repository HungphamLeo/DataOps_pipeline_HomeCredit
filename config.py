"""
Project-wide configuration bootstrap.

Single source of truth cho:
  - Load .env (dotenv)
  - Expose path của mọi file/thư mục quan trọng qua hàm getter
  - Load và parse từng YAML config với ${VAR:-default} expansion

Tất cả module khác trong project CHỈ import từ đây.
KHÔNG tự tính Path, KHÔNG tự mở YAML ở bên ngoài file này.

Env vars (đặt trong .env hoặc Docker environment):
  CONFIG_PATH           — path tới homecredit_config.yaml
                          mặc định: <root>/cli/flows/config/homecredit_config.yaml
  STACK_CONFIG_PATH     — path tới stack.yaml (MinIO/Spark)
                          mặc định: <root>/platforms/config/stack.yaml
  LOGGER_CONFIG_PATH    — path tới logger_config.yaml
                          mặc định: <root>/log/config/logger_config.yaml
  SOURCE_DIR            — thư mục chứa source CSV
                          mặc định: <root>/cli/ingestion/source_data
  DESIGN_METADATA_PATH  — path tới Design modeling_doc.csv
                          mặc định: <root>/cli/ingestion/design_modelling/Design modeling_doc.csv
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# 1. Project root — anchor theo file này (ở root repo), không dùng parents[n]
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# 2. Load .env nếu có (dotenv optional — không crash nếu thiếu thư viện)
# ---------------------------------------------------------------------------
def _load_dotenv() -> None:
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path=env_file, override=False)
    except ImportError:
        # python-dotenv chưa cài — parse thủ công (chỉ key=value đơn giản)
        with env_file.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value


_load_dotenv()


# ---------------------------------------------------------------------------
# 3. Path resolvers — ưu tiên env var, fallback về default trong repo
# ---------------------------------------------------------------------------
def get_config_path() -> Path:
    """Path tới homecredit_config.yaml."""
    raw = os.getenv("CONFIG_PATH")
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else PROJECT_ROOT / p
    return PROJECT_ROOT / "cli" / "flows" / "config" / "homecredit_config.yaml"


def get_stack_config_path() -> Path:
    """Path tới stack.yml (MinIO / Spark)."""
    raw = os.getenv("STACK_CONFIG_PATH")
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else PROJECT_ROOT / p
    return PROJECT_ROOT / "platforms" / "config" / "stack.yml"


def get_logger_config_path() -> Path:
    """Path tới logger_config.yaml."""
    raw = os.getenv("LOGGER_CONFIG_PATH")
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else PROJECT_ROOT / p
    return PROJECT_ROOT / "log" / "config" / "logger_config.yaml"


# ---------------------------------------------------------------------------
# 4. YAML loader với ${VAR:-default} expansion
# ---------------------------------------------------------------------------
def _expand(value: Any) -> Any:
    """Expand ${VAR:-default} placeholders trong YAML values."""
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, str) and value.startswith("${"):
        expression = value[2:-1]
        name, _, default = expression.partition(":-")
        return os.environ.get(name.strip(), default)
    return value


def load_yaml(path: Path) -> dict:
    """Load và expand một YAML file. Raise FileNotFoundError nếu không tồn tại."""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open(encoding="utf-8") as f:
        return _expand(yaml.safe_load(f) or {})


def get_source_dir() -> Path:
    """Thư mục chứa source CSV (Bronze ingestion input)."""
    raw = os.getenv("SOURCE_DIR")
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else PROJECT_ROOT / p
    return PROJECT_ROOT / "cli" / "ingestion" / "source_data"


def get_design_metadata_path() -> Path:
    """Path tới Design modeling_doc.csv."""
    raw = os.getenv("DESIGN_METADATA_PATH")
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else PROJECT_ROOT / p
    return (
        PROJECT_ROOT
        / "cli"
        / "ingestion"
        / "design_modelling"
        / "Design modeling_doc.csv"
    )


# ---------------------------------------------------------------------------
# 5. Convenience loaders — dùng trực tiếp trong các flow/platform module
# ---------------------------------------------------------------------------
def load_pipeline_config() -> dict:
    """Load homecredit_config.yaml, trả về toàn bộ dict (có project_params key)."""
    return load_yaml(get_config_path())


def load_stack_config() -> dict:
    """Load stack.yaml (MinIO, Spark config)."""
    return load_yaml(get_stack_config_path())
