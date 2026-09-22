"""Metadata and configuration helpers shared by configurable Prefect flows."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

import yaml
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "cli" / "config" / "homecredit_config.yaml"
STACK_CONFIG_PATH = PROJECT_ROOT / "platforms" / "config" / "stack.yaml"
DEFAULT_DESIGN_PATH = (
    PROJECT_ROOT / "cli" / "ingestion" / "design_modelling" / "Design modeling_doc.csv"
)


def _expand(value: Any) -> Any:
    try:
        if isinstance(value, dict):
            return {key: _expand(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_expand(item) for item in value]
        if isinstance(value, str) and value.startswith("${"):
            expression = value[2:-1]
            name, _, default = expression.partition(":-")
            return os.environ.get(name, default)
        return value
    except Exception as e:
        logger.exception("config_value_expansion_failed error=%s", e)
        raise


def load_config() -> dict:
    try:
        with CONFIG_PATH.open(encoding="utf-8") as file:
            config = _expand(yaml.safe_load(file) or {})
        logger.info("pipeline_config_loaded path=%s", CONFIG_PATH)
        return config
    except Exception as e:
        logger.exception("pipeline_config_load_failed path=%s error=%s", CONFIG_PATH, e)
        raise


def load_stack_config() -> dict:
    try:
        with STACK_CONFIG_PATH.open(encoding="utf-8") as file:
            config = _expand(yaml.safe_load(file) or {})
        logger.info("stack_config_loaded path=%s", STACK_CONFIG_PATH)
        return config
    except Exception as e:
        logger.exception("stack_config_load_failed path=%s error=%s", STACK_CONFIG_PATH, e)
        raise


def load_design_metadata() -> list[dict[str, str]]:
    """Read source-to-target rows from the design contract CSV."""
    config = load_config().get("project_params", {})
    configured_path = config.get("paths", {}).get("design_metadata")
    design_path = (
        PROJECT_ROOT / configured_path
        if configured_path
        else DEFAULT_DESIGN_PATH
    )
    try:
        with design_path.open(encoding="utf-8-sig", newline="") as file:
            next(file, None)
            rows = list(csv.DictReader(file))
        logger.info("design_metadata_loaded path=%s rows=%d", design_path, len(rows))
        return rows
    except Exception as e:
        logger.exception("design_metadata_load_failed path=%s error=%s", design_path, e)
        raise


def load_source_columns(config: dict) -> dict[str, set[str]]:
    """Read source headers so derived recipe inputs can be checked cheaply."""
    try:
        source_columns = {}
        source_dir = PROJECT_ROOT / config["paths"]["source_dir"]
        for source in config.get("sources", []):
            source_path = source_dir / source["file"]
            with source_path.open(
                encoding=config["runtime"].get("csv_encoding", "utf-8"),
                newline="",
            ) as file:
                source_columns[source["name"]] = set(next(csv.reader(file)))
        return source_columns
    except Exception as e:
        logger.exception("source_columns_load_failed error=%s", e)
        raise


def validate_recipe(
    recipe: dict,
    metadata: list[dict[str, str]],
    source_columns: dict[str, set[str]] | None = None,
) -> None:
    """Validate configured mappings against the design contract and source headers."""
    try:
        sources = {
            source.strip()
            for row in metadata
            for source in row.get("Source_Table.Column", "").split(",")
            if source.strip() and source.strip().lower() not in {
                "derived", "system timestamp", "system flag"
            }
        }
        missing = []
        for mapping in recipe.get("mappings", []):
            source = mapping.get("source")
            source_name, _, column_name = source.partition(".") if source else ("", "", "")
            exists_in_source = (
                source_name in (source_columns or {})
                and column_name in source_columns[source_name]
            )
            if source and source not in sources and not exists_in_source:
                missing.append(source)
        if missing:
            raise ValueError(
                f"Configured mappings are missing from the design contract: {sorted(set(missing))}"
            )
    except Exception as e:
        logger.exception("recipe_validation_failed error=%s", e)
        raise
