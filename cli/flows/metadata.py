"""Metadata and configuration helpers shared by configurable Prefect flows."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "cli" / "config" / "homecredit_config.yaml"
STACK_CONFIG_PATH = PROJECT_ROOT / "platforms" / "config" / "stack.yaml"
DEFAULT_DESIGN_PATH = (
    PROJECT_ROOT / "cli" / "ingestion" / "design_modelling" / "Design modeling_doc.csv"
)


def _expand(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, str) and value.startswith("${"):
        expression = value[2:-1]
        name, _, default = expression.partition(":-")
        return os.environ.get(name, default)
    return value


def load_config() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as file:
        return _expand(yaml.safe_load(file) or {})


def load_stack_config() -> dict:
    with STACK_CONFIG_PATH.open(encoding="utf-8") as file:
        return _expand(yaml.safe_load(file) or {})


def load_design_metadata() -> list[dict[str, str]]:
    """Read source-to-target rows from the design contract CSV."""
    config = load_config().get("project_params", {})
    configured_path = config.get("paths", {}).get("design_metadata")
    design_path = (
        PROJECT_ROOT / configured_path
        if configured_path
        else DEFAULT_DESIGN_PATH
    )
    with design_path.open(encoding="utf-8-sig", newline="") as file:
        # The workbook export contains a descriptive title row before the CSV header.
        next(file, None)
        return list(csv.DictReader(file))


def load_source_columns(config: dict) -> dict[str, set[str]]:
    """Read source headers so derived recipe inputs can be checked cheaply."""
    source_columns = {}
    source_dir = PROJECT_ROOT / config["paths"]["source_dir"]
    for source in config.get("sources", []):
        source_path = source_dir / source["file"]
        with source_path.open(encoding=config["runtime"].get("csv_encoding", "utf-8"), newline="") as file:
            source_columns[source["name"]] = set(next(csv.reader(file)))
    return source_columns


def validate_recipe(
    recipe: dict,
    metadata: list[dict[str, str]],
    source_columns: dict[str, set[str]] | None = None,
) -> None:
    """Validate configured mappings against the design contract and source headers."""
    sources = {
        source.strip()
        for row in metadata
        for source in row.get("Source_Table.Column", "").split(",")
        if source.strip() and source.strip().lower() not in {"derived", "system timestamp", "system flag"}
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
