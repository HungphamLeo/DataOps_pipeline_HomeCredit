"""Metadata and configuration helpers shared by configurable Prefect flows."""

from __future__ import annotations

import csv

from config import (
    get_design_metadata_path,
    get_source_dir,
    load_pipeline_config,
    load_stack_config,  # noqa: F401 — re-exported for existing callers
)
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

__all__ = [
    "load_config",
    "load_stack_config",
    "load_design_metadata",
    "load_source_columns",
    "validate_recipe",
]


def load_config() -> dict:
    """Load homecredit_config.yaml via the central config bootstrap."""
    try:
        cfg = load_pipeline_config()
        logger.info("pipeline_config_loaded")
        return cfg
    except Exception as e:
        logger.exception("pipeline_config_load_failed error=%s", e)
        raise


def load_design_metadata() -> list[dict[str, str]]:
    """Read source-to-target rows from the design contract CSV."""
    design_path = get_design_metadata_path()
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
        source_columns: dict[str, set[str]] = {}
        source_dir = get_source_dir()
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
            source: str | None = mapping.get("source")
            if not source:
                continue
            source_name, _, column_name = source.partition(".")
            sc = source_columns or {}
            exists_in_source = (
                source_name in sc and column_name in sc[source_name]
            )
            if source not in sources and not exists_in_source:
                missing.append(source)
        if missing:
            raise ValueError(
                f"Configured mappings are missing from the design contract: {sorted(set(missing))}"
            )
    except Exception as e:
        logger.exception("recipe_validation_failed error=%s", e)
        raise
