"""
Module: platforms.prefect_orchestra.prefect_main
Layer: Platform Orchestration Subsystem - Prefect
Responsibility: Centralized configuration manager và logger factory cho các Prefect flow.
  - ConfigLoader / FileConfigLoader: load YAML qua config.py (không tự tính path)
  - LoggerFactory / DefaultLoggerFactory: thin wrapper quanh logger_manager
  - PrefectETLPipelineConfig: DI-friendly config accessor dùng trong @flow functions

Does NOT contain: hardcoded paths, direct print(), pipeline business logic.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from config import load_pipeline_config, load_stack_config  # noqa: F401
from log.config.logger_setup import logger_manager


# ---------------------------------------------------------------------------
# ConfigLoader — abstract + file-based implementation
# ---------------------------------------------------------------------------

class ConfigLoader(ABC):
    """Abstract Base Class for loading pipeline configuration dictionaries."""

    @abstractmethod
    def load(self) -> Dict[str, Any]:
        """Load and return configuration dict."""


class FileConfigLoader(ConfigLoader):
    """
    Load pipeline config through the central config bootstrap (config.py).
    Không tự mở YAML — delegate hoàn toàn cho load_pipeline_config().
    """

    def load(self) -> Dict[str, Any]:
        return load_pipeline_config()


class StackConfigLoader(ConfigLoader):
    """Load stack config (MinIO / Spark) through config.py."""

    def load(self) -> Dict[str, Any]:
        return load_stack_config()


# ---------------------------------------------------------------------------
# LoggerFactory
# ---------------------------------------------------------------------------

class LoggerFactory(ABC):
    """Abstract Base Class for logger creation in orchestration layer."""

    @abstractmethod
    def get_logger(self, name: str) -> logging.Logger:
        """Retrieve configured logger by name."""


class DefaultLoggerFactory(LoggerFactory):
    """Default logger factory using platform logger_manager singleton."""

    def get_logger(self, name: str) -> logging.Logger:
        return logger_manager.get_logger(name)


# ---------------------------------------------------------------------------
# PrefectETLPipelineConfig
# ---------------------------------------------------------------------------

class PrefectETLPipelineConfig:
    """
    Centralized configuration accessor for ETL pipelines running under Prefect.

    Usage (inside a @flow or @task):
        pipeline_cfg = PrefectETLPipelineConfig()
        pg = pipeline_cfg.get_postgres_config()

    Accepts optional DI overrides for testability:
        pipeline_cfg = PrefectETLPipelineConfig(
            config_loader=MockConfigLoader(),
            logger_factory=MockLoggerFactory(),
        )
    """

    def __init__(
        self,
        config_loader: Optional[ConfigLoader] = None,
        logger_factory: Optional[LoggerFactory] = None,
    ):
        self._loader = config_loader or FileConfigLoader()
        self._logger_factory = logger_factory or DefaultLoggerFactory()
        self._loggers: Dict[str, logging.Logger] = {}
        self._internal_logger = self._logger_factory.get_logger(__name__)
        self._config: Dict[str, Any] = {}
        self._load_config()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_config(self) -> None:
        try:
            self._internal_logger.debug("prefect_configuration_loading")
            raw = self._loader.load()
            # homecredit_config.yaml has a top-level `project_params` key.
            self._config = raw.get("project_params", raw)
            self._internal_logger.info("prefect_configuration_loaded")
        except Exception as exc:
            self._config = {}
            self._internal_logger.exception(
                "prefect_configuration_load_failed error=%s", exc
            )

    def _get_logger(self, key: str) -> logging.Logger:
        if key not in self._loggers:
            self._loggers[key] = self._logger_factory.get_logger(key)
        return self._loggers[key]

    # ------------------------------------------------------------------
    # Named loggers — lazy, cached
    # ------------------------------------------------------------------

    @property
    def ingestion_logger(self) -> logging.Logger:
        return self._get_logger(f"{__name__}.ingestion")

    @property
    def bronze_logger(self) -> logging.Logger:
        return self._get_logger(f"{__name__}.bronze")

    @property
    def silver_logger(self) -> logging.Logger:
        return self._get_logger(f"{__name__}.silver")

    @property
    def gold_logger(self) -> logging.Logger:
        return self._get_logger(f"{__name__}.gold")

    @property
    def serving_logger(self) -> logging.Logger:
        return self._get_logger(f"{__name__}.serving")

    @property
    def data_quality_logger(self) -> logging.Logger:
        return self._get_logger(f"{__name__}.data_quality")

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def config(self) -> Dict[str, Any]:
        return self._config or {}

    def reload(self) -> None:
        """Force re-load config from source (e.g. after env var change)."""
        self._load_config()
        self._loggers.clear()

    def get_environment(self) -> str:
        return self.config.get("environment", "development")

    def is_production(self) -> bool:
        return self.get_environment() == "production"

    def is_development(self) -> bool:
        return self.get_environment() == "development"

    def get_postgres_config(self) -> Dict[str, Any]:
        return self.config.get("postgres", {})

    def get_postgresql_schema(self) -> str:
        return self.get_postgres_config().get("schema", "stg")
