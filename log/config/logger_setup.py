import logging
import logging.config
from pathlib import Path
from typing import Dict, Any, List, Optional
import yaml


def _ensure_log_dir(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

class LoggerManager:
    _instance: Optional['LoggerManager'] = None
    _loggers: Dict[str, logging.Logger] = {}

    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else Path(__file__).parent / "config" / "logger_config.yaml"
        self.config: Dict[str, Any] = {}
        self._initialized = False
        if config_path is not None:
            self.configure(config_path)

    def configure(self, config_path: Optional[str] = None) -> None:
        """Configure or reconfigure logging from a YAML file path."""
        if config_path is not None:
            self.config_path = Path(config_path)
        self._loggers.clear()
        self.config = self._load_config()
        self._configure_logging()
        self._initialized = True

    def configure_from_project_config(self, project_config_path: str) -> None:
        """Configure logging from an ETL project YAML config file."""
        self._loggers.clear()
        project_config_path = Path(project_config_path)
        try:
            with open(project_config_path, 'r', encoding='utf-8') as f: # pragma: no cover
                project_config = yaml.safe_load(f) or {}
        except Exception as exc: # pragma: no cover
            raise RuntimeError(f"Failed to load project config for logger: {exc}")

        # Lấy cấu hình logger từ project_params hoặc gốc
        logger_config_section = project_config.get("project_params", {}).get("logger")
        if not logger_config_section:
            logger_config_section = project_config.get("logger", {})

        self.config = self._build_dict_config_from_project_logger(logger_config_section)
        self._configure_logging()
        self._initialized = True

    def _build_dict_config_from_project_logger(self, project_logger: Dict[str, Any]) -> Dict[str, Any]:
        formatter_config = {
            "format": '{"time":"%(asctime)s", "level":"%(levelname)s", "message":"%(message)s", "caller":"%(pathname)s:%(lineno)d"}',
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        }
        
        # Sử dụng formatters được định nghĩa trong config nếu có
        formatters = project_logger.get("formatters", {"default": formatter_config})
        
        # Chọn formatter dựa trên môi trường
        env = project_logger.get("environment", "local")
        console_formatter = "json" if env in ["staging", "production"] else "console"

        handlers: Dict[str, Any] = {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": console_formatter,
                "level": project_logger.get("level", "INFO"),
            }
        }
        loggers: Dict[str, Any] = {}
        category_handlers: Dict[str, List[str]] = {
            "ingestion_log": [],
            "storage_log": [],
            "processing_log": [],
            "governance_log": [],
        }

        def build_handlers(tree: Dict[str, Any], prefix: str) -> None:
            """Hàm đệ quy để xây dựng cấu hình handler từ cây config."""
            for key, value in tree.items():
                if not isinstance(value, dict):
                    continue
                nested_prefix = f"{prefix}.{key}" if prefix else key
                if "files" in value and "storage_path" in value:
                    storage_path = Path(value["storage_path"])
                    category = prefix.split(".")[0] if prefix else nested_prefix.split(".")[0]
                    for level_name, filename in value.get("files", {}).items():
                        handler_name = f"{nested_prefix}.{level_name}"
                        handler_path = storage_path / filename
                        handlers[handler_name] = {
                            "class": "logging.handlers.RotatingFileHandler",
                            "formatter": "json", # File log luôn là JSON
                            "level": level_name.upper(),
                            "filename": str(handler_path),
                            "maxBytes": int(value.get("max_size_mb", 10)) * 1024 * 1024,
                            "backupCount": value.get("backup_count", 3),
                        }
                        _ensure_log_dir(handler_path)
                        if category in category_handlers:
                            category_handlers[category].append(handler_name)
                    logger_name = f"logger.{nested_prefix}"
                    loggers[logger_name] = {
                        "level": value.get("level", project_logger.get("level", "DEBUG")),
                        "handlers": [f"{nested_prefix}.{level_name}" for level_name in value.get("files", {})],
                        "propagate": False,
                    }
                else:
                    build_handlers(value, nested_prefix)

        build_handlers(project_logger.get("ingestion_log", {}), "ingestion_log")
        build_handlers(project_logger.get("storage_log", {}), "storage_log")
        build_handlers(project_logger.get("processing_log", {}), "processing_log")
        build_handlers(project_logger.get("governance_log", {}), "governance_log")

        for category, handler_names in category_handlers.items():
            if handler_names:
                loggers[f"logger.{category}"] = {
                    "level": project_logger.get("level", "INFO"),
                    "handlers": handler_names,
                    "propagate": False,
                }

        # Root logger sẽ ghi ra console và file debug chung nếu có
        root_handlers = ["console"]
        # Tìm một handler file debug bất kỳ để thêm vào root
        debug_file_handler = next((name for name in handlers if name.endswith(".debug")), None)
        if debug_file_handler:
            root_handlers.append(debug_file_handler)

        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": formatters,
            "handlers": handlers,
            "loggers": loggers,
            "root": {
                "level": project_logger.get("level", "INFO"),
                "handlers": root_handlers,
            },
        }

    def _load_config(self) -> Dict[str, Any]:
        """Load logger config từ YAML, với fallback mặc định."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
            if isinstance(config, dict):
                for handler in config.get("handlers", {}).values():
                    if isinstance(handler, dict) and "filename" in handler:
                        _ensure_log_dir(handler["filename"])
            return config # pragma: no cover
        except FileNotFoundError:
            # Fallback config mặc định
            return {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "format": '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        "datefmt": "%Y-%m-%dT%H:%M:%S"
                    }
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "formatter": "default",
                        "level": "INFO"
                    },
                    "file_debug": {
                        "class": "logging.handlers.RotatingFileHandler",
                        "filename": Path(__file__).parent / "logs" / "debug.log",
                        "maxBytes": 10 * 1024 * 1024,
                        "backupCount": 3,
                        "formatter": "default",
                        "level": "DEBUG"
                    },
                    # Thêm handlers cho info, warning, error tương tự
                },
                "root": {
                    "level": "INFO",
                    "handlers": ["console", "file_debug"]
                }
            }

    def _configure_logging(self):
        """Cấu hình logging từ dict."""
        if not isinstance(self.config, dict):
            self.config = {}
        logging.config.dictConfig(self.config)

    def get_logger(self, module_name: str, logger_name: Optional[str] = None) -> logging.Logger:
        """
        Lấy logger cho module cụ thể.
        :param module_name: Tên module (sử dụng __name__), ví dụ: 'platforms.ingestion.cophieu68.extract'
        :param logger_name: Tên logger cụ thể nếu muốn ghi đè mặc định.
        :return: Logger instance
        """
        cache_key = logger_name or module_name
        if cache_key in self._loggers:
            return self._loggers[cache_key]

        if logger_name:
            logger = logging.getLogger(logger_name)
        else:
            # Nếu module nằm trong ingestion/processing/storage/governance, cố gắng dùng logger chuyên biệt.
            normalized = module_name.replace('platforms.', '')
            if 'ingestion' in normalized:
                logger = logging.getLogger('logger.ingestion_log')
            elif 'processing' in normalized:
                logger = logging.getLogger('logger.processing_log')
            elif 'storage' in normalized:
                logger = logging.getLogger('logger.storage_log')
            elif 'orchestration' in normalized or 'metadata' in normalized or 'governance' in normalized:
                logger = logging.getLogger('logger.governance_log')
            else:
                logger = logging.getLogger(module_name)

        self._loggers[cache_key] = logger
        return logger

# Singleton instance
logger_manager = LoggerManager()