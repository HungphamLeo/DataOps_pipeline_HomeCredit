"""Config-driven raw CSV ingestion into the Bronze layer."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.tasks import task_input_hash

from cli.flows.metadata import PROJECT_ROOT, load_config
from cli.flows.serving.ge_validator import run_ge_checkpoint
from log.config.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)


def _source_path(config: dict, source: dict) -> Path:
    return PROJECT_ROOT / config["paths"]["source_dir"] / source["file"]


@task(
    log_prints=False,
)
def ingest_source_to_bronze(source: dict, config: dict) -> str:
    source_path = _source_path(config, source)
    bronze = config["bronze"]
    load_date = datetime.utcnow().strftime(config["runtime"]["load_date_format"])
    table_name = source["name"]

    logger.info("bronze_ingest_started table=%s source=%s", table_name, source_path)
    if not source_path.exists():
        logger.error("bronze_source_missing table=%s source=%s", table_name, source_path)
        raise FileNotFoundError(f"Source file does not exist: {source_path}")

    frame = pd.read_csv(
        source_path,
        low_memory=config["runtime"]["csv_low_memory"],
        encoding=config["runtime"].get("csv_encoding", "utf-8"),
    )
    metadata = bronze["metadata_columns"]
    frame[metadata["load_timestamp"]] = datetime.utcnow().isoformat()
    frame[metadata["source_file"]] = source["file"]
    frame[metadata["load_date"]] = load_date

    output_dir = (
        PROJECT_ROOT / config["paths"]["bronze_dir"]
        / f"{bronze['output_prefix']}{table_name}"
        / f"{bronze['partition_column']}={load_date}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "data.parquet"
    frame.to_parquet(
        output_path,
        engine=config["runtime"]["parquet_engine"],
        index=False,
    )
    logger.info(
        "bronze_ingest_completed table=%s rows=%d path=%s",
        table_name, len(frame), output_path,
    )
    return str(output_dir)


@flow(
    name="Bronze Ingest Flow",
    log_prints=False,
    task_runner=ConcurrentTaskRunner(),
    description="Ingest configured source files into partitioned Bronze Parquet.",
)
def bronze_ingest_flow() -> list[str]:
    config = load_config()["project_params"]
    sources = config["sources"]
    if not sources:
        raise ValueError("No Bronze sources configured")

    logger.info("bronze_flow_started source_count=%d", len(sources))
    task_options = ingest_source_to_bronze.with_options(
        retries=config["runtime"]["retries"],
        retry_delay_seconds=config["runtime"]["retry_delay_seconds"],
        cache_key_fn=task_input_hash,
        cache_expiration=timedelta(days=config["runtime"]["cache_days"]),
    )
    futures = [task_options.submit(source, config) for source in sources]
    outputs = [future.result() for future in futures]

    ge = config["great_expectations"]
    if ge["enabled"]:
        run_ge_checkpoint.submit(
            checkpoint_name=ge["bronze_checkpoint"],
            ge_root_dir=str(PROJECT_ROOT / config["paths"]["ge_root_dir"]),
            enabled=True,
        )
    logger.info("bronze_flow_completed table_count=%d", len(outputs))
    return outputs


if __name__ == "__main__":
    bronze_ingest_flow()
