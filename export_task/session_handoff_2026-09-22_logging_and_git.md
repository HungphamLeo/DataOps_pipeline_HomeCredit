<<<<<<< HEAD

=======
>>>>>>> 09a18825ac998a988eb0b043cca184006e39b1d9
# Session Handoff: Production Logging, Pipeline Diagnostics, and Git Branches

**Date:** 2026-09-22  
**Repository:** `HungphamLeo/DataOps_pipeline_HomeCredit`  
**Working branch:** `21092026_refactor_staging_stage`  
**Upstream:** `origin/21092026_refactor_staging_stage`

## 1. Session scope

This session continued the Home Credit DataOps pipeline work:

- Refactor production logging across `platforms` and `cli/flows/serving`.
- Ensure every `except Exception` captures and logs the actual exception.
- Track a Docker pipeline failure from `pipeline-error.log`.
- Explain how to publish and checkout branches from terminal.

## 2. Logging refactor completed

The active platform and serving modules were updated to use structured error logging:

```python
except Exception as e:
    logger.exception("operation_failed error=%s", e)
    raise
```

Updated areas:

- `cli/flows/serving/bronze_flow.py`
- `cli/flows/serving/staging_flow.py`
- `cli/flows/serving/metadata.py`
- `cli/flows/serving/daily_pipeline.py`
- `cli/flows/serving/ge_validator.py`
- `cli/main.py`
- `platforms/processing/spark_stack/spark_session.py`
- `platforms/processing/config/delta_utils.py`
- `platforms/storage/minio/minio_storage.py`
- `platforms/storage/postgre/base_postgre.py`
- `log/config/logger_setup.py`

The logging changes include:

- Actual exception text in log messages through `error=%s`.
- Full traceback through `logger.exception(...)`.
- Re-raising errors at pipeline/platform boundaries.
- Cleanup of Spark sessions in `finally`.
- Explicit context such as source, target, table, schema, path, and operation.
- No credentials or passwords in log messages.

## 3. Logger routing

The logger configuration was expanded with technology-specific files:

- `log/log_storage/spark.log`
- `log/log_storage/minio.log`
- `log/log_storage/postgres.log`
- `log/log_storage/processing.log`
- `log/log_storage/platforms.log`
- `log/log_storage/orchestration.log`
- `log/log_storage/cli.log`
- `log/log_storage/pipeline.log`
- `log/log_storage/pipeline-error.log`

The JSON formatter now supports optional context fields:

- `run_id`
- `component`
- `operation`
- `table`
- `source`
- `target`

Note: `log/config/logger_config.yaml` is ignored by the repository rule `*.yaml`. Confirm whether this file should be force-added to Git if the configuration is intended to be versioned:

```bash
git add -f log/config/logger_config.yaml
```

## 4. Pipeline error tracking

The attached pipeline log showed this first failure:

```text
FileNotFoundError:
/app/cli/cli/config/homecredit_config.yaml
```

The source path calculation in `cli/flows/serving/metadata.py` was incorrect because:

```python
Path(__file__).resolve().parents[2]
```

resolved to `/app/cli` inside the container, then appended another `cli` directory.

It was corrected to:

```python
PROJECT_ROOT = Path(__file__).resolve().parents[3]
```

With the Docker layout:

```dockerfile
COPY cli ./cli
```

the expected path is now:

```text
/app/cli/config/homecredit_config.yaml
```

The same root is used for:

- `platforms/config/stack.yaml`
- `cli/ingestion/design_modelling/Design modeling_doc.csv`

## 5. Second pipeline error: Prefect and AnyIO compatibility

After the path error, Prefect also failed with:

```text
TypeError: Can't instantiate abstract class GatherTaskGroup
with abstract method create_task
```

The error occurred inside Prefect's async task-group handling and is separate from the missing config file.

Observed local versions:

```text
prefect 2.20.25
anyio 4.14.1
pydantic 2.13.4
```

The project requirements currently contain:

```text
prefect[spark]==2.20.0
```

Recommended next step is to pin AnyIO to the compatible 3.x line in `requirements.txt`:

```text
anyio>=3.7.1,<4
```

Then rebuild the pipeline image without cache and verify the versions inside Docker:

```bash
docker compose -f infra_homecredit/docker-compose.yml build --no-cache pipeline
docker compose -f infra_homecredit/docker-compose.yml run --rm pipeline \
  python -c "from importlib.metadata import version; import prefect; print(version('anyio')); print(prefect.__version__)"
```

Do not consider the Docker runtime fully verified until:

1. The config path error is gone.
2. The Prefect flow can start without `GatherTaskGroup` failure.
3. Spark can initialize.
4. Bronze can write to MinIO.
5. Staging can write at least one target table to PostgreSQL.

## 6. Exception logging verification

Repository-wide search confirmed there are no remaining bare blocks matching:

```python
except Exception:
```

The active code uses:

```python
except Exception as e:
```

or an equivalent named exception variable, and logs the error before re-raising or returning an explicit failure result.

Validation completed:

```text
Python compileall: passed
git diff --check: passed
No print(...) in active platforms/serving areas
Logger configuration smoke test: passed
```

The host environment does not have PySpark installed, so full Spark import and end-to-end execution still require Docker validation.

## 7. Git branch clarification

Ubuntu and Windows were checked against different repositories.

Ubuntu project:

```text
DataOps_pipeline_HomeCredit
```

It contains:

```text
21092026_refactor_staging_stage
```

Windows project:

```text
DataOps_pipeline_WebCraw
```

It does not contain the HomeCredit branch, so this command fails there:

```powershell
git checkout 21092026_refactor_staging_stage
```

To use the HomeCredit branch on Windows, clone the correct repository:

```powershell
cd C:\Users\Admin\Downloads\Project\Github
git clone https://github.com/HungphamLeo/DataOps_pipeline_HomeCredit.git
cd DataOps_pipeline_HomeCredit
git fetch origin
git switch --track origin/21092026_refactor_staging_stage
```

Only change the Windows repository remote if that directory actually contains HomeCredit source code. Do not mix WebCraw and HomeCredit histories.

## 8. Current Git state

At the time of export:

```text
Branch: 21092026_refactor_staging_stage
Upstream: origin/21092026_refactor_staging_stage
Ahead: 0
Behind: 0
Uncommitted changes: 0
```

## 9. Recommended continuation order

1. Add and verify the AnyIO compatibility pin.
2. Rebuild the Docker pipeline image from scratch.
3. Test `metadata.load_config()` inside the container.
4. Test Prefect flow import and a minimal flow invocation.
5. Start PostgreSQL and MinIO services.
6. Run Bronze ingestion and inspect `minio.log` and `pipeline-error.log`.
7. Run one Staging target and verify the `stg` PostgreSQL schema.
8. Review whether the logger YAML should be force-added despite the global YAML ignore rule.
9. Revisit the remaining production concerns:
   - explicit join metadata instead of join inference;
   - append/partition semantics for Bronze;
   - Prefect deployment registration versus direct one-shot Compose execution.

<<<<<<< HEAD

=======
>>>>>>> 09a18825ac998a988eb0b043cca184006e39b1d9
