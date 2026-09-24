# Session Handoff - Home Credit DataOps Pipeline

Date: 2026-09-21
Repository: `/home/hungpham/DataOps_pipeline_HomeCredit`

## 1. User direction and target architecture

The desired architecture was simplified to one clear pipeline:

```text
Source CSV
  -> MinIO Bronze
  -> Spark reads Bronze
  -> Design modeling_doc.csv source-to-target mapping
  -> Spark transformations
  -> PostgreSQL schema stg
```

The intended responsibilities are:

- `platforms`: technology configuration and adapters only.
- `cli/flows`: orchestration and data processing.
- `Design modeling_doc.csv`: business source-to-target mapping and transformation logic.
- `homecredit_config.yaml`: runtime, paths, connection, retry, and infrastructure settings; it should not duplicate business recipes.
- Bronze: preserve source data as closely as possible and add only `_load_ts`, `_source_file`, and `_load_date`.
- Staging: read Bronze, transform according to the design CSV, and write target tables to PostgreSQL.

## 2. Source files and target tables

Source files under `cli/ingestion/source_data/`:

```text
application.csv
bureau.csv
bureau_balance.csv
previous_application.csv
installments_payments.csv
POS_CASH_balance.csv
credit_card_balance.csv
```

The following files are explicitly excluded:

```text
sample_submission.csv
HomeCredit_columns_description.csv
```

The design CSV is:

```text
cli/ingestion/design_modelling/Design modeling_doc.csv
```

It currently defines these target tables:

```text
Dim_Date
Dim_Application_Status
Dim_Contract_Type
Dim_Customer
Fact_Loan_Repayment
Fact_Loan_Application
Fact_Bureau_Credit
Fact_Bureau_Monthly_Snapshot
Fact_Credit_Balance
Fact_POS_CASH_balance
```

The target PostgreSQL schema is `stg`.

## 3. Work completed during this session

### 3.1 Metadata and flow simplification

`cli/flows/metadata.py` was extended with:

- `load_stack_config()`
- configurable design CSV path loading
- design CSV title-row handling
- source metadata loading
- source-to-design validation helpers

The former pandas recipe-oriented implementation was replaced by simpler Spark-oriented flows:

- `cli/flows/serving/bronze/bronze_flow.py`
- `cli/flows/serving/staging/staging_flow.py`

The old files at:

- `cli/flows/serving/bronze_flow.py`
- `cli/flows/serving/staging_flow.py`

were removed from the active path.

### 3.2 Bronze flow

The active Bronze flow:

- discovers all CSV files in the configured source directory;
- excludes the two configured files;
- derives the table name from the filename;
- reads CSV using Spark;
- adds:
  - `_load_ts`
  - `_source_file`
  - `_load_date`
- writes Parquet to MinIO:

```text
s3a://<bucket>/bronze/<source_table>/
```

partitioned by `_load_date`.

Bronze does not apply business transformations.

### 3.3 Staging flow

The active Staging flow:

- groups rows from `Design modeling_doc.csv` by `Target_Table_Name`;
- collects source references from `Source_Table.Column`;
- reads the required Bronze tables from MinIO;
- attempts to join multiple source tables using shared keys;
- applies Spark SQL expressions from `Transformation_Logic`;
- applies target casts from `Column_Type`;
- writes each target through JDBC to PostgreSQL schema `stg`.

`Dim_Date` has no source mapping, so it is generated from:

```yaml
project_params:
  staging:
    date_dimension:
      start_date: "2020-01-01"
      end_date: "2030-12-31"
```

The source mapping validation found two naming differences and the flow now normalizes:

```text
SK_BUREAU_ID -> SK_ID_BUREAU
```

and accepts mappings with multiple references such as:

```text
application.SK_ID_CURR / previous_application.SK_ID_PREV
```

The final validation reported:

```text
missing source columns: 0
```

### 3.4 Legacy pipeline wrapper

`cli/flows/bronze_silver_pipeline.py` was reduced to a compatibility wrapper. It no longer contains the previous hard-coded `SOURCE_TABLES` and table-building logic. Its function delegates to the active Bronze and Staging flows.

This preserves old deployment references while keeping a single implementation.

### 3.5 CLI and daily flow

`cli/main.py` runs:

```python
bronze_ingest_flow()
staging_transform_flow()
```

The old direct Spark pipeline is no longer the active CLI path.

`cli/flows/serving/daily_pipeline.py` no longer imports the unavailable mart flow. It coordinates Bronze and Staging only.

## 4. Configuration state

`cli/config/homecredit_config.yaml` was simplified to contain:

- environment name;
- logger config path;
- source and design paths;
- runtime retry settings;
- PostgreSQL connection settings;
- PostgreSQL schema `stg`;
- excluded Bronze files;
- Spark Bronze schema inference;
- date dimension range.

`platforms/config/stack.yaml` contains technology configuration:

- MinIO endpoint, credentials, bucket, and Bronze prefix;
- Spark app name, master, memory;
- Spark Maven packages:
  - Delta Spark
  - Hadoop AWS
  - PostgreSQL JDBC driver

The old PostgreSQL block was removed from `stack.yaml` to avoid duplicating PostgreSQL settings. PostgreSQL runtime settings now live in `homecredit_config.yaml`.

## 5. Docker work

### 5.1 Dockerfile

`infra/Dockerfile` was corrected to:

- use:

```dockerfile
FROM python:3.11-slim-bookworm
```

- install Java 17 for PySpark;
- install build dependencies and PostgreSQL client libraries;
- copy only existing directories:
  - `cli`
  - `platforms`
  - `log`
- avoid copying `.env`;
- run:

```dockerfile
CMD ["python", "-m", "cli.main"]
```

The base image was changed from unpinned `python:3.11-slim` because that tag resolved to Debian Trixie, where `openjdk-17-jre-headless` was unavailable. Bookworm provides the expected Java 17 package.

### 5.2 Docker Compose

`infra/docker-compose.yml` currently defines:

```text
postgres
minio
minio-init
prefect-server
prefect-worker
pipeline
```

The `pipeline` service:

- builds from repository root using `infra/Dockerfile`;
- connects to PostgreSQL via hostname `postgres`;
- connects to MinIO via `minio:9000`;
- waits for PostgreSQL health and successful MinIO initialization;
- runs `python -m cli.main`.

The nonexistent SQL init mount was removed because the repository has no `sql/` directory.

The Compose configuration was validated successfully with:

```bash
docker compose -f infra/docker-compose.yml config --quiet
```

### 5.3 Docker ignore

`.dockerignore` was added to exclude:

- `.git`
- `.env`
- virtual environments
- Python cache
- runtime logs
- local data
- Excel files

## 6. Errors encountered and fixes

### 6.1 Java package error

Initial Docker build failed:

```text
Package 'openjdk-17-jre-headless' is not available
```

Cause:

```text
python:3.11-slim
```

resolved to Debian Trixie, whose default repository did not provide Java 17.

Fix:

```dockerfile
FROM python:3.11-slim-bookworm
```

Keep:

```dockerfile
openjdk-17-jre-headless
```

### 6.2 Prefect/Pydantic import error

The pipeline container then failed while importing the flow decorator:

```text
TypeError: 'type' object is not iterable
```

The failure occurred in Prefect parameter schema creation at `@flow`, before pipeline execution.

Dependency state in the repository:

```text
prefect[spark]==2.20.0
pydantic==2.11.7  # before the latest patch
pydantic-settings==2.12.0  # before the latest patch
```

The attempted fix was:

```text
pydantic==2.9.2
pydantic-settings==2.5.2
```

The Prefect server and worker images were also pinned from `prefect:2-latest` to:

```text
prefecthq/prefect:2.20.0
```

Important: this dependency fix was not fully rebuilt and verified before the session ended. The next session must rebuild the pipeline image and verify the import.

The local virtual environment was not representative of the Docker image. It reported:

```text
prefect 2.20.25
pydantic 2.13.4
pydantic-settings 2.14.2
```

Do not use that local result as proof that the image is fixed.

## 7. Validation completed

Successful checks:

```bash
python -m py_compile \
  cli/main.py \
  cli/flows/metadata.py \
  cli/flows/serving/bronze/bronze_flow.py \
  cli/flows/serving/staging/staging_flow.py \
  cli/flows/bronze_silver_pipeline.py
```

```bash
git diff --check
```

```bash
docker compose -f infra/docker-compose.yml config --quiet
```

YAML parsing passed for:

```text
cli/config/homecredit_config.yaml
platforms/config/stack.yaml
infra/docker-compose.yml
```

Source-to-design coverage passed:

```text
missing source columns: 0
```

PySpark was unavailable in the host virtual environment:

```text
ModuleNotFoundError: No module named 'pyspark'
```

Therefore, Spark/MinIO/PostgreSQL end-to-end execution has not been verified on the host.

The Docker build with Bookworm got past base image resolution and started installing system packages. The full build result was not collected to completion because the build was still running/interrupted during the session.

## 8. Important remaining technical risks

### 8.1 Design transformation execution is still incomplete

The design CSV contains business logic such as:

- `Lookup Dim_Customer`
- `Lookup Dim_Contract_Type`
- `Lookup Dim_Application_Status`
- joins between previous applications and applications;
- date conversion expressions;
- SCD behavior;
- fact foreign key lookups.

The current simplified staging flow can execute basic Spark SQL expressions, but it does not yet fully implement all semantic lookup/SCD behavior. It may fall back to a direct source expression for complex natural-language transformation logic. This must be addressed before treating target facts as production-correct.

### 8.2 Join inference is simplistic

For multi-source targets, the current flow chooses:

```text
SK_ID_CURR if available, otherwise the first common column
```

This is not sufficient for all design relationships, especially:

- `installments_payments.SK_ID_PREV` -> `previous_application.SK_ID_PREV` -> customer;
- `bureau_balance.SK_ID_BUREAU` -> `bureau.SK_ID_BUREAU`;
- foreign keys to dimensions.

Explicit join metadata should be added to the design contract or a small separate technical mapping file.

### 8.3 Bronze overwrite behavior

The current Bronze write uses `mode("overwrite")` at the source table path and partitions by `_load_date`. Confirm whether the desired behavior is:

- overwrite the current load partition only; or
- append historical load partitions.

For production reprocessing, dynamic partition overwrite or a load-run identifier may be needed.

### 8.4 Prefect server and pipeline runtime

The current `pipeline` service runs the flow directly as a one-shot container. The Prefect worker is present but no deployment is currently registered for the active flow. Decide whether to:

- keep one-shot Compose execution for local runs; or
- create a Prefect deployment and have the worker execute it.

## 9. Recommended next-session commands

From repository root:

```bash
docker compose -f infra/docker-compose.yml build \
  --no-cache \
  --progress=plain \
  pipeline
```

After a successful build:

```bash
docker run --rm infra-pipeline:latest java -version
docker run --rm infra-pipeline:latest \
  python -c "import prefect, pydantic, pyspark; print(prefect.__version__, pydantic.__version__, pyspark.__version__)"
```

Then start infrastructure:

```bash
docker compose -f infra/docker-compose.yml up -d \
  postgres minio minio-init prefect-server prefect-worker
```

Check service state:

```bash
docker compose -f infra/docker-compose.yml ps
```

Run the pipeline:

```bash
docker compose -f infra/docker-compose.yml up pipeline
```

Inspect logs:

```bash
docker compose -f infra/docker-compose.yml logs -f pipeline
```

Check PostgreSQL target tables:

```bash
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U admin_homecredit -d homecredit_mart \
  -c "SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema = 'stg'
      ORDER BY table_name;"
```

## 10. Current changed-file summary

Relevant changed or newly added paths:

```text
.dockerignore
cli/flows/bronze_silver_pipeline.py
cli/flows/metadata.py
cli/flows/serving/bronze/bronze_flow.py
cli/flows/serving/staging/staging_flow.py
cli/flows/serving/daily_pipeline.py
cli/main.py
infra/Dockerfile
infra/docker-compose.yml
requirements.txt
platforms/config/stack.yaml
cli/config/homecredit_config.yaml
```

The worktree also contained deleted legacy files at:

```text
cli/flows/serving/bronze_flow.py
cli/flows/serving/staging_flow.py
```

Do not reset or discard unrelated worktree changes without reviewing them first.

