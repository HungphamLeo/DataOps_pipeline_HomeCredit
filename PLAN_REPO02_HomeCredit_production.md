# PLAN: DataOps_pipeline_HomeCredit → Production-Grade (homecredit-lending-platform)

**Goal:** Fix các tồn đọng hiện tại (uncommitted changes), sau đó nâng chuẩn production cho lending data pipeline — Kimball Gold layer, Data Contracts, PII protection, và full observability.

**Scope:** Ưu tiên sửa tồn đọng ngay (Priority 0), rồi áp dụng production standards theo thứ tự. Không thay đổi Bronze ingestion architecture hiện tại.

**Non-Goals:** Không migrate sang real-time streaming, không thêm ML scoring trong plan này.

---

## Sub-Task 00 — Fix Tồn Đọng Hiện Tại (PRIORITY NOW)

**Intent:**
3 files có uncommitted changes từ branch `23092026_hung_debug_prefect_and_config`. Cần commit hoặc discard trước khi làm bất cứ sub-task nào khác để tránh conflict. Đây là P0 — không làm ST tiếp theo khi chưa xong.

**Expected Outcomes:**
- `fact_bureau_monthly_snapshot.sql`: logic month-based SK đúng, không tạo NULL FK
- `platforms/config/stack.yaml`: config values production-safe (không hardcode passwords)
- `platforms/processing/spark_stack/spark_session.py`: SparkSession config đúng (Delta + S3A + memory settings)
- Branch clean, commit message theo Conventional Commits: `fix(sql): correct bureau monthly snapshot SK derivation`

**Todo List:**
1. Đọc `cli/flows/sql/staging/fact_bureau_monthly_snapshot.sql` — review logic SK derivation
2. Fix SK calculation: `TO_NUMBER(TO_CHAR(month_date, 'YYYYMM'))` hoặc surrogate key pattern
3. Đọc `platforms/config/stack.yaml` — kiểm tra có hardcoded credentials không
4. Nếu có credentials: replace với env var placeholders `${DB_PASSWORD}`, thêm `.env.example`
5. Đọc `platforms/processing/spark_stack/spark_session.py` — kiểm tra memory config, Delta config
6. Fix SparkSession: `spark.executor.memory`, `spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension`
7. `git add` + `git commit -m "fix(sql,config,spark): resolve uncommitted changes from debug session"`

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/cli/flows/sql/staging/fact_bureau_monthly_snapshot.sql` ← MODIFIED
- File: `DataOps_pipeline_HomeCredit/platforms/config/stack.yaml` ← MODIFIED
- File: `DataOps_pipeline_HomeCredit/platforms/processing/spark_stack/spark_session.py` ← MODIFIED
- Constraint: commit trước mọi changes khác

**Status:** `[ ] pending`

---

## Sub-Task 01 — Gold Layer với dbt Core (Kimball Dimensional Model)

**Intent:**
Pipeline hiện tại dừng ở Silver (PostgreSQL `stg` schema). Không có Gold aggregation layer — không thể serve BI dashboards trực tiếp. Production: dbt Core tạo Gold layer implements Kimball star schema, với tests và documentation.

**Expected Outcomes:**
- `dbt/` folder tạo trong repo với `dbt_project.yml`, profiles
- `models/staging/`: stg_loan_application, stg_bureau_credit, stg_pos_cash (1:1 map từ PostgreSQL stg tables)
- `models/marts/lending/`: fact_loan_performance, dim_customer, dim_date, dim_product
- `models/marts/risk/`: mart_credit_risk_summary (aggregated per customer per month)
- dbt tests: `not_null`, `unique`, `relationships` FK checks, `accepted_values`
- `dbt docs generate` → docs site browsable locally
- `dbt test` pass 100% trên sample data

**Todo List:**
1. Cài `dbt-postgres>=1.8` (thêm `requirements-dbt.txt`)
2. Tạo `dbt/dbt_project.yml` — project name `homecredit_lending`, profile `homecredit`
3. Tạo `dbt/profiles.yml` (gitignored) + `dbt/profiles.yml.example`
4. Tạo staging models: `stg_loan_application.sql`, `stg_bureau_credit.sql` (select + rename + cast từ stg schema)
5. Tạo dimension models: `dim_customer.sql` (SCD2 via dbt snapshot), `dim_date.sql` (date spine)
6. Tạo fact models: `fact_loan_performance.sql` — join staging tables, compute repayment metrics
7. Tạo risk mart: `mart_credit_risk_summary.sql` — PDO (Points Doubling Odds) prep aggregation
8. Thêm `schema.yml` tests: unique/not_null cho surrogate keys, FK relationships
9. `dbt run && dbt test` — verify pass
10. Tạo `docs/adr/ADR-001-gold-layer-dbt.md` — ghi lý do dùng dbt vs pure SQL

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/cli/flows/sql/staging/schema.sql` — source schema (10 tables)
- File: `DataOps_pipeline_HomeCredit/governance/analyze_business_context.md` — business context
- Theory: Kimball star schema → Fact (additive measures) + Dim (descriptive attributes) + Conformed Dims
- Skill: `dbt-core-deployment`, `kimball-data-warehouse-toolkit`

**Status:** `[ ] pending`

---

## Sub-Task 02 — dbt Snapshot (SCD Type 2) cho dim_customer

**Intent:**
`dim_customer` trong `schema.sql` hiện tại là type 1 (overwrite). Lending business cần lịch sử thay đổi customer attributes (địa chỉ, nghề nghiệp, income bracket) cho risk modeling. dbt Snapshots implement SCD2 tự động bằng cách so sánh hash của tracked columns.

**Expected Outcomes:**
- `dbt/snapshots/snap_customer.sql` với `strategy=check`, `unique_key=customer_id`, `check_cols=[...]`
- Columns `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id` tự động populated
- Query lịch sử: `SELECT * FROM snap_customer WHERE dbt_valid_to IS NULL` = current records
- `dbt snapshot` chạy daily (integrate vào Prefect daily_pipeline.py)
- Test: insert updated customer → verify old row `dbt_valid_to` không NULL

**Todo List:**
1. Đọc `cli/flows/sql/staging/schema.sql` — xác định columns của Dim_Customer cần track
2. Tạo `dbt/snapshots/snap_customer.sql` với `check` strategy, tracked cols: address, occupation, income_bracket
3. Config `dbt_project.yml`: `snapshots` materialization = `snapshot`
4. Tích hợp `dbt snapshot` vào Prefect flow: thêm `dbt_snapshot_task` sau Silver write
5. Viết `dbt/tests/snap_customer_no_duplicates.sql` — custom test check uniqueness per snapshot date
6. Tạo `dbt/macros/generate_surrogate_key.sql` (dbt_utils macro hoặc custom SHA256)

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/cli/flows/sql/staging/schema.sql` (Dim_Customer columns)
- File: `DataOps_pipeline_HomeCredit/cli/flows/serving/daily_pipeline.py` (thêm snapshot task vào đây)
- Theory: SCD2 → full audit trail → required for credit risk model reproducibility

**Status:** `[ ] pending`

---

## Sub-Task 03 — PII Protection + GDPR Crypto-Shredding

**Intent:**
Loan data chứa PII cực kỳ nhạy cảm: CMND, số điện thoại, địa chỉ, income, loan amount. Pipeline hiện tại ghi plain-text vào Bronze Delta files. Production: encrypt per-customer, khi xóa user → xóa encryption key (Crypto-Shredding) → ciphertext vô nghĩa mà không cần `DELETE` trên toàn bộ dataset.

**Expected Outcomes:**
- PII columns trong Bronze: `national_id`, `phone`, `address` → `AES-256-GCM` encrypted
- Mỗi customer có `encryption_key_id` → key lưu trong Vault (local: HashiCorp Vault, prod: Azure Key Vault)
- Khi delete request: xóa key khỏi Vault → ciphertext không decrypt được (Crypto-Shredding)
- Không có PII plain-text trong logs (`detect-secrets` pre-commit hook)
- `governance/pii_catalog.yaml` liệt kê tất cả PII columns + classification (C1/C2/C3)

**Todo List:**
1. Tạo `governance/pii_catalog.yaml` — liệt kê PII fields: column_name, table, classification, encryption_required
2. Implement `PIIEncryptor` class trong `platforms/security/pii_encryptor.py` — AES-256-GCM, key từ Vault
3. `docker-compose.yml`: thêm HashiCorp Vault service (dev mode) cho local development
4. Inject `PIIEncryptor` vào `bronze_flow.py` — encrypt PII trước write Delta
5. Implement `delete_user_data()` method: Vault key delete → verify ciphertext unreadable
6. Thêm `detect-secrets` vào `.pre-commit-config.yaml` — block commit nếu có PII pattern
7. Tạo `docs/adr/ADR-002-gdpr-crypto-shredding.md`

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/cli/flows/serving/bronze_flow.py`
- File: `DataOps_pipeline_HomeCredit/cli/flows/sql/staging/schema.sql` (columns cần encrypt)
- Theory: GDPR Crypto-Shredding → delete encryption key ≡ delete all data cho key đó → O(1) delete thay vì O(N) scan

**Status:** `[ ] pending`

---

## Sub-Task 04 — Data Contracts + Great Expectations Production Config

**Intent:**
GE checkpoints (`platforms/great_expectations/`) đang run nhưng không rõ threshold và không có alerting. Production: Data Contract định nghĩa explicit SLA (completeness ≥ 99%, no nulls trên PK, referential integrity), failure → pipeline halt + Prefect alert notification.

**Expected Outcomes:**
- `governance/contracts/loan_application_contract.yaml` — schema + quality SLA per column
- GE Expectation Suite updated với explicit thresholds từ contract
- Khi GE validation fail: Prefect task fail với structured error `{rule_violated, column, severity}`
- Validation results lưu vào PostgreSQL `audit.dq_results` table (không chỉ local JSON)
- Weekly data quality report tự generate từ `audit.dq_results`

**Todo List:**
1. Đọc `platforms/great_expectations/` — xem current expectations và checkpoints
2. Tạo `governance/contracts/loan_application_contract.yaml` — định nghĩa: column, not_null, type, min/max, regex
3. Cập nhật GE expectations từ contract YAML (script parse contract → generate GE expectation suite)
4. Tạo PostgreSQL table `audit.dq_results`: `run_id, table_name, rule, passed, failed_count, timestamp`
5. Wrap GE checkpoint runner: persist results vào `audit.dq_results` sau mỗi run
6. Cập nhật Prefect flow: GE fail → Prefect `raise ValueError` với structured context → task FAILED
7. Tạo `dbt/models/reporting/rpt_data_quality_weekly.sql` — aggregate `audit.dq_results`

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/platforms/great_expectations/` (existing checkpoints)
- File: `DataOps_pipeline_HomeCredit/cli/flows/serving/ge_validator.py`
- Theory: BFT Circuit Breaker → Contract violation = Byzantine fault → halt propagation

**Status:** `[ ] pending`

---

## Sub-Task 05 — Spark Production Config (Memory + Partition Tuning)

**Intent:**
`spark_session.py` hiện tại (modified, uncommitted) chưa có production memory tuning. Với HomeCredit dataset (5M+ loan records), default Spark config sẽ OOM hoặc spill to disk. Production: explicit executor memory, partition sizing, adaptive query execution (AQE).

**Expected Outcomes:**
- `spark_session.py` có explicit production config: executor memory, driver memory, parallelism
- AQE enabled: `spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.coalescePartitions.enabled=true`
- Partition count = `max(200, input_size_bytes / 128MB)` — tránh too-small partitions
- Delta Lake optimizations: `optimizeWrite=true`, `autoCompact=true` (small file problem)
- Spark metrics export sang Prometheus qua `spark.metrics.conf`

**Todo List:**
1. Đọc `platforms/processing/spark_stack/spark_session.py` (current modified state)
2. Thêm production config block: `executor.memory=4g`, `driver.memory=2g`, `executor.cores=2`
3. Enable AQE: `spark.sql.adaptive.enabled`, `spark.sql.adaptive.skewJoin.enabled`
4. Delta auto-optimize: `spark.databricks.delta.optimizeWrite.enabled=true`, `delta.autoOptimize.autoCompact=true`
5. Thêm Prometheus JMX exporter config: `spark.metrics.conf=metrics.properties`
6. Tạo `platforms/processing/config/spark_production.yaml` — externalize config (không hardcode trong Python)
7. Update `stack.yaml` với Spark config section: `spark.executor_memory`, `spark.cores`, `spark.partitions`

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/platforms/processing/spark_stack/spark_session.py` ← MODIFIED
- File: `DataOps_pipeline_HomeCredit/platforms/config/stack.yaml` ← MODIFIED
- Theory: Small File Problem → Delta `autoCompact` → reduce metadata overhead + scan time

**Status:** `[ ] pending`

---

## Sub-Task 06 — OpenTelemetry + Structured Logging + Alerting

**Intent:**
Không có distributed tracing. Khi daily pipeline fail lúc 3AM, không có visibility vào: bước nào fail, tại sao, data volume bao nhiêu. Production: OTel traces + structured JSON logs + Prefect email/Slack alert.

**Expected Outcomes:**
- Mọi Prefect `@flow` và `@task` emit OTel spans
- Mọi log: JSON với `trace_id`, `span_id`, `correlation_id`, `run_id`, `pipeline_step`, `records_count`
- Prefect alert: khi flow fail → email hoặc Slack notification (dùng Prefect Automations)
- `docker-compose.yml`: Jaeger, Prometheus, Grafana services
- Grafana dashboard: pipeline_duration, records_processed, error_rate, GE validation results

**Todo List:**
1. Thêm `opentelemetry-sdk>=1.25`, `opentelemetry-exporter-otlp-proto-grpc>=1.25` vào `requirements.txt`
2. Tạo `shared/telemetry.py` — OTel tracer init + Prometheus metrics registry
3. Wrap Prefect flows với OTel spans: bronze_flow, silver_flow, ge_validator
4. Reconfigure structlog: thêm OTel context processor
5. Configure Prefect Automation: flow failure → send notification (email/Slack webhook)
6. `docker-compose.yml`: thêm Jaeger, Prometheus, Grafana
7. Tạo Grafana dashboard JSON `grafana/homecredit_pipeline.json`

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/cli/flows/serving/daily_pipeline.py`
- File: `DataOps_pipeline_HomeCredit/cli/flows/serving/bronze_flow.py`
- Standard: Prefect `Automation` → requires Prefect Cloud or self-hosted server

**Status:** `[ ] pending`

---

## Sub-Task 07 — GitHub Actions CI/CD Pipeline

**Intent:**
Không có automated pipeline. Uncommitted changes (ST00) là triệu chứng: không có CI gate ngăn bad code vào main. Production: CI chạy mỗi PR — lint, test, dbt test, GE validation trên sample data.

**Expected Outcomes:**
- `.github/workflows/ci.yml`: lint (ruff) → test (pytest) → dbt test (sample DB) → docker build
- `.github/workflows/cd.yml`: on merge to main → tag release → update CHANGELOG
- `CHANGELOG.md` tạo mới với template Keep a Changelog
- Pre-commit: ruff, mypy, detect-secrets, sqlfluff (SQL linting)
- `Makefile` với targets: `make test`, `make lint`, `make dbt-test`, `make docker-up`

**Todo List:**
1. Tạo `.github/workflows/ci.yml` — lint + test + dbt parse/compile
2. Thêm `ruff`, `mypy`, `sqlfluff`, `pytest-cov` vào `requirements-dev.txt`
3. Tạo `pyproject.toml` — ruff + mypy config
4. Tạo `.sqlfluffrc` — sqlfluff dialect=postgres, templater=dbt
5. Tạo `Makefile` với targets: test, lint, dbt-run, docker-up, docker-down
6. Tạo `.pre-commit-config.yaml` — ruff, detect-secrets, sqlfluff
7. Tạo `CHANGELOG.md` — initial entry cho current codebase state

**Relevant Context:**
- File: `DataOps_pipeline_HomeCredit/.github/` (kiểm tra existing config)
- File: `DataOps_pipeline_HomeCredit/requirements.txt`
- Dependency: ST01 (Gold/dbt) phải xong trước CI có dbt test job

**Status:** `[ ] pending`

---

## Dependency Order

```
ST00 (Fix tồn đọng — P0)       → bắt buộc trước mọi ST khác
ST01 (dbt Gold Layer)          → ST02 (dbt Snapshot depend on dbt setup)
ST03 (PII Protection)          → ST04 (Data Contracts reference PII catalog)
ST05 (Spark Tuning)            → ST01 (Gold layer cần Spark tốt)
ST01 + ST04                    → ST06 (Observability wrap cả hai)
ST01 + ST02 + ST03 + ST04      → ST07 (CI test tất cả)
```

## Production Checklist

| Concern | Sub-Task | Theory Applied |
|---------|----------|----------------|
| Uncommitted debt | ST00 | Technical Debt → Fix ngay trước scale |
| Analytics layer | ST01 | Kimball Dimensional Modeling → Fact + Dim |
| Historical tracking | ST02 | SCD Type 2 → Audit Trail for Risk Models |
| PII / GDPR | ST03 | Crypto-Shredding → AES-256-GCM + Key Vault |
| Data quality | ST04 | BFT Circuit Breaker → Contract Halt Propagation |
| Compute reliability | ST05 | AQE + Delta autoCompact → Small File Problem |
| Visibility | ST06 | OpenTelemetry → MTTR Reduction |
| Automation | ST07 | DORA → Deployment Frequency + Change Fail Rate |
