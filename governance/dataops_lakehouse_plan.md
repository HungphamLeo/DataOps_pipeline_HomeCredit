# DataOps Lakehouse Plan — Home Credit Risk
## Architecture: Bronze → Staging → Mart (MinIO + Delta Lake + Spark + PostgreSQL)

---

## Top-Level Overview

**Goal:** Xây dựng DataOps Lakehouse pipeline hoàn chỉnh cho bài toán Credit Risk Modelling và Reporting tại Home Credit, theo kiến trúc 3 tầng **Bronze → Staging → Mart**:

- **Bronze:** Raw CSV → Delta Lake tables lưu trên MinIO (S3-compatible object storage)
- **Staging:** PySpark đọc Delta Lake từ MinIO, transform + aggregate bằng Spark SQL, ghi ngược ra Delta Lake trên MinIO
- **Mart:** PySpark đọc Staging Delta Lake, chạy SQL transform, write ra **PostgreSQL** phục vụ BI/Report và ML input

**Orchestration:** Prefect (mở rộng từ `daily_pipeline.py` hiện có)  
**Data Quality:** Great Expectations với Spark backend  
**CI/CD hook:** `scripts/check_downstream_impact.py` hiện có + GitHub Actions  
**Schedule:** Cron `0 1 * * *` (1 AM daily)  
**Environment:** On-premise / Local — MinIO + Spark local mode + PostgreSQL local

---

## Stack Technologies

| Component | Tool | Vai trò |
|---|---|---|
| Object Storage | **MinIO** | Lưu trữ Bronze & Staging dạng Delta Lake (S3-compatible) |
| Table Format | **Delta Lake** | ACID transactions, time travel, schema evolution trên MinIO |
| Compute | **Apache Spark (local mode)** | Đọc/ghi Delta Lake, transform, aggregate |
| Mart Storage | **PostgreSQL** | Lưu Mart tables cho BI/Reporting/Model input |
| Orchestration | **Prefect** | Flow scheduling, dependency, retry |
| Data Quality | **Great Expectations** | Validation tại mỗi tầng |
| Config | **YAML** | `prefect/config/pipeline_config.yaml` |

---

## Kiến trúc tổng quan

```
[Raw CSV Sources]
sql/dev/source_data/*.csv
        |
        | Prefect: bronze_flow.py
        v
[BRONZE — MinIO + Delta Lake]
s3a://lakehouse/bronze/<table>/
        |
        | Prefect: staging_flow.py
        | PySpark reads Delta, runs Spark SQL transforms
        v
[STAGING — MinIO + Delta Lake]
s3a://lakehouse/staging/<table>/
        |
        | Prefect: mart_flow.py
        | PySpark reads Delta, runs SQL, writes to PostgreSQL
        v
[MART — PostgreSQL]
DB: homecredit_mart
Schema: mart.*
```

---

## Chi tiết thiết kế từng tầng

---

### BRONZE Layer — MinIO + Delta Lake

**Bucket path:** `s3a://lakehouse/bronze/`  
**Nguyên tắc:**
- Lưu nguyên bản dữ liệu từ source CSV, không transform business logic
- Thêm metadata: `_load_ts` (timestamp), `_source_file` (tên file gốc)
- Format: **Delta Lake** — hỗ trợ ACID, time travel, append-only safe
- Partition: `_load_date=YYYY-MM-DD` để dễ reprocess theo ngày
- Không xóa data cũ — append nếu run lại (idempotent nhờ partition overwrite)

| Bảng Bronze | MinIO Path | Source CSV | Grain |
|---|---|---|---|
| `bronze_application` | `bronze/application/` | HC_application_train.csv | 1 row = 1 loan application |
| `bronze_bureau` | `bronze/bureau/` | HC_bureau.csv | 1 row = 1 credit bureau record |
| `bronze_bureau_balance` | `bronze/bureau_balance/` | HC_bureau_balance.csv | 1 row = 1 month of bureau credit |
| `bronze_previous_application` | `bronze/previous_application/` | HC_previous_application.csv | 1 row = 1 prior loan application |
| `bronze_installments_payments` | `bronze/installments_payments/` | HC_installments_payments.csv | 1 row = 1 payment installment |
| `bronze_pos_cash_balance` | `bronze/pos_cash_balance/` | HC_POS_CASH_balance.csv | 1 row = 1 month POS/CASH |
| `bronze_credit_card_balance` | `bronze/credit_card_balance/` | HC_credit_card_balance.csv | 1 row = 1 month credit card |

**Write strategy:** `delta.write.mode("overwrite").option("replaceWhere", "_load_date = '{date}'")`  
→ Partition overwrite — chạy lại cùng ngày không nhân đôi data (idempotent)

**Metadata columns thêm vào mỗi bảng:**
```
_load_ts        TIMESTAMP   -- thời điểm nạp vào Bronze
_source_file    STRING      -- tên file nguồn (e.g. "HC_application_train.csv")
_load_date      DATE        -- partition key
```

---

### STAGING Layer — MinIO + Delta Lake (Spark SQL Transform)

**Bucket path:** `s3a://lakehouse/staging/`  
**Nguyên tắc:**
- PySpark đọc Bronze Delta tables từ MinIO
- Transform bằng **Spark SQL scripts** (`.sql` files trong `sql/staging/`)
- Aggregate các bảng lịch sử về grain **SK_ID_CURR** (1 row per customer)
- Ghi ra Delta Lake trên MinIO với mode `overwrite` (full refresh mỗi run)
- Mỗi bảng staging = 1 Spark SQL script + 1 Prefect task

| Bảng Staging | MinIO Path | Grain | Nguồn Bronze |
|---|---|---|---|
| `stg_application` | `staging/stg_application/` | SK_ID_CURR | bronze_application |
| `stg_bureau_summary` | `staging/stg_bureau_summary/` | SK_ID_CURR | bronze_bureau + bronze_bureau_balance |
| `stg_prev_application_summary` | `staging/stg_prev_application_summary/` | SK_ID_CURR | bronze_previous_application |
| `stg_installment_summary` | `staging/stg_installment_summary/` | SK_ID_CURR | bronze_installments_payments |
| `stg_pos_cash_summary` | `staging/stg_pos_cash_summary/` | SK_ID_CURR | bronze_pos_cash_balance |
| `stg_credit_card_summary` | `staging/stg_credit_card_summary/` | SK_ID_CURR | bronze_credit_card_balance |

**Spark SQL script files (sẽ tạo trong `sql/staging/`):**

```
sql/staging/
├── stg_application.sql
├── stg_bureau_summary.sql
├── stg_prev_application_summary.sql
├── stg_installment_summary.sql
├── stg_pos_cash_summary.sql
└── stg_credit_card_summary.sql
```

**Key derived features per staging table:**

#### `stg_application` — Clean + Feature Engineering từ bronze_application
```sql
-- Derived features:
AGE_YEARS                = ABS(DAYS_BIRTH) / 365.0
EMPLOYED_YEARS           = CASE WHEN DAYS_EMPLOYED = 365243 THEN NULL
                                ELSE ABS(DAYS_EMPLOYED) / 365.0 END
DAYS_EMPLOYED_ANOMALY    = CASE WHEN DAYS_EMPLOYED = 365243 THEN 1 ELSE 0 END
INCOME_CREDIT_RATIO      = AMT_INCOME_TOTAL / NULLIF(AMT_CREDIT, 0)
ANNUITY_INCOME_RATIO     = AMT_ANNUITY / NULLIF(AMT_INCOME_TOTAL, 0)
CREDIT_GOODS_RATIO       = AMT_CREDIT / NULLIF(AMT_GOODS_PRICE, 0)
```

#### `stg_bureau_summary` — Aggregate credit history per customer
```sql
-- JOIN bronze_bureau + bronze_bureau_balance, GROUP BY SK_ID_CURR:
BUREAU_LOAN_COUNT        = COUNT(DISTINCT SK_ID_BUREAU)
BUREAU_ACTIVE_COUNT      = COUNT FILTER (CREDIT_ACTIVE = 'Active')
BUREAU_CLOSED_COUNT      = COUNT FILTER (CREDIT_ACTIVE = 'Closed')
BUREAU_MAX_OVERDUE       = MAX(AMT_CREDIT_MAX_OVERDUE)
BUREAU_TOTAL_DEBT        = SUM(AMT_CREDIT_SUM_DEBT)
BUREAU_TOTAL_CREDIT      = SUM(AMT_CREDIT_SUM)
BUREAU_AVG_DPD           = AVG(CREDIT_DAY_OVERDUE)
BUREAU_BAD_STATUS_RATE   = COUNT(STATUS IN '1','2','3','4','5') / COUNT(STATUS)
```

#### `stg_prev_application_summary` — Aggregate prior loan behavior
```sql
-- GROUP BY SK_ID_CURR từ bronze_previous_application:
PREV_APP_COUNT           = COUNT(SK_ID_PREV)
PREV_APPROVED_COUNT      = COUNT FILTER (NAME_CONTRACT_STATUS = 'Approved')
PREV_REFUSED_COUNT       = COUNT FILTER (NAME_CONTRACT_STATUS = 'Refused')
PREV_APPROVAL_RATE       = PREV_APPROVED_COUNT / NULLIF(PREV_APP_COUNT, 0)
PREV_AVG_CREDIT          = AVG(AMT_CREDIT)
PREV_MAX_CREDIT          = MAX(AMT_CREDIT)
PREV_AVG_ANNUITY         = AVG(AMT_ANNUITY)
```

#### `stg_installment_summary` — Aggregate payment behavior
```sql
-- GROUP BY SK_ID_CURR từ bronze_installments_payments:
INSTALMENT_COUNT         = COUNT(*)
INSTALMENT_LATE_COUNT    = COUNT FILTER (DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT)
INSTALMENT_LATE_RATE     = INSTALMENT_LATE_COUNT / NULLIF(INSTALMENT_COUNT, 0)
INSTALMENT_AVG_DELAY     = AVG(DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT) WHERE late
INSTALMENT_AMT_SHORTFALL = SUM(AMT_INSTALMENT - AMT_PAYMENT) WHERE underpaid
```

#### `stg_pos_cash_summary` — Aggregate POS/CASH loan monthly status
```sql
-- GROUP BY SK_ID_CURR từ bronze_pos_cash_balance:
POS_MONTHS_COUNT         = COUNT(*)
POS_MAX_DPD              = MAX(SK_DPD)
POS_AVG_DPD              = AVG(SK_DPD)
POS_OVERDUE_MONTHS       = COUNT FILTER (SK_DPD > 0)
POS_OVERDUE_RATE         = POS_OVERDUE_MONTHS / NULLIF(POS_MONTHS_COUNT, 0)
```

#### `stg_credit_card_summary` — Aggregate credit card monthly behavior
```sql
-- GROUP BY SK_ID_CURR từ bronze_credit_card_balance:
CC_MONTHS_COUNT          = COUNT(*)
CC_AVG_BALANCE           = AVG(AMT_BALANCE)
CC_MAX_BALANCE           = MAX(AMT_BALANCE)
CC_AVG_UTILIZATION       = AVG(AMT_BALANCE / NULLIF(AMT_CREDIT_LIMIT_ACTUAL, 0))
CC_AVG_PAYMENT_RATIO     = AVG(AMT_PAYMENT_TOTAL_CURRENT / NULLIF(AMT_INST_MIN_REGULARITY, 0))
CC_MAX_DPD               = MAX(SK_DPD)
```

---

### MART Layer — PostgreSQL

**Database:** `homecredit_mart`  
**Schema:** `mart`  
**Nguyên tắc:**
- PySpark đọc tất cả Staging Delta tables từ MinIO
- Chạy SQL transform (JOIN + business logic)
- Write ra PostgreSQL bằng JDBC connector
- **Write strategy:** `TRUNCATE + INSERT` (idempotent, full refresh)
- 3 mart tables phục vụ 3 mục đích khác nhau

| Bảng Mart | PostgreSQL Table | Grain | Mục đích |
|---|---|---|---|
| `mart_risk_model_features` | `mart.risk_model_features` | SK_ID_CURR | Wide feature table → input cho ML model training |
| `mart_risk_report_summary` | `mart.risk_report_summary` | SK_ID_CURR | Business-friendly subset + RISK_TIER → BI dashboard |
| `mart_default_cohort` | `mart.default_cohort` | GROUP + COHORT_VALUE | Default rate aggregation → Report |

**SQL transform scripts (sẽ tạo trong `sql/marts/`):**

```
sql/marts/
├── mart_risk_model_features.sql
├── mart_risk_report_summary.sql
└── mart_default_cohort.sql
```

#### `mart.risk_model_features` — DDL (PostgreSQL)
```sql
CREATE TABLE mart.risk_model_features (
    sk_id_curr              INTEGER PRIMARY KEY,
    target                  SMALLINT,           -- 0/1 label
    -- Application features
    amt_income_total        NUMERIC(18,2),
    amt_credit              NUMERIC(18,2),
    amt_annuity             NUMERIC(18,2),
    amt_goods_price         NUMERIC(18,2),
    ext_source_1            NUMERIC(18,8),
    ext_source_2            NUMERIC(18,8),
    ext_source_3            NUMERIC(18,8),
    age_years               NUMERIC(6,2),
    employed_years          NUMERIC(6,2),
    days_employed_anomaly   SMALLINT,
    income_credit_ratio     NUMERIC(12,6),
    annuity_income_ratio    NUMERIC(12,6),
    credit_goods_ratio      NUMERIC(12,6),
    name_contract_type      VARCHAR(50),
    code_gender             VARCHAR(5),
    name_income_type        VARCHAR(100),
    name_education_type     VARCHAR(100),
    name_family_status      VARCHAR(100),
    name_housing_type       VARCHAR(100),
    region_rating_client    SMALLINT,
    -- Bureau summary features
    bureau_loan_count       INTEGER,
    bureau_active_count     INTEGER,
    bureau_closed_count     INTEGER,
    bureau_max_overdue      NUMERIC(18,2),
    bureau_total_debt       NUMERIC(18,2),
    bureau_total_credit     NUMERIC(18,2),
    bureau_avg_dpd          NUMERIC(10,4),
    bureau_bad_status_rate  NUMERIC(8,6),
    -- Previous application features
    prev_app_count          INTEGER,
    prev_approved_count     INTEGER,
    prev_refused_count      INTEGER,
    prev_approval_rate      NUMERIC(8,6),
    prev_avg_credit         NUMERIC(18,2),
    prev_max_credit         NUMERIC(18,2),
    -- Installment features
    instalment_count        INTEGER,
    instalment_late_count   INTEGER,
    instalment_late_rate    NUMERIC(8,6),
    instalment_avg_delay    NUMERIC(10,4),
    instalment_amt_shortfall NUMERIC(18,2),
    -- POS/CASH features
    pos_months_count        INTEGER,
    pos_max_dpd             INTEGER,
    pos_avg_dpd             NUMERIC(10,4),
    pos_overdue_months      INTEGER,
    pos_overdue_rate        NUMERIC(8,6),
    -- Credit card features
    cc_months_count         INTEGER,
    cc_avg_balance          NUMERIC(18,2),
    cc_max_balance          NUMERIC(18,2),
    cc_avg_utilization      NUMERIC(8,6),
    cc_avg_payment_ratio    NUMERIC(8,6),
    cc_max_dpd              INTEGER,
    -- Audit
    _loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `mart.risk_report_summary` — DDL (PostgreSQL)
```sql
CREATE TABLE mart.risk_report_summary (
    sk_id_curr              INTEGER PRIMARY KEY,
    target                  SMALLINT,
    age_years               NUMERIC(6,2),
    name_income_type        VARCHAR(100),
    name_education_type     VARCHAR(100),
    name_family_status      VARCHAR(100),
    name_housing_type       VARCHAR(100),
    amt_income_total        NUMERIC(18,2),
    amt_credit              NUMERIC(18,2),
    income_credit_ratio     NUMERIC(12,6),
    ext_source_2            NUMERIC(18,8),
    bureau_max_overdue      NUMERIC(18,2),
    bureau_bad_status_rate  NUMERIC(8,6),
    instalment_late_rate    NUMERIC(8,6),
    pos_max_dpd             INTEGER,
    cc_avg_utilization      NUMERIC(8,6),
    risk_tier               VARCHAR(10),        -- 'High' / 'Medium' / 'Low'
    _loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- RISK_TIER logic:
-- 'High'   : ext_source_2 < 0.3  AND bureau_max_overdue > 0
-- 'Low'    : ext_source_2 > 0.5  AND bureau_bad_status_rate < 0.1
-- 'Medium' : everything else
```

#### `mart.default_cohort` — DDL (PostgreSQL)
```sql
CREATE TABLE mart.default_cohort (
    cohort_dim      VARCHAR(100),   -- dimension name e.g. 'name_income_type'
    cohort_value    VARCHAR(200),   -- dimension value e.g. 'Working'
    total_count     INTEGER,
    default_count   INTEGER,
    default_rate    NUMERIC(8,6),
    _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cohort_dim, cohort_value)
);
-- Cohort dimensions to compute:
-- name_income_type, name_education_type, name_family_status,
-- name_housing_type, region_rating_client, name_contract_type
```

---

## Cấu trúc thư mục đề xuất

```
DataOps_pipeline_HomeCredit/
├── sql/
│   ├── dev/source_data/              -- (existing) raw CSV sources
│   ├── dev/set_up_table/             -- (existing) DDL files
│   ├── staging/                      -- NEW: Spark SQL transform scripts
│   │   ├── stg_application.sql
│   │   ├── stg_bureau_summary.sql
│   │   ├── stg_prev_application_summary.sql
│   │   ├── stg_installment_summary.sql
│   │   ├── stg_pos_cash_summary.sql
│   │   └── stg_credit_card_summary.sql
│   ├── marts/                        -- NEW: mart SQL (Spark SQL → PostgreSQL)
│   │   ├── mart_risk_model_features.sql
│   │   ├── mart_risk_report_summary.sql
│   │   └── mart_default_cohort.sql
│   └── ddl/                          -- NEW: PostgreSQL DDL
│       ├── create_mart_schema.sql
│       └── create_mart_tables.sql
│
├── prefect/
│   ├── config/
│   │   └── pipeline_config.yaml      -- NEW: paths, schedule, DB conn, Spark config
│   └── flow/
│       ├── daily_pipeline.py         -- REFACTOR: master flow, CronSchedule
│       ├── bronze_flow.py            -- NEW: CSV → Delta Lake on MinIO
│       ├── staging_flow.py           -- NEW: Delta Lake → Spark SQL → Delta Lake
│       └── mart_flow.py              -- NEW: Delta Lake → Spark SQL → PostgreSQL
│
├── spark/                            -- NEW: Spark session factory + utilities
│   ├── spark_session.py              -- SparkSession với Delta + MinIO (S3A) config
│   └── delta_utils.py                -- helper: read/write Delta tables
│
├── great_expectations/               -- NEW: GE project
│   ├── great_expectations.yml
│   ├── expectations/
│   │   ├── bronze_application_suite.json
│   │   ├── stg_application_suite.json
│   │   └── mart_risk_model_features_suite.json
│   └── checkpoints/
│       ├── bronze_checkpoint.yml
│       ├── staging_checkpoint.yml
│       └── mart_checkpoint.yml
│
├── scripts/
│   ├── check_downstream_impact.py    -- (existing)
│   └── validate_sql_syntax.py        -- NEW: SQL lint helper for CI
│
├── log/
│   └── ge_validations/               -- GE HTML/JSON validation reports
│
├── docker/                           -- NEW: docker-compose for MinIO + PostgreSQL
│   └── docker-compose.yml
│
├── requirements.txt                  -- UPDATE: thêm delta-spark, pyspark, GE, prefect
└── analyze_governance/
    └── dataops_lakehouse_plan.md     -- this file
```

---

## Infrastructure Setup (MinIO + PostgreSQL via Docker)

`docker/docker-compose.yml` sẽ spin up:
- **MinIO** on port 9000 (API) + 9001 (Console)
  - Bucket: `lakehouse` với folders: `bronze/`, `staging/`
  - Access key: configurable via env vars
- **PostgreSQL** on port 5432
  - Database: `homecredit_mart`
  - Schema: `mart`

---

## Spark Configuration

`spark/spark_session.py` sẽ tạo SparkSession với:
- `spark.jars.packages`: `io.delta:delta-core`, `org.apache.hadoop:hadoop-aws`
- `spark.hadoop.fs.s3a.endpoint`: MinIO endpoint (e.g. `http://localhost:9000`)
- `spark.hadoop.fs.s3a.access.key` / `secret.key`: từ env vars / config YAML
- `spark.sql.extensions`: `io.delta.sql.DeltaSparkSessionExtension`
- `spark.sql.catalog.spark_catalog`: `org.apache.spark.sql.delta.catalog.DeltaCatalog`

---

## Data Quality với Great Expectations

**Strategy:** Mỗi tầng có 1 GE expectation suite + 1 checkpoint. GE dùng Spark backend (SparkDFDataset) để validate Spark DataFrames trước khi write.

### Bronze Expectations (`bronze_application_suite`):
```
expect_column_to_exist: [SK_ID_CURR, TARGET, AMT_CREDIT, AMT_INCOME_TOTAL]
expect_column_values_to_not_be_null: SK_ID_CURR (100%)
expect_column_values_to_be_unique: SK_ID_CURR
expect_column_values_to_be_in_set: TARGET → [0, 1]
expect_table_row_count_to_be_between: min=100000, max=500000
```

### Staging Expectations (`stg_application_suite`):
```
expect_column_values_to_not_be_null: [SK_ID_CURR, AGE_YEARS, INCOME_CREDIT_RATIO]
expect_column_values_to_be_between: AGE_YEARS → (18, 100)
expect_column_values_to_be_between: INCOME_CREDIT_RATIO → (0, 100)
expect_column_values_to_be_between: ANNUITY_INCOME_RATIO → (0, 1)
expect_column_proportion_of_unique_values_to_be_between: SK_ID_CURR → 1.0
```

### Mart Expectations (`mart_risk_model_features_suite`):
```
expect_table_row_count_to_equal: row count = stg_application row count
expect_column_values_to_be_between: bureau_bad_status_rate → (0, 1)
expect_column_values_to_be_between: instalment_late_rate → (0, 1)
expect_column_values_to_be_in_set: risk_tier (report) → ['High','Medium','Low']
```

**GE validation results:** lưu tại `log/ge_validations/` dạng JSON + HTML  
**Fail behavior:** nếu GE checkpoint fail → Prefect task raise exception → pipeline dừng, không tiếp tục tầng tiếp theo

---

## Prefect Orchestration

### Master Flow (daily_pipeline.py):
```
master_pipeline_flow (CronSchedule: "0 1 * * *")
├── bronze_ingest_flow
│   ├── task: load_csv_to_bronze(table) × 7  [parallel]
│   └── task: run_ge_bronze_checkpoint
├── staging_transform_flow
│   ├── task: build_stg_application
│   ├── task: build_stg_bureau_summary
│   ├── task: build_stg_prev_application_summary
│   ├── task: build_stg_installment_summary
│   ├── task: build_stg_pos_cash_summary
│   ├── task: build_stg_credit_card_summary
│   └── task: run_ge_staging_checkpoint
└── mart_build_flow
    ├── task: build_mart_risk_model_features
    ├── task: build_mart_risk_report_summary
    ├── task: build_mart_default_cohort
    └── task: run_ge_mart_checkpoint
```

**Task settings:** `retries=2, retry_delay_seconds=30`  
**Config:** tất cả paths, credentials, schedule đọc từ `prefect/config/pipeline_config.yaml`

---

## Idempotency Strategy

| Tầng | Strategy | Lý do |
|---|---|---|
| Bronze | Delta Lake partition overwrite theo `_load_date` | Chạy lại cùng ngày → overwrite đúng partition, data ngày khác an toàn |
| Staging | Delta Lake `overwrite` (full refresh) | Staging là derived data, recompute từ Bronze mỗi lần |
| Mart | PostgreSQL `TRUNCATE + INSERT` trong transaction | Full refresh, atomic, không duplicate |

---

## Sub-Tasks

---

### Sub-Task 1 — Infrastructure Setup: Docker Compose + MinIO + PostgreSQL

**Intent:** Tạo `docker/docker-compose.yml` để spin up MinIO và PostgreSQL local. Đây là infrastructure foundation cho toàn bộ pipeline.

**Expected Outcomes:**
- `docker-compose up` chạy được MinIO (port 9000/9001) và PostgreSQL (port 5432)
- MinIO bucket `lakehouse` được tạo tự động với folders `bronze/`, `staging/`
- PostgreSQL database `homecredit_mart` và schema `mart` được khởi tạo
- `prefect/config/pipeline_config.yaml` chứa tất cả connection settings

**Todo List:**
1. Tạo `docker/docker-compose.yml` với services: `minio`, `postgres`, `minio-init` (mc client tạo bucket)
2. Tạo `sql/ddl/create_mart_schema.sql` — `CREATE SCHEMA IF NOT EXISTS mart`
3. Tạo `sql/ddl/create_mart_tables.sql` — DDL 3 mart tables (theo DDL trong plan này)
4. Tạo `prefect/config/pipeline_config.yaml` với sections: `minio`, `postgres`, `spark`, `paths`, `schedule`, `great_expectations`
5. Tạo `.env.example` template cho secrets (MinIO keys, PostgreSQL password)
6. Update `requirements.txt`: thêm `pyspark`, `delta-spark`, `great-expectations`, `psycopg2-binary`, `boto3`, `prefect`

**Relevant Context:**
- Delta Lake + Spark cần `delta-spark` package tương thích với PySpark version
- MinIO dùng S3A protocol: endpoint `http://localhost:9000`
- PostgreSQL DDL nằm trong section MART Layer của plan này

**Status:** [ ] pending

---

### Sub-Task 2 — Spark Session Factory + Delta Lake Utilities

**Intent:** Tạo `spark/spark_session.py` và `spark/delta_utils.py` — shared utilities để tất cả flows dùng chung SparkSession đã config sẵn Delta + MinIO.

**Expected Outcomes:**
- `get_spark_session()` trả về SparkSession configured với Delta Lake extensions và S3A MinIO config
- `read_delta(spark, path)` và `write_delta(df, path, mode, partition_by)` utility functions
- Credentials đọc từ `pipeline_config.yaml` (không hard-code)
- Có thể test: `python spark/spark_session.py` chạy không lỗi

**Todo List:**
1. Tạo `spark/spark_session.py` với function `get_spark_session(config_path)`:
   - Set Delta Lake packages (`delta-core` JAR)
   - Set S3A configs: endpoint, access key, secret key, path style access
   - Set Delta extensions và catalog
2. Tạo `spark/delta_utils.py` với:
   - `read_delta(spark, minio_path)` → DataFrame
   - `write_delta(df, minio_path, mode="overwrite", partition_by=None)` → write Delta table
   - `write_jdbc(df, table_name, pg_conn_str, mode="overwrite")` → write to PostgreSQL via JDBC
3. Test SparkSession connect được MinIO (viết 1 dummy DataFrame ra `s3a://lakehouse/test/`)

**Relevant Context:**
- `pipeline_config.yaml` từ Sub-Task 1 chứa MinIO và PostgreSQL config
- Spark JAR packages: `io.delta:delta-core_2.12:2.4.0`, `org.apache.hadoop:hadoop-aws:3.3.4`
- S3A path style: `spark.hadoop.fs.s3a.path.style.access = true` (required cho MinIO)

**Status:** [ ] pending

---

### Sub-Task 3 — Bronze Flow: CSV → Delta Lake trên MinIO

**Intent:** Tạo `prefect/flow/bronze_flow.py` — đọc 7 CSV sources, thêm metadata columns, ghi Delta Lake vào MinIO với partition overwrite strategy.

**Expected Outcomes:**
- `python prefect/flow/bronze_flow.py` chạy được
- 7 Delta tables tồn tại tại `s3a://lakehouse/bronze/<table>/`
- Mỗi table có columns: tất cả raw columns + `_load_ts`, `_source_file`, `_load_date`
- Chạy lại cùng ngày → partition bị overwrite, không nhân đôi

**Todo List:**
1. Tạo `prefect/flow/bronze_flow.py` với `@flow def bronze_ingest_flow()`
2. Task `load_csv_to_bronze(table_name, csv_path, delta_path)`:
   - Đọc CSV bằng Spark (`spark.read.csv`)
   - Thêm `_load_ts = current_timestamp()`, `_source_file`, `_load_date`
   - Ghi Delta với `replaceWhere "_load_date = '{today}'"` (partition overwrite)
3. Chạy 7 tasks — có thể chạy parallel trong Prefect (dùng `allow_failure=False`)
4. Tích hợp call vào `daily_pipeline.py` master flow

**Relevant Context:**
- 7 source CSV trong `sql/dev/source_data/`
- Paths cấu hình trong `pipeline_config.yaml`
- Delta write utilities từ `spark/delta_utils.py`
- `prefect/flow/daily_pipeline.py` hiện tại có logic CSV load cần migrate/replace

**Status:** [ ] pending

---

### Sub-Task 4 — Staging SQL Scripts + Staging Flow

**Intent:** Tạo 6 Spark SQL scripts (`sql/staging/`) và `prefect/flow/staging_flow.py` — đọc Bronze Delta tables, transform + aggregate bằng Spark SQL, ghi Staging Delta tables ra MinIO.

**Expected Outcomes:**
- 6 SQL scripts trong `sql/staging/` chạy đúng logic aggregate
- `python prefect/flow/staging_flow.py` chạy được
- 6 Staging Delta tables tại `s3a://lakehouse/staging/<table>/`
- Mỗi table có grain SK_ID_CURR, đầy đủ derived features

**Todo List:**
1. Tạo `sql/staging/stg_application.sql` — clean + 6 derived features (AGE_YEARS, EMPLOYED_YEARS, DAYS_EMPLOYED_ANOMALY, INCOME_CREDIT_RATIO, ANNUITY_INCOME_RATIO, CREDIT_GOODS_RATIO)
2. Tạo `sql/staging/stg_bureau_summary.sql` — JOIN bureau + bureau_balance, 8 aggregate features
3. Tạo `sql/staging/stg_prev_application_summary.sql` — 7 aggregate features
4. Tạo `sql/staging/stg_installment_summary.sql` — 5 aggregate features (bao gồm late rate, delay days)
5. Tạo `sql/staging/stg_pos_cash_summary.sql` — 5 aggregate features
6. Tạo `sql/staging/stg_credit_card_summary.sql` — 6 aggregate features
7. Tạo `prefect/flow/staging_flow.py` với `@flow def staging_transform_flow()`:
   - Mỗi SQL file tương ứng 1 `@task build_stg_<name>(spark, config)`
   - Spark đọc SQL script từ file, register Bronze tables làm temp views, `spark.sql(query)`, ghi Delta ra MinIO
8. Tích hợp vào `daily_pipeline.py`

**Relevant Context:**
- Tất cả SQL logic và feature definitions trong section STAGING Layer của plan này
- Bronze Delta paths từ Sub-Task 3
- Spark SQL cần Bronze tables được register làm `createOrReplaceTempView` trước khi query

**Status:** [ ] pending

---

### Sub-Task 5 — Mart SQL Scripts + Mart Flow (→ PostgreSQL)

**Intent:** Tạo 3 SQL scripts (`sql/marts/`) và `prefect/flow/mart_flow.py` — đọc Staging Delta từ MinIO, JOIN + transform bằng Spark SQL, write ra PostgreSQL.

**Expected Outcomes:**
- 3 SQL scripts trong `sql/marts/` đúng logic business
- `python prefect/flow/mart_flow.py` chạy được
- 3 PostgreSQL tables tại `homecredit_mart.mart.*` đã được populate
- TRUNCATE + INSERT → idempotent khi chạy lại

**Todo List:**
1. Tạo `sql/marts/mart_risk_model_features.sql` — LEFT JOIN tất cả 6 staging tables trên SK_ID_CURR
2. Tạo `sql/marts/mart_risk_report_summary.sql` — SELECT subset + CASE WHEN RISK_TIER logic
3. Tạo `sql/marts/mart_default_cohort.sql` — UNION ALL multiple GROUP BY per cohort dimension
4. Tạo `prefect/flow/mart_flow.py` với `@flow def mart_build_flow()`:
   - Task `build_mart_<name>(spark, config)`: đọc SQL, register staging views, `spark.sql()`, write JDBC → PostgreSQL
   - TRUNCATE PostgreSQL table trước khi INSERT (trong task hoặc SQL)
5. Tích hợp vào `daily_pipeline.py`

**Relevant Context:**
- DDL của 3 mart tables trong section MART Layer của plan này
- RISK_TIER logic: `ext_source_2 < 0.3 AND bureau_max_overdue > 0 → 'High'`, `ext_source_2 > 0.5 AND bureau_bad_status_rate < 0.1 → 'Low'`, else `'Medium'`
- PostgreSQL JDBC: `jdbc:postgresql://localhost:5432/homecredit_mart`
- `write_jdbc` utility từ `spark/delta_utils.py`

**Status:** [ ] pending

---

### Sub-Task 6 — Great Expectations Integration

**Intent:** Setup GE project, tạo expectation suites cho Bronze + Staging + Mart, tích hợp GE checkpoints vào Prefect flows.

**Expected Outcomes:**
- `great_expectations/` project initialized
- 3 expectation suite JSON files đúng theo spec trong plan
- GE checkpoint chạy được sau mỗi flow
- Kết quả validation lưu tại `log/ge_validations/`
- Prefect flow fail nếu GE validation fail

**Todo List:**
1. Init GE project: tạo `great_expectations/great_expectations.yml` với Spark + Filesystem datasource
2. Tạo `great_expectations/expectations/bronze_application_suite.json` (5 expectations)
3. Tạo `great_expectations/expectations/stg_application_suite.json` (5 expectations)
4. Tạo `great_expectations/expectations/mart_risk_model_features_suite.json` (4 expectations)
5. Tạo 3 checkpoint YAML files (bronze, staging, mart)
6. Tạo utility `scripts/run_ge_checkpoint.py` — nhận checkpoint name, chạy GE, return pass/fail
7. Thêm GE task cuối mỗi Prefect flow: `run_ge_bronze_checkpoint`, `run_ge_staging_checkpoint`, `run_ge_mart_checkpoint`
8. Config GE validation results output → `log/ge_validations/`

**Relevant Context:**
- GE expectations spec trong section "Data Quality với Great Expectations" của plan này
- GE với Spark backend: dùng `SparkDFDataset` hoặc GE v1 Spark datasource
- Prefect task phải `raise` nếu GE trả về `success=False`

**Status:** [ ] pending

---

### Sub-Task 7 — Master Flow Refactor + Scheduling + End-to-End Test

**Intent:** Hoàn thiện `daily_pipeline.py` thành master orchestration flow, wire tất cả sub-flows, thêm CronSchedule, test chạy end-to-end.

**Expected Outcomes:**
- `python prefect/flow/daily_pipeline.py` chạy full pipeline từ đầu đến cuối
- Prefect UI hiển thị flow run với 3 sub-flows và tasks
- CronSchedule `0 1 * * *` được register
- Log cuối mỗi run: row counts mỗi tầng, GE pass/fail status
- Toàn bộ pipeline là idempotent — chạy 2 lần cho kết quả giống nhau

**Todo List:**
1. Refactor `daily_pipeline.py`: remove old task logic, import và chain 3 sub-flows theo đúng thứ tự
2. Thêm `CronSchedule("0 1 * * *")` vào master flow definition
3. Thêm flow-level summary log: tổng rows per tầng, GE status, thời gian chạy
4. Test idempotency: chạy 2 lần liên tiếp → kiểm tra row count không đổi
5. Kiểm tra `scripts/check_downstream_impact.py` hoạt động với cấu trúc mới
6. Verify 3 PostgreSQL mart tables có data và schema đúng sau khi chạy

**Relevant Context:**
- `daily_pipeline.py` hiện tại có old task logic (`load_raw_data`, `validate_data`, `build_curated_dataset`, `save_curated_dataset`) — cần migrate validate_data logic sang GE Sub-Task 6
- Prefect 2.x scheduling: `flow.serve(schedule=CronSchedule(...))` hoặc `flow.deploy(...)`
- Sub-flows được gọi như regular Python functions bên trong master flow

**Status:** [ ] pending

---

## Thứ tự thực hiện

```
Sub-Task 1 (Docker + Infrastructure + Config)
    → Sub-Task 2 (Spark Session + Delta Utilities)
        → Sub-Task 3 (Bronze Flow: CSV → Delta Lake)
            → Sub-Task 4 (Staging Flow: Spark SQL → Delta Lake)
                → Sub-Task 5 (Mart Flow: Spark SQL → PostgreSQL)
                    → Sub-Task 6 (Great Expectations Integration)
                        → Sub-Task 7 (Master Flow + E2E Test)
```

---

## Dependencies (requirements.txt additions)

```
pyspark==3.5.0
delta-spark==3.0.0
great-expectations==0.18.x
prefect>=2.14.0
psycopg2-binary
boto3
sqlalchemy
pandas
pyarrow
python-dotenv
```
