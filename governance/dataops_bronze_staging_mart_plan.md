
# DataOps Pipeline Plan — Home Credit Risk (Bronze → Staging → Mart)

## Top-Level Overview

**Goal:** Thiết kế và xây dựng một DataOps pipeline hoàn chỉnh cho bài toán Credit Risk Modelling và Reporting của Home Credit, theo kiến trúc 3 tầng: **Bronze → Staging → Mart**, với Prefect orchestration, Great Expectations data quality, và lưu trữ dưới dạng Parquet/CSV có phân vùng.

**Scope:**
- 7 nguồn CSV raw → Bronze (lưu trữ nguyên bản)
- Bronze → Staging (chuẩn hóa, join, aggregate, tạo features)
- Staging → Mart (wide table phục vụ model & BI report)
- Data Quality với Great Expectations ở mỗi tầng
- Prefect flows lập lịch và orchestrate toàn bộ pipeline

**Non-goals:**
- Không training model ML trong pipeline (model chạy riêng trong `scripts/`)
- Không deploy model serving
- Không migrate sang database engine (DuckDB/Postgres) trong giai đoạn này — lưu Parquet

**Sources:** `sql/dev/source_data/*.csv` (7 files)  
**Orchestration:** `prefect/flow/` (mở rộng từ `daily_pipeline.py` hiện có)  
**Output:** `data/bronze/`, `data/staging/`, `data/mart/`

## Kiến trúc dữ liệu tổng quan

---
```
[Raw CSV Sources]
       |
       v
[BRONZE Layer]          -- Raw copy, typed, partitioned by load_date
       |
       v
[STAGING Layer]         -- Cleaned, joined, aggregated features per SK_ID_CURR
       |
       v
[MART Layer]            -- Wide analytical tables for modeling + reporting
```
---

## Chi tiết thiết kế từng tầng và bảng

### BRONZE Layer — `data/bronze/`

**Nguyên tắc:**
- Lưu nguyên bản dữ liệu từ source, không transform business logic.
- Thêm metadata columns: `_load_ts` (timestamp nạp), `_source_file` (tên file gốc).
- Lưu dạng Parquet, phân vùng theo `_load_date=YYYY-MM-DD/`.
- Không xóa dữ liệu cũ — append-only để tái xử lý được.

| Bảng Bronze | Source File | Grain | Columns thêm |
|---|---|---|---|
| `bronze_application` | HC_application_train.csv | 1 dòng = 1 đơn vay | `_load_ts`, `_source_file` |
| `bronze_bureau` | HC_bureau.csv | 1 dòng = 1 credit bureau record | `_load_ts`, `_source_file` |
| `bronze_bureau_balance` | HC_bureau_balance.csv | 1 dòng = 1 tháng 1 bureau | `_load_ts`, `_source_file` |
| `bronze_previous_application` | HC_previous_application.csv | 1 dòng = 1 đơn vay trước | `_load_ts`, `_source_file` |
| `bronze_installments_payments` | HC_installments_payments.csv | 1 dòng = 1 kỳ thanh toán | `_load_ts`, `_source_file` |
| `bronze_pos_cash_balance` | HC_POS_CASH_balance.csv | 1 dòng = 1 tháng POS/CASH | `_load_ts`, `_source_file` |
| `bronze_credit_card_balance` | HC_credit_card_balance.csv | 1 dòng = 1 tháng thẻ tín dụng | `_load_ts`, `_source_file` |

### STAGING Layer — `data/staging/`

**Nguyên tắc:**
- Làm sạch, chuẩn hóa kiểu dữ liệu, xử lý null.
- Tạo derived features từ các cột raw.
- Aggregate các bảng lịch sử (bureau, installments, POS, credit_card) về grain `SK_ID_CURR`.
- Mỗi bảng staging tương ứng 1 domain logic.

| Bảng Staging | Grain | Nguồn Bronze | Mô tả |
|---|---|---|---|
| `stg_application` | SK_ID_CURR | bronze_application | Clean + derived features: AGE_YEARS, EMPLOYED_YEARS, INCOME_CREDIT_RATIO, ANNUITY_INCOME_RATIO |
| `stg_bureau_summary` | SK_ID_CURR | bronze_bureau + bronze_bureau_balance | Aggregate: số lượng credits, tổng nợ, max overdue, % active, avg DPD từ bureau_balance |
| `stg_prev_application_summary` | SK_ID_CURR | bronze_previous_application | Aggregate: số lần xin vay trước, tỷ lệ approve/reject, avg credit amount trước |
| `stg_installment_summary` | SK_ID_CURR | bronze_installments_payments | Aggregate: % thanh toán đúng hạn, avg payment delay days, tổng thiếu/thừa tiền |
| `stg_pos_cash_summary` | SK_ID_CURR | bronze_pos_cash_balance | Aggregate: avg DPD, max DPD, tổng tháng active, % tháng overdue |
| `stg_credit_card_summary` | SK_ID_CURR | bronze_credit_card_balance | Aggregate: avg balance, avg utilization rate, max balance, avg payment ratio |

**Key derived features tại Staging:**

`stg_application`:
- `AGE_YEARS` = abs(DAYS_BIRTH) / 365
- `EMPLOYED_YEARS` = abs(DAYS_EMPLOYED) / 365 (clip nếu > 50 năm — outlier mã hoá)
- `INCOME_CREDIT_RATIO` = AMT_INCOME_TOTAL / AMT_CREDIT
- `ANNUITY_INCOME_RATIO` = AMT_ANNUITY / AMT_INCOME_TOTAL
- `CREDIT_GOODS_RATIO` = AMT_CREDIT / AMT_GOODS_PRICE
- `DAYS_EMPLOYED_ANOMALY` = flag (DAYS_EMPLOYED == 365243 → mã hoá "unemployed")

`stg_bureau_summary`:
- `BUREAU_LOAN_COUNT` = count(SK_ID_BUREAU) per SK_ID_CURR
- `BUREAU_ACTIVE_COUNT` = count where CREDIT_ACTIVE = 'Active'
- `BUREAU_CLOSED_COUNT` = count where CREDIT_ACTIVE = 'Closed'
- `BUREAU_MAX_OVERDUE` = max(AMT_CREDIT_MAX_OVERDUE)
- `BUREAU_TOTAL_DEBT` = sum(AMT_CREDIT_SUM_DEBT)
- `BUREAU_AVG_DPD` = avg DPD from bureau_balance STATUS
- `BUREAU_BAD_STATUS_RATE` = % months with STATUS in ('1','2','3','4','5')

`stg_prev_application_summary`:
- `PREV_APP_COUNT` = total prior applications
- `PREV_APPROVED_COUNT` = approved ones
- `PREV_REFUSED_COUNT` = refused ones
- `PREV_APPROVAL_RATE` = approved / total
- `PREV_AVG_CREDIT` = avg(AMT_CREDIT)
- `PREV_MAX_CREDIT` = max(AMT_CREDIT)

`stg_installment_summary`:
- `INSTALMENT_COUNT` = total records
- `INSTALMENT_LATE_COUNT` = count where DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT
- `INSTALMENT_LATE_RATE` = late / total
- `INSTALMENT_AVG_DELAY_DAYS` = avg(DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT) where late
- `INSTALMENT_AMT_SHORTFALL` = sum(AMT_INSTALMENT - AMT_PAYMENT) where underpaid

`stg_pos_cash_summary`:
- `POS_MONTHS_COUNT` = total months observed
- `POS_MAX_DPD` = max(SK_DPD)
- `POS_AVG_DPD` = avg(SK_DPD)
- `POS_OVERDUE_MONTHS` = count where SK_DPD > 0

`stg_credit_card_summary`:
- `CC_MONTHS_COUNT` = total months
- `CC_AVG_BALANCE` = avg(AMT_BALANCE)
- `CC_MAX_BALANCE` = max(AMT_BALANCE)
- `CC_AVG_UTILIZATION` = avg(AMT_BALANCE / AMT_CREDIT_LIMIT_ACTUAL)
- `CC_AVG_PAYMENT_RATIO` = avg(AMT_PAYMENT_TOTAL_CURRENT / AMT_INST_MIN_REGULARITY)

### MART Layer — `data/mart/`

**Nguyên tắc:**
- Join tất cả staging tables về 1 wide table per SK_ID_CURR.
- Phục vụ 2 mục đích: (1) ML model training và (2) BI/Reporting.
- Tách biệt mart cho model vs report để kiểm soát cột và logic.

| Bảng Mart | Grain | Mô tả |
|---|---|---|
| `mart_risk_model_features` | SK_ID_CURR | Wide table: toàn bộ features từ tất cả staging, dùng cho ML model training/scoring |
| `mart_risk_report_summary` | SK_ID_CURR | Subset các cột business-friendly cho report/dashboard |
| `mart_default_cohort` | GROUP + TARGET | Aggregate: default rate theo cohort (income_type, education, region...) phục vụ BI |

**`mart_risk_model_features` — key columns:**
- Tất cả cột từ `stg_application` (demographics + loan info + derived features)
- Tất cả aggregate từ `stg_bureau_summary`
- Tất cả aggregate từ `stg_prev_application_summary`
- Tất cả aggregate từ `stg_installment_summary`
- Tất cả aggregate từ `stg_pos_cash_summary`
- Tất cả aggregate từ `stg_credit_card_summary`
- `TARGET` = label chính

**`mart_risk_report_summary` — key columns:**
- SK_ID_CURR, TARGET
- AGE_YEARS, NAME_INCOME_TYPE, NAME_EDUCATION_TYPE, NAME_FAMILY_STATUS
- AMT_INCOME_TOTAL, AMT_CREDIT, INCOME_CREDIT_RATIO
- EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3
- BUREAU_LOAN_COUNT, BUREAU_MAX_OVERDUE, BUREAU_BAD_STATUS_RATE
- INSTALMENT_LATE_RATE, POS_MAX_DPD, CC_AVG_UTILIZATION
- `RISK_TIER` = derived bucket (High/Medium/Low) dựa trên EXT_SOURCE và overdue

**`mart_default_cohort` — key columns:**
- `COHORT_DIM` (dimension name, e.g. "income_type")
- `COHORT_VALUE` (e.g. "Working")
- `TOTAL_COUNT`, `DEFAULT_COUNT`, `DEFAULT_RATE`

## Cấu trúc thư mục đề xuất

---
```
DataOps_pipeline_HomeCredit/
├── data/
│   ├── bronze/
│   │   ├── bronze_application/
│   │   ├── bronze_bureau/
│   │   ├── bronze_bureau_balance/
│   │   ├── bronze_previous_application/
│   │   ├── bronze_installments_payments/
│   │   ├── bronze_pos_cash_balance/
│   │   └── bronze_credit_card_balance/
│   ├── staging/
│   │   ├── stg_application/
│   │   ├── stg_bureau_summary/
│   │   ├── stg_prev_application_summary/
│   │   ├── stg_installment_summary/
│   │   ├── stg_pos_cash_summary/
│   │   └── stg_credit_card_summary/
│   └── mart/
│       ├── mart_risk_model_features/
│       ├── mart_risk_report_summary/
│       └── mart_default_cohort/
├── prefect/
│   ├── config/
│   │   └── pipeline_config.yaml       -- paths, schedule, GE config
│   └── flow/
│       ├── daily_pipeline.py          -- existing (to be refactored)
│       ├── bronze_flow.py             -- NEW: ingest raw → bronze
│       ├── staging_flow.py            -- NEW: bronze → staging
│       └── mart_flow.py               -- NEW: staging → mart
├── great_expectations/
│   ├── expectations/
│   │   ├── bronze_application.json
│   │   ├── stg_application.json
│   │   └── mart_risk_model_features.json
│   └── checkpoints/
│       ├── bronze_checkpoint.yaml
│       ├── staging_checkpoint.yaml
│       └── mart_checkpoint.yaml
├── scripts/                           -- unchanged EDA + model notebooks
└── analyze_governance/
    └── dataops_bronze_staging_mart_plan.md  -- this file
```
---

## Data Quality với Great Expectations

**Chiến lược:** Mỗi tầng có 1 checkpoint GE riêng, chạy sau khi tầng đó hoàn thành.

### Bronze expectations:
- `expect_column_to_exist`: SK_ID_CURR, TARGET (application)
- `expect_column_values_to_not_be_null`: SK_ID_CURR (all tables)
- `expect_column_values_to_be_unique`: SK_ID_CURR (bronze_application only)
- `expect_column_values_to_be_in_set`: TARGET → [0, 1]
- `expect_table_row_count_to_be_between`: min rows threshold

### Staging expectations:
- `expect_column_values_to_not_be_null`: tất cả key derived features
- `expect_column_values_to_be_between`: AGE_YEARS (18, 100), INCOME_CREDIT_RATIO (0, 50)
- `expect_column_values_to_be_between`: BUREAU_BAD_STATUS_RATE (0, 1)
- `expect_column_pair_values_A_to_be_greater_than_B`: tổng_count > 0 cho summary tables

### Mart expectations:
- `expect_table_row_count_to_equal`: mart row count = stg_application row count
- `expect_column_proportion_of_unique_values_to_be_between`: SK_ID_CURR → 1.0
- `expect_column_values_to_be_between`: default rate trong mart_default_cohort (0, 1)

## Prefect Orchestration

### Flow structure:
---
```
master_pipeline_flow
├── bronze_ingest_flow       (load 7 CSVs → bronze Parquet)
│   └── GE bronze_checkpoint
├── staging_transform_flow   (bronze → 6 staging tables)
│   └── GE staging_checkpoint
└── mart_build_flow          (staging → 3 mart tables)
    └── GE mart_checkpoint
```
---

### Lập lịch:
- **Daily run:** 01:00 AM mỗi ngày (batch reprocess toàn bộ data)
- Dùng Prefect `CronSchedule("0 1 * * *")`
- `prefect/config/pipeline_config.yaml` chứa settings: paths, thresholds GE, schedule

### Error handling:
- Mỗi task có retry = 2, retry_delay = 30s
- Nếu GE checkpoint fail → flow raise exception, không tiếp tục tầng tiếp theo
- Log output lưu vào `log/` (đã tồn tại trong project)

## Sub-Tasks

### Sub-Task 1 — Cấu trúc thư mục và config

**Intent:** Tạo toàn bộ thư mục data layer và file config pipeline. Đây là foundation để các sub-task sau có nơi write output.

**Expected Outcomes:**
- Tồn tại `data/bronze/`, `data/staging/`, `data/mart/` với subfolders
- `prefect/config/pipeline_config.yaml` chứa paths, schedule, GE settings
- `great_expectations/` structure khởi tạo

**Todo List:**
1. Tạo thư mục `data/bronze/` với 7 subfolders tương ứng 7 nguồn
2. Tạo thư mục `data/staging/` với 6 subfolders
3. Tạo thư mục `data/mart/` với 3 subfolders
4. Tạo `prefect/config/pipeline_config.yaml` với config paths, schedule, retry settings
5. Tạo `great_expectations/expectations/` và `great_expectations/checkpoints/` folders
6. Thêm `.gitkeep` vào các thư mục data để git track

**Relevant Context:**
- Cấu trúc thư mục định nghĩa trong section "Cấu trúc thư mục đề xuất" bên trên
- `prefect/config/` hiện tại đang trống

**Status:** [ ] pending

### Sub-Task 2 — Bronze Flow: Ingest Raw CSV → Bronze Parquet

**Intent:** Tạo `prefect/flow/bronze_flow.py` — đọc 7 file CSV source, thêm metadata columns, lưu Parquet vào `data/bronze/`. Refactor `daily_pipeline.py` để gọi flow này.

**Expected Outcomes:**
- File `prefect/flow/bronze_flow.py` chạy được standalone
- Output: 7 Parquet files trong `data/bronze/<table_name>/` với columns `_load_ts`, `_source_file`
- `daily_pipeline.py` refactored thành master flow gọi sub-flows

**Todo List:**
1. Tạo `prefect/flow/bronze_flow.py` với `@flow def bronze_ingest_flow()`
2. Mỗi source CSV có 1 `@task` load_<table>() riêng, thêm `_load_ts` và `_source_file`
3. Save output dạng `data/bronze/<table>/data.parquet`
4. Refactor `daily_pipeline.py`: đổi thành `master_pipeline_flow`, import và gọi `bronze_ingest_flow`
5. Thêm retry logic (retries=2, retry_delay_seconds=30) cho mỗi task

**Relevant Context:**
- Logic load hiện tại trong `prefect/flow/daily_pipeline.py` (task `load_raw_data`)
- Source paths: `sql/dev/source_data/HC_*.csv`
- 7 source tables: application, bureau, bureau_balance, previous_application, installments_payments, pos_cash_balance, credit_card_balance

**Status:** [ ] pending

### Sub-Task 3 — Staging Flow: Bronze → Staging Aggregations

**Intent:** Tạo `prefect/flow/staging_flow.py` — đọc Bronze Parquet, transform + aggregate về grain SK_ID_CURR, lưu 6 staging Parquet tables.

**Expected Outcomes:**
- File `prefect/flow/staging_flow.py` chạy được
- Output: 6 Parquet files trong `data/staging/<table>/`
- Tất cả derived features trong section "Staging Layer" đã được tính đúng

**Todo List:**
1. Tạo `prefect/flow/staging_flow.py` với `@flow def staging_transform_flow()`
2. Task `build_stg_application`: clean + 6 derived features từ bronze_application
3. Task `build_stg_bureau_summary`: join bronze_bureau + bronze_bureau_balance, aggregate 7 features về SK_ID_CURR
4. Task `build_stg_prev_application_summary`: aggregate bronze_previous_application về SK_ID_CURR (6 features)
5. Task `build_stg_installment_summary`: aggregate bronze_installments_payments về SK_ID_CURR (5 features)
6. Task `build_stg_pos_cash_summary`: aggregate bronze_pos_cash_balance về SK_ID_CURR (4 features)
7. Task `build_stg_credit_card_summary`: aggregate bronze_credit_card_balance về SK_ID_CURR (5 features)
8. Save mỗi table dạng Parquet vào `data/staging/<table>/`
9. Tích hợp vào `master_pipeline_flow` trong `daily_pipeline.py`

**Relevant Context:**
- Logic build_curated_dataset hiện tại trong `prefect/flow/daily_pipeline.py` là seed cho stg_application
- Tất cả derived features được define trong section "STAGING Layer" của plan này
- Lưu ý: DAYS_EMPLOYED == 365243 là mã hoá "unemployed" → cần flag riêng

**Status:** [ ] pending

### Sub-Task 4 — Mart Flow: Staging → Mart Wide Tables

**Intent:** Tạo `prefect/flow/mart_flow.py` — join 6 staging tables thành 3 mart tables phục vụ ML và reporting.

**Expected Outcomes:**
- File `prefect/flow/mart_flow.py` chạy được
- `data/mart/mart_risk_model_features/` — wide table đủ features cho model
- `data/mart/mart_risk_report_summary/` — subset business-friendly columns
- `data/mart/mart_default_cohort/` — aggregate default rate theo 5+ cohort dimensions

**Todo List:**
1. Tạo `prefect/flow/mart_flow.py` với `@flow def mart_build_flow()`
2. Task `build_mart_risk_model_features`: LEFT JOIN stg_application + 5 staging summaries trên SK_ID_CURR
3. Task `build_mart_risk_report_summary`: select subset columns + tính RISK_TIER bucket từ EXT_SOURCE và overdue
4. Task `build_mart_default_cohort`: pivot/melt theo các dimension: NAME_INCOME_TYPE, NAME_EDUCATION_TYPE, NAME_FAMILY_STATUS, NAME_HOUSING_TYPE, REGION_RATING_CLIENT → tính DEFAULT_RATE
5. Save 3 mart tables dạng Parquet
6. Tích hợp vào `master_pipeline_flow`

**Relevant Context:**
- Join key: SK_ID_CURR (có trong tất cả staging tables)
- RISK_TIER logic: EXT_SOURCE_2 < 0.3 AND BUREAU_MAX_OVERDUE > 0 → "High", EXT_SOURCE_2 > 0.5 AND BUREAU_BAD_STATUS_RATE < 0.1 → "Low", còn lại → "Medium"
- mart_risk_model_features sẽ là input cho `scripts/Model ML_*.ipynb`

**Status:** [ ] pending

### Sub-Task 5 — Great Expectations Data Quality Setup

**Intent:** Thiết lập Great Expectations expectations và checkpoints cho cả 3 tầng. GE được gọi từ mỗi flow sau khi write data.

**Expected Outcomes:**
- `great_expectations/` initialized với 3 expectation suites
- Mỗi tầng có 1 checkpoint YAML
- Mỗi Prefect flow gọi GE checkpoint sau khi write, fail nếu quality check fail
- GE validation results lưu vào `log/ge_validations/`

**Todo List:**
1. Init Great Expectations project: `great_expectations init` (hoặc tạo manual structure nếu không có GE CLI)
2. Tạo `great_expectations/expectations/bronze_application.json` với 5 expectations (định nghĩa trong section Data Quality bên trên)
3. Tạo `great_expectations/expectations/stg_application.json` với range checks
4. Tạo `great_expectations/expectations/mart_risk_model_features.json` với completeness checks
5. Tạo 3 checkpoint YAML files tương ứng
6. Tạo utility function `scripts/run_ge_checkpoint.py` (hoặc inline trong flow) gọi GE validation
7. Tích hợp GE call vào cuối mỗi Prefect flow (bronze_flow, staging_flow, mart_flow)
8. Cấu hình save validation results tới `log/ge_validations/`

**Relevant Context:**
- GE expectations chi tiết trong section "Data Quality với Great Expectations" của plan này
- `log/` thư mục đã tồn tại trong project
- Prefect flow nên catch GE ValidationError và fail task nếu xảy ra

**Status:** [ ] pending

### Sub-Task 6 — Master Flow + Scheduling

**Intent:** Hoàn thiện `daily_pipeline.py` thành master orchestration flow, thêm Prefect CronSchedule, và tạo config YAML cho toàn bộ pipeline.

**Expected Outcomes:**
- `prefect/flow/daily_pipeline.py` là entry point duy nhất để chạy toàn bộ pipeline
- Pipeline chạy được với `python prefect/flow/daily_pipeline.py`
- `prefect/config/pipeline_config.yaml` đầy đủ settings
- Schedule: `0 1 * * *` (1 AM daily)

**Todo List:**
1. Refactor `daily_pipeline.py`: import và chain `bronze_ingest_flow → staging_transform_flow → mart_build_flow`
2. Thêm `CronSchedule` với cron `"0 1 * * *"` vào master flow
3. Tạo `prefect/config/pipeline_config.yaml`: source paths, output paths, retry settings, GE config paths, schedule
4. Master flow đọc config từ YAML thay vì hard-code paths
5. Thêm flow-level logging: log start/end time, row counts mỗi tầng, GE pass/fail status
6. Test chạy toàn bộ pipeline end-to-end

**Relevant Context:**
- `daily_pipeline.py` hiện tại có logic cũ cần được preserve hoặc migrate hợp lý
- `prefect/config/` hiện tại trống — đây là nơi YAML config sẽ được tạo
- Prefect 2.x API: dùng `@flow(name=..., description=...)` và `serve()` hoặc `deploy()` cho scheduling

**Status:** [ ] pending

## Thứ tự thực hiện

---
```
Sub-Task 1 (Folders + Config)
    → Sub-Task 2 (Bronze Flow)
        → Sub-Task 3 (Staging Flow)
            → Sub-Task 4 (Mart Flow)
                → Sub-Task 5 (GE Data Quality)
                    → Sub-Task 6 (Master Flow + Schedule)
```
---

Mỗi sub-task có thể được review và test độc lập trước khi tiến sang bước tiếp theo.
