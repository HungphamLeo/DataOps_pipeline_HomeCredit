
# Session Handoff: Full Refactor Summary — Config, Spark, Bronze/Silver

**Date:** 2026-09-23 (final session)
**Branch:** `23092026_hung_debug_prefect_and_config`
**Scope:** Tổng kết toàn bộ refactor session này từ config centralization đến bronze/silver simplification.

---

## 1. Tổng quan các thay đổi

### 1.1 Files đã tạo mới
| File | Mục đích |
|---|---|
| `config.py` (root) | Single source of truth: load `.env`, expose path getters, load YAML |
| `.env.example` | Document tất cả env vars cần thiết |
| `export_task/session_handoff_2026-09-23_spark_refactor.md` | Spark integration fixes |
| `export_task/session_handoff_2026-09-23_bronze_silver_simplify.md` | Bronze/Silver simplification |
| `export_task/session_handoff_2026-09-23_knowledge_map.md` | Mapping kiến thức allskill → code |

### 1.2 Files đã refactor
| File | Thay đổi chính |
|---|---|
| `cli/flows/serving/metadata.py` | Xóa `PROJECT_ROOT`, `_expand()`, tự mở YAML — dùng `config.py` |
| `cli/flows/serving/bronze_flow.py` | 1 hàm per source file, Delta format, không dùng `@task` |
| `cli/flows/serving/silver_flow.py` | 1 hàm per target table, Spark SQL từ `.sql` files |
| `cli/flows/serving/daily_pipeline.py` | Xóa `BASE_DIR`, dùng `PrefectETLPipelineConfig` |
| `cli/main.py` | Dùng `get_logger_config_path()`, bỏ path hardcode |
| `log/config/logger_setup.py` | Dùng `get_logger_config_path()`, fallback console-only |
| `platforms/prefect_orchestra/prefect_main.py` | Fix `logging` import, `ConfigLoader` dùng `config.py` |
| `platforms/processing/spark_stack/spark_session.py` | Fix stale docstring |
| `platforms/config/stack.yml` | Thêm `sql_extensions` key rõ ràng |

### 1.3 SQL files viết lại
`cli/flows/sql/staging/` — 10 files từ DDL (CREATE TABLE) → SELECT transformation queries.

---

## 2. Kiến trúc pipeline hiện tại

```
.env
  └─► config.py (get_*_path, load_pipeline_config, load_stack_config)
        │
        ├─► bronze_flow.py (@flow)
        │     ├─ load_application()
        │     ├─ load_bureau()
        │     ├─ load_bureau_balance()
        │     ├─ load_previous_application()
        │     ├─ load_installments_payments()
        │     ├─ load_pos_cash_balance()
        │     └─ load_credit_card_balance()
        │           └─► MinIO s3a://lakehouse/bronze/<table>/ (Delta format)
        │
        └─► silver_flow.py (@flow)
              ├─ build_dim_date()         → dim_date.sql
              ├─ build_dim_application_status() → dim_application_status.sql
              ├─ build_dim_contract_type() → dim_contract_type.sql
              ├─ build_dim_customer()     → dim_customer.sql
              ├─ build_fact_loan_repayment()    → fact_loan_repayment.sql
              ├─ build_fact_loan_application()  → fact_loan_application.sql
              ├─ build_fact_bureau_credit()     → fact_bureau_credit.sql
              ├─ build_fact_bureau_monthly_snapshot() → ...sql
              ├─ build_fact_credit_balance()    → fact_credit_balance.sql
              └─ build_fact_pos_cash_balance()  → fact_pos_cash_balance.sql
                    └─► PostgreSQL schema stg (auto-created by Spark JDBC)
```

---

## 3. Remaining concerns

1. **FK surrogate key lookup chưa implement** — `Contract_Type_SK`, `Status_SK` trong fact tables dùng placeholder `-1`
2. **SCD Type 2 cho Dim_Customer** — hiện là full refresh, Is_Current_Flag luôn 'Y'
3. **anyio pin** — `requirements.txt` cần `anyio>=3.7.1,<4`
4. **Docker rebuild** — chưa test end-to-end trong container
5. **Bronze Delta migration** — nếu có Parquet cũ cần rerun Bronze trước Silver

---

## 4. Chạy pipeline

```bash
# Từ project root, chạy trực tiếp
python -m cli.main

# Hoặc qua Docker Compose
docker compose -f infra/docker-compose.yml up pipeline
```

**Không cần tạo bảng thủ công** — Spark JDBC `mode="overwrite"` tự DROP+CREATE table khi write.
