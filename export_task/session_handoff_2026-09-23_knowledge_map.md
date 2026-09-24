
# Knowledge Map: Allskill → HomeCredit DataOps Pipeline

> Mỗi quyết định thiết kế trong repo đều có gốc rễ từ kiến thức sách.
> Document này giải thích **tại sao code viết như vậy** dựa trên lý thuyết tương ứng.

---

## 1. Data Engineering Lifecycle → Kiến trúc Pipeline
**Nguồn:** `skill-11` — Fundamentals of Data Engineering (Joe Reis)

### 5 Stages → mapping vào repo:

| Stage | Code trong repo | Chi tiết |
|---|---|---|
| Generation | `cli/ingestion/source_data/*.csv` | 7 file CSV từ Home Credit |
| Storage | MinIO (S3A) + PostgreSQL | Object storage Bronze + RDBMS Staging |
| Ingestion | `bronze_flow.py` | CSV → Delta Lake trên MinIO |
| Transformation | `silver_flow.py` + SQL files | Bronze → stg schema |
| Serving | PostgreSQL schema `stg` | BI/Analytics query vào đây |

### 6 Undercurrents → cách tổ chức code:

| Undercurrent | Triển khai trong repo |
|---|---|
| Security | Credentials qua `.env`, không hardcode |
| Data Management | `Design modeling_doc.csv` là data contract |
| DataOps | Prefect orchestration, structured logging, idempotent writes |
| Data Architecture | Bronze/Silver/Gold Medallion layering |
| Orchestration | `daily_pipeline.py` → `@flow` Prefect |
| Software Engineering | `config.py` single source of truth, 1 function per table |

---

## 2. Kimball Dimensional Modeling → Design modeling_doc.csv
**Nguồn:** `skill-05` — Kimball Data Warehouse Toolkit

### 4-Step Process áp dụng:
```
1. Business Process → PRC_01 (Loan Application), PRC_02 (Repayment)...
2. Declare Grain    → "1 row = 1 installment payment" (Fact_Loan_Repayment)
3. Identify Dims    → Dim_Date, Dim_Customer, Dim_Contract_Type, Dim_Application_Status
4. Identify Facts   → AMT_INSTALMENT, AMT_PAYMENT, Days_Past_Due...
```

### Fact Table Types trong repo:
| Kimball Type | Bảng | Lý do |
|---|---|---|
| Transaction Fact | Fact_Loan_Repayment, Fact_Loan_Application | 1 row = 1 sự kiện rời rạc |
| Periodic Snapshot | Fact_Credit_Balance, Fact_POS_CASH_balance, Fact_Bureau_Monthly_Snapshot | Chụp trạng thái cuối tháng |

### SCD trong Dim_Customer:
```sql
-- SCD Type 2: Lưu lịch sử thay đổi customer
Effective_Date  TIMESTAMP  -- khi bản ghi bắt đầu hiệu lực
Expiry_Date     TIMESTAMP  -- '9999-12-31' = đang active
Is_Current_Flag CHAR(1)    -- 'Y' / 'N'
```

> **Surrogate Key Rule (Kimball):** Mọi dim đều có `_SK` tách khỏi business key `_BK`. Fact join qua `_SK` — không join theo natural key.

---

## 3. Spark Definitive Guide → bronze_flow.py & silver_flow.py
**Nguồn:** `skill-02` — Spark: The Definitive Guide

### Concept 1: SparkSession không serializable
**Bug cũ:**
```python
@task
def ingest_source_to_bronze(source_path, config, stack):
    spark = SparkSession.getActiveSession()  # Không guaranteed có session!
```
**Fix:**
```python
def load_application(spark: SparkSession, config, stack, load_date):
    ...  # spark truyền trực tiếp từ @flow

@flow
def bronze_ingest_flow():
    spark = get_spark_session(stack)  # 1 lần duy nhất
    load_application(spark, config, stack, load_date)
```

### Concept 2: Lazy Evaluation
Khi gọi `.read.csv()`, `.withColumn()`, `.select()` — Spark **chỉ build DAG, chưa đọc data**. Data chỉ được đọc khi gọi Action (`.write`, `.count()`).

```python
df = spark.read.csv(str(source))          # Transformation — chưa có gì
df = add_bronze_metadata(df, table_name)  # Transformation — chưa có gì
write_delta_partitioned(df, path)         # ACTION → trigger execution
```

### Concept 3: Wide vs Narrow Transformation
| Type | Trong repo | Tác động |
|---|---|---|
| Narrow (no shuffle) | `.withColumn()`, `CAST()`, `COALESCE()` | Nhanh |
| Wide (shuffle) | `DENSE_RANK() OVER (ORDER BY ...)` | Chậm hơn — shuffle |
| Join | LEFT JOIN trong fact_loan_repayment.sql | Wide — bottleneck |

### Concept 4: Temp Views cho Spark SQL
```python
# silver_flow.py pattern
spark.read.format("delta").load(bronze_path) \
    .createOrReplaceTempView("bronze_application")  # Register view

df = spark.sql(open("dim_customer.sql").read())     # Catalyst optimizes SQL

df.write.jdbc(url=..., table="stg.Dim_Customer", mode="overwrite")
```

---

## 4. DDIA (Kleppmann) → Storage Design
**Nguồn:** `skill-04` — Designing Data-Intensive Applications

### Delta Lake = LSM-Tree based ACID:
Delta Lake dùng Transaction Log (append-only WAL) + Parquet files — tương tự LSM-Tree pattern.

```
lakehouse/bronze/application/
  ├── _delta_log/
  │     ├── 00000000000000000000.json  ← WAL entry (commit log)
  └── _load_date=2026-09-23/
        └── part-00000.parquet
```

### OLTP vs OLAP — tại sao có 2 storage?
| | MinIO Bronze (Delta) | PostgreSQL stg |
|---|---|---|
| Paradigm | OLAP / Object Storage | OLTP / Row-oriented |
| Optimized for | Column scan, batch write | Row lookup, ad-hoc query |
| Write | Append + partition overwrite | DROP + CREATE (overwrite mode) |

### Idempotency — rerun-safe:
```python
df.write.format("delta")
  .mode("overwrite")
  .option("partitionOverwriteMode", "dynamic")  # exactly-once per partition
  .partitionBy("_load_date")
  .save(path)
# Chạy lại cùng ngày → chỉ overwrite partition đó, ngày khác không bị ảnh hưởng
```

---

## 5. Config Pattern → 12-Factor App

### Vấn đề gốc rễ của `parents[n]`:
`parents[3]` phụ thuộc vào số tầng thư mục. Trong Docker, khi module được import khác cách, depth thay đổi → `FileNotFoundError`.

### Fix — 12-Factor App Principle III (Config via Environment):
```python
# config.py — anchor theo vị trí file này (ở root repo)
PROJECT_ROOT = Path(__file__).resolve().parent

def get_config_path() -> Path:
    raw = os.getenv("CONFIG_PATH")          # Docker override
    if raw:
        return Path(raw)
    return PROJECT_ROOT / "cli" / "flows" / "config" / "homecredit_config.yaml"
```

---

## 6. Quick Reference: Concept → Code

| Kiến thức trong sách | Áp dụng trong repo | File |
|---|---|---|
| Medallion Architecture | bronze_flow → silver_flow → stg schema | bronze/silver_flow.py |
| SparkSession not serializable | Không dùng @task, truyền spark trực tiếp | bronze_flow.py |
| Lazy Evaluation → Actions trigger | write_delta_partitioned() là Action | delta_utils.py |
| Kimball Surrogate Key | `hash(SK_ID_BUREAU)` → `Bureau_Credit_SK` | *.sql |
| SCD Type 2 | Effective_Date, Expiry_Date, Is_Current_Flag | dim_customer.sql |
| Idempotent writes | partitionOverwriteMode=dynamic | delta_utils.py |
| 12-Factor Config via Env | config.py + .env | config.py |
| ACID via WAL (LSM-Tree) | Delta Lake _delta_log/ transaction log | stack.yml |
| OLTP vs OLAP duality | MinIO (scan) + PostgreSQL (lookup) | silver_flow.py |
| DAG-based Orchestration | Prefect @flow: dims → facts order | silver_flow.py |
| Wide Transformation = Shuffle | DENSE_RANK OVER tốn kém, chỉ dùng dim nhỏ | dim_*.sql |
| Single Responsibility Principle | 1 hàm = 1 bảng, 1 file = 1 concern | bronze_flow.py |
