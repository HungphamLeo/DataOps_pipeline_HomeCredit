# Spark Knowledge Base — HomeCredit DataOps Pipeline

> Tài liệu này tổng hợp toàn bộ kiến thức Spark đã học, debug, và áp dụng thực tế trong project.
> Cập nhật liên tục theo từng session. Ưu tiên đọc file này trước khi làm việc với Spark trong repo.
>
> **Merged từ:** `session_handoff_2026-09-23_spark_refactor.md` + `spark.md`
> **Cập nhật lần cuối:** 2026-09-24

---

## 1. Kiến trúc Spark trong project này

```
local[*] mode (single-node Docker)
│
├── Driver JVM  ← duy nhất, chạy trong container
│     ├── SparkContext
│     ├── DAGScheduler
│     ├── TaskScheduler
│     └── Executor (chạy chung process với Driver trong local mode)
│
├── stack.yaml  ← cấu hình Spark + MinIO
└── spark_session.py  ← factory, tạo session 1 lần duy nhất per pipeline run
```

**Quan trọng — local[*] mode:**
- `driver_memory` = toàn bộ RAM JVM có thể dùng
- `executor_memory` bị **ignore hoàn toàn** trong local mode
- Mọi thứ — driver, executor, shuffle — đều chạy chung 1 JVM heap

---

## 2. SparkSession — Quy tắc bất biến

### Rule 1: Tạo 1 lần, truyền trực tiếp
```python
# ✅ ĐÚNG — tạo ở @flow level, truyền xuống function
@flow
def bronze_ingest_flow():
    spark = get_spark_session(stack)   # 1 lần duy nhất
    load_application(spark, config, stack, load_date)
    load_bureau(spark, config, stack, load_date)

def load_application(spark: SparkSession, config, stack, load_date):
    ...  # nhận spark từ caller, không tự tạo
```

```python
# ❌ SAI — getActiveSession() trong @task không reliable
@task
def ingest_source_to_bronze(source_path, config, stack):
    spark = SparkSession.getActiveSession()  # Không đảm bảo có session!
```

### Rule 2: SparkSession không serializable
- Không được pass SparkSession qua Prefect `@task` boundary
- Không được pickle SparkSession (multiprocessing, celery...)
- Lý do: SparkSession giữ JVM reference, không thể serialize sang process khác

### Rule 3: Stop trong finally
```python
spark = None
try:
    spark = get_spark_session(stack)
    ...
finally:
    if spark is not None:
        spark.stop()   # giải phóng JVM resources
```

---

## 3. Lazy Evaluation — hiểu đúng để debug

Spark **không làm gì** khi gọi Transformation. Chỉ build DAG.
Data thực sự được đọc/xử lý khi gọi **Action**.

```python
df = spark.read.csv(str(source))           # Transformation — DAG node, chưa đọc file
df = df.withColumn("_load_ts", F.current_timestamp())  # Transformation — DAG node
df = df.withColumn("_source_file", F.lit(table_name))  # Transformation — DAG node

write_delta_partitioned(df, path)          # ACTION → Spark mới thực sự đọc CSV + write Delta
```

**Hệ quả thực tế:**
- Log `bronze_load_started` → `delta_partitioned_write_started` có thể chênh vài giây
  vì Spark đang build DAG, chưa đọc data
- Lỗi schema, lỗi file path → chỉ xuất hiện lúc **Action**, không phải lúc `.read()`
- `df.count()` sau `df.write` sẽ trigger **re-execution DAG từ đầu** → tốn gấp đôi tài nguyên

```python
# ❌ SAI — df.count() sau write → đọc lại file CSV lần 2
out = _write_bronze(df, stack, "application", load_date)
logger.info("rows=%d", df.count())   # trigger re-read CSV!

# ✅ ĐÚNG — count trước write nếu cần, hoặc bỏ qua
rows = df.count()                    # 1 action
write_delta_partitioned(df, path)    # 1 action khác (re-read)
# Hoặc: chỉ log path, không log rows để tránh double-read
```

---

## 4. Transformations — Wide vs Narrow

| Type | Ví dụ | Tác động | Trong repo |
|---|---|---|---|
| **Narrow** (no shuffle) | `.withColumn()`, `CAST()`, `COALESCE()`, `filter()` | Nhanh, no network | `add_bronze_metadata()` |
| **Wide** (shuffle) | `groupBy()`, `distinct()`, `DENSE_RANK() OVER` | Chậm hơn, shuffle data | `dim_application_status.sql` |
| **Join** | `LEFT JOIN` | Wide nếu large table | `fact_loan_repayment.sql` |

**Shuffle partition tuning:**
```yaml
# stack.yaml
spark:
  shuffle_partitions: 8   # default Spark = 200, quá nhiều cho local[*]
                           # 8 phù hợp với dataset ~60M rows trên single node
```

---

## 5. Memory Management — Tránh OOM

### Anatomy of Spark memory (local mode)

```
JVM Heap (driver_memory = 3g)
├── Spark Memory (spark.memory.fraction = 0.6 → 1.8g)
│     ├── Execution Memory  (1 - storageFraction = 0.7 → 1.26g)
│     │     └── shuffle, join, sort, aggregation
│     └── Storage Memory   (storageFraction = 0.3 → 0.54g)
│           └── cache, broadcast
└── User Memory (0.4 → 1.2g)
      └── Python UDFs, user data structures, overhead
```

### Config chống OOM đã apply

```yaml
# platforms/config/stack.yaml
spark:
  driver_memory: ${SPARK_DRIVER_MEMORY:-3g}
  shuffle_partitions: ${SPARK_SHUFFLE_PARTITIONS:-8}
  max_records_per_file: ${SPARK_MAX_RECORDS_PER_FILE:-500000}
```

```python
# platforms/processing/spark_stack/spark_session.py
.config("spark.sql.shuffle.partitions", shuffle_partitions)
.config("spark.sql.files.maxRecordsPerFile", max_records)
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.memory.fraction", "0.6")
.config("spark.memory.storageFraction", "0.3")
```

### Tại sao KHÔNG tự tách batch bằng Python

```python
# ❌ ĐỪNG làm cái này — anti-pattern
def load_large_table(spark, source, config, stack, load_date):
    size_gb = os.path.getsize(source) / 1e9
    if size_gb > 2:
        for chunk in read_in_chunks(source):  # loop Python
            write_delta_partitioned(chunk, path)
```

**Lý do:**
1. Spark đã chia data thành partition ngay khi `.read.csv()` — không load toàn bộ vào RAM
2. Tự loop Python = nhiều Spark job nhỏ, mỗi job có JVM startup overhead
3. Không rerun-safe — nếu chunk 3/5 fail, không biết đã write đến đâu
4. `maxRecordsPerFile` trong Spark làm đúng việc này ở write level

**Cách đúng:** Để Spark tự chia partition, tune `shuffle_partitions` + `maxRecordsPerFile`.

---

## 6. Delta Lake Write Patterns

### Bronze — Partition Overwrite (idempotent)

```python
df.write.format("delta")
  .mode("overwrite")
  .option("partitionOverwriteMode", "dynamic")  # chỉ overwrite partition _load_date hiện tại
  .partitionBy("_load_date")
  .save(path)
```

**Behavior:** Chạy lại cùng ngày → chỉ overwrite partition `_load_date=2026-09-24`.
Các ngày khác không bị ảnh hưởng. Đây là **exactly-once per partition**.

### Silver → PostgreSQL (JDBC)

```python
df.write.jdbc(
    url="jdbc:postgresql://host:port/db",
    table="stg.Dim_Customer",
    mode="overwrite",   # DROP + CREATE table
    properties={...}
)
```

**mode="overwrite":** Spark tự DROP và CREATE lại table → không cần tạo schema thủ công.

---

## 7. Spark SQL + Temp Views Pattern

```python
# Đăng ký Bronze data như 1 temp view trong Spark session
spark.read.format("delta").load(bronze_path) \
    .createOrReplaceTempView("bronze_application")

# Chạy SQL file — Catalyst optimizer xử lý
df = spark.sql(open("sql/staging/dim_customer.sql").read())

# Write kết quả
df.write.jdbc(url=..., table="stg.Dim_Customer", mode="overwrite")
```

**Lưu ý:** Temp view chỉ tồn tại trong Spark session hiện tại.
Nếu session bị stop/restart → cần register lại tất cả views.

---

## 8. Adaptive Query Execution (AQE)

Đã bật trong project:
```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**AQE tự động:**
- Merge nhiều partition nhỏ thành ít partition lớn hơn sau shuffle (tránh small file problem)
- Chuyển Sort-Merge Join sang Broadcast Join nếu 1 bên đủ nhỏ
- Xử lý data skew tự động

---

## 9. S3A / MinIO — Các WARN thường gặp

| WARN | Nguyên nhân | Hành động |
|---|---|---|
| `MetricsConfig: Cannot locate configuration: hadoop-metrics2-s3a-file-system.properties` | Hadoop tìm config Prometheus metrics, không thấy → dùng default | **Bỏ qua** — vô hại |
| `Truncated the string representation of a plan` | Query plan quá dài, Spark cắt ngắn log | **Bỏ qua**, hoặc tăng `spark.sql.debug.maxToStringFields` |
| `WARN S3ABlockOutputStream: Application invoked the Syncable API` | Write pattern không optimal với S3 | **Bỏ qua** trong dev |

---

## 10. Docker Image — Lesson Learned

### openjdk-17 bị drop trên Debian Trixie

**Vấn đề:** Tag `python:3.11-slim-bookworm` bị Docker Hub re-point sang Trixie (Debian 13).
Debian Trixie đã drop `openjdk-17-jre-headless`, chỉ còn `openjdk-21`.

**Fix — pin version đầy đủ trong Dockerfile:**
```dockerfile
# ❌ Floating tag — có thể bị re-point
FROM python:3.11-slim-bookworm

# ✅ Pin cứng — không bao giờ bị re-point
FROM python:3.11.13-slim-bookworm
```

**Rule:** Mọi base image trong Dockerfile production **bắt buộc pin patch version**.

---

## 11. stack.yaml — Key Reference

```yaml
spark:
  app_name: homecredit-bronze-silver
  master: ${SPARK_MASTER:-local[*]}
  driver_memory: ${SPARK_DRIVER_MEMORY:-3g}       # local mode: chỉ cái này có hiệu lực
  executor_memory: ${SPARK_EXECUTOR_MEMORY:-3g}   # local mode: bị ignore, dùng khi switch cluster
  shuffle_partitions: ${SPARK_SHUFFLE_PARTITIONS:-8}
  max_records_per_file: ${SPARK_MAX_RECORDS_PER_FILE:-500000}
  sql_extensions: io.delta.sql.DeltaSparkSessionExtension
  packages:
    - io.delta:delta-spark_2.12:3.1.0
    - org.apache.hadoop:hadoop-aws:3.3.4
    - org.postgresql:postgresql:42.7.3
```

**Tuning theo môi trường:**

| Env | `SPARK_DRIVER_MEMORY` | `SPARK_SHUFFLE_PARTITIONS` | Ghi chú |
|---|---|---|---|
| Dev local (< 8g RAM) | `2g` | `8` | Để OS + Docker còn RAM |
| Dev local (>= 16g RAM) | `4g` | `16` | Thoải mái hơn |
| Production cluster | `4g` (driver) | `200` | executor_memory mới có hiệu lực |

---

## 12. Fixes đã thực hiện — Session 2026-09-23

### Fix 1: SparkSession trong @task (Bug cứng)

**Root cause:** `SparkSession.getActiveSession()` bên trong Prefect `@task` không reliable.
SparkSession không serializable — không được pass qua worker boundary (Spark Definitive Guide).

```python
# TRƯỚC — SAI
@task
def ingest_source_to_bronze(source_path: str, config, stack):
    spark = SparkSession.getActiveSession()  # Không đảm bảo có session

# SAU — ĐÚNG
def _ingest_one(spark: SparkSession, source: Path, config, stack):
    ...  # Spark session truyền trực tiếp từ @flow

@flow
def bronze_ingest_flow():
    spark = get_spark_session(stack)
    outputs = [_ingest_one(spark, path, config, stack) for path in files]
```

### Fix 2: Bronze write format Parquet → Delta

```python
# TRƯỚC
frame.write.mode("overwrite").partitionBy("_load_date").parquet(output)

# SAU — dùng helper từ delta_utils, không duplicate logic
frame = add_bronze_metadata(frame, source.name)
frame = frame.withColumn("_load_date", F.lit(load_date))
write_delta_partitioned(frame, output, partition_col="_load_date")
```

**Trade-off:** Delta format yêu cầu `delta-spark` package. Lợi ích: ACID, time travel, rerun-safe.

### Fix 3: datetime.utcnow() deprecated (Python 3.12+)

```python
# TRƯỚC
datetime.utcnow().strftime(...)

# SAU
datetime.now(timezone.utc).strftime(...)
```

### Fix 4: Lazy import trong flow body

```python
# TRƯỚC — import bên trong function body gây khó trace
def silver_flow():
    _ensure_schema(config)
    from platforms.processing.spark_stack.spark_session import get_spark_session  # lazy
    spark = get_spark_session(stack)

# SAU — top-level import
from platforms.processing.spark_stack.spark_session import get_spark_session

def silver_flow():
    spark = get_spark_session(stack)
```

### Fix 5: SequentialTaskRunner không cần thiết

Sau khi xóa `@task`, `SequentialTaskRunner` không còn ý nghĩa.
Đã xóa khỏi cả `bronze_flow.py` và `silver_flow.py`.

### Fix 6: stack.yaml — thêm sql_extensions explicit

```yaml
spark:
  sql_extensions: io.delta.sql.DeltaSparkSessionExtension
  # Thêm để spark_session.py không phải dùng fallback hardcode
```

---

## 13. Logger Convention — Spark layer

| Layer | Logger source | Lý do |
|---|---|---|
| `bronze_flow.py` | `PrefectETLPipelineConfig().bronze_logger` | Flow-level orchestration |
| `silver_flow.py` | `PrefectETLPipelineConfig().silver_logger` | Flow-level orchestration |
| `spark_session.py` | `logger_manager.get_logger(__name__)` | Infrastructure layer |
| `delta_utils.py` | `logger_manager.get_logger(__name__)` | Infrastructure layer |

**Rationale:** `PrefectETLPipelineConfig` chỉ dành cho flow layer. Infrastructure layer dùng
`logger_manager` trực tiếp để tránh circular import:
`spark_session.py → prefect_main.py → config.py → vòng lặp`.

---

## 14. Config Centralization — File Mapping

```
.env (env vars)
  └─► config.py  (get_*_path(), load_pipeline_config(), load_stack_config())
        └─► metadata.py       (re-export, load_config, load_design_metadata)
        └─► prefect_main.py   (PrefectETLPipelineConfig)
        └─► bronze_flow.py
        └─► silver_flow.py
        └─► cli/main.py
```

**Nguyên tắc đã chốt:**
- `config.py` ở root = single source of truth cho mọi path và YAML load
- Không có `parents[n]` hay hardcode path trong flow/platform code
- Mọi override qua env var trong `.env`

---

## 15. Checklist khi thêm bảng mới vào Bronze/Silver

- [ ] Bronze: thêm `load_<table>()` function trong `bronze_flow.py`
- [ ] Bronze: thêm vào `_LOADERS` list
- [ ] Silver: viết SQL file trong `cli/flows/sql/staging/<table>.sql` (SELECT, không phải CREATE TABLE)
- [ ] Silver: thêm `build_<table>()` function trong `silver_flow.py`
- [ ] Silver: đảm bảo register đúng bronze views trước khi gọi `spark.sql()`
- [ ] Silver: Dim trước → Fact sau (FK integrity)
- [ ] Validate: `python -m py_compile cli/flows/serving/bronze_flow.py`
