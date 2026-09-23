
# Session Handoff: Spark Integration Review & Config Centralization

**Date:** 2026-09-23
**Branch:** `23092026_hung_debug_prefect_and_config`
**Scope:** Review Spark usage pattern, centralize config via `.env`, fix design issues theo skill-02 (Spark Definitive Guide) + skill-11 (Fundamentals of Data Engineering).

---

## 1. Allskill routing cho session này

| Skill | File | Relevance |
|---|---|---|
| Spark Architecture & PySpark Patterns | `skill-02-spark-definitive-guide.md` | SparkSession serialization rule, Delta write strategy |
| Data Engineering Lifecycle | `skill-11-fundamentals-data-engineering.md` | Bronze/Silver/Gold layering, DataOps undercurrents |

---

## 2. Config centralization hoàn thành (từ các session trước)

### Nguyên tắc đã chốt:
- **`config.py` ở root** là single source of truth cho mọi path và YAML load.
- Tất cả path đều đến từ env var trong `.env` — không có `parents[n]` hay hardcode trong flow/platform code.
- Các getter hiện có: `get_config_path()`, `get_stack_config_path()`, `get_logger_config_path()`, `get_source_dir()`, `get_design_metadata_path()`.

### File mapping:
```
.env (env vars)
  └─► config.py  (get_*_path(), load_pipeline_config(), load_stack_config())
        └─► cli/flows/serving/metadata.py  (re-export, load_config, load_design_metadata)
        └─► platforms/prefect_orchestra/prefect_main.py  (PrefectETLPipelineConfig)
        └─► cli/flows/serving/bronze_flow.py
        └─► cli/flows/serving/silver_flow.py
        └─► cli/main.py
```

---

## 3. Spark integration fixes (session này)

### 3.1 Bug cứng: SparkSession trong @task

**Root cause:** `SparkSession.getActiveSession()` bên trong Prefect `@task` không reliable.
Theo Spark Definitive Guide: SparkSession không serializable — không được pass qua worker boundary.

**Fix:** Xóa `@task ingest_source_to_bronze`, chuyển thành plain function `_ingest_one()` chạy trong `@flow` context. Spark session tạo một lần ở `@flow` level và truyền trực tiếp.

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

### 3.2 Bronze write format: Parquet → Delta

**Root cause:** `bronze_flow.py` dùng `.write.parquet()` thuần trong khi `delta_utils.py` đã có `write_delta_partitioned()` với dynamic partition overwrite.

**Fix:** Dùng `add_bronze_metadata()` + `write_delta_partitioned()` từ `delta_utils` — không duplicate logic.

```python
# TRƯỚC
frame.write.mode("overwrite").partitionBy("_load_date").parquet(output)

# SAU
frame = add_bronze_metadata(frame, source.name)
frame = frame.withColumn("_load_date", F.lit(load_date))
write_delta_partitioned(frame, output, partition_col="_load_date")
```

**Trade-off:** Delta format yêu cầu `delta-spark` package (đã có trong `stack.yml`). Lợi ích: ACID, time travel, rerun-safe partition overwrite.

### 3.3 datetime.utcnow() deprecated

```python
# TRƯỚC
datetime.utcnow().strftime(...)

# SAU
datetime.now(timezone.utc).strftime(...)
```

### 3.4 Lazy import trong flow body

```python
# TRƯỚC (silver_flow.py)
_ensure_schema(config)
from platforms.processing.spark_stack.spark_session import get_spark_session  # lazy
spark = get_spark_session(stack)

# SAU
from platforms.processing.spark_stack.spark_session import get_spark_session  # top-level
...
spark = get_spark_session(stack)
```

### 3.5 SequentialTaskRunner không cần thiết

Sau khi xóa `@task`, `SequentialTaskRunner` không còn ý nghĩa. Đã xóa khỏi cả `bronze_flow.py` và `silver_flow.py`.

---

## 4. stack.yml — sql_extensions

Thêm explicit key để `spark_session.py` không dùng fallback hardcode:

```yaml
spark:
  sql_extensions: io.delta.sql.DeltaSparkSessionExtension
```

---

## 5. Logger layer convention (đã chốt)

| Layer | Logger source |
|---|---|
| `cli/flows/serving/bronze_flow.py` | `PrefectETLPipelineConfig().bronze_logger` |
| `cli/flows/serving/silver_flow.py` | `PrefectETLPipelineConfig().silver_logger` |
| `cli/flows/serving/daily_pipeline.py` | `PrefectETLPipelineConfig().serving_logger` |
| `platforms/processing/spark_stack/` | `logger_manager.get_logger(__name__)` (infrastructure layer) |
| `platforms/processing/config/` | `logger_manager.get_logger(__name__)` (infrastructure layer) |
| `cli/flows/serving/metadata.py` | `logger_manager.get_logger(__name__)` (utility layer) |

**Rationale:** `PrefectETLPipelineConfig` chỉ dành cho flow-level orchestration layer. Platform/infrastructure layer dùng `logger_manager` trực tiếp tránh circular import (`spark_session.py` → `prefect_main.py` → `config.py` → có thể gây vòng lặp nếu không cẩn thận).

---

## 6. Validation

```text
python -m py_compile cli/flows/serving/bronze_flow.py    ✅
python -m py_compile cli/flows/serving/silver_flow.py    ✅
python -m py_compile platforms/processing/spark_stack/spark_session.py  ✅
python -m py_compile platforms/processing/config/delta_utils.py  ✅
```

---

## 7. Remaining concerns (chưa giải quyết)

1. **anyio pin** — `requirements.txt` cần `anyio>=3.7.1,<4` để fix `GatherTaskGroup` error.
2. **Docker rebuild** — Chưa test end-to-end trong container sau tất cả các thay đổi.
3. **Bronze → Delta migration** — Nếu đã có data Parquet cũ trong MinIO, cần migrate hoặc rerun Bronze để tạo Delta log.
4. **Join metadata** — `silver_flow.py` vẫn dùng join inference đơn giản (`SK_ID_CURR` hoặc first common column). Cần explicit join contract cho multi-source targets.
5. **stack.yml path** — `config.py` default cho `STACK_CONFIG_PATH` là `platforms/config/stack.yaml` nhưng file thực là `stack.yml`. Cần đồng bộ extension (xem mục 8).

---

## 8. Action item quan trọng

`config.py` default `STACK_CONFIG_PATH` trỏ `platforms/config/stack.yaml` nhưng file thực là `platforms/config/stack.yml`. Cần thêm `STACK_CONFIG_PATH=platforms/config/stack.yml` vào `.env` hoặc đổi extension file về `.yaml`.
