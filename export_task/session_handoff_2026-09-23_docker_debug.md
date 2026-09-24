
# Session Handoff: Docker Debug — Dependency Resolution & Config File Issues

**Date:** 2026-09-23 (late session)
**Branch:** `23092026_hung_debug_prefect_and_config`
**Scope:** Debug toàn bộ chuỗi lỗi Docker container từ `ModuleNotFoundError` đến `FileNotFoundError: stack.yml`.

---

## 1. Tóm tắt chuỗi lỗi theo thứ tự xuất hiện

| # | Lỗi | Root Cause | Fix |
|---|---|---|---|
| 1 | `ModuleNotFoundError: No module named 'cli'` | `cli/__init__.py` không tồn tại | Tạo `__init__.py` cho `cli/`, `platforms/`, `log/`, `log/config/` |
| 2 | `FileNotFoundError: /app/cli/flows/config/homecredit_config.yaml` | File bị `.gitignore`, không trong build context | Xóa khỏi `.gitignore`, copy file vào `cli/flows/config/` trên Ubuntu |
| 3 | `TypeError: Can't instantiate abstract class GatherTaskGroup` | `anyio` version quá mới (4.14.x), Prefect 2.20 test với 4.4.x | Pin `anyio==4.4.0` |
| 4 | `TimeoutError` trong `asgi_lifespan` startup | `asgi-lifespan` cũ không tương thích `anyio 4.x` | Pin `asgi-lifespan==2.1.0` |
| 5 | pip `ResolutionImpossible`: `anyio<4` conflict | Prefect 2.20.0 requires `anyio>=4.4.0,<5` — pin `<4` là sai | Pin cứng `anyio==4.4.0` |
| 6 | pip `ResolutionImpossible`: `httpcore==0.16.3` conflict | `prefect 2.20` requires `httpcore>=1.0.5`, `httpx 0.23.x` requires `httpcore<0.17` | Dùng `httpx==0.25.2` + `httpcore==1.0.5` |
| 7 | `FileNotFoundError: /app/platforms/config/stack.yml` | File untracked hoặc không vào Docker image | Đang điều tra — xem mục 5 |

---

## 2. Files đã tạo mới

| File | Lý do |
|---|---|
| `cli/__init__.py` | Python cần file này để nhận `cli` là package |
| `platforms/__init__.py` | Python cần file này để nhận `platforms` là package |
| `log/__init__.py` | Python cần file này để nhận `log` là package |
| `log/config/__init__.py` | Python cần file này để nhận `log.config` là package |

**Lesson learned:** Mọi thư mục Python được import theo kiểu `from cli.flows...` đều **bắt buộc phải có `__init__.py`**. Không có file này → `ModuleNotFoundError` dù `PYTHONPATH=/app` đúng.

---

## 3. Dependency resolution — bài học từ pip conflict

### Constraint chain thực tế của `prefect 2.20.0`

```
prefect 2.20.0
  ├── anyio >= 4.4.0, < 5.0.0
  │     └── KHÔNG phải anyio 3.x (sai lầm ban đầu)
  │     └── KHÔNG phải anyio 4.14.x (GatherTaskGroup internal API thay đổi)
  │     └── Pin cứng: anyio==4.4.0  ← version prefect đã test
  │
  ├── httpcore >= 1.0.5, < 2.0.0
  │     └── httpx 0.23.x dùng httpcore < 0.17 → CONFLICT
  │     └── Cần httpx >= 0.25.0 để có httpcore >= 1.0.5
  │     └── Pin: httpx==0.25.2, httpcore==1.0.5
  │
  └── asgi-lifespan (dùng nội bộ cho ephemeral mode)
        └── Version cũ không hỗ trợ anyio 4.x → TimeoutError khi startup
        └── Pin: asgi-lifespan==2.1.0
```

### Combination đã xác nhận hoạt động

```
prefect==2.20.0
anyio==4.4.0
httpx==0.25.2
httpcore==1.0.5
asgi-lifespan==2.1.0
```

### Quy tắc cho lần sau

> **Khi upgrade `prefect` hoặc thêm package async mới:** Luôn kiểm tra dependency chain của `anyio`, `httpx`, `httpcore` trước. 3 package này liên quan chặt chẽ với nhau và với `prefect`. Không để pip tự resolve — **pin cứng** tất cả.

---

## 4. Sửa 6 issues từ code review

### Issue #1 — `anyio` pin
```diff
- anyio<5.0.0,>=4.4.0
+ anyio==4.4.0
```

### Issue #2 — `.env.example` documentation sai
```diff
- # STACK_CONFIG_PATH=platforms/config/stack.yaml
+ # STACK_CONFIG_PATH=platforms/config/stack.yml
```

### Issue #3 — FK Surrogate Key placeholder hardcode `1`
3 SQL files đã được viết lại với CTE để resolve SK thực:
- `fact_loan_application.sql` → `dim_contract` CTE + `dim_status` CTE
- `fact_pos_cash_balance.sql` → `dim_contract` CTE, JOIN `'Cash loans'`
- `fact_credit_balance.sql` → `dim_contract` CTE, JOIN `'Revolving loans'`
- `silver_flow.py` → thêm `_register_bronze_view(..., "application")` cho 2 fact builder

### Issue #4 — `Dim_Customer` `monotonically_increasing_id()` non-deterministic
```sql
-- Trước: non-deterministic, không rerun-safe
monotonically_increasing_id() AS Customer_SK

-- Sau: deterministic hash, rerun-safe + deduplicate
WITH ranked AS (
    SELECT ..., row_number() OVER (PARTITION BY SK_ID_CURR ...) AS rn
    FROM bronze_application WHERE SK_ID_CURR IS NOT NULL
)
SELECT abs(hash(Customer_BK)) AS Customer_SK, ...
FROM ranked WHERE rn = 1
```

### Issue #5 — Docstring sai `daily_pipeline.py`
```diff
- 1. Bronze: Ingest CSV thô → Parquet, phân vùng theo _load_date.
+ 1. Bronze: Ingest CSV thô → Delta Lake (MinIO), phân vùng theo _load_date.
```

### Issue #6 — `logger_setup.py` dùng `parents[2]`
```python
# Trước
filename = Path(__file__).parents[2] / filename

# Sau: ưu tiên config.PROJECT_ROOT, fallback parents[2]
try:
    from config import PROJECT_ROOT
    filename = PROJECT_ROOT / filename
except Exception:
    filename = Path(__file__).parents[2] / filename
```

---

## 5. Vấn đề còn tồn đọng — `stack.yml` không vào Docker image

### Triệu chứng
```
FileNotFoundError: Config file not found: /app/platforms/config/stack.yml
```

### Điều đã biết
- File `stack.yml` **tồn tại trên disk Ubuntu** tại `platforms/config/stack.yml`
- `.gitignore` trên Ubuntu KHÔNG ignore file này
- Dockerfile có `COPY platforms ./platforms` — trông đúng

### Cần điều tra thêm (chưa kết luận)
```bash
# Chạy 3 lệnh này trên Ubuntu để xác định root cause
ls -la ~/DataOps_pipeline_HomeCredit/platforms/config/

docker run --rm homecredit_pipeline \
  find /app/platforms -name "*.yml" -o -name "*.yaml" 2>/dev/null

cat ~/DataOps_pipeline_HomeCredit/.dockerignore
```

### Khả năng nguyên nhân (theo thứ tự xác suất)
1. **File untracked + Docker layer cache** — image được build từ lần trước khi file tồn tại, cache không bị invalidate
2. **`.dockerignore` có pattern ẩn** block `platforms/config/`
3. **File tồn tại nhưng ở path khác** trên Ubuntu (giống vấn đề `cli/config/` vs `cli/flows/config/`)

### Fix tạm thời (nếu cần chạy ngay)
```bash
# Mount trực tiếp file vào container
docker compose -f infra_homecredit/docker-compose.yml run --rm \
  -v ~/DataOps_pipeline_HomeCredit/platforms/config/stack.yml:/app/platforms/config/stack.yml:ro \
  pipeline
```

### Fix dứt điểm
```bash
# Rebuild --no-cache để force Docker copy lại toàn bộ
docker compose -f infra_homecredit/docker-compose.yml build --no-cache pipeline
docker compose -f infra_homecredit/docker-compose.yml up pipeline
```

---

## 6. Docker compose fix — syntax error `volumes`

```yaml
# Trước (SAI — dòng lạc trong global volumes block)
volumes:
  postgres_data:
  minio_data:
  - .:/app          ← syntax error

# Sau (ĐÚNG — xóa dòng lạc, thêm volume mount đúng vào pipeline service)
volumes:
  postgres_data:
  minio_data:

# pipeline service được thêm:
volumes:
  - ../cli/ingestion/source_data:/app/cli/ingestion/source_data:ro
```

---

## 7. Trạng thái pipeline khi kết thúc session

```
✅ Module import OK          — __init__.py đã tạo đủ
✅ prefect_configuration_loaded — homecredit_config.yaml đã vào image
✅ anyio/httpx/asgi-lifespan OK — dependency pinned đúng
✅ Flow khởi động OK         — "Created flow run 'courageous-leopard'"
❌ stack.yml missing          — /app/platforms/config/stack.yml not found
⏳ Bronze chưa chạy được     — blocked bởi stack.yml
⏳ Silver chưa chạy được     — blocked bởi Bronze
```

---

## 8. Checkpoint xác nhận pipeline hoàn toàn hoạt động

Khi `stack.yml` được fix, pipeline sẽ thực sự chạy khi log có đủ các dòng sau:

```
✅ prefect_configuration_loaded
✅ pipeline_config_loaded
✅ spark_session_ready app=homecredit-bronze-silver
✅ bronze_load_completed table=application path=s3a://lakehouse/bronze/application
✅ bronze_flow_completed tables=7
✅ silver_schema_ready schema=stg
✅ silver_flow_completed tables=10
✅ master_pipeline_completed duration_seconds=...
```
