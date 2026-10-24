
# 📒 Sổ Tay Dữ Liệu — HomeCredit DataOps Pipeline

> Dành cho fresher. Giải thích từng khái niệm từ cơ bản → nguyên lý → cách áp dụng trong project.
> Mỗi công nghệ đều có 3 phần: **Học thuật** (lý thuyết là gì), **Thực chiến** (project làm gì), **Tại sao** (lý do chọn cách đó).

## Mục lục

1. [Tổng quan hệ thống — Bức tranh toàn cảnh](#1-tổng-quan-hệ-thống)
2. [Python — Ngôn ngữ kết nối mọi thứ](#2-python)
3. [Docker — Đóng gói môi trường](#3-docker)
4. [MinIO — Kho lưu trữ Bronze](#4-minio)
5. [Apache Spark — Động cơ xử lý dữ liệu](#5-apache-spark)
6. [Delta Lake — Bảo vệ dữ liệu](#6-delta-lake)
7. [PostgreSQL — Cơ sở dữ liệu serving](#7-postgresql)
8. [Prefect — Điều phối pipeline](#8-prefect)
9. [Medallion Architecture — Triết lý thiết kế](#9-medallion-architecture)
10. [Kimball Star Schema — Mô hình dữ liệu](#10-kimball-star-schema)
11. [12-Factor App — Nguyên tắc cấu hình](#11-12-factor-app)
12. [Luồng chạy thực tế từ đầu đến cuối](#12-luồng-chạy-thực-tế)

## 1. Tổng quan hệ thống

### Bức tranh toàn cảnh

Hãy tưởng tượng hệ thống này như một **nhà máy chế biến thực phẩm**:

---
```
[Nguyên liệu thô]     [Kho lạnh]        [Chế biến]        [Cửa hàng]
  CSV files      →    MinIO/Delta   →   PySpark SQL   →   PostgreSQL
  (7 file CSV)       (Bronze layer)    (Silver layer)     (schema stg)
      ↑                                                        ↓
  Kaggle data                                           BI / Analytics
```
---

- **CSV files** = nguyên liệu thô, chưa qua xử lý
- **MinIO** = kho lạnh lưu nguyên liệu dạng thô (Bronze)
- **PySpark** = máy chế biến, chuyển hóa thành sản phẩm (Silver)
- **PostgreSQL** = cửa hàng, nơi khách hàng (BI tools) lấy dữ liệu

**Tại sao cần nhiều lớp như vậy?**

Vì nếu xử lý thẳng CSV → PostgreSQL:
- Không có lịch sử (chạy lại → mất data cũ)
- Không audit được (ai sửa cái gì, lúc nào)
- Không tái xử lý được nếu logic sai

## 2. Python

### Học thuật

Python là ngôn ngữ lập trình **interpreted** (thông dịch từng dòng, không cần biên dịch trước).
Điểm mạnh trong Data Engineering: thư viện phong phú, cú pháp đơn giản, kết nối được mọi hệ thống.

**Các khái niệm cốt lõi dùng trong project:**

#### 2.1 Module và Package

---
```
project/
├── cli/               ← Package (thư mục có __init__.py)
│   ├── __init__.py    ← File này biến thư mục thành "package" Python
│   └── main.py        ← Module (1 file Python = 1 module)
└── config.py          ← Module ở root
```
---

---
```python
# Import từ package cli, module main
from cli.flows.serving.bronze_flow import bronze_ingest_flow

# Nếu KHÔNG có __init__.py → Python không nhận ra "cli" là package
# → ModuleNotFoundError: No module named 'cli'  ← lỗi ta đã gặp!
```
---

**Lesson learned từ project:** Lúc đầu quên tạo `__init__.py` trong `cli/`, `platforms/`, `log/`
→ container Docker bị lỗi `ModuleNotFoundError`. Fix: tạo file `__init__.py` rỗng trong mỗi thư mục.

#### 2.2 Environment Variables và .env

---
```python
import os

# Đọc biến môi trường — pattern chuẩn
db_password = os.getenv("POSTGRES_PASSWORD", "default_value")
#                         ↑ tên biến            ↑ giá trị mặc định nếu không set
```
---

**Tại sao không hardcode password trong code?**

---
```python
# ❌ SAI — hardcode trong code
db_password = "admin123"  # Commit lên GitHub → lộ password cho cả thế giới

# ✅ ĐÚNG — đọc từ environment
db_password = os.getenv("POSTGRES_PASSWORD")  # Giá trị chỉ tồn tại trên máy bạn
```
---

**Trong project này:** File [`config.py`](../config.py) là file duy nhất đọc `.env` và expose ra các getter function. Không module nào khác được tự đọc `.env`.

#### 2.3 Path — Đường dẫn file

---
```python
from pathlib import Path

# ❌ Cách cũ — dễ sai khi chạy từ thư mục khác
path = "../cli/flows/config/config.yaml"  # Relative path — sai nếu chạy từ nơi khác

# ✅ Cách đúng — anchor theo vị trí file hiện tại
PROJECT_ROOT = Path(__file__).resolve().parent
# __file__   = path của file Python đang chạy
# .resolve() = chuyển thành absolute path
# .parent    = lấy thư mục chứa file đó

config_path = PROJECT_ROOT / "cli" / "flows" / "config" / "config.yaml"
```
---

**Lesson learned từ project:** Bug cũ dùng `Path(__file__).parents[3]` — số 3 phụ thuộc vào độ sâu thư mục. Khi Docker copy file vào `/app/`, độ sâu thay đổi → `FileNotFoundError`.

## 3. Docker

### Học thuật

Docker là công nghệ **containerization** — đóng gói ứng dụng cùng toàn bộ dependencies vào một "hộp" (container) có thể chạy ở bất kỳ máy nào.

**Analogon dễ hiểu:** Container giống như **hộp cơm văn phòng**:
- Có đủ cơm, thức ăn, muỗng đũa bên trong
- Mang đi đâu ăn cũng được (máy Windows, Linux, Mac)
- Không ảnh hưởng đến "bếp ăn" (máy host) bên ngoài

#### 3.1 Dockerfile — Công thức làm hộp cơm

---
```dockerfile
# Bước 1: Lấy nền tảng — Python 3.11 trên Debian Bookworm
FROM python:3.11.13-slim
#         ↑ pin patch version cứng — tránh Docker Hub re-tag sang Debian Trixie
#           (Trixie đã drop openjdk-17, gây lỗi khi build — lesson learned!)

# Bước 2: Cài Java — Spark cần JVM để chạy
RUN apt-get install -y openjdk-17-jre-headless

# Bước 3: Cài Python packages
COPY requirements.txt .
RUN pip install -r requirements.txt

# Bước 4: Copy source code vào container
COPY config.py .
COPY cli ./cli
COPY platforms ./platforms

# Bước 5: Lệnh chạy khi container start
CMD ["python", "-m", "cli.main"]
```
---

**Tại sao `COPY requirements.txt` trước `COPY cli`?**

Docker build theo từng **layer** (lớp). Nếu layer không thay đổi → dùng **cache**. `requirements.txt` ít thay đổi hơn code → để trước để pip install không bị chạy lại mỗi lần sửa code.

#### 3.2 docker-compose.yml — Điều phối nhiều container

Project có 5 containers cần chạy cùng nhau và biết nhau:

---
```yaml
services:
  postgres:          # Container 1: Database
  minio:             # Container 2: Object storage
  minio-init:        # Container 3: Tạo bucket lần đầu
  prefect-server:    # Container 4: Orchestration UI
  pipeline:          # Container 5: Code Python của chúng ta
    depends_on:
      postgres:
        condition: service_healthy   # Chờ postgres healthy rồi mới chạy
      minio-init:
        condition: service_completed_successfully
```
---

**Networks — Các container nói chuyện với nhau thế nào?**

---
```yaml
networks:
  homecredit-net:
    driver: bridge   # Tạo mạng nội bộ riêng
```
---

Bên trong network này, containers gọi nhau bằng **service name** thay vì IP:

---
```python
# Từ container "pipeline" gọi container "postgres"
jdbc_url = "jdbc:postgresql://postgres:5432/homecredit_mart"
#                              ↑ service name, không phải localhost!
```
---

**Ports — Mở cửa ra bên ngoài:**

---
```yaml
ports:
  - "5433:5432"   # host_port:container_port
```
---

Từ máy bạn: kết nối `localhost:5433` → Docker map sang `postgres:5432` bên trong.

#### 3.3 Volumes — Lưu data không bị mất khi container restart

---
```yaml
volumes:
  postgres_data:    # Named volume — data PostgreSQL
  minio_data:       # Named volume — data MinIO

# Mount file từ host vào container (read-only)
- ../cli/ingestion/source_data:/app/cli/ingestion/source_data:ro
#  ↑ đường dẫn trên máy bạn         ↑ đường dẫn trong container    ↑ chỉ đọc
```
---

## 4. MinIO

### Học thuật

MinIO là **object storage** — hệ thống lưu trữ file theo mô hình **bucket/object**, tương tự Amazon S3.

**Object Storage vs File System:**

| | File System (ổ cứng thường) | Object Storage (MinIO/S3) |
|---|---|---|
| Đơn vị lưu | File trong thư mục | Object trong bucket |
| Truy cập | Path (`/home/user/data.csv`) | URL (`s3://bucket/data.csv`) |
| Metadata | OS-level | Custom key-value |
| Scale | Giới hạn 1 máy | Phân tán, petabyte |
| Phù hợp | Code, config | Big data, backup, media |

**Trong project:** MinIO đóng vai trò **data lake** — lưu toàn bộ raw data (Bronze layer) dưới dạng file Parquet/Delta.

### Thực chiến

---
```
MinIO bucket: lakehouse
└── bronze/
    ├── application/
    │   ├── _delta_log/          ← Delta transaction log (WAL)
    │   └── _load_date=2026-09-24/
    │         ├── part-00000.parquet
    │         └── part-00001.parquet
    ├── bureau/
    ├── bureau_balance/
    └── ...
```
---

**Cách Spark kết nối MinIO:**

---
```python
# spark_session.py — config S3A connector
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
.config("spark.hadoop.fs.s3a.access.key", "minioadmin")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
#                                                  ↑ MinIO cần cái này
#                                                    S3 thật thì không cần
```
---

**Tại sao dùng `s3a://` thay vì `s3://`?**

- `s3://` = AWS S3 native connector (chỉ dùng với AWS thật)
- `s3a://` = Hadoop S3A connector (dùng được với MinIO, Ceph, bất kỳ S3-compatible nào)

## 5. Apache Spark

### Học thuật

Spark là **distributed data processing engine** — xử lý dữ liệu lớn bằng cách chia nhỏ công việc.

**Analogy:** Thay vì 1 người đọc 27 triệu dòng → chia cho 8 người, mỗi người đọc ~3.4 triệu dòng cùng lúc.

#### 5.1 Kiến trúc Spark

---
```
Driver Program (bộ não)
    ├── SparkContext — kết nối với cluster
    ├── DAGScheduler — lập kế hoạch thực thi
    └── TaskScheduler — phân phối task cho Executor

Executor (cơ bắp — có thể nhiều máy)
    ├── Task 1 — xử lý partition 1
    ├── Task 2 — xử lý partition 2
    └── Task N — xử lý partition N
```
---

**Trong project (local[*] mode):** Driver và Executor chạy chung 1 JVM trong Docker container. Không có cluster thật — đây là **single-node Spark** phù hợp để học và dev.

#### 5.2 RDD → DataFrame → Dataset (lịch sử phát triển)

---
```python
# Thế hệ 1: RDD (Resilient Distributed Dataset) — low level, khó dùng
rdd = spark.sparkContext.textFile("data.csv")
result = rdd.map(lambda x: x.split(",")).filter(lambda x: x[0] == "1")

# Thế hệ 2: DataFrame — giống pandas, dễ hơn, có Catalyst optimizer
df = spark.read.csv("data.csv", header=True)
result = df.filter(df.id == "1")

# Trong project này: dùng DataFrame API + Spark SQL
```
---

#### 5.3 Lazy Evaluation — Concept quan trọng nhất

---
```python
# Dòng này KHÔNG đọc file CSV
df = spark.read.csv("application.csv")         # Chỉ tạo "kế hoạch"

# Dòng này KHÔNG làm gì
df = df.withColumn("_load_date", F.lit("2026-09-24"))  # Thêm vào kế hoạch

# Dòng này MỚI thực sự chạy — Spark thực thi toàn bộ kế hoạch từ đầu
df.write.format("delta").save("s3a://lakehouse/bronze/application")
```
---

**Tại sao Lazy?** Spark gom tất cả transformations lại → tối ưu hóa trước khi chạy (Catalyst Optimizer). Giống như bạn lên kế hoạch cả chuyến đi trước, tối ưu lộ trình, rồi mới lái xe.

#### 5.4 Partition — Đơn vị song song hóa

---
```
DataFrame với 27M rows
└── Partition 1: rows 1 → 3.4M
└── Partition 2: rows 3.4M → 6.8M
└── ...
└── Partition 8: rows 23.6M → 27M

→ 8 cores xử lý song song = nhanh hơn 8x so với 1 core
```
---

**Config trong project:**
---
```yaml
# stack.yaml
spark:
  shuffle_partitions: 8   # Spark chia 8 phần khi shuffle data
                           # Default là 200 → quá nhiều cho local mode
```
---

#### 5.5 Tại sao KHÔNG dùng @task với Spark

---
```python
# ❌ SAI — SparkSession không serializable
@task  # Prefect task chạy trong worker process riêng
def ingest_csv(source_path):
    spark = SparkSession.getActiveSession()  # Session không tồn tại trong process mới!

# ✅ ĐÚNG — tạo SparkSession ở @flow level
@flow
def bronze_ingest_flow():
    spark = get_spark_session(stack)  # Tạo 1 lần trong main process

    load_application(spark, ...)  # Truyền spark trực tiếp
    load_bureau(spark, ...)       # Không qua task boundary
```
---

**Nguyên lý:** SparkSession giữ JVM reference — không thể "đóng gói" (serialize) và gửi sang process khác. Prefect `@task` chạy trong process riêng → mất JVM reference.

#### 5.6 Wide vs Narrow Transformation

---
```
NARROW (không cần shuffle — nhanh):
  Partition 1 → xử lý → Partition 1 mới
  Partition 2 → xử lý → Partition 2 mới
  Ví dụ: .withColumn(), .filter(), CAST()

WIDE (cần shuffle — chậm hơn):
  Partition 1 ↘
  Partition 2 → SHUFFLE → Partition mới (gộp data từ nhiều nơi)
  Partition 3 ↗
  Ví dụ: .groupBy(), DENSE_RANK() OVER(), JOIN
```
---

**Trong project:** `dim_application_status.sql` dùng `DENSE_RANK()` → wide transformation. Chỉ dùng cho bảng Dim nhỏ, không dùng cho bảng Fact lớn.

## 6. Delta Lake

### Học thuật

Delta Lake là **table format** — thêm khả năng ACID transactions vào file Parquet thông thường.

**Vấn đề với Parquet thuần:**

---
```
Bạn đang write file Parquet → máy chết giữa chừng
→ File bị corrupt một nửa
→ Không biết đã write đến đâu
→ Phải xóa hết, chạy lại từ đầu
```
---

**Delta Lake giải quyết bằng Transaction Log (WAL):**

---
```
lakehouse/bronze/application/
├── _delta_log/
│   ├── 00000000000000000000.json  ← "Tôi sẽ thêm file part-00000.parquet"
│   ├── 00000000000000000001.json  ← "Tôi đã thêm thành công"
│   └── ...                        ← Mỗi lần write = 1 commit entry
└── _load_date=2026-09-24/
    └── part-00000.parquet
```
---

Giống như **sổ kế toán** — ghi rõ từng giao dịch, nếu máy chết giữa chừng thì rollback về trạng thái trước.

### Thực chiến — Partition Overwrite (idempotent)

---
```python
df.write.format("delta")
  .mode("overwrite")
  .option("partitionOverwriteMode", "dynamic")  # ← Key option
  .partitionBy("_load_date")
  .save("s3a://lakehouse/bronze/application")
```
---

**`partitionOverwriteMode=dynamic` nghĩa là gì?**

---
```
Lần 1 chạy (ngày 2026-09-23):
  Tạo: _load_date=2026-09-23/part-00000.parquet

Lần 2 chạy (ngày 2026-09-24):
  Tạo: _load_date=2026-09-24/part-00000.parquet
  GIỮ NGUYÊN: _load_date=2026-09-23/ (không xóa!)

Chạy lại ngày 2026-09-24:
  OVERWRITE: _load_date=2026-09-24/ (xóa và tạo lại partition này)
  GIỮ NGUYÊN: _load_date=2026-09-23/

→ Idempotent: chạy bao nhiêu lần cũng cho kết quả như nhau
```
---

**Tại sao quan trọng?** Pipeline production thường bị fail và cần retry. Nếu không idempotent → mỗi lần retry tạo data trùng lặp → chaos.

## 7. PostgreSQL

### Học thuật

PostgreSQL là **relational database** (RDBMS) — lưu data theo bảng với hàng và cột, có quan hệ giữa các bảng.

**OLTP vs OLAP — hai paradigm khác nhau:**

| | OLTP (PostgreSQL) | OLAP (Spark + Delta) |
|---|---|---|
| Tối ưu cho | Đọc/ghi 1 row nhanh | Scan nhiều triệu rows |
| Cách lưu | Row-oriented | Column-oriented |
| Ví dụ | Tra cứu 1 customer | Tổng hợp doanh thu cả năm |
| Trong project | Serving layer (stg schema) | Bronze layer (MinIO) |

**Tại sao Silver không lưu thẳng Delta mà lưu PostgreSQL?**

Vì BI tools (Tableau, Power BI, Metabase) dễ connect PostgreSQL hơn. PostgreSQL có SQL standard, index, foreign key → phù hợp cho adhoc query.

### Thực chiến — Spark JDBC write

---
```python
df.write.jdbc(
    url="jdbc:postgresql://postgres:5432/homecredit_mart",
    table="stg.Dim_Customer",
    mode="overwrite",    # DROP table + CREATE lại
    properties={
        "user": "admin_homecredit",
        "password": "...",
        "driver": "org.postgresql.Driver",
        "currentSchema": "stg",
    }
)
```
---

**`mode="overwrite"` nghĩa là:** Spark tự `DROP TABLE IF EXISTS stg."Dim_Customer"` rồi `CREATE TABLE` mới. Không cần tạo bảng thủ công trước.

**Schema `stg` là gì?**

PostgreSQL có khái niệm **schema** — như thư mục con trong database:

---
```sql
homecredit_mart (database)
└── stg (schema) — staging/silver data
    ├── Dim_Customer
    ├── Dim_Date
    ├── Fact_Loan_Application
    └── ...
```
---

## 8. Prefect

### Học thuật

Prefect là **workflow orchestration** — điều phối thứ tự chạy các bước trong pipeline, xử lý lỗi, retry, logging.

**Vấn đề không có orchestration:**

---
```bash
# Chạy thủ công — dễ quên bước, không tự retry khi fail
python bronze_flow.py
python silver_flow.py   # Nếu bronze_flow.py fail → silver chạy với data cũ!
```
---

**Với Prefect:**

---
```python
@flow
def master_pipeline_flow():
    bronze_result = bronze_ingest_flow()   # Chạy trước
    if bronze_result:                       # Chỉ chạy silver nếu bronze thành công
        staging_transform_flow()
```
---

### Thực chiến — @flow và @task

---
```python
from prefect import flow, task

# @flow = đơn vị lớn, có tracking trong Prefect UI
@flow(name="Bronze Source To MinIO")
def bronze_ingest_flow():
    spark = get_spark_session(stack)
    load_application(spark, ...)  # Gọi plain function, không @task
    load_bureau(spark, ...)

# @task = đơn vị nhỏ hơn, có thể retry độc lập
# ← Trong project này KHÔNG dùng @task cho Spark (vì serialization issue)
```
---

**Prefect UI** tại `http://localhost:4201` — xem trạng thái từng flow run, log, thời gian chạy.

## 9. Medallion Architecture

### Học thuật

Medallion Architecture là pattern tổ chức data lake theo 3 lớp: **Bronze → Silver → Gold**.

Được đặt tên theo kim loại quý — data càng được xử lý → càng "quý" hơn.

---
```
BRONZE (Raw)          SILVER (Cleaned)       GOLD (Business)
─────────────         ────────────────       ───────────────
Dữ liệu thô          Đã clean, transform    Aggregation,
Giữ nguyên 100%      Dimensional model      metrics, KPI
Có thể replay        Fact + Dim tables       Dashboard-ready
```
---

**Tại sao giữ Bronze?**

Nếu logic Silver bị sai → có thể reprocess từ Bronze (nguyên liệu thô vẫn còn).
Không có Bronze = mất nguyên liệu gốc = phải tải lại từ source (tốn thời gian).

### Thực chiến trong project

---
```
Bronze (MinIO Delta):               Silver (PostgreSQL stg):
  bronze/application/         →       Dim_Customer
  bronze/bureau/              →       Dim_Date
  bronze/bureau_balance/      →       Dim_Contract_Type
  bronze/previous_application/→       Dim_Application_Status
  bronze/installments_payments/→      Fact_Loan_Application
  bronze/pos_cash_balance/    →       Fact_Loan_Repayment
  bronze/credit_card_balance/ →       Fact_Bureau_Credit
                                       Fact_Bureau_Monthly_Snapshot
                                       Fact_Credit_Balance
                                       Fact_POS_CASH_Balance
```
---

## 10. Kimball Star Schema

### Học thuật

**Kimball Dimensional Modeling** là phương pháp thiết kế data warehouse của Ralph Kimball.
Mục tiêu: tổ chức data để BI query **nhanh** và **dễ hiểu**.

**Star Schema = Fact table ở giữa + Dimension tables xung quanh:**

---
```
                 Dim_Date
                    │
Dim_Customer ── Fact_Loan_Application ── Dim_Contract_Type
                    │
              Dim_Application_Status
```
---

**Fact table** = chứa **số đo** (measure): AMT_CREDIT, AMT_ANNUITY, số ngày trễ hạn...
**Dimension table** = chứa **ngữ cảnh** (context): ai, khi nào, loại gì...

#### 4 bước thiết kế Kimball (áp dụng trong project):

---
```
1. Business Process  → "Vay tiền Home Credit"
2. Grain            → "1 row = 1 lần trả góp"  (Fact_Loan_Repayment)
3. Dimensions       → Khi nào? (Date), Ai? (Customer), Loại gì? (Contract_Type)
4. Facts/Measures   → Số tiền phải trả, số tiền đã trả, số ngày trễ
```
---

#### Surrogate Key vs Natural Key

---
```sql
-- Natural Key (từ source system) — có thể thay đổi, không ổn định
SK_ID_CURR = 123456   -- ID từ hệ thống gốc của Home Credit

-- Surrogate Key (tự generate) — ổn định, độc lập với source
Customer_SK = abs(hash('123456'))   -- Hash deterministic, rerun-safe
```
---

**Tại sao dùng hash thay vì `monotonically_increasing_id()`?**

---
```python
# ❌ SAI — non-deterministic
Customer_SK = monotonically_increasing_id()  # Mỗi lần chạy cho số khác nhau!

# ✅ ĐÚNG — deterministic (cùng input → cùng output mọi lần)
Customer_SK = abs(hash(SK_ID_CURR))  # Rerun 100 lần → cùng kết quả
```
---

#### SCD Type 2 — Lưu lịch sử thay đổi

---
```sql
-- Dim_Customer có 3 cột SCD Type 2:
Effective_Date  = '2026-01-01'   -- Bản ghi này có hiệu lực từ khi nào
Expiry_Date     = '9999-12-31'   -- '9999-12-31' = đang active
Is_Current_Flag = 'Y'            -- 'Y' = bản ghi hiện tại

-- Khi customer thay đổi địa chỉ:
-- Bản ghi cũ: Expiry_Date = '2026-09-24', Is_Current_Flag = 'N'
-- Bản ghi mới: Effective_Date = '2026-09-24', Is_Current_Flag = 'Y'
```
---

## 11. 12-Factor App

### Học thuật

12-Factor App là tập hợp 12 nguyên tắc xây dựng ứng dụng **cloud-native**, dễ deploy, dễ scale.

**Nguyên tắc III — Config via Environment** (áp dụng nhiều nhất trong project):

> "Mọi config thay đổi theo môi trường (dev/staging/prod) phải lưu trong environment variables, không phải trong code."

### Thực chiến

---
```python
# config.py — áp dụng 12-Factor App Principle III

PROJECT_ROOT = Path(__file__).resolve().parent  # Anchor theo file này

def get_stack_config_path() -> Path:
    raw = os.getenv("STACK_CONFIG_PATH")   # Đọc từ env var trước
    if raw:
        return Path(raw)
    return PROJECT_ROOT / "platforms" / "config" / "stack.yaml"  # Fallback default
```
---

**Tại sao cần `config.py` làm single source of truth?**

Trước đây mỗi file tự tính path riêng:
---
```python
# metadata.py
path = Path(__file__).parents[3] / "cli" / "config" / "config.yaml"  # parents[3]???

# spark_session.py
path = Path(__file__).parents[2] / "config" / "stack.yaml"  # parents[2]???
```
---

Vấn đề: Trong Docker, các file được copy vào `/app/` → depth thay đổi → `parents[n]` sai → `FileNotFoundError`.

Sau refactor: tất cả đều import `from config import get_stack_config_path` → 1 chỗ duy nhất để sửa.

## 12. Luồng chạy thực tế từ đầu đến cuối

### Khi bạn chạy `docker compose up pipeline`

---
```
1. Docker đọc docker-compose.yml
   └── Chờ postgres healthy (healthcheck pg_isready)
   └── Chờ minio-init completed (tạo bucket lakehouse)

2. Container "pipeline" start
   └── Chạy: python -m cli.main

3. cli/main.py
   └── Setup logger
   └── Gọi master_pipeline_flow()

4. daily_pipeline.py — master_pipeline_flow()
   └── Gọi bronze_ingest_flow()
   └── Gọi staging_transform_flow()

5. bronze_flow.py — bronze_ingest_flow()
   ├── load_config() → đọc homecredit_config.yaml
   ├── load_stack_config() → đọc stack.yaml (Spark + MinIO config)
   ├── get_spark_session(stack) → khởi động JVM, tải JAR packages (~60s lần đầu)
   ├── load_application(spark, ...) → đọc CSV, write Delta s3a://lakehouse/bronze/application
   ├── load_bureau(spark, ...)      → ~45 giây
   ├── load_bureau_balance(spark, ...)  → ~3-5 phút (27M rows!)
   ├── load_previous_application(spark, ...)
   ├── load_installments_payments(spark, ...)
   ├── load_pos_cash_balance(spark, ...)
   ├── load_credit_card_balance(spark, ...)
   └── spark.stop()  → dọn dẹp JVM

6. silver_flow.py — staging_transform_flow()
   ├── get_spark_session(stack) → khởi động JVM lần 2
   ├── _register_bronze_view(spark, "application") → đọc Delta, tạo temp view
   ├── build_dim_date()          → spark.sql(dim_date.sql) → write JDBC PostgreSQL
   ├── build_dim_customer()      → spark.sql(dim_customer.sql) → write JDBC
   ├── build_dim_contract_type() → spark.sql(dim_contract_type.sql) → write JDBC
   ├── build_dim_app_status()    → spark.sql(dim_application_status.sql) → write JDBC
   ├── build_fact_loan_app()     → spark.sql(fact_loan_application.sql) → write JDBC
   ├── build_fact_loan_repayment() → ...
   ├── build_fact_bureau_credit() → ...
   ├── build_fact_bureau_monthly_snapshot() → fact_bureau_monthly_snapshot.sql
   ├── build_fact_credit_balance() → ...
   ├── build_fact_pos_cash_balance() → ...
   └── spark.stop()

7. Pipeline hoàn thành
   └── Log: master_pipeline_completed duration_seconds=...
   └── Container exit (restart: "no")
```
---

### Khi có lỗi — cách đọc traceback

---
```
pyspark.errors.AnalysisException: [UNRESOLVED_COLUMN] column `SK_BUREAU_ID` cannot be resolved
                                                        ↑ Đây là tên cột SAI trong SQL file
Did you mean: [`SK_ID_BUREAU`, ...]
               ↑ Spark gợi ý tên đúng — luôn đọc phần "Did you mean"!
```
---

**File lỗi:** Nhìn vào stack trace:
---
```
File "/app/cli/flows/serving/silver_flow.py", line 150
    _build_and_write(spark, "fact_bureau_monthly_snapshot", ...)
→ Mở file: cli/flows/sql/staging/fact_bureau_monthly_snapshot.sql
→ Tìm cột SK_BUREAU_ID → sửa thành SK_ID_BUREAU
```
---

## Tóm tắt nhanh — Mỗi công nghệ làm gì

| Công nghệ | Vai trò trong project | Câu hỏi nó trả lời |
|---|---|---|
| **Python** | Glue language — kết nối mọi thứ | "Code logic ở đâu?" |
| **Docker** | Đóng gói môi trường | "Chạy trên máy nào cũng được?" |
| **MinIO** | Lưu raw data (Bronze) | "Data thô để ở đâu?" |
| **Spark** | Xử lý data lớn song song | "Làm sao xử lý 60M rows?" |
| **Delta Lake** | ACID cho object storage | "Chạy lại không bị duplicate?" |
| **PostgreSQL** | Serving layer cho BI | "BI tool query ở đâu?" |
| **Prefect** | Điều phối thứ tự chạy | "Bronze xong mới chạy Silver?" |
| **Medallion** | Kiến trúc tổ chức data | "Data được lưu theo tầng nào?" |
| **Kimball** | Thiết kế bảng cho analytics | "Dim/Fact design như thế nào?" |
| **12-Factor** | Config best practice | "Password để ở đâu?" |
