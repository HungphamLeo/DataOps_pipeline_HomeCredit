# HomeCredit DataOps Pipeline

Dự án DataOps pipeline xây dựng hệ thống Medallion Architecture (Bronze → Silver) trên nền tảng PySpark, Delta Lake, MinIO, PostgreSQL và Prefect — containerised hoàn toàn bằng Docker Compose.

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Tech stack](#2-tech-stack)
3. [Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
4. [Yêu cầu môi trường](#4-yêu-cầu-môi-trường)
5. [Cài đặt & Cấu hình](#5-cài-đặt--cấu-hình)
6. [Chạy pipeline](#6-chạy-pipeline)
7. [Theo dõi & Kiểm tra kết quả](#7-theo-dõi--kiểm-tra-kết-quả)
8. [Luồng dữ liệu chi tiết](#8-luồng-dữ-liệu-chi-tiết)
9. [Tuning & Vận hành](#9-tuning--vận-hành)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Tổng quan kiến trúc

```
Source CSV files
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Prefect @flow — bronze_ingest_flow)      │
│                                                          │
│  CSV  ──►  PySpark  ──►  Delta Lake  ──►  MinIO          │
│           (infer schema,    (ACID,          (S3A          │
│            add metadata)    partitioned      object       │
│                             by _load_date)   storage)     │
└──────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Prefect @flow — silver_transform_flow)   │
│                                                          │
│  Delta Lake  ──►  Spark SQL (.sql files)  ──►  PostgreSQL│
│  (Bronze views    (Dimensional modeling,     schema: stg │
│   temp views)      Kimball Star Schema)                  │
└──────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│  SERVING LAYER  (PostgreSQL schema: stg)                 │
│                                                          │
│  Dim_Date         Dim_Customer        Dim_Contract_Type  │
│  Dim_AppStatus    Fact_LoanApp        Fact_LoanRepayment │
│  Fact_BureauCredit   Fact_BureauMonthly                  │
│  Fact_CreditBalance  Fact_POSCashBalance                 │
└──────────────────────────────────────────────────────────┘
```

---

## 2. Tech stack

| Thành phần | Công nghệ | Phiên bản | Vai trò |
|---|---|---|---|
| **Orchestration** | Prefect | 2.20.0 | Pipeline scheduling, flow tracking |
| **Processing** | PySpark | 3.5.1 | Distributed data transformation |
| **Table Format** | Delta Lake | 3.1.0 | ACID transactions, time travel |
| **Object Storage** | MinIO | latest | S3-compatible Bronze data lake |
| **Data Warehouse** | PostgreSQL | 14 | Silver/Serving layer |
| **Containerization** | Docker Compose | 3.9 | Toàn bộ stack chạy trên 1 máy |
| **Config** | python-dotenv + PyYAML | — | 12-Factor App config management |
| **Logging** | structlog + colorlog | — | Structured JSON logs |

---

## 3. Cấu trúc thư mục

```
DataOps_pipeline_HomeCredit/
├── cli/
│   ├── flows/
│   │   ├── config/
│   │   │   └── homecredit_config.yaml      # Pipeline config (tables, paths, runtime)
│   │   ├── serving/
│   │   │   ├── bronze_flow.py              # Bronze ingestion: CSV → Delta Lake
│   │   │   ├── silver_flow.py              # Silver transform: Delta → PostgreSQL
│   │   │   ├── daily_pipeline.py           # Master flow: gọi Bronze + Silver
│   │   │   └── metadata.py                 # Config loader helpers
│   │   └── sql/staging/                    # 10 SQL transformation files
│   ├── ingestion/
│   │   └── source_data/                    # ← đặt CSV files tại đây
│   └── main.py                             # Entrypoint
├── platforms/
│   ├── config/
│   │   └── stack.yaml                      # Spark + MinIO config
│   ├── processing/
│   │   ├── spark_stack/spark_session.py    # SparkSession factory
│   │   └── config/delta_utils.py           # Delta write helpers
│   └── prefect_orchestra/prefect_main.py   # PrefectETLPipelineConfig
├── log/
│   ├── config/logger_config.yaml           # Logger routing config
│   └── log_storage/                        # Runtime log files
├── infra_homecredit/
│   ├── docker-compose.yml                  # Toàn bộ stack: 5 services
│   └── Dockerfile                          # Pipeline image (Python 3.11 + Java 17)
├── config.py                               # Single source of truth — paths + YAML loader
├── .env.example                            # Template env vars
└── requirements.txt                        # Python dependencies
```

---

## 4. Yêu cầu môi trường

### Phần mềm bắt buộc

| Phần mềm | Phiên bản tối thiểu | Kiểm tra |
|---|---|---|
| Docker Desktop | 24.x+ | `docker --version` |
| Docker Compose | 2.x+ (plugin) | `docker compose version` |
| Git | 2.x+ | `git --version` |

> **RAM khuyến nghị:** tối thiểu 8 GB (Spark cần 3 GB, MinIO + PostgreSQL + Prefect ~2 GB)

### Dữ liệu nguồn — CSV files (tải từ Kaggle)

Dataset: [Home Credit Default Risk](https://www.kaggle.com/c/home-credit-default-risk/data)

Tải về và đặt vào thư mục `cli/ingestion/source_data/`:

```
cli/ingestion/source_data/
├── application.csv               (~307K rows)
├── bureau.csv                    (~1.7M rows)
├── bureau_balance.csv            (~27M rows)
├── previous_application.csv      (~1.7M rows)
├── installments_payments.csv     (~13.6M rows)
├── POS_CASH_balance.csv          (~10M rows)
└── credit_card_balance.csv       (~3.8M rows)
```

---

## 5. Cài đặt & Cấu hình

### Bước 1 — Clone repository

```bash
git clone https://github.com/HungphamLeo/DataOps_pipeline_HomeCredit.git
cd DataOps_pipeline_HomeCredit
```

### Bước 2 — Tạo file `.env`

```bash
cp .env.example .env
```

Mở `.env` và chỉnh các giá trị sau (phần còn lại giữ default là được):

```dotenv
# PostgreSQL
POSTGRES_USER=admin_homecredit
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=homecredit_mart

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_secure_password
MINIO_BUCKET=lakehouse

# Spark (tuỳ RAM máy)
SPARK_DRIVER_MEMORY=3g        # Nếu máy < 8GB RAM: dùng 2g
SPARK_SHUFFLE_PARTITIONS=8
```

### Bước 3 — Đặt CSV files vào đúng thư mục

```bash
# Linux/macOS
cp ~/Downloads/home-credit-default-risk/*.csv cli/ingestion/source_data/

# Windows (PowerShell)
Copy-Item "$HOME\Downloads\home-credit-default-risk\*.csv" "cli\ingestion\source_data\"
```

---

## 6. Chạy pipeline

### Lần đầu — Build image và khởi động toàn bộ stack

```bash
# Từ thư mục gốc của project
docker compose -f infra_homecredit/docker-compose.yml up --build -d
```

Lệnh này sẽ:
1. Build Docker image pipeline (Python 3.11 + Java 17 + tất cả dependencies)
2. Khởi động PostgreSQL, MinIO, Prefect Server, Prefect Worker
3. Chờ healthcheck các service
4. Tạo bucket `lakehouse` trên MinIO
5. Chạy pipeline container

> ⏱️ Lần đầu build có thể mất **5–15 phút** do tải Spark JAR packages (~500MB).

### Chạy lại pipeline (không build lại image)

```bash
docker compose -f infra_homecredit/docker-compose.yml up pipeline
```

### Chỉ khởi động infrastructure (không chạy pipeline)

```bash
docker compose -f infra_homecredit/docker-compose.yml up -d postgres minio minio-init prefect-server prefect-worker
```

### Dừng toàn bộ stack

```bash
docker compose -f infra_homecredit/docker-compose.yml down
```

### Dừng và xóa toàn bộ data (reset hoàn toàn)

```bash
docker compose -f infra_homecredit/docker-compose.yml down -v
```

---

## 7. Theo dõi & Kiểm tra kết quả

### Theo dõi log pipeline realtime

```bash
docker logs homecredit_pipeline --follow
```

### Các checkpoint xác nhận pipeline chạy thành công

```
✅ prefect_configuration_loaded
✅ pipeline_config_loaded
✅ spark_session_ready        app=homecredit-bronze-silver
✅ bronze_load_completed      table=application   rows=307511
✅ bronze_load_completed      table=bureau
✅ bronze_load_completed      table=bureau_balance
✅ bronze_load_completed      table=previous_application
✅ bronze_load_completed      table=installments_payments
✅ bronze_load_completed      table=pos_cash_balance
✅ bronze_load_completed      table=credit_card_balance
✅ bronze_flow_completed      tables=7
✅ silver_schema_ready        schema=stg
✅ silver_flow_completed      tables=10
✅ master_pipeline_completed  duration_seconds=...
```

### Truy cập các UI

| Service | URL | Credentials |
|---|---|---|
| **Prefect UI** | http://localhost:4201 | — |
| **MinIO Console** | http://localhost:9003 | minioadmin / (từ .env) |
| **PostgreSQL** | localhost:5433 | admin_homecredit / (từ .env) |

### Kiểm tra data trong PostgreSQL

```sql
-- Kết nối: host=localhost port=5433 db=homecredit_mart user=admin_homecredit

-- Xem tất cả bảng đã tạo trong schema stg
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'stg'
ORDER BY table_name;

-- Kiểm tra số row từng bảng
SELECT 'Dim_Customer'       AS tbl, COUNT(*) FROM stg."Dim_Customer"       UNION ALL
SELECT 'Dim_Date'           AS tbl, COUNT(*) FROM stg."Dim_Date"           UNION ALL
SELECT 'Fact_Loan_Application', COUNT(*) FROM stg."Fact_Loan_Application"  UNION ALL
SELECT 'Fact_Loan_Repayment',   COUNT(*) FROM stg."Fact_Loan_Repayment";
```

### Kiểm tra Bronze data trong MinIO

Mở MinIO Console tại http://localhost:9003, truy cập bucket `lakehouse/bronze/`.
Mỗi bảng sẽ có thư mục Delta với cấu trúc:

```
lakehouse/bronze/
├── application/
│   ├── _delta_log/
│   └── _load_date=2026-09-24/
│         └── part-00000.parquet
├── bureau/
├── bureau_balance/
└── ...
```

---

## 8. Luồng dữ liệu chi tiết

### Bronze Layer — CSV → Delta Lake

| Source file | Bronze table | ~Rows |
|---|---|---|
| `application.csv` | `bronze/application` | 307,511 |
| `bureau.csv` | `bronze/bureau` | 1,716,428 |
| `bureau_balance.csv` | `bronze/bureau_balance` | 27,299,925 |
| `previous_application.csv` | `bronze/previous_application` | 1,670,214 |
| `installments_payments.csv` | `bronze/installments_payments` | 13,605,401 |
| `POS_CASH_balance.csv` | `bronze/pos_cash_balance` | 10,001,358 |
| `credit_card_balance.csv` | `bronze/credit_card_balance` | 3,840,312 |

### Silver Layer — Delta Lake → PostgreSQL (schema: stg)

**Dimensions (build trước):**

| Target table | SQL file | Source Bronze views |
|---|---|---|
| `Dim_Date` | `dim_date.sql` | generated |
| `Dim_Application_Status` | `dim_application_status.sql` | bronze_previous_application |
| `Dim_Contract_Type` | `dim_contract_type.sql` | bronze_application |
| `Dim_Customer` | `dim_customer.sql` | bronze_application |

**Facts (build sau dims):**

| Target table | SQL file | Source Bronze views |
|---|---|---|
| `Fact_Loan_Application` | `fact_loan_application.sql` | bronze_application, bronze_previous_application |
| `Fact_Loan_Repayment` | `fact_loan_repayment.sql` | bronze_installments_payments, bronze_previous_application |
| `Fact_Bureau_Credit` | `fact_bureau_credit.sql` | bronze_bureau |
| `Fact_Bureau_Monthly_Snapshot` | `fact_bureau_monthly_snapshot.sql` | bronze_bureau_balance |
| `Fact_Credit_Balance` | `fact_credit_balance.sql` | bronze_credit_card_balance |
| `Fact_POS_CASH_Balance` | `fact_pos_cash_balance.sql` | bronze_pos_cash_balance |

---

## 9. Tuning & Vận hành

### Điều chỉnh Spark memory theo RAM máy

Chỉnh trong `.env` trước khi chạy:

| RAM máy | `SPARK_DRIVER_MEMORY` | `SPARK_SHUFFLE_PARTITIONS` |
|---|---|---|
| < 8 GB | `2g` | `4` |
| 8–16 GB | `3g` | `8` |
| > 16 GB | `4g` | `16` |

### Rebuild image sau khi thay đổi code

```bash
docker compose -f infra_homecredit/docker-compose.yml build --no-cache pipeline
docker compose -f infra_homecredit/docker-compose.yml up pipeline
```

### Chạy lại chỉ Bronze (nếu Silver fail)

```bash
# Vào container và chạy trực tiếp
docker compose -f infra_homecredit/docker-compose.yml run --rm pipeline \
  python -c "from cli.flows.serving.bronze_flow import bronze_ingest_flow; bronze_ingest_flow()"
```

---

## 10. Troubleshooting

| Lỗi | Nguyên nhân | Fix |
|---|---|---|
| `Package openjdk-17-jre-headless is not available` | Docker Hub re-tag base image sang Debian Trixie | Dockerfile đã pin `python:3.11.13-slim-bookworm` — rebuild `--no-cache` |
| `FileNotFoundError: stack.yaml` | File chưa được COPY vào image | Rebuild `--no-cache`, kiểm tra `.dockerignore` |
| `Can't instantiate abstract class GatherTaskGroup` | anyio version sai | `requirements.txt` đã pin `anyio==4.4.0` — rebuild image |
| `ModuleNotFoundError: No module named 'cli'` | Thiếu `__init__.py` | Đã tạo đủ `__init__.py` trong `cli/`, `platforms/`, `log/` |
| Container OOM khi chạy `bureau_balance` | Spark dùng hết RAM | Giảm `SPARK_DRIVER_MEMORY=2g` trong `.env` |
| `MetricsConfig: Cannot locate hadoop-metrics2` | Hadoop metrics config không tồn tại | **Bỏ qua** — WARN vô hại |
| PostgreSQL healthcheck fail | DB chưa sẵn sàng | Pipeline có `depends_on: condition: service_healthy` — tự chờ |

---

## Branch hiện tại

```
23092026_hung_debug_prefect_and_config
```

Xem lịch sử thay đổi chi tiết trong thư mục [`export_task/`](export_task/).
