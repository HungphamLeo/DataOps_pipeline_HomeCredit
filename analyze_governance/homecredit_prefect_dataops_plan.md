# Home Credit Risk DataOps Plan with Prefect

## Mục tiêu
Xây dựng một pipeline DataOps cho dự án Home Credit Risk, tập trung vào:
- ingest dữ liệu raw từ thư mục source_data,
- validate schema và business rules cơ bản,
- tạo curated dataset dùng cho EDA và modeling,
- orchestrate bằng Prefect thay vì Airflow.

## Kiến trúc đề xuất

### 1. Raw layer
- Nguồn dữ liệu: các file CSV trong `sql/dev/source_data/`
- Mục tiêu: lưu trữ dữ liệu gốc không bị chỉnh sửa trực tiếp

### 2. Validation layer
- Kiểm tra các cột bắt buộc có tồn tại
- Kiểm tra không có duplicate `SK_ID_CURR`
- Kiểm tra `TARGET` có giá trị hợp lệ `0/1`

### 3. Curated layer
- Tạo một bảng trung gian dùng cho EDA và mô hình
- Bao gồm các biến quan trọng như:
  - `TARGET`
  - `AMT_INCOME_TOTAL`
  - `AMT_CREDIT`
  - `AMT_ANNUITY`
  - `AMT_GOODS_PRICE`
  - `EXT_SOURCE_1/2/3`
  - các feature dérieved như `AGE_YEARS`, `INCOME_CREDIT_RATIO`

### 4. Orchestration layer
- Prefect flow chạy tuần tự các task:
  1. load raw data
  2. validate data
  3. build curated dataset
  4. save output

## Output
Pipeline tạo ra file curated tại:
- `data/curated/homecredit_risk_curated.csv`

## Cách chạy
```bash
cd /mnt/c/Users/Admin/Downloads/Project/Github/DataOps_pipeline_HomeCredit
python prefect/flow/daily_pipeline.py
```
