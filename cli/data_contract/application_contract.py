# governance/contracts/application_contract.py
"""
Data Contract cho Bronze layer — application dataset (Home Credit).

Severity levels:
  CRITICAL → Circuit Breaker: pipeline dừng ngay lập tức
  WARNING  → DLQ: record bẩn bị cách ly, pipeline tiếp tục

Nguồn: Home Credit Default Risk schema
"""
from __future__ import annotations

import pandera.pyspark as pa
from pandera.pyspark import DataFrameSchema, Column


# ---------------------------------------------------------------------------
# CRITICAL rules — vi phạm → halt toàn bộ pipeline
# ---------------------------------------------------------------------------
APPLICATION_CRITICAL = DataFrameSchema(
    columns={
        "SK_ID_CURR": Column(
            dtype="long",
            nullable=False,                    # PK không được NULL
            checks=pa.Check.greater_than(0),   # PK phải dương
        ),
        "TARGET": Column(
            dtype="long",
            nullable=True,
            checks=pa.Check.isin([0, 1, None]),  # chỉ 0/1 hoặc NULL
        ),
        "AMT_CREDIT": Column(
            dtype="double",
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
    },
    name="application_critical",
    strict=False,          # chỉ validate cột được khai báo, bỏ qua cột khác
)

# ---------------------------------------------------------------------------
# WARNING rules — vi phạm → quarantine DLQ, không halt
# ---------------------------------------------------------------------------
APPLICATION_WARNING = DataFrameSchema(
    columns={
        "AMT_INCOME_TOTAL": Column(
            dtype="double",
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
        "DAYS_BIRTH": Column(
            dtype="long",
            nullable=True,
            checks=pa.Check.less_than(0),      # Tất cả giá trị âm (age offset)
        ),
        "NAME_CONTRACT_TYPE": Column(
            dtype="string",
            nullable=True,
            checks=pa.Check.isin([
                "Cash loans", "Revolving loans", None
            ]),
        ),
    },
    name="application_warning",
    strict=False,
)