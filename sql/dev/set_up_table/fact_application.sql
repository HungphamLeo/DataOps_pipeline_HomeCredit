-- Bảng fact chính về các đơn xin vay
CREATE TABLE fact_application (
    application_id INT PRIMARY KEY,             -- PK, từ SK_ID_CURR

    -- Foreign Keys tới các bảng Dimension
    client_skey BIGINT NOT NULL REFERENCES dim_client(client_skey),
    location_skey BIGINT REFERENCES dim_location(location_skey),
    application_start_date_skey INT REFERENCES dim_date(date_skey),

    -- Các thước đo (Measures)
    total_income_amount DECIMAL(18, 2),
    credit_amount DECIMAL(18, 2),
    annuity_amount DECIMAL(18, 2),
    goods_price_amount DECIMAL(18, 2),
    ext_source_1 DECIMAL(18, 8),
    ext_source_2 DECIMAL(18, 8),
    ext_source_3 DECIMAL(18, 8),

    -- Các chiều thoái hóa (Degenerate Dimensions)
    contract_type VARCHAR(50),
    days_birth INT,
    days_employed INT,
    target SMALLINT, -- 0 hoặc 1

    -- Ràng buộc
    CONSTRAINT chk_target CHECK (target IN (0, 1))
);
