-----------------
-- Bảng fact về các đơn vay trước đó
CREATE TABLE fact_previous_application (
    prev_application_id INT PRIMARY KEY,        -- PK, từ SK_ID_PREV
    current_application_id INT NOT NULL REFERENCES fact_application(application_id), -- FK đến đơn vay hiện tại
    
    decision_date_skey INT REFERENCES dim_date(date_skey),

    -- Measures
    application_amount DECIMAL(18, 2),
    credit_amount DECIMAL(18, 2),
    down_payment_amount DECIMAL(18, 2),
    days_decision INT,

    -- Degenerate Dimensions
    contract_status VARCHAR(50),
    reject_reason VARCHAR(100),
    payment_type VARCHAR(100),
    product_combination VARCHAR(255)
);