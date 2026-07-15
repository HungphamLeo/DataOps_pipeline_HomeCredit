-- Bảng fact về các khoản vay từ Credit Bureau
CREATE TABLE fact_bureau_loan (
    bureau_loan_id INT PRIMARY KEY,             -- PK, từ SK_BUREAU_ID
    application_id INT NOT NULL REFERENCES fact_application(application_id), -- FK đến đơn vay hiện tại

    -- Measures
    credit_day_overdue INT,
    credit_sum_amount DECIMAL(18, 2),
    credit_sum_debt_amount DECIMAL(18, 2),
    credit_sum_overdue_amount DECIMAL(18, 2),
    max_overdue_amount DECIMAL(18, 2),
    days_credit INT,

    -- Degenerate Dimensions
    credit_active_status VARCHAR(50),
    credit_currency VARCHAR(10),
    credit_type VARCHAR(100)
);