
-- Bảng snapshot chi tiết các kỳ thanh toán
CREATE TABLE fact_installment_payment (
    prev_application_id INT NOT NULL REFERENCES fact_previous_application(prev_application_id),
    installment_number INT NOT NULL,
    installment_version INT NOT NULL,

    due_date_skey INT REFERENCES dim_date(date_skey),
    payment_date_skey INT REFERENCES dim_date(date_skey),

    -- Measures
    installment_amount DECIMAL(18, 2),
    payment_amount DECIMAL(18, 2),
    
    -- PK
    PRIMARY KEY (prev_application_id, installment_number, installment_version)
);
