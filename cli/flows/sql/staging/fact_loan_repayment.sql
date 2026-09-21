-- 1. Fact Thu nợ / Thanh toán kỳ trả góp (Transaction Fact)
CREATE TABLE Fact_Loan_Repayment (
    Repayment_SK        BIGINT PRIMARY KEY,
    Customer_SK         BIGINT NOT NULL REFERENCES Dim_Customer(Customer_SK),
    Due_Date_SK         INT NOT NULL REFERENCES Dim_Date(Date_SK),
    Payment_Date_SK     INT NOT NULL REFERENCES Dim_Date(Date_SK),
    Loan_ID             VARCHAR(50) NOT NULL,          -- Degenerate Dim (SK_ID_PREV)
    Instalment_Number   INT NOT NULL,                  -- Degenerate Dim (NUM_INSTALMENT_NUMBER)
    AMT_INSTALMENT      DECIMAL(18, 2) NOT NULL,
    AMT_PAYMENT         DECIMAL(18, 2) NOT NULL,
    Underpaid_Amount    DECIMAL(18, 2) NOT NULL,       -- AMT_INSTALMENT - AMT_PAYMENT
    Days_Past_Due       INT NOT NULL                   -- DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT
);







