-- 5. Fact Dư nợ thẻ tín dụng định kỳ cuối tháng (Periodic Snapshot Fact)
CREATE TABLE Fact_Credit_Balance (
    Credit_Balance_SK           BIGINT PRIMARY KEY,
    Customer_SK                 BIGINT NOT NULL REFERENCES Dim_Customer(Customer_SK),
    Month_Date_SK               INT NOT NULL REFERENCES Dim_Date(Date_SK),
    Contract_Type_SK            INT NOT NULL REFERENCES Dim_Contract_Type(Contract_Type_SK),
    Card_Account_ID             VARCHAR(50) NOT NULL,  -- Degenerate Dim (SK_ID_PREV)
    AMT_BALANCE                 DECIMAL(18, 2) NOT NULL,
    AMT_CREDIT_LIMIT_ACTUAL     DECIMAL(18, 2) NULL,
    AMT_PAYMENT_TOTAL_CURRENT   DECIMAL(18, 2) NULL
);
