
-- 2. Fact Nộp & Xét duyệt đơn vay (Transaction Fact)
CREATE TABLE Fact_Loan_Application (
    Application_SK      BIGINT PRIMARY KEY,
    Customer_SK         BIGINT NOT NULL REFERENCES Dim_Customer(Customer_SK),
    Decision_Date_SK    INT NOT NULL REFERENCES Dim_Date(Date_SK),
    Contract_Type_SK    INT NOT NULL REFERENCES Dim_Contract_Type(Contract_Type_SK),
    Status_SK           INT NOT NULL REFERENCES Dim_Application_Status(Status_SK),
    Loan_ID             VARCHAR(50) NOT NULL,          -- Degenerate Dim (SK_ID_CURR / SK_ID_PREV)
    AMT_APPLICATION     DECIMAL(18, 2) NULL,
    AMT_CREDIT          DECIMAL(18, 2) NOT NULL,
    AMT_ANNUITY         DECIMAL(18, 2) NULL,
    AMT_GOODS_PRICE     DECIMAL(18, 2) NULL,
    TARGET              SMALLINT NULL                  -- Cờ nợ xấu: 0 (bình thường), 1 (vỡ nợ)
);