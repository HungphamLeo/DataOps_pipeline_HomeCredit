
-- 3. Fact Khoản vay ngoài từ tổ chức tín dụng CIC (Transaction Fact)
CREATE TABLE Fact_Bureau_Credit (
    Bureau_Credit_SK       BIGINT PRIMARY KEY,
    Customer_SK            BIGINT NOT NULL REFERENCES Dim_Customer(Customer_SK),
    Credit_Date_SK         INT NOT NULL REFERENCES Dim_Date(Date_SK),
    Bureau_ID              VARCHAR(50) NOT NULL,       -- Degenerate Dim (SK_BUREAU_ID)
    CREDIT_ACTIVE          VARCHAR(50) NOT NULL,       -- Closed / Active
    CREDIT_TYPE            VARCHAR(100) NOT NULL,      -- Consumer credit, Credit card, Car loan...
    AMT_CREDIT_SUM         DECIMAL(18, 2) NULL,
    AMT_CREDIT_MAX_OVERDUE DECIMAL(18, 2) NULL
);