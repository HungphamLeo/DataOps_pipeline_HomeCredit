-- 6. Fact Dư nợ trả góp hàng tháng (Periodic Snapshot Fact)
CREATE TABLE Fact_POS_CASH_balance (
    Pos_Cash_SK            BIGINT PRIMARY KEY,
    Customer_SK            BIGINT NOT NULL REFERENCES Dim_Customer(Customer_SK),
    Month_Date_SK          INT NOT NULL REFERENCES Dim_Date(Date_SK),
    Contract_Type_SK       INT NOT NULL REFERENCES Dim_Contract_Type(Contract_Type_SK),
    Loan_ID                VARCHAR(50) NOT NULL,       -- Degenerate Dim (SK_ID_PREV)
    CNT_INSTALMENT         SMALLINT NULL,
    CNT_INSTALMENT_FUTURE  SMALLINT NULL,
    SK_DPD                 INT NOT NULL,
    SK_DPD_DEF             INT NOT NULL
);