-- Indexes cho bảng Fact_Loan_Repayment
CREATE INDEX idx_repay_customer ON Fact_Loan_Repayment (Customer_SK);
CREATE INDEX idx_repay_due_date ON Fact_Loan_Repayment (Due_Date_SK);
CREATE INDEX idx_repay_loan_id  ON Fact_Loan_Repayment (Loan_ID);

-- Indexes cho bảng Fact_Loan_Application
CREATE INDEX idx_app_customer   ON Fact_Loan_Application (Customer_SK);
CREATE INDEX idx_app_dec_date   ON Fact_Loan_Application (Decision_Date_SK);
CREATE INDEX idx_app_status     ON Fact_Loan_Application (Status_SK);

-- Indexes cho các bảng Periodic Snapshot
CREATE INDEX idx_bureau_snap_date ON Fact_Bureau_Monthly_Snapshot (Month_Date_SK);
CREATE INDEX idx_bureau_snap_id   ON Fact_Bureau_Monthly_Snapshot (Bureau_ID);

CREATE INDEX idx_credit_bal_cust  ON Fact_Credit_Balance (Customer_SK, Month_Date_SK);
CREATE INDEX idx_pos_cash_cust    ON Fact_POS_CASH_balance (Customer_SK, Month_Date_SK);

-- Indexes cho bảng Dim_Customer (Lookup Natural Key & SCD2)
CREATE INDEX idx_cust_bk          ON Dim_Customer (Customer_BK, Is_Current_Flag);