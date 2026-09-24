CREATE SCHEMA IF NOT EXISTS stg;       -- Lớp Staging (Dữ liệu thô/tạm thời)
ALTER TABLE Dim_Date                   SET SCHEMA stg;
ALTER TABLE Dim_Customer               SET SCHEMA stg;
ALTER TABLE Dim_Contract_Type          SET SCHEMA stg;
ALTER TABLE Dim_Application_Status     SET SCHEMA stg;

ALTER TABLE Fact_Loan_Application      SET SCHEMA stg;
ALTER TABLE Fact_Loan_Repayment        SET SCHEMA stg;
ALTER TABLE Fact_Credit_Balance        SET SCHEMA stg;
ALTER TABLE Fact_POS_CASH_balance      SET SCHEMA stg;
ALTER TABLE Fact_Bureau_Credit         SET SCHEMA stg;
ALTER TABLE Fact_Bureau_Monthly_Snapshot SET SCHEMA stg;