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


-- Audit schema cho Data Quality results
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.dq_results (
    run_id          VARCHAR(36)   NOT NULL,
    table_name      VARCHAR(100)  NOT NULL,
    severity        VARCHAR(10)   NOT NULL,  -- CRITICAL | WARNING
    rule_name       VARCHAR(200)  NOT NULL,
    column_name     VARCHAR(100),
    failed_count    BIGINT        DEFAULT 0,
    total_count     BIGINT        DEFAULT 0,
    passed          BOOLEAN       NOT NULL,
    run_at          TIMESTAMP     DEFAULT NOW()
);