-- Fact_Credit_Balance: từ credit_card_balance join Dim_Contract_Type inline
-- Input temp views : bronze_credit_card_balance, bronze_application
-- Output           : Fact_Credit_Balance
--
-- MONTHS_BALANCE âm → last_day(add_months(current_date, n)) → YYYYMMDD
-- Contract_Type_SK : 'Revolving loans' — resolved bằng JOIN vào dim_contract CTE

WITH dim_contract AS (
    SELECT
        CAST(dense_rank() OVER (ORDER BY Contract_Type_Code) AS INT) AS Contract_Type_SK,
        Contract_Type_Code
    FROM (
        SELECT DISTINCT TRIM(NAME_CONTRACT_TYPE) AS Contract_Type_Code
        FROM bronze_application
        WHERE NAME_CONTRACT_TYPE IS NOT NULL
    ) t
)
SELECT
    -- Surrogate Key
    abs(hash(concat_ws('_',
        CAST(c.SK_ID_PREV     AS STRING),
        CAST(c.MONTHS_BALANCE AS STRING)
    )))                                                                         AS Credit_Balance_SK,

    -- FK → Dim_Customer
    COALESCE(CAST(c.SK_ID_CURR AS BIGINT), -1)                                  AS Customer_SK,

    -- FK → Dim_Date (tháng báo cáo — cuối tháng)
    COALESCE(
        CAST(date_format(last_day(add_months(current_date(), CAST(c.MONTHS_BALANCE AS INT))), 'yyyyMMdd') AS INT),
        -1
    )                                                                           AS Month_Date_SK,

    -- FK → Dim_Contract_Type (resolved via JOIN — 'Revolving loans')
    COALESCE(dc.Contract_Type_SK, -1)                                           AS Contract_Type_SK,

    CAST(c.SK_ID_PREV AS STRING)                                                AS Card_Account_ID,
    COALESCE(CAST(c.AMT_BALANCE               AS DECIMAL(18,2)), 0.00)         AS AMT_BALANCE,
    CAST(c.AMT_CREDIT_LIMIT_ACTUAL            AS DECIMAL(18,2))                 AS AMT_CREDIT_LIMIT_ACTUAL,
    COALESCE(CAST(c.AMT_PAYMENT_TOTAL_CURRENT AS DECIMAL(18,2)), 0.00)         AS AMT_PAYMENT_TOTAL_CURRENT

FROM bronze_credit_card_balance c
LEFT JOIN dim_contract dc
    ON dc.Contract_Type_Code = 'Revolving loans'
