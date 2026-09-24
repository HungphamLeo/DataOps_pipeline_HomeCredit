-- Fact_POS_CASH_balance: từ POS_CASH_balance join Dim_Contract_Type inline
-- Input temp views : bronze_pos_cash_balance, bronze_application
-- Output           : Fact_POS_CASH_balance
--
-- MONTHS_BALANCE âm → last_day(add_months(current_date, n)) → YYYYMMDD
-- Contract_Type_SK : 'Cash loans' — resolved bằng JOIN vào dim_contract CTE

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
        CAST(p.SK_ID_PREV     AS STRING),
        CAST(p.MONTHS_BALANCE AS STRING)
    )))                                                                         AS Pos_Cash_SK,

    -- FK → Dim_Customer
    COALESCE(CAST(p.SK_ID_CURR AS BIGINT), -1)                                  AS Customer_SK,

    -- FK → Dim_Date (tháng báo cáo — cuối tháng)
    COALESCE(
        CAST(date_format(last_day(add_months(current_date(), CAST(p.MONTHS_BALANCE AS INT))), 'yyyyMMdd') AS INT),
        -1
    )                                                                           AS Month_Date_SK,

    -- FK → Dim_Contract_Type (resolved via JOIN — 'Cash loans')
    COALESCE(dc.Contract_Type_SK, -1)                                           AS Contract_Type_SK,

    CAST(p.SK_ID_PREV AS STRING)                                                AS Loan_ID,
    CAST(p.CNT_INSTALMENT        AS SMALLINT)                                   AS CNT_INSTALMENT,
    CAST(p.CNT_INSTALMENT_FUTURE AS SMALLINT)                                   AS CNT_INSTALMENT_FUTURE,
    COALESCE(CAST(p.SK_DPD     AS INT), 0)                                      AS SK_DPD,
    COALESCE(CAST(p.SK_DPD_DEF AS INT), 0)                                      AS SK_DPD_DEF

FROM bronze_pos_cash_balance p
LEFT JOIN dim_contract dc
    ON dc.Contract_Type_Code = 'Cash loans'
