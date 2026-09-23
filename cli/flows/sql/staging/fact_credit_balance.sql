-- Fact_Credit_Balance: từ credit_card_balance
-- Input temp view : bronze_credit_card_balance
-- Output          : Fact_Credit_Balance
--
-- MONTHS_BALANCE âm → last_day(add_months(current_date, n)) → YYYYMMDD

SELECT
    -- Surrogate Key
    abs(hash(concat_ws('_',
        CAST(SK_ID_PREV     AS STRING),
        CAST(MONTHS_BALANCE AS STRING)
    )))                                                                         AS Credit_Balance_SK,

    -- FK → Dim_Customer
    COALESCE(CAST(SK_ID_CURR AS BIGINT), -1)                                    AS Customer_SK,

    -- FK → Dim_Date (tháng báo cáo — cuối tháng)
    COALESCE(
        CAST(date_format(last_day(add_months(current_date(), CAST(MONTHS_BALANCE AS INT))), 'yyyyMMdd') AS INT),
        -1
    )                                                                           AS Month_Date_SK,

    -- FK → Dim_Contract_Type (Revolving loans — resolve trong silver flow)
    COALESCE(CAST(1 AS INT), -1)                                                AS Contract_Type_SK,

    CAST(SK_ID_PREV AS STRING)                                                  AS Card_Account_ID,
    COALESCE(CAST(AMT_BALANCE               AS DECIMAL(18,2)), 0.00)           AS AMT_BALANCE,
    CAST(AMT_CREDIT_LIMIT_ACTUAL            AS DECIMAL(18,2))                   AS AMT_CREDIT_LIMIT_ACTUAL,
    COALESCE(CAST(AMT_PAYMENT_TOTAL_CURRENT AS DECIMAL(18,2)), 0.00)           AS AMT_PAYMENT_TOTAL_CURRENT

FROM bronze_credit_card_balance
