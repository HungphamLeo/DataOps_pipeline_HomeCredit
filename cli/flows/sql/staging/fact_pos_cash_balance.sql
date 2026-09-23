-- Fact_POS_CASH_balance: từ POS_CASH_balance
-- Input temp view : bronze_pos_cash_balance
-- Output          : Fact_POS_CASH_balance
--
-- MONTHS_BALANCE âm → last_day(add_months(current_date, n)) → YYYYMMDD

SELECT
    -- Surrogate Key
    abs(hash(concat_ws('_',
        CAST(SK_ID_PREV     AS STRING),
        CAST(MONTHS_BALANCE AS STRING)
    )))                                                                         AS Pos_Cash_SK,

    -- FK → Dim_Customer
    COALESCE(CAST(SK_ID_CURR AS BIGINT), -1)                                    AS Customer_SK,

    -- FK → Dim_Date (tháng báo cáo — cuối tháng)
    COALESCE(
        CAST(date_format(last_day(add_months(current_date(), CAST(MONTHS_BALANCE AS INT))), 'yyyyMMdd') AS INT),
        -1
    )                                                                           AS Month_Date_SK,

    -- FK → Dim_Contract_Type ('Cash loans' — resolve trong silver flow)
    COALESCE(CAST(1 AS INT), -1)                                                AS Contract_Type_SK,

    CAST(SK_ID_PREV AS STRING)                                                  AS Loan_ID,
    CAST(CNT_INSTALMENT        AS SMALLINT)                                     AS CNT_INSTALMENT,
    CAST(CNT_INSTALMENT_FUTURE AS SMALLINT)                                     AS CNT_INSTALMENT_FUTURE,
    COALESCE(CAST(SK_DPD     AS INT), 0)                                        AS SK_DPD,
    COALESCE(CAST(SK_DPD_DEF AS INT), 0)                                        AS SK_DPD_DEF

FROM bronze_pos_cash_balance
