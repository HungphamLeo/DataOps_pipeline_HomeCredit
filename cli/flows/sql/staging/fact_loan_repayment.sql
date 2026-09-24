-- Fact_Loan_Repayment: từ installments_payments join previous_application
-- Input temp views : bronze_installments_payments, bronze_previous_application
-- Output           : Fact_Loan_Repayment

SELECT
    -- Surrogate Key: hash(SK_ID_PREV + NUM_INSTALMENT_NUMBER)
    abs(hash(concat_ws('_',
        CAST(ip.SK_ID_PREV              AS STRING),
        CAST(ip.NUM_INSTALMENT_NUMBER   AS STRING)
    )))                                                                         AS Repayment_SK,

    -- FK → Dim_Customer via previous_application.SK_ID_CURR
    COALESCE(CAST(pa.SK_ID_CURR AS BIGINT), -1)                                AS Customer_SK,

    -- FK → Dim_Date (hạn trả)
    COALESCE(
        CAST(date_format(date_add(DATE'2020-01-01', CAST(ip.DAYS_INSTALMENT AS INT)), 'yyyyMMdd') AS INT),
        -1
    )                                                                           AS Due_Date_SK,

    -- FK → Dim_Date (ngày trả thực tế)
    COALESCE(
        CAST(date_format(date_add(DATE'2020-01-01', CAST(ip.DAYS_ENTRY_PAYMENT AS INT)), 'yyyyMMdd') AS INT),
        -1
    )                                                                           AS Payment_Date_SK,

    CAST(ip.SK_ID_PREV AS STRING)                                               AS Loan_ID,
    CAST(ip.NUM_INSTALMENT_NUMBER AS INT)                                       AS Instalment_Number,
    COALESCE(CAST(ip.AMT_INSTALMENT AS DECIMAL(18,2)), 0.00)                   AS AMT_INSTALMENT,
    COALESCE(CAST(ip.AMT_PAYMENT    AS DECIMAL(18,2)), 0.00)                   AS AMT_PAYMENT,

    -- Derived measures
    COALESCE(CAST(ip.AMT_INSTALMENT AS DECIMAL(18,2)), 0.0)
        - COALESCE(CAST(ip.AMT_PAYMENT AS DECIMAL(18,2)), 0.0)                 AS Underpaid_Amount,

    CASE
        WHEN ip.DAYS_ENTRY_PAYMENT > ip.DAYS_INSTALMENT
            THEN CAST(ip.DAYS_ENTRY_PAYMENT - ip.DAYS_INSTALMENT AS INT)
        ELSE 0
    END                                                                         AS Days_Past_Due

FROM bronze_installments_payments ip
LEFT JOIN bronze_previous_application pa
    ON ip.SK_ID_PREV = pa.SK_ID_PREV
