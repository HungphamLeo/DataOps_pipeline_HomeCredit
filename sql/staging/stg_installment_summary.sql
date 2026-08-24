-- Aggregate hành vi thanh toán theo SK_ID_CURR
SELECT
    SK_ID_CURR,
    COUNT(*)                                                            AS INSTALMENT_COUNT,
    SUM(CASE WHEN DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT THEN 1 ELSE 0 END) AS INSTALMENT_LATE_COUNT,
    SUM(CASE WHEN DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)                                           AS INSTALMENT_LATE_RATE,
    AVG(
        CASE WHEN DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT
             THEN DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT
             ELSE NULL
        END
    )                                                                   AS INSTALMENT_AVG_DELAY_DAYS,
    SUM(
        CASE WHEN AMT_INSTALMENT > AMT_PAYMENT
             THEN AMT_INSTALMENT - AMT_PAYMENT
             ELSE 0
        END
    )                                                                   AS INSTALMENT_AMT_SHORTFALL
FROM bronze_installments_payments
GROUP BY SK_ID_CURR
