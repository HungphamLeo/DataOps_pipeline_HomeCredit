-- Aggregate hành vi thẻ tín dụng hàng tháng theo SK_ID_CURR
SELECT
    SK_ID_CURR,
    COUNT(*)                                                                    AS CC_MONTHS_COUNT,
    AVG(AMT_BALANCE)                                                            AS CC_AVG_BALANCE,
    MAX(AMT_BALANCE)                                                            AS CC_MAX_BALANCE,
    AVG(AMT_BALANCE / NULLIF(AMT_CREDIT_LIMIT_ACTUAL, 0))                      AS CC_AVG_UTILIZATION,
    AVG(AMT_PAYMENT_TOTAL_CURRENT / NULLIF(AMT_INST_MIN_REGULARITY, 0))        AS CC_AVG_PAYMENT_RATIO,
    MAX(SK_DPD)                                                                 AS CC_MAX_DPD
FROM bronze_credit_card_balance
GROUP BY SK_ID_CURR
