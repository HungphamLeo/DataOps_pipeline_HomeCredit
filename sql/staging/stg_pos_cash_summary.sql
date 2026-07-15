-- Aggregate trạng thái POS/CASH hàng tháng theo SK_ID_CURR
SELECT
    SK_ID_CURR,
    COUNT(*)                                     AS POS_MONTHS_COUNT,
    MAX(SK_DPD)                                  AS POS_MAX_DPD,
    AVG(SK_DPD)                                  AS POS_AVG_DPD,
    SUM(CASE WHEN SK_DPD > 0 THEN 1 ELSE 0 END) AS POS_OVERDUE_MONTHS,
    SUM(CASE WHEN SK_DPD > 0 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)                    AS POS_OVERDUE_RATE
FROM bronze_pos_cash_balance
GROUP BY SK_ID_CURR
