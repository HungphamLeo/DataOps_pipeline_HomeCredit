-- Aggregate lịch sử đơn vay trước theo SK_ID_CURR
SELECT
    SK_ID_CURR,
    COUNT(SK_ID_PREV)                                           AS PREV_APP_COUNT,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Approved' THEN 1 ELSE 0 END) AS PREV_APPROVED_COUNT,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Refused'  THEN 1 ELSE 0 END) AS PREV_REFUSED_COUNT,
    SUM(CASE WHEN NAME_CONTRACT_STATUS = 'Approved' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(SK_ID_PREV), 0)                         AS PREV_APPROVAL_RATE,
    AVG(AMT_CREDIT)                                            AS PREV_AVG_CREDIT,
    MAX(AMT_CREDIT)                                            AS PREV_MAX_CREDIT,
    AVG(AMT_ANNUITY)                                           AS PREV_AVG_ANNUITY
FROM bronze_previous_application
GROUP BY SK_ID_CURR
