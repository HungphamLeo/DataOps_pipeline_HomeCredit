-- Tập hợp subset columns business-friendly + tính RISK_TIER
SELECT
    app.SK_ID_CURR,
    app.TARGET,
    app.AGE_YEARS,
    app.NAME_INCOME_TYPE,
    app.NAME_EDUCATION_TYPE,
    app.NAME_FAMILY_STATUS,
    app.NAME_HOUSING_TYPE,
    app.AMT_INCOME_TOTAL,
    app.AMT_CREDIT,
    app.INCOME_CREDIT_RATIO,
    app.EXT_SOURCE_1,
    app.EXT_SOURCE_2,
    app.EXT_SOURCE_3,
    bureau.BUREAU_LOAN_COUNT,
    bureau.BUREAU_MAX_OVERDUE,
    bureau.BUREAU_AVG_BAD_STATUS_RATE,
    inst.INSTALMENT_LATE_RATE,
    pos.POS_MAX_DPD,
    cc.CC_AVG_UTILIZATION,
    CASE
        WHEN app.EXT_SOURCE_2 < 0.3  AND bureau.BUREAU_MAX_OVERDUE > 0       THEN 'High'
        WHEN app.EXT_SOURCE_2 > 0.5  AND bureau.BUREAU_AVG_BAD_STATUS_RATE < 0.1 THEN 'Low'
        ELSE 'Medium'
    END AS RISK_TIER
FROM stg_application AS app
LEFT JOIN stg_bureau_summary            AS bureau ON app.SK_ID_CURR = bureau.SK_ID_CURR
LEFT JOIN stg_installment_summary       AS inst   ON app.SK_ID_CURR = inst.SK_ID_CURR
LEFT JOIN stg_pos_cash_summary          AS pos    ON app.SK_ID_CURR = pos.SK_ID_CURR
LEFT JOIN stg_credit_card_summary       AS cc     ON app.SK_ID_CURR = cc.SK_ID_CURR
