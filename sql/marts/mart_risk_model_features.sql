SELECT
    app.*,
    bureau.BUREAU_LOAN_COUNT,
    bureau.BUREAU_ACTIVE_COUNT,
    bureau.BUREAU_CLOSED_COUNT,
    bureau.BUREAU_MAX_OVERDUE,
    bureau.BUREAU_TOTAL_DEBT,
    bureau.BUREAU_TOTAL_CREDIT,
    bureau.BUREAU_AVG_DPD,
    bureau.BUREAU_AVG_BAD_STATUS_RATE,
    prev.PREV_APP_COUNT,
    prev.PREV_APPROVED_COUNT,
    prev.PREV_REFUSED_COUNT,
    prev.PREV_APPROVAL_RATE,
    prev.PREV_AVG_CREDIT,
    prev.PREV_MAX_CREDIT,
    inst.INSTALMENT_COUNT,
    inst.INSTALMENT_LATE_COUNT,
    inst.INSTALMENT_LATE_RATE,
    inst.INSTALMENT_AVG_DELAY,
    inst.INSTALMENT_AMT_SHORTFALL,
    pos.POS_MONTHS_COUNT,
    pos.POS_MAX_DPD,
    pos.POS_AVG_DPD,
    pos.POS_OVERDUE_MONTHS,
    pos.POS_OVERDUE_RATE,
    cc.CC_MONTHS_COUNT,
    cc.CC_AVG_BALANCE,
    cc.CC_MAX_BALANCE,
    cc.CC_AVG_UTILIZATION,
    cc.CC_AVG_PAYMENT_RATIO,
    cc.CC_MAX_DPD
FROM stg_application AS app
LEFT JOIN stg_bureau_summary AS bureau ON app.SK_ID_CURR = bureau.SK_ID_CURR
LEFT JOIN stg_prev_application_summary AS prev ON app.SK_ID_CURR = prev.SK_ID_CURR
LEFT JOIN stg_installment_summary AS inst ON app.SK_ID_CURR = inst.SK_ID_CURR
LEFT JOIN stg_pos_cash_summary AS pos ON app.SK_ID_CURR = pos.SK_ID_CURR
LEFT JOIN stg_credit_card_summary AS cc ON app.SK_ID_CURR = cc.SK_ID_CURR