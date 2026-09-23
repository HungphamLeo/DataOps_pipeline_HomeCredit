-- Fact_Bureau_Monthly_Snapshot: từ bureau_balance
-- Input temp view : bronze_bureau_balance
-- Output          : Fact_Bureau_Monthly_Snapshot
--
-- MONTHS_BALANCE là số âm tương đối (-1 = tháng trước, -2 = 2 tháng trước...).
-- Quy đổi về ngày cuối tháng: add_months(current_date, MONTHS_BALANCE) → last_day → YYYYMMDD

SELECT
    -- Surrogate Key
    abs(hash(concat_ws('_',
        CAST(SK_BUREAU_ID    AS STRING),
        CAST(MONTHS_BALANCE  AS STRING)
    )))                                                                         AS Bureau_Snapshot_SK,

    -- FK → Dim_Date (tháng báo cáo — cuối tháng)
    COALESCE(
        CAST(date_format(last_day(add_months(current_date(), CAST(MONTHS_BALANCE AS INT))), 'yyyyMMdd') AS INT),
        -1
    )                                                                           AS Month_Date_SK,

    CAST(SK_BUREAU_ID AS STRING)                                                AS Bureau_ID,
    COALESCE(TRIM(STATUS), 'X')                                                 AS STATUS_CODE,
    CAST(MONTHS_BALANCE AS SMALLINT)                                            AS MONTHS_BALANCE

FROM bronze_bureau_balance
