-- Fact_Bureau_Credit: từ bureau
-- Input temp view : bronze_bureau
-- Output          : Fact_Bureau_Credit

SELECT
    -- Surrogate Key
    abs(hash(CAST(SK_ID_BUREAU AS STRING)))                                     AS Bureau_Credit_SK,

    -- FK → Dim_Customer
    COALESCE(CAST(SK_ID_CURR AS BIGINT), -1)                                    AS Customer_SK,

    -- FK → Dim_Date (ngày mở khoản tín dụng)
    COALESCE(
        CAST(date_format(date_add(DATE'2020-01-01', CAST(DAYS_CREDIT AS INT)), 'yyyyMMdd') AS INT),
        -1
    )                                                                            AS Credit_Date_SK,

    CAST(SK_ID_BUREAU AS STRING)                                                AS Bureau_ID,
    COALESCE(TRIM(CREDIT_ACTIVE), 'Unknown')                                    AS CREDIT_ACTIVE,
    COALESCE(TRIM(CREDIT_TYPE),   'Unknown')                                    AS CREDIT_TYPE,
    CAST(AMT_CREDIT_SUM          AS DECIMAL(18,2))                              AS AMT_CREDIT_SUM,
    CAST(AMT_CREDIT_MAX_OVERDUE  AS DECIMAL(18,2))                              AS AMT_CREDIT_MAX_OVERDUE

FROM bronze_bureau
