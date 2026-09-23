-- Fact_Loan_Application: từ application left join previous_application
-- Input temp views : bronze_application, bronze_previous_application
-- Output           : Fact_Loan_Application

SELECT
    -- Surrogate Key
    abs(hash(CAST(a.SK_ID_CURR AS STRING)))                                     AS Application_SK,

    -- FK → Dim_Customer (lookup via Customer_BK = SK_ID_CURR, Is_Current_Flag='Y')
    -- Silver flow sẽ resolve sau khi Dim_Customer được write; dùng SK_ID_CURR làm proxy
    COALESCE(CAST(a.SK_ID_CURR AS BIGINT), -1)                                  AS Customer_SK,

    -- FK → Dim_Date (DAYS_DECISION từ previous_application)
    COALESCE(
        CAST(date_format(date_add(DATE'2020-01-01', CAST(pa.DAYS_DECISION AS INT)), 'yyyyMMdd') AS INT),
        -1
    )                                                                            AS Decision_Date_SK,

    -- FK → Dim_Contract_Type (resolve bằng Contract_Type_Code)
    COALESCE(CAST(1 AS INT), -1)                                                AS Contract_Type_SK,

    -- FK → Dim_Application_Status
    COALESCE(CAST(1 AS INT), -1)                                                AS Status_SK,

    -- Degenerate dims
    COALESCE(CAST(pa.SK_ID_PREV AS STRING), CAST(a.SK_ID_CURR AS STRING))       AS Loan_ID,

    -- Measures
    CAST(pa.AMT_APPLICATION AS DECIMAL(18,2))                                   AS AMT_APPLICATION,
    COALESCE(CAST(a.AMT_CREDIT AS DECIMAL(18,2)), 0.00)                        AS AMT_CREDIT,
    CAST(a.AMT_ANNUITY AS DECIMAL(18,2))                                        AS AMT_ANNUITY,
    CAST(a.AMT_GOODS_PRICE AS DECIMAL(18,2))                                    AS AMT_GOODS_PRICE,
    CAST(a.TARGET AS SMALLINT)                                                  AS TARGET

FROM bronze_application a
LEFT JOIN bronze_previous_application pa
    ON a.SK_ID_CURR = pa.SK_ID_CURR
