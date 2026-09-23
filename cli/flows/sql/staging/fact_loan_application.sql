-- Fact_Loan_Application: từ application left join previous_application
-- Input temp views : bronze_application, bronze_previous_application,
--                    bronze_dim_contract_type, bronze_dim_application_status
-- Output           : Fact_Loan_Application
--
-- Contract_Type_SK : resolve bằng JOIN vào Dim_Contract_Type (built inline từ bronze)
-- Status_SK        : resolve bằng JOIN vào Dim_Application_Status (built inline từ bronze)

WITH dim_contract AS (
    SELECT
        CAST(dense_rank() OVER (ORDER BY Contract_Type_Code) AS INT) AS Contract_Type_SK,
        Contract_Type_Code
    FROM (
        SELECT DISTINCT TRIM(NAME_CONTRACT_TYPE) AS Contract_Type_Code
        FROM bronze_application
        WHERE NAME_CONTRACT_TYPE IS NOT NULL
    ) t
),
dim_status AS (
    SELECT
        CAST(dense_rank() OVER (ORDER BY Contract_Status, Reject_Reason) AS INT) AS Status_SK,
        Contract_Status,
        Reject_Reason
    FROM (
        SELECT DISTINCT
            COALESCE(TRIM(NAME_CONTRACT_STATUS), 'Unknown') AS Contract_Status,
            COALESCE(TRIM(CODE_REJECT_REASON),   'XAP')     AS Reject_Reason
        FROM bronze_previous_application
        WHERE NAME_CONTRACT_STATUS IS NOT NULL
           OR CODE_REJECT_REASON   IS NOT NULL
    ) t
)
SELECT
    -- Surrogate Key
    abs(hash(CAST(a.SK_ID_CURR AS STRING)))                                     AS Application_SK,

    -- FK → Dim_Customer (Customer_BK = SK_ID_CURR)
    COALESCE(CAST(a.SK_ID_CURR AS BIGINT), -1)                                  AS Customer_SK,

    -- FK → Dim_Date (DAYS_DECISION từ previous_application)
    COALESCE(
        CAST(date_format(date_add(DATE'2020-01-01', CAST(pa.DAYS_DECISION AS INT)), 'yyyyMMdd') AS INT),
        -1
    )                                                                            AS Decision_Date_SK,

    -- FK → Dim_Contract_Type (resolved via JOIN)
    COALESCE(dc.Contract_Type_SK, -1)                                           AS Contract_Type_SK,

    -- FK → Dim_Application_Status (resolved via JOIN)
    COALESCE(ds.Status_SK, -1)                                                  AS Status_SK,

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
LEFT JOIN dim_contract dc
    ON TRIM(a.NAME_CONTRACT_TYPE) = dc.Contract_Type_Code
LEFT JOIN dim_status ds
    ON COALESCE(TRIM(pa.NAME_CONTRACT_STATUS), 'Unknown') = ds.Contract_Status
   AND COALESCE(TRIM(pa.CODE_REJECT_REASON),   'XAP')    = ds.Reject_Reason
