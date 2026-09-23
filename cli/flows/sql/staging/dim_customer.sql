-- Dim_Customer: SCD Type 2 — full refresh (initial load, Is_Current_Flag = 'Y' for all)
-- Input temp view : bronze_application
-- Output          : Dim_Customer

SELECT
    monotonically_increasing_id()                                      AS Customer_SK,
    CAST(SK_ID_CURR AS STRING)                                         AS Customer_BK,
    CAST(DAYS_BIRTH AS INT)                                            AS DAYS_BIRTH,
    COALESCE(UPPER(TRIM(CODE_GENDER)),        'XNA')                   AS CODE_GENDER,
    COALESCE(TRIM(NAME_FAMILY_STATUS),        'Unknown')               AS NAME_FAMILY_STATUS,
    COALESCE(TRIM(NAME_EDUCATION_TYPE),       'Unknown')               AS NAME_EDUCATION_TYPE,
    CAST(REGION_RATING_CLIENT AS SMALLINT)                             AS REGION_RATING_CLIENT,
    ROUND(CAST(AMT_INCOME_TOTAL AS DECIMAL(18,2)), 2)                  AS AMT_INCOME_TOTAL,
    CAST(DAYS_EMPLOYED AS INT)                                         AS DAYS_EMPLOYED,
    current_timestamp()                                                AS Effective_Date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP)                           AS Expiry_Date,
    'Y'                                                                AS Is_Current_Flag
FROM bronze_application
