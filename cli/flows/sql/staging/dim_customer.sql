-- Dim_Customer: SCD Type 2 — incremental MERGE pattern
-- Input temp views : bronze_application, dim_customer_current (nếu tồn tại — xem ghi chú)
-- Output           : Dim_Customer
--
-- SCD Type 2 Strategy:
--   INITIAL LOAD (bảng chưa tồn tại / empty):
--     → Tất cả rows từ bronze, Is_Current_Flag='Y', Expiry_Date='9999-12-31'
--
--   INCREMENTAL LOAD (bảng đã có data):
--     1. Detect changed rows: so sánh attrs với snapshot hiện tại
--        (DAYS_BIRTH, CODE_GENDER, NAME_FAMILY_STATUS, NAME_EDUCATION_TYPE,
--         REGION_RATING_CLIENT, AMT_INCOME_TOTAL, DAYS_EMPLOYED)
--     2. Expire old record:   Is_Current_Flag='N', Expiry_Date=current_timestamp
--     3. Insert new version:  Is_Current_Flag='Y', Effective_Date=current_timestamp
--
-- NOTE: SQL này chạy initial load (write mode=overwrite từ silver_flow).
--       MERGE incremental cần implement ở silver_flow.py khi chuyển sang append mode.
--       Xem: session_handoff_2026-09-23_bronze_silver_simplify.md §5 Known limitations

WITH ranked AS (
    -- Deduplicate: nếu SK_ID_CURR xuất hiện nhiều lần trong bronze, lấy 1 row
    SELECT
        CAST(SK_ID_CURR AS STRING)                                     AS Customer_BK,
        CAST(DAYS_BIRTH AS INT)                                        AS DAYS_BIRTH,
        COALESCE(UPPER(TRIM(CODE_GENDER)),        'XNA')               AS CODE_GENDER,
        COALESCE(TRIM(NAME_FAMILY_STATUS),        'Unknown')           AS NAME_FAMILY_STATUS,
        COALESCE(TRIM(NAME_EDUCATION_TYPE),       'Unknown')           AS NAME_EDUCATION_TYPE,
        CAST(REGION_RATING_CLIENT AS SMALLINT)                         AS REGION_RATING_CLIENT,
        ROUND(CAST(AMT_INCOME_TOTAL AS DECIMAL(18,2)), 2)              AS AMT_INCOME_TOTAL,
        CAST(DAYS_EMPLOYED AS INT)                                     AS DAYS_EMPLOYED,
        row_number() OVER (PARTITION BY SK_ID_CURR ORDER BY SK_ID_CURR) AS rn
    FROM bronze_application
    WHERE SK_ID_CURR IS NOT NULL
)
SELECT
    abs(hash(Customer_BK))                                             AS Customer_SK,
    Customer_BK,
    DAYS_BIRTH,
    CODE_GENDER,
    NAME_FAMILY_STATUS,
    NAME_EDUCATION_TYPE,
    REGION_RATING_CLIENT,
    AMT_INCOME_TOTAL,
    DAYS_EMPLOYED,
    current_timestamp()                                                AS Effective_Date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP)                           AS Expiry_Date,
    'Y'                                                                AS Is_Current_Flag
FROM ranked
WHERE rn = 1
