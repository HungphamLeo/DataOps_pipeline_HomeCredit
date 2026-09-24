-- Dim_Contract_Type: từ application.NAME_CONTRACT_TYPE
-- Input temp view : bronze_application
-- Output          : Dim_Contract_Type

SELECT
    CAST(dense_rank() OVER (ORDER BY Contract_Type_Code) AS INT) AS Contract_Type_SK,
    Contract_Type_Code,
    Contract_Type_Code                                            AS Contract_Type_Name
FROM (
    SELECT DISTINCT
        TRIM(NAME_CONTRACT_TYPE) AS Contract_Type_Code
    FROM bronze_application
    WHERE NAME_CONTRACT_TYPE IS NOT NULL
) distinct_types
