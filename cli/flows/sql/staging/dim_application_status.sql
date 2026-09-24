-- Dim_Application_Status: Junk dimension từ previous_application
-- Input temp view : bronze_previous_application
-- Output          : Dim_Application_Status

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
) distinct_statuses
