-- Dim_Date: Generated from date range, no Bronze source
-- Registered temp view: date_range (produced by silver_flow._build_dim_date)
-- Input  : date_range (columns: Full_Date DATE)
-- Output : Dim_Date

SELECT
    CAST(date_format(Full_Date, 'yyyyMMdd') AS INT)     AS Date_SK,
    Full_Date,
    CAST(dayofmonth(Full_Date) AS TINYINT)              AS Day_of_Month,
    CAST(month(Full_Date)      AS TINYINT)              AS Month_Number,
    date_format(Full_Date, 'MMMM')                      AS Month_Name,
    CAST(quarter(Full_Date)    AS TINYINT)              AS Quarter_Number,
    CAST(year(Full_Date)       AS SMALLINT)             AS Year_Number,
    (dayofweek(Full_Date) IN (1, 7))                    AS Is_Weekend
FROM date_range
