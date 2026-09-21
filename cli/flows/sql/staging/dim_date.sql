-- 1. Dimension Ngày tháng (Role-Playing Dimension)
CREATE TABLE Dim_Date (
    Date_SK             INT PRIMARY KEY,              -- Định dạng YYYYMMDD (ví dụ: 20260921)
    Full_Date           DATE NOT NULL,
    Day_of_Month        SMALLINT NOT NULL,
    Month_Number        SMALLINT NOT NULL,
    Month_Name          VARCHAR(15) NOT NULL,
    Quarter_Number      SMALLINT NOT NULL,
    Year_Number         SMALLINT NOT NULL,
    Is_Weekend          BOOLEAN NOT NULL
);
