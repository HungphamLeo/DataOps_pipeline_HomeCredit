-- 3. Dimension Loại hình hợp đồng tín dụng
CREATE TABLE Dim_Contract_Type (
    Contract_Type_SK    INT PRIMARY KEY,
    Contract_Type_Code  VARCHAR(50) NOT NULL,         -- Cash loans, Revolving loans...
    Contract_Type_Name  VARCHAR(100) NOT NULL
);