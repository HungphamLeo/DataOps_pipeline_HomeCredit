
-- 4. Dimension Khách hàng (Hỗ trợ SCD Type 2)
CREATE TABLE Dim_Customer (
    Customer_SK          BIGINT PRIMARY KEY,          -- Khóa đại diện thay thế (Surrogate Key)
    Customer_BK          VARCHAR(50) NOT NULL,        -- Khóa tự nhiên từ nguồn (SK_ID_CURR)
    DAYS_BIRTH           INT NULL,
    CODE_GENDER          VARCHAR(10) NOT NULL,
    NAME_FAMILY_STATUS   VARCHAR(50) NOT NULL,        -- SCD2
    NAME_EDUCATION_TYPE  VARCHAR(100) NOT NULL,       -- SCD2
    REGION_RATING_CLIENT SMALLINT NULL,               -- SCD2
    AMT_INCOME_TOTAL     DECIMAL(18, 2) NULL,         -- SCD2
    DAYS_EMPLOYED        INT NULL,                    -- SCD2
    Effective_Date       TIMESTAMP NOT NULL,          -- Ngày bắt đầu hiệu lực bản ghi SCD2
    Expiry_Date          TIMESTAMP NOT NULL,          -- Ngày hết hiệu lực (mặc định 9999-12-31)
    Is_Current_Flag      CHAR(1) NOT NULL             -- 'Y' nếu là bản ghi mới nhất, 'N' nếu là lịch sử
);

