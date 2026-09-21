
-- 4. Fact Trạng thái nợ CIC định kỳ theo tháng (Periodic Snapshot Fact)
CREATE TABLE Fact_Bureau_Monthly_Snapshot (
    Bureau_Snapshot_SK  BIGINT PRIMARY KEY,
    Month_Date_SK       INT NOT NULL REFERENCES Dim_Date(Date_SK),
    Bureau_ID           VARCHAR(50) NOT NULL,          -- Degenerate Dim nối về Fact_Bureau_Credit
    STATUS_CODE         CHAR(2) NOT NULL,              -- C, 0, 1, 2, 3, 4, 5
    MONTHS_BALANCE      SMALLINT NOT NULL
);