
-- 2. Junk Dimension Trạng thái & Lý do xét duyệt đơn vay
CREATE TABLE Dim_Application_Status (
    Status_SK           INT PRIMARY KEY,
    Contract_Status     VARCHAR(50) NOT NULL,         -- Approved, Refused, Canceled, Unused
    Reject_Reason       VARCHAR(100) NOT NULL         -- LIMIT, SCOT, XAP...
);
