-- Bảng lưu thông tin khách hàng, theo dõi lịch sử thay đổi
CREATE TABLE dim_client (
    client_skey BIGINT PRIMARY KEY,          -- Khóa thay thế (Surrogate Key)
    client_bkey VARCHAR(50) NOT NULL,        -- Khóa nghiệp vụ (Business Key - ID khách hàng duy nhất được tạo ra)

    -- Các thuộc tính áp dụng SCD Type 2
    gender VARCHAR(10),
    income_type VARCHAR(100),
    education_type VARCHAR(100),
    family_status VARCHAR(50),
    housing_type VARCHAR(50),
    occupation_type VARCHAR(100),
    children_count INT,
    family_members_count INT,
    has_car_flag BOOLEAN,
    has_realty_flag BOOLEAN,

    -- Cột theo dõi SCD Type 2
    valid_from_ts TIMESTAMP NOT NULL,        -- Thời điểm phiên bản bắt đầu có hiệu lực
    valid_to_ts TIMESTAMP,                   -- Thời điểm phiên bản hết hiệu lực
    is_current BOOLEAN NOT NULL,             -- Cờ xác định phiên bản hiện tại

    -- Metadata
    source_application_id INT,               -- ID của đơn vay đã tạo ra hoặc cập nhật phiên bản này
    inserted_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ràng buộc để đảm bảo mỗi khách hàng chỉ có một phiên bản `is_current` = true
CREATE UNIQUE INDEX uix_dim_client_bkey_current ON dim_client (client_bkey) WHERE is_current = true;
