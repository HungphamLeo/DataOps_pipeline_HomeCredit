
-- Bảng chiều thời gian
CREATE TABLE dim_date (
    date_skey INT PRIMARY KEY,        -- Khóa thay thế (VD: 20260711)
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month_of_year INT NOT NULL,
    quarter_of_year INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);