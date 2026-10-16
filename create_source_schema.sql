-- ============================================================
-- SCHEMA DATABASE NGUỒN (postgres-source) — retail_source
-- Chạy trên container postgres-source (port 5433)
-- ============================================================

DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(150) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'cancelled')),
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

-- BẮT BUỘC: REPLICA IDENTITY FULL để Debezium ghi lại đầy đủ giá trị CŨ
-- khi có UPDATE/DELETE (mặc định Postgres chỉ ghi lại khóa chính).
ALTER TABLE orders REPLICA IDENTITY FULL;
