-- ============================================================
-- TẦNG SILVER (postgres-warehouse) — dữ liệu đã làm sạch
-- Chạy sau create_warehouse_schema.sql
-- ============================================================

-- Debezium mã hóa NUMERIC thành chuỗi base64 (số nguyên big-endian, two's complement).
-- Giá trị thật = số nguyên / 10^scale. Với cột NUMERIC(10,2) thì scale = 2.
CREATE OR REPLACE FUNCTION silver.decode_debezium_decimal(encoded TEXT, scale INT)
RETURNS NUMERIC AS $$
DECLARE
    raw BYTEA;
    nbytes INT;
    val NUMERIC;
BEGIN
    IF encoded IS NULL THEN
        RETURN NULL;
    END IF;

    raw := decode(encoded, 'base64');
    nbytes := length(raw);

    IF nbytes > 7 THEN
        RAISE EXCEPTION 'decimal too large to decode (% bytes)', nbytes;
    END IF;

    val := ('x' || lpad(encode(raw, 'hex'), 16, '0'))::bit(64)::bigint;

    -- bit đầu của byte đầu tiên = 1 nghĩa là số âm
    IF get_byte(raw, 0) >= 128 THEN
        val := val - power(2::numeric, (8 * nbytes)::numeric);
    END IF;

    RETURN val / power(10::numeric, scale::numeric);
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- Silver: mỗi đơn hàng đúng 1 dòng, luôn là trạng thái MỚI NHẤT.
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id INT PRIMARY KEY,
    customer_name TEXT,
    product_name TEXT,
    quantity INT,
    unit_price NUMERIC(10, 2),
    total_amount NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    last_op CHAR(1),
    last_kafka_offset BIGINT,
    silver_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Watermark: nhớ Bronze đã xử lý tới id nào, lần sau chỉ xử lý phần mới (incremental).
CREATE TABLE IF NOT EXISTS silver.etl_watermark (
    job_name TEXT PRIMARY KEY,
    last_bronze_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
