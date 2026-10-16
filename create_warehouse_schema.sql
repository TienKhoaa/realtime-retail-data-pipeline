-- ============================================================
-- WAREHOUSE (postgres-warehouse) — retail_warehouse
-- 3 schema theo Medallion Architecture + bảng Bronze lưu event thô
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze: lưu NGUYÊN TRẠNG từng CDC event từ Kafka (không làm sạch, không biến đổi).
-- Mỗi dòng = 1 message Kafka. Cặp (topic, partition, offset) là duy nhất,
-- nên đọc lại cùng message nhiều lần cũng không bị trùng (idempotent).
CREATE TABLE IF NOT EXISTS bronze.orders_raw (
    id BIGSERIAL PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    op CHAR(1),                       -- c = create, u = update, d = delete, r = snapshot read
    payload JSONB NOT NULL,           -- toàn bộ nội dung event (before/after/source/...)
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
