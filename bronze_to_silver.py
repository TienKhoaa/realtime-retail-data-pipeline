"""
Job Bronze -> Silver (incremental).

Mỗi lần chạy:
    1. Đọc watermark (Bronze đã xử lý tới id nào)
    2. Lấy các event MỚI trong bronze.orders_raw
    3. Với mỗi đơn hàng, chọn event mới nhất, giải mã giá/thời gian
    4. Upsert vào silver.orders và cập nhật watermark (cùng 1 transaction)

Cách chạy:
    python bronze_to_silver.py          # chạy lặp mỗi 10 giây (Ctrl+C để dừng)
    python bronze_to_silver.py --once   # chạy 1 lần rồi thoát
"""

import sys
import time

import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": "5434",  # cổng của postgres-warehouse đã map ra ngoài
    "database": "retail_warehouse",
    "user": "dw_user",
    "password": "dw_pass",
}

JOB_NAME = "bronze_to_silver"
INTERVAL_SECONDS = 10

UPSERT_SQL = """
WITH events AS (
    SELECT id, kafka_offset, op,
           COALESCE(payload->'after', payload->'before') AS r   -- DELETE chỉ có 'before'
    FROM bronze.orders_raw
    WHERE id > %(from_id)s AND id <= %(to_id)s
),
latest AS (
    SELECT DISTINCT ON ((r->>'order_id')::int) *
    FROM events
    ORDER BY (r->>'order_id')::int, kafka_offset DESC
)
INSERT INTO silver.orders
    (order_id, customer_name, product_name, quantity, unit_price, status,
     created_at, updated_at, is_deleted, last_op, last_kafka_offset)
SELECT
    (r->>'order_id')::int,
    r->>'customer_name',
    r->>'product_name',
    (r->>'quantity')::int,
    silver.decode_debezium_decimal(r->>'unit_price', 2),
    r->>'status',
    to_timestamp((r->>'created_at')::bigint / 1000000.0),   -- micro giây -> timestamp
    to_timestamp((r->>'updated_at')::bigint / 1000000.0),
    (op = 'd'),
    op,
    kafka_offset
FROM latest
ON CONFLICT (order_id) DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    product_name = EXCLUDED.product_name,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    status = EXCLUDED.status,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    is_deleted = EXCLUDED.is_deleted,
    last_op = EXCLUDED.last_op,
    last_kafka_offset = EXCLUDED.last_kafka_offset,
    silver_updated_at = now()
WHERE EXCLUDED.last_kafka_offset > silver.orders.last_kafka_offset  -- không để event cũ đè event mới
"""


def run_once(conn) -> int:
    """Xử lý phần Bronze mới. Trả về số dòng Silver được thêm/cập nhật."""
    with conn:  # tự commit nếu thành công, rollback nếu lỗi
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO silver.etl_watermark (job_name) VALUES (%s) ON CONFLICT DO NOTHING",
                (JOB_NAME,),
            )
            cur.execute(
                "SELECT last_bronze_id FROM silver.etl_watermark WHERE job_name = %s FOR UPDATE",
                (JOB_NAME,),
            )
            from_id = cur.fetchone()[0]

            cur.execute("SELECT COALESCE(max(id), 0) FROM bronze.orders_raw")
            to_id = cur.fetchone()[0]

            if to_id <= from_id:
                return 0

            cur.execute(UPSERT_SQL, {"from_id": from_id, "to_id": to_id})
            affected = cur.rowcount

            cur.execute(
                "UPDATE silver.etl_watermark SET last_bronze_id = %s, updated_at = now() "
                "WHERE job_name = %s",
                (to_id, JOB_NAME),
            )
            print(f"[SILVER] Bronze id {from_id} -> {to_id}: {affected} dòng được cập nhật")
            return affected


def main():
    once = "--once" in sys.argv
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        if once:
            if run_once(conn) == 0:
                print("[SILVER] Không có event mới.")
            return

        print(f"=== Bronze -> Silver, chạy mỗi {INTERVAL_SECONDS}s (Ctrl+C để dừng) ===\n")
        while True:
            run_once(conn)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nĐã dừng.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
