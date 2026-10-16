"""
Chạy toàn bộ phần biến đổi dữ liệu trong 1 tiến trình:
    Bronze -> Silver (incremental, dùng lại run_once của bronze_to_silver.py)
    Silver -> Gold   (làm mới bảng KPI khi Silver có thay đổi)

Cách chạy:
    python run_transforms.py          # chạy lặp mỗi 10 giây (Ctrl+C để dừng)
    python run_transforms.py --once   # chạy 1 lần rồi thoát
"""

import sys
import time

import psycopg2

from bronze_to_silver import DB_CONFIG, INTERVAL_SECONDS
from bronze_to_silver import run_once as bronze_to_silver

# Ngày tính theo giờ Việt Nam (UTC+7, không có giờ mùa hè)
DAILY_SQL = """
INSERT INTO gold.daily_sales
    (sales_date, total_orders, completed_orders, cancelled_orders, pending_orders,
     revenue, avg_order_value, cancel_rate)
SELECT
    ((created_at AT TIME ZONE 'UTC') + INTERVAL '7 hours')::date AS sales_date,
    count(*),
    count(*) FILTER (WHERE status = 'completed'),
    count(*) FILTER (WHERE status = 'cancelled'),
    count(*) FILTER (WHERE status = 'pending'),
    COALESCE(sum(total_amount) FILTER (WHERE status = 'completed'), 0),
    round(avg(total_amount) FILTER (WHERE status = 'completed'), 2),
    round(100.0 * count(*) FILTER (WHERE status = 'cancelled')
          / NULLIF(count(*) FILTER (WHERE status IN ('completed', 'cancelled')), 0), 2)
FROM silver.orders
WHERE NOT is_deleted
GROUP BY 1
"""

PRODUCT_SQL = """
INSERT INTO gold.product_sales
    (product_name, orders_completed, units_sold, revenue, revenue_rank)
SELECT
    product_name,
    count(*),
    sum(quantity),
    sum(total_amount),
    rank() OVER (ORDER BY sum(total_amount) DESC)
FROM silver.orders
WHERE status = 'completed' AND NOT is_deleted
GROUP BY product_name
"""

MINUTE_SQL = """
INSERT INTO gold.minute_sales
    (sales_minute, total_orders, completed_orders, cancelled_orders, revenue)
SELECT
    date_trunc('minute', (created_at AT TIME ZONE 'UTC') + INTERVAL '7 hours'),
    count(*),
    count(*) FILTER (WHERE status = 'completed'),
    count(*) FILTER (WHERE status = 'cancelled'),
    COALESCE(sum(total_amount) FILTER (WHERE status = 'completed'), 0)
FROM silver.orders
WHERE NOT is_deleted
  AND created_at >= now() - INTERVAL '60 minutes'   -- cửa sổ trượt: chỉ giữ 60 phút gần nhất
GROUP BY 1
"""


def refresh_gold(conn):
    """Xóa và dựng lại 2 bảng Gold trong 1 transaction (dashboard không thấy bảng rỗng)."""
    with conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold.daily_sales")
            cur.execute(DAILY_SQL)
            days = cur.rowcount

            cur.execute("DELETE FROM gold.product_sales")
            cur.execute(PRODUCT_SQL)
            products = cur.rowcount

            cur.execute("DELETE FROM gold.minute_sales")
            cur.execute(MINUTE_SQL)
            minutes = cur.rowcount
    print(f"[GOLD] Đã làm mới: {days} ngày, {products} sản phẩm, {minutes} phút")


def run_cycle(conn, force_gold: bool):
    changed = bronze_to_silver(conn)
    if changed or force_gold:
        refresh_gold(conn)


def main():
    once = "--once" in sys.argv
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        if once:
            run_cycle(conn, force_gold=True)
            return

        print(f"=== Bronze -> Silver -> Gold, chạy mỗi {INTERVAL_SECONDS}s (Ctrl+C để dừng) ===\n")
        first = True
        while True:
            run_cycle(conn, force_gold=first)
            first = False
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nĐã dừng.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
