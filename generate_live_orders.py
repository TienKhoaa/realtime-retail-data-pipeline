"""
Script giả lập luồng đơn hàng liên tục phát sinh theo thời gian thực,
ghi vào database nguồn (postgres-source) để Debezium bắt CDC.

Mô phỏng 3 loại sự kiện thật:
    - INSERT: đơn hàng mới được tạo (status='pending')
    - UPDATE: đơn hàng được xử lý xong (status='completed') hoặc bị hủy
    - (DELETE không dùng vì ít gặp trong thực tế bán lẻ)

Yêu cầu:
    pip install psycopg2-binary faker

Cách chạy:
    python generate_live_orders.py
"""

import random
import time
from datetime import datetime

import psycopg2
from faker import Faker

DB_CONFIG = {
    "host": "localhost",
    "port": "5433",  # cổng của postgres-source đã map ra ngoài
    "database": "retail_source",
    "user": "source_user",
    "password": "source_pass",
}

fake = Faker("vi_VN")

PRODUCTS = [
    ("Tai nghe bluetooth", 350000),
    ("Áo thun nam", 150000),
    ("Nồi cơm điện", 890000),
    ("Cà phê hòa tan", 65000),
    ("Kem dưỡng da", 220000),
    ("Sổ tay ghi chú", 35000),
    ("Bàn phím cơ", 650000),
    ("Giày sneaker nam", 780000),
]

# Đơn hàng đang "pending" chờ được cập nhật trạng thái (mô phỏng quy trình xử lý đơn thật)
pending_order_ids: list[int] = []


def create_new_order(cur) -> int:
    """Tạo 1 đơn hàng mới (INSERT), trạng thái ban đầu là 'pending'."""
    customer_name = fake.name()
    product_name, unit_price = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    cur.execute(
        """INSERT INTO orders (customer_name, product_name, quantity, unit_price, status)
           VALUES (%s, %s, %s, %s, 'pending') RETURNING order_id""",
        (customer_name, product_name, quantity, unit_price),
    )
    order_id = cur.fetchone()[0]
    print(f"[INSERT] Đơn hàng mới #{order_id}: {customer_name} mua {quantity}x {product_name}")
    return order_id


def update_order_status(cur, order_id: int):
    """Cập nhật trạng thái đơn hàng đang pending thành completed hoặc cancelled (UPDATE)."""
    new_status = random.choices(["completed", "cancelled"], weights=[85, 15])[0]
    cur.execute(
        """UPDATE orders SET status = %s, updated_at = now()
           WHERE order_id = %s""",
        (new_status, order_id),
    )
    print(f"[UPDATE] Đơn hàng #{order_id} -> {new_status}")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print("=== Bắt đầu giả lập luồng đơn hàng real-time (Ctrl+C để dừng) ===\n")

    try:
        while True:
            # 70% khả năng tạo đơn mới, 30% khả năng cập nhật 1 đơn đang chờ
            if pending_order_ids and random.random() < 0.3:
                order_id = pending_order_ids.pop(
                    random.randrange(len(pending_order_ids))
                )
                update_order_status(cur, order_id)
            else:
                order_id = create_new_order(cur)
                pending_order_ids.append(order_id)

            # Giới hạn danh sách chờ để không phình to vô hạn
            if len(pending_order_ids) > 50:
                pending_order_ids.pop(0)

            time.sleep(random.uniform(1, 3))  # mô phỏng đơn hàng đến ngẫu nhiên mỗi 1-3 giây

    except KeyboardInterrupt:
        print("\nĐã dừng giả lập.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
