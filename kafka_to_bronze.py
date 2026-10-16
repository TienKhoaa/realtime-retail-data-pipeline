"""
Consumer: đọc CDC events từ Kafka topic retail.public.orders
và ghi NGUYÊN TRẠNG vào bronze.orders_raw (postgres-warehouse).

Yêu cầu:
    pip install confluent-kafka psycopg2-binary

Cách chạy:
    python kafka_to_bronze.py        (Ctrl+C để dừng)
"""

import json

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import Json

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "bronze-writer",
    "auto.offset.reset": "earliest",   # lần đầu chạy: đọc từ message đầu tiên
    "enable.auto.commit": False,       # chỉ commit offset SAU KHI đã ghi DB thành công
}
TOPIC = "retail.public.orders"

DB_CONFIG = {
    "host": "localhost",
    "port": "5434",  # cổng của postgres-warehouse đã map ra ngoài
    "database": "retail_warehouse",
    "user": "dw_user",
    "password": "dw_pass",
}

INSERT_SQL = """
    INSERT INTO bronze.orders_raw
        (kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, op, payload)
    VALUES (%s, %s, %s, to_timestamp(%s / 1000.0), %s, %s)
    ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
"""


def extract_payload(value: dict) -> dict:
    """Debezium có thể bọc event trong {"schema":..., "payload":...} — lấy phần payload nếu có."""
    return value.get("payload", value)


def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print(f"=== Đang lắng nghe topic '{TOPIC}' -> bronze.orders_raw (Ctrl+C để dừng) ===\n")
    count = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("Lỗi Kafka:", msg.error())
                continue
            if msg.value() is None:   # tombstone sau DELETE, bỏ qua
                consumer.commit(message=msg)
                continue

            event = extract_payload(json.loads(msg.value().decode("utf-8")))
            _, ts_ms = msg.timestamp()

            cur.execute(
                INSERT_SQL,
                (msg.topic(), msg.partition(), msg.offset(), ts_ms, event.get("op"), Json(event)),
            )
            consumer.commit(message=msg)

            count += 1
            print(f"[BRONZE] #{count} offset={msg.offset()} op={event.get('op')}")

    except KeyboardInterrupt:
        print(f"\nĐã dừng. Tổng số event đã ghi trong phiên này: {count}")
    finally:
        consumer.close()
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
