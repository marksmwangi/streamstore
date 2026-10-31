import json

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("🟢 Consumer is listening for messages for orders topic ...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ an error occurred",msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"📦Received order:{order['quantity']} * {order['item']} from {order['user_id']} for id {order['orderid']}")
except KeyboardInterrupt:
    print("\n 🔴 Consumer is stopped by user...")
finally:
    consumer.close()