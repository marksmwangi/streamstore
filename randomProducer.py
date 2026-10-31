
import json
import uuid
import random
import time

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

items = ["mangoes", "bananas", "oranges", "apples", "avocados"]
users = ["Sam-pius", "John", "Mary", "David", "Peter"]

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Delivered: {msg.value().decode('utf-8')} | "
            f"Topic: {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )

try:
    while True:
        order = {
            "orderid": str(uuid.uuid4()),
            "user_id": random.choice(users),
            "item": random.choice(items),
            "quantity": random.randint(1, 10)
        }

        value = json.dumps(order).encode("utf-8")

        producer.produce(
            topic="orders",
            value=value,
            callback=delivery_report
        )

        producer.poll(0)
        print(f"Producing order: {order}")

        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping order generator...")

finally:
    producer.flush()
