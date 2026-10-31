import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)
order = {
    "orderid": str(uuid.uuid4()),
    "user_id": "Sam-pius",
    "item": "mangoes",
    "quantity":1
}
def delivery_report(err, msg):
    if err:
        print(f"❌❌❌Message delivery failed: {err}")
    else:
        print(f"✅Message delivered to {msg.value().decode("utf-8")} message")
        print(f"topic: {msg.topic()} partition: {msg.partition()} offset: {msg.offset()}")
value = json.dumps(order).encode("utf-8")
producer.produce(
    topic="orders",
    value=value,
    callback= delivery_report)
producer.flush()