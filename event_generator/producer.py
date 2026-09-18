import json
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

TOPIC = "orders-events"

def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery FAILED: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset{msg.offset()}")

def publish_event(op_type: str, table_name: str, payload: dict):
    event = {
        "op_type": op_type,
        "table_name": table_name,
        "event_timestamp": payload.get("event_timestamp"),
        "payload": payload,
    }
    producer.produce(
        TOPIC,
        value=json.dumps(event, default=str).encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush(10)