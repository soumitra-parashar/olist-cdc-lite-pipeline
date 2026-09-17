import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    api_version=(2, 5, 0),
)

TOPIC = "order_events"

def publish_event(op_type: str, table_name: str, payload: dict):
    event = {
        "op_type": op_type,
        "table_name": table_name,
        "event_timestamp": payload.get("event_timestamp"),
        "payload": payload,
    }
    future = producer.send(TOPIC, value=event)
    try:
        record_metadata = future.get(timeout=10)
        print(f"Delivered to {record_metadata.topic} partition {record_metadata.partition}offset {record_metadata.offset}")
    except Exception as e:
        print(f"Kafka delivery FAILED: {e}")
    
    producer.flush()