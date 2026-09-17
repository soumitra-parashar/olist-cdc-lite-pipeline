import uuid
from datetime import datetime, UTC
from db import engine, orders
from producer import publish_event

def insert_new_order():
    order_id = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    
    
    row = {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": "created",
        "order_purchase_timestamp": now,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": now,
    }
    
    #1. Write to postgres
    
    with engine.begin() as conn:
        conn.execute(orders.insert().values(**row))
        
    #2. Publish the matching event to Kafka (dual-write)
    
    event_payload = {**row, "event_timestamp": now.isoformat()}
    publish_event(op_type="insert", table_name="orders", payload=event_payload)
    
    print(f"Inserted order {order_id}")
    return order_id

if __name__ == "__main__":
    insert_new_order()