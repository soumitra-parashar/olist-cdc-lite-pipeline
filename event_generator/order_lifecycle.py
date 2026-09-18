import random
from datetime import datetime, UTC
from sqlalchemy import select

from db import engine, orders
from producer import publish_event

STATUS_PROGRESSION = ["created", "approved", "processing", "shipped", "delivered"]

def advance_random_order():
    with engine.begin() as conn:
        
        result = conn.execute(
            select(orders.c.order_id, orders.c.order_status)
            .where(orders.c.order_status.in_(STATUS_PROGRESSION[:-1]))
            .order_by(orders.c.order_purchase_timestamp.desc())
            .limit(1)
        ).fetchone()
        
        
        if result is None:
            print("No order available to advance - insert one first.")
            return
        
        order_id, current_status = result
        current_index = STATUS_PROGRESSION.index(current_status)
        new_status = STATUS_PROGRESSION[current_index + 1]
        now = datetime.now(UTC)
        
        
        update_values = {"order_status": new_status}
        if new_status == "approved":
            update_values["order_approved_at"] = now
        elif new_status == "shipped":
            update_values["order_delivered_carrier_date"] = now
        elif new_status == "delivered":
            update_values["order_delivered_customer_date"] = now
        
        conn.execute(
            orders.update().where(orders.c.order_id == order_id).values(**update_values)
        )
    event_payload = {"order_id": order_id, **update_values, "event_timestamp": now.isoformat()}
    publish_event(op_type="update", table_name="orders", payload=event_payload)
    
    print(f"Advanced order {order_id}: {current_status} → {new_status}")

if __name__ == "__main__":
    advance_random_order()