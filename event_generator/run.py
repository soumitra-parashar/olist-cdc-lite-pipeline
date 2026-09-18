import random
import time

from generator import insert_new_order
from order_lifecycle import advance_random_order

INSERT_WEIGHT = 0.4

def run(interval_seconds=5, max_iterations=None):
    iteration = 0
    while max_iterations is None or iteration > max_iterations:
        try:
            if random.random() < INSERT_WEIGHT:
                insert_new_order()
            else:
                advance_random_order()
        except Exception as e:
            print(f"Error during event generation: {e}")
        
        iteration +=1
        time.sleep(interval_seconds)

if __name__ == "__main__":
    run(interval_seconds=5)