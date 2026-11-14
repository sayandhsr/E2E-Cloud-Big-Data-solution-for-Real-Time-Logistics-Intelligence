import json
import random
import time

from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:29092"})


def on_delivery(err, msg):
    if err:
        print("FAILED:", err)
    else:
        print(f"sent key={msg.key().decode()} partition={msg.partition()} offset={msg.offset()}")


for _ in range(10):
    vehicle_id = f"VH-{random.randint(1, 5):03d}"
    event = {
        "vehicle_id": vehicle_id,
        "speed_kmh": round(random.uniform(20, 90), 1),
        "ts": time.time(),
    }
    producer.produce("vehicle.telemetry", key=vehicle_id, value=json.dumps(event), callback=on_delivery)
    producer.poll(0)
    time.sleep(0.5)

producer.flush()