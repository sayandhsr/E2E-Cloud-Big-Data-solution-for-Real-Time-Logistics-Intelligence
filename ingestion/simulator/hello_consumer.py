import json

from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "lesson2-reader",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["vehicle.telemetry"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("error:", msg.error())
            continue
        event = json.loads(msg.value())
        print(f"partition={msg.partition()} offset={msg.offset()} {event}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()