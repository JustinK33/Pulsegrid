import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "events",
    bootstrap_servers="localhost:19092",
    auto_offset_reset="earliest"
    )

for msg in consumer:
    message = msg.value.decode("utf-8")
    print(message)