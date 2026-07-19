import json

from kafka import KafkaProducer
import pandas as pd

df = pd.read_csv("2/events.csv")
df = df.astype(object).where(pd.notna(df), None)

producer = KafkaProducer(bootstrap_servers='localhost:19092')

for record in df.to_dict(orient="records"):
    message = json.dumps(record).encode("utf-8")
    producer.send("events", message)

producer.flush()
producer.close()

print("Finished sending all events")