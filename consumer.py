import json, time, psycopg
from kafka import KafkaConsumer
from pydantic import BaseModel, ValidationError
from enum import StrEnum

class EventEnum(StrEnum):
    VIEW = "view"
    ADD = "addtocart"
    TRANSACTION = "transaction"

class Event(BaseModel):
    timestamp: int
    visitorid: int
    event: EventEnum
    itemid: int
    transactionid: int | None = None

consumer = KafkaConsumer(
    "events",
    bootstrap_servers="localhost:19092",
    auto_offset_reset="earliest",
    group_id="pulsegrid-consumer-group"
    )

valid_events = []
failed_events = []

with psycopg.connect("dbname=pulsegrid user=justin password=password host=localhost port=5433") as conn:
    with conn.cursor() as cur:
        # initial set of time
        last_flush = time.time()
                
        for msg in consumer:
            message = msg.value.decode("utf-8")
            print(f"received: {message[:80]}") 

            try:
                validate = Event.model_validate_json(message)
                valid_events.append(validate)
            except ValidationError as e:
                print(e)
                failed = {"message": message, "error": str(e)}
                failed_events.append(failed)

            if len(valid_events) >= 100 or (time.time() - last_flush >= 10): 
                if valid_events:
                    rows = [(e.timestamp, e.visitorid, e.event.value, e.itemid, e.transactionid) for e in valid_events]
                    cur.executemany("INSERT INTO event (timestamp, visitorid, event, itemid, transactionid) VALUES (%s, %s, %s, %s, %s)", rows)
                    conn.commit()
                    print(f"flushed {len(valid_events)} to postgres")
                    valid_events = []
                if failed_events: # dead letter queue
                    rows = [(f["raw"], f["error"]) for f in failed_events]
                    cur.executemany("INSERT INTO failed_events (raw_message, error) VALUES (%s, %s)", rows)
                    conn.commit()
                    print(f"flushed {len(failed_events)} to postgres")
                    failed_events = []
                last_flush = time.time() # reset the time to 0 again


# SELECT event, COUNT(*) AS TOTAL FROM event GROUP BY event ORDER BY total DESC;