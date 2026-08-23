from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from pulsegrid.config import KafkaSettings


def publish_events(input_path: Path, settings: KafkaSettings | None = None) -> int:
    kafka_settings = settings or KafkaSettings()
    df = pd.read_csv(input_path)
    df = df.astype(object).where(pd.notna(df), None)

    producer = KafkaProducer(bootstrap_servers=kafka_settings.bootstrap_servers)
    sent_count = 0
    for record in df.to_dict(orient="records"):
        message = json.dumps(record).encode("utf-8")
        producer.send(kafka_settings.topic, message)
        sent_count += 1

    producer.flush()
    producer.close()
    return sent_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish Pulsegrid raw events to Kafka.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw/events.csv"),
        help="Path to the raw events CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    count = publish_events(args.input)
    print(f"Finished sending {count} events")


if __name__ == "__main__":
    main()
