from __future__ import annotations

import argparse
import time

import psycopg
from kafka import KafkaConsumer
from pydantic import ValidationError

from pulsegrid.config import KafkaSettings, PostgresSettings
from pulsegrid.database import ensure_schema
from pulsegrid.models import Event


def _flush_valid_events(cursor: psycopg.Cursor, events: list[Event]) -> None:
    rows = [
        (event.timestamp, event.visitorid, event.event.value, event.itemid, event.transactionid)
        for event in events
    ]
    cursor.executemany(
        """
        INSERT INTO event (timestamp, visitorid, event, itemid, transactionid)
        VALUES (%s, %s, %s, %s, %s)
        """,
        rows,
    )


def _flush_failed_events(cursor: psycopg.Cursor, failed_events: list[dict[str, str]]) -> None:
    rows = [(event["raw_message"], event["error"]) for event in failed_events]
    cursor.executemany(
        "INSERT INTO failed_events (raw_message, error) VALUES (%s, %s)",
        rows,
    )


def _flush_batches(
    cursor: psycopg.Cursor,
    conn: psycopg.Connection,
    valid_events: list[Event],
    failed_events: list[dict[str, str]],
) -> None:
    if valid_events:
        _flush_valid_events(cursor, valid_events)
        conn.commit()
        print(f"flushed {len(valid_events)} valid events to postgres")

    if failed_events:
        _flush_failed_events(cursor, failed_events)
        conn.commit()
        print(f"flushed {len(failed_events)} failed events to postgres")


def consume_events(
    max_records: int | None = None,
    idle_timeout_seconds: int | None = None,
    batch_size: int = 100,
    flush_interval_seconds: int = 10,
    kafka_settings: KafkaSettings | None = None,
    postgres_settings: PostgresSettings | None = None,
) -> int:
    kafka_config = kafka_settings or KafkaSettings()
    postgres_config = postgres_settings or PostgresSettings()
    ensure_schema(postgres_config)

    consumer = KafkaConsumer(
        kafka_config.topic,
        bootstrap_servers=kafka_config.bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=kafka_config.consumer_group,
        value_deserializer=lambda value: value.decode("utf-8"),
    )

    consumed_count = 0
    valid_events: list[Event] = []
    failed_events: list[dict[str, str]] = []
    last_flush = time.time()

    with psycopg.connect(postgres_config.dsn) as conn:
        with conn.cursor() as cur:
            while max_records is None or consumed_count < max_records:
                remaining = None if max_records is None else max_records - consumed_count
                poll_limit = batch_size if remaining is None else min(batch_size, remaining)
                timeout_ms = 1000 if idle_timeout_seconds is None else idle_timeout_seconds * 1000
                records = consumer.poll(timeout_ms=timeout_ms, max_records=poll_limit)

                if not records:
                    if idle_timeout_seconds is not None:
                        break
                    continue

                for messages in records.values():
                    for msg in messages:
                        raw_message = msg.value
                        print(f"received: {raw_message[:80]}")
                        consumed_count += 1

                        try:
                            valid_events.append(Event.model_validate_json(raw_message))
                        except ValidationError as error:
                            failed_events.append(
                                {"raw_message": raw_message, "error": str(error)}
                            )

                should_flush_for_size = len(valid_events) + len(failed_events) >= batch_size
                should_flush_for_time = time.time() - last_flush >= flush_interval_seconds
                if should_flush_for_size or should_flush_for_time:
                    _flush_batches(cur, conn, valid_events, failed_events)
                    valid_events.clear()
                    failed_events.clear()
                    last_flush = time.time()

            _flush_batches(cur, conn, valid_events, failed_events)

    consumer.close()
    return consumed_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Pulsegrid Kafka events into Postgres.")
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--idle-timeout", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    consumed_count = consume_events(
        max_records=args.max_records,
        idle_timeout_seconds=args.idle_timeout,
        batch_size=args.batch_size,
    )
    print(f"Finished consuming {consumed_count} events")


if __name__ == "__main__":
    main()
