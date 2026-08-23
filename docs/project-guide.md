# Pulsegrid Project Guide

This doc explains the project in plain language.
It is meant to help someone understand what each file does, how the pieces connect, and what data engineering habits this project is practicing.

## The Big Picture

Pulsegrid is a small data pipeline.
It starts with raw CSV files.
The producer reads the CSV data and sends each row to RedPanda (basically kafka replacement).
The consumer reads messages from Kafka, validates them, and writes the good rows into Postgres.
Bad rows go into a separate table so they are not lost.
A transform step then rebuilds a set of summary tables from the loaded rows.
Airflow sits above the pipeline and runs the steps in order.

The flow looks like this:

```text
data/raw/events.csv
        |
        v
producer.py
        |
        v
Redpanda Kafka topic: events
        |
        v
consumer.py
        |
        v
Postgres tables: event and failed_events
        |
        v
transform.sql
        |
        v
Postgres tables: event_clean, event_daily_counts,
                 visitor_funnel, item_popularity
```

The first three stages are extract and load.
The last stage is the transform, which is what makes this an ETL pipeline rather than only ingestion.

## Why Airflow Is Here

Airflow is a workflow scheduler.
It is useful when you want to say, "run this step first, then this step, then this step."
It also gives you logs, retries, task status, and a web UI.

In this project, Airflow runs a small version of the pipeline:

1. Create the Postgres tables if they do not exist.
2. Publish events from the CSV file into Kafka.
3. Consume a limited number of Kafka messages into Postgres.
4. Rebuild the curated summary tables from whatever has been loaded.

The consumer has a limit because Airflow tasks should finish.
An infinite Kafka consumer is better as a separate service, not as a normal Airflow task.

## File Guide

### `docker-compose.yml`

This defines the local development services.
It starts Redpanda, Postgres for project data, and the Airflow services.

There are two Postgres containers on purpose.
`pulsegrid-postgres` stores the data for this project.
`airflow-postgres` stores Airflow's own metadata, such as DAG runs and task state.

Keeping those separate makes the system easier to reason about.
Project data and Airflow internals should not be mixed together.

### `dags/pulsegrid_event_pipeline.py`

This is the Airflow DAG.
A DAG is a set of tasks and dependencies.

This DAG has four tasks:

1. `init_postgres_schema` creates the database tables.
2. `publish_raw_events` sends CSV rows to Kafka.
3. `consume_events_to_postgres` reads Kafka messages into Postgres.
4. `build_summary_tables` rebuilds the curated analytics tables.

The chain at the bottom defines the order:

```python
(
    init_postgres_schema
    >> publish_raw_events
    >> consume_events_to_postgres
    >> build_summary_tables
)
```

That means the schema task runs first, then the producer, then the consumer, then the transform.

### `src/pulsegrid/config.py`

This file reads settings from environment variables.
Examples include the Kafka address, database host, database name, and database password.

This is a good practice because code should not hard-code secrets or local machine settings.
The same code can run locally, in Docker, or later in a cloud environment by changing environment variables.

### `src/pulsegrid/models.py`

This file defines the shape of an event.
It uses Pydantic to validate incoming Kafka messages.

The model says an event should have a timestamp, visitor ID, event type, item ID, and an optional transaction ID.
The event type is restricted to known values like `view`, `addtocart`, and `transaction`.

This is useful because bad data is common.
Validation catches problems early instead of silently writing messy data into the database.

### `src/pulsegrid/database.py`

This file handles database setup.
`run_sql_file` executes any SQL file in `sql/` against Postgres, and `ensure_schema` is the thin wrapper that runs `schema.sql`.

Keeping this in one place means the CLI, the consumer, Airflow, and the transform step all share the same connection handling.

### `src/pulsegrid/init_db.py`

This is a small command-line entry point for database initialization.
Airflow uses it in the first DAG task.

It exists so Airflow can run a simple command instead of knowing database details directly.

### `src/pulsegrid/sql/schema.sql`

This file defines the database tables.
The `event` table stores valid events.
The `failed_events` table stores messages that failed validation.

Keeping failed events is a good habit.
It lets you inspect bad data later instead of losing it.

### `src/pulsegrid/streaming/producer.py`

The producer reads `events.csv`, converts each row to JSON, and sends each JSON message to Kafka.
It has a function called `publish_events` so other code can reuse it.
It also has a CLI so you can run it directly from the terminal.

This split is a good Python habit.
Reusable work goes in functions.
Command-line parsing stays near the edge of the program.

### `src/pulsegrid/streaming/consumer.py`

The consumer reads messages from Kafka.
Each message is validated with the `Event` model.
Valid events are inserted into the `event` table.
Invalid events are inserted into `failed_events`.

The consumer batches database writes instead of inserting one row at a time.
Batching is faster and is closer to how real data pipelines are usually written.

The consumer also supports `--max-records` and `--idle-timeout`.
Those options make it usable from Airflow because the task can finish.

### `src/pulsegrid/sql/transform.sql`

This is the transform stage.
It reads the raw `event` table and rebuilds four curated tables:

- `event_clean` is the deduplicated, typed version of `event`.
  It converts the epoch millisecond `timestamp` into a real UTC timestamp and a `event_date` column.
- `event_daily_counts` is one row per day and event type, with event counts and distinct visitor and item counts.
- `visitor_funnel` is one row per visitor, pivoting the event types into `views`, `add_to_carts`, and `transactions` columns, plus first and last seen times.
- `item_popularity` is one row per item, with the same pivot plus two derived conversion rates.

The rates use `NULLIF` on the denominator.
Dividing by zero would otherwise abort the whole transform.

Each table is dropped and recreated.
That is a full refresh, which is the simplest thing that is correct when re-run.
DDL is transactional in Postgres, so the drop and the rebuild commit together and a reader never sees a missing table.

`event_clean` uses `SELECT DISTINCT` because the producer republishes the entire CSV on every DAG run and the loader has no upsert key.
Without that, every run would inflate the counts.
This is a full scan, so it would need to become an incremental load keyed on a natural id before the raw table gets large.

### `src/pulsegrid/transform.py`

This is the command-line entry point Airflow calls for the transform task.
It runs `transform.sql` and then prints the row count of each curated table.

Printing the counts is cheap observability.
The Airflow log for the task tells you how much data the run actually produced.

## Python Practices Used Here

### Use Functions Instead Of Only Scripts

The first version of the project had top-level scripts.
That is fine when learning, but it gets hard to reuse and test.

Now the producer and consumer expose functions.
That makes it easier to call them from Airflow, tests, or other Python code.

### Keep Configuration Out Of Code

Database passwords and service addresses come from environment variables.
This avoids hard-coded secrets and makes the project easier to run in different environments.

### Validate Data At The Boundary

Kafka messages are external input.
External input should not be trusted blindly.

The consumer validates each message before writing it to Postgres.
This keeps the main table cleaner and makes bad records easier to debug.

### Separate Raw Data From Code

Raw CSV files live in `data/raw/`.
Application code lives in `src/pulsegrid/`.
Notebooks live in `notebooks/`.

This makes it clearer what is source code, what is data, and what is exploration.

### Keep SQL In A SQL File

The table definitions live in `schema.sql`.
That is easier to read than hiding long SQL strings inside Python.

### Make Pipeline Steps Repeatable

The schema uses `CREATE TABLE IF NOT EXISTS`.
That means running the setup step more than once will not fail just because the tables already exist.

Repeatable steps are important in data engineering because pipelines get rerun often.

## What To Improve Next

Add tests for the event model and the transform SQL.
Add a small sample CSV that is safe to commit.
Give the raw loader a natural key so the transform does not have to deduplicate with a full scan.
Join `item_properties` and `category_tree` into the curated layer, since nothing reads them yet.
Add sessionization to `event_clean`.
Add Snowflake as a cloud data warehouse after the local Postgres pipeline is working well.
