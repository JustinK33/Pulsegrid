# Pulsegrid Project Guide

This doc explains the project in plain language.
It is meant to help someone understand what each file does, how the pieces connect, and what data engineering habits this project is practicing.

## The Big Picture

Pulsegrid is a small data pipeline.
It starts with raw CSV files.
The producer reads the CSV data and sends each row to Kafka.
Redpanda acts like Kafka in this project.
The consumer reads messages from Kafka, validates them, and writes the good rows into Postgres.
Bad rows go into a separate table so they are not lost.
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
```

## Why Airflow Is Here

Airflow is not a database.
Airflow is not Kafka.
Airflow is not where long-running stream code should live forever.

Airflow is a workflow scheduler.
It is useful when you want to say, "run this step first, then this step, then this step."
It also gives you logs, retries, task status, and a web UI.

In this project, Airflow runs a small version of the pipeline:

1. Create the Postgres tables if they do not exist.
2. Publish events from the CSV file into Kafka.
3. Consume a limited number of Kafka messages into Postgres.

The consumer has a limit because Airflow tasks should finish.
An infinite Kafka consumer is better as a separate service, not as a normal Airflow task.

## File Guide

### `README.md`

The README is the front door of the project.
It gives a quick explanation, setup commands, and a learning plan.
It should stay short enough that someone can skim it.

### `docs/project-guide.md`

This file is the longer explanation.
It explains the purpose of the main files and the ideas behind them.
When the project grows, this is a good place to add notes about decisions, tradeoffs, and lessons learned.

### `.env.example`

This shows which environment variables the project expects.
It is safe to commit because it uses fake example values.

The real `.env` file should stay local.
That is where passwords and local settings go.

### `.gitignore`

This tells Git which files should not be committed.
It ignores local secrets, virtual environments, Python cache files, logs, and raw data.

This is important because raw datasets can be large and secrets should never be stored in Git.

### `.dockerignore`

This tells Docker which files should not be copied into the image build context.
It keeps builds smaller and avoids sending local-only files like `.env`, `venv/`, logs, and raw data into Docker.

### `requirements.txt`

This lists the Python packages used by the project.
For example, `pandas` reads CSV files, `pydantic` validates event data, `kafka-python` talks to Kafka, and `psycopg` talks to Postgres.

### `docker-compose.yml`

This defines the local development services.
It starts Redpanda, Postgres for project data, and the Airflow services.

There are two Postgres containers on purpose.
`pulsegrid-postgres` stores the data for this project.
`airflow-postgres` stores Airflow's own metadata, such as DAG runs and task state.

Keeping those separate makes the system easier to reason about.
Project data and Airflow internals should not be mixed together.

### `Dockerfile.airflow`

This builds the Airflow image used by Docker Compose.
It starts from the official Airflow image and installs this project's Python dependencies.

This is cleaner than installing packages every time the container starts.

### `data/raw/`

This folder holds the raw CSV files.
Raw data is the original input data.
It should usually be treated as read-only.

The CSV files are ignored by Git.
The `.gitkeep` file exists only so the empty folder can still be tracked.

### `notebooks/`

This folder is for exploration.
Notebooks are good for learning, plotting, and trying ideas.

Reusable pipeline logic should not live only in notebooks.
If code is needed every time the pipeline runs, it belongs in `src/pulsegrid/`.

### `dags/pulsegrid_event_pipeline.py`

This is the Airflow DAG.
A DAG is a set of tasks and dependencies.

This DAG has three tasks:

1. `init_postgres_schema` creates the database tables.
2. `publish_raw_events` sends CSV rows to Kafka.
3. `consume_events_to_postgres` reads Kafka messages into Postgres.

The line at the bottom defines the order:

```python
init_postgres_schema >> publish_raw_events >> consume_events_to_postgres
```

That means the schema task runs first, then the producer, then the consumer.

### `src/pulsegrid/__init__.py`

This makes `pulsegrid` a Python package.
That lets you run code with commands like:

```bash
python -m pulsegrid.streaming.producer
```

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
It reads the SQL schema file and runs it against Postgres.

Keeping schema setup in one place makes it easier to reuse from the CLI, the consumer, and Airflow.

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

Add tests for the event model.
Add a small sample CSV that is safe to commit.
Add summary tables for analytics.
Add a final Airflow task that checks row counts after loading.
Think about idempotency before loading the full dataset repeatedly.
