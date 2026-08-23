# Pulsegrid

Pulsegrid is a learning project for practicing data engineering with Kafka-compatible streaming, Postgres storage, notebooks, and Airflow orchestration.

## What Airflow Adds

Airflow is a scheduler and orchestrator for data workflows.
It lets you define a pipeline as a DAG, which means directed acyclic graph.
In plain terms, you write tasks and dependencies, and Airflow runs them in order, retries failures, records logs, and shows run history in a web UI.

For this project, Airflow should coordinate finite jobs around the pipeline.
It should not replace Redpanda/Kafka, Postgres, or an always-running stream processor.
That is why the example DAG initializes the database schema, publishes raw CSV events into Kafka, then consumes a bounded number of events into Postgres.

## Project Structure

```text
.
├── dags/                     # Airflow DAG definitions
├── data/raw/                 # Local raw CSV files, ignored by Git
├── notebooks/                # Exploratory analysis
├── src/pulsegrid/            # Reusable application code
│   ├── sql/schema.sql        # Postgres table definitions
│   └── streaming/            # Kafka producer and consumer entry points
├── docker-compose.yml        # Redpanda, app Postgres, and Airflow services
├── Dockerfile.airflow        # Airflow image with project dependencies
└── requirements.txt          # Python dependencies
```

For a longer file-by-file explanation, read [docs/project-guide.md](docs/project-guide.md).

## Local Setup

Create a local `.env` file from `.env.example` and choose your own `POSTGRES_PASS`.
Do not commit `.env`.

Start the local stack:

```bash
docker compose up --build
```

Run the producer manually:

```bash
PYTHONPATH=src python -m pulsegrid.streaming.producer --input data/raw/events.csv
```

Run the consumer manually:

```bash
PYTHONPATH=src python -m pulsegrid.streaming.consumer --max-records 1000 --idle-timeout 30
```

Open Airflow at `http://localhost:8080`.
The local development username and password are both `airflow`.

## Successful Airflow Run

After triggering `pulsegrid_event_pipeline`, all three tasks should finish successfully.
The run should look like this in Airflow:

![Successful Airflow DAG run](docs/images/airflow-successful-dag-run.png)

## Suggested Learning Plan

1. Understand the existing pipeline.
   Follow one event from `data/raw/events.csv` into Kafka and then into Postgres.

2. Learn Airflow DAG basics.
   Read `dags/pulsegrid_event_pipeline.py` and identify each task and dependency.

3. Make tasks idempotent.
   Re-running a DAG should not corrupt results or create confusing duplicates.

4. Add observability.
   Add row counts, validation failure counts, and simple queries after loading.

5. Add tests.
   Start with unit tests for event validation and a small integration test for writing events to Postgres.

6. Add a transformation layer.
   Create summary tables for event counts, transactions, and visitor behavior.

7. Treat notebooks as consumers of curated data.
   Keep exploration in notebooks, but keep repeatable pipeline logic in `src/pulsegrid`.
