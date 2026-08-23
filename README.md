# Pulsegrid

Pulsegrid is a learning project for practicing data engineering with Kafka-compatible streaming, Postgres storage, notebooks, and Airflow orchestration.

## What Airflow Adds

Airflow is a scheduler and orchestrator for data workflows.
It lets you define a pipeline as a DAG, which means directed acyclic graph.
In plain terms, you write tasks and dependencies, and Airflow runs them in order, retries failures, records logs, and shows run history in a web UI.

For this project, Airflow should coordinate finite jobs around the pipeline.
It should not replace Redpanda/Kafka, Postgres, or an always-running stream processor.
That is why the example DAG initializes the database schema, publishes raw CSV events into Kafka, consumes a bounded number of events into Postgres, then rebuilds the curated summary tables.

## Pipeline Stages

| Stage | Code | What it does |
| --- | --- | --- |
| Extract | `src/pulsegrid/streaming/producer.py` | Reads `events.csv` and publishes each row as JSON to the Kafka topic |
| Load | `src/pulsegrid/streaming/consumer.py` | Validates each message with Pydantic, batch-inserts valid rows into `event` and rejects into `failed_events` |
| Transform | `src/pulsegrid/sql/transform.sql` | Rebuilds `event_clean` plus three analytics tables from the loaded rows |

The transform step is a full refresh, so it is safe to re-run.
It also deduplicates, because re-running the DAG republishes the same CSV and the loader has no upsert key.

## Project Structure

```text
.
├── dags/                     # Airflow DAG definitions
├── data/raw/                 # Local raw CSV files, ignored by Git
├── notebooks/                # Exploratory analysis
├── src/pulsegrid/            # Reusable application code
│   ├── sql/schema.sql        # Postgres table definitions
│   ├── sql/transform.sql     # Curated summary table definitions
│   ├── transform.py          # Rebuilds the curated tables
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

Rebuild the curated summary tables manually:

```bash
PYTHONPATH=src python -m pulsegrid.transform
```

Open Airflow at `http://localhost:8080`.
The local development username and password are both `airflow`.

## Successful Airflow Run

After triggering `pulsegrid_event_pipeline`, all four tasks should finish successfully.
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

6. Extend the transformation layer.
   `transform.sql` already builds daily counts, a visitor funnel, and item popularity.
   Next would be sessionization and joining `item_properties` and `category_tree`.

7. Treat notebooks as consumers of curated data.
   Keep exploration in notebooks, but keep repeatable pipeline logic in `src/pulsegrid`.
