from __future__ import annotations

from pathlib import Path

import psycopg

from pulsegrid.config import PostgresSettings


SQL_DIR = Path(__file__).with_name("sql")
SCHEMA_PATH = SQL_DIR / "schema.sql"
TRANSFORM_PATH = SQL_DIR / "transform.sql"


def run_sql_file(path: Path, settings: PostgresSettings | None = None) -> None:
    db_settings = settings or PostgresSettings()
    with psycopg.connect(db_settings.dsn) as conn:
        conn.execute(path.read_text())
        conn.commit()


def ensure_schema(settings: PostgresSettings | None = None) -> None:
    run_sql_file(SCHEMA_PATH, settings)
