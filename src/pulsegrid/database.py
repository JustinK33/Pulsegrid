from __future__ import annotations

from pathlib import Path

import psycopg

from pulsegrid.config import PostgresSettings


SCHEMA_PATH = Path(__file__).with_name("sql").joinpath("schema.sql")


def ensure_schema(settings: PostgresSettings | None = None) -> None:
    db_settings = settings or PostgresSettings()
    with psycopg.connect(db_settings.dsn) as conn:
        conn.execute(SCHEMA_PATH.read_text())
        conn.commit()
