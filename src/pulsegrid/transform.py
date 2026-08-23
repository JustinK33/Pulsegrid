from __future__ import annotations

import psycopg

from pulsegrid.config import PostgresSettings
from pulsegrid.database import TRANSFORM_PATH, run_sql_file


SUMMARY_TABLES = ("event_clean", "event_daily_counts", "visitor_funnel", "item_popularity")


def build_summary_tables(settings: PostgresSettings | None = None) -> dict[str, int]:
    """Rebuild the curated tables and return the row count of each one."""
    db_settings = settings or PostgresSettings()
    run_sql_file(TRANSFORM_PATH, db_settings)

    with psycopg.connect(db_settings.dsn) as conn:
        return {
            table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in SUMMARY_TABLES
        }


def main() -> None:
    for table, row_count in build_summary_tables().items():
        print(f"{table}: {row_count} rows")


if __name__ == "__main__":
    main()
