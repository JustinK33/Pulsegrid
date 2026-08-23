from __future__ import annotations

import os
from dataclasses import dataclass, field


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(f"Missing required environment variable: {name}")


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    )
    topic: str = field(default_factory=lambda: os.getenv("PULSEGRID_KAFKA_TOPIC", "events"))
    consumer_group: str = field(
        default_factory=lambda: os.getenv(
            "PULSEGRID_KAFKA_CONSUMER_GROUP", "pulsegrid-consumer-group"
        )
    )


@dataclass(frozen=True)
class PostgresSettings:
    host: str = field(default_factory=lambda: os.getenv("PULSEGRID_DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("PULSEGRID_DB_PORT", "5433")))
    database: str = field(default_factory=lambda: os.getenv("PULSEGRID_DB_NAME", "pulsegrid"))
    user: str = field(default_factory=lambda: os.getenv("PULSEGRID_DB_USER", "justin"))
    password: str = field(default_factory=lambda: _required_env("POSTGRES_PASS"))

    @property
    def dsn(self) -> str:
        return (
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password} "
            f"host={self.host} "
            f"port={self.port}"
        )
