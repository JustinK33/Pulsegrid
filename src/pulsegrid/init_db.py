from __future__ import annotations

from pulsegrid.database import ensure_schema


def main() -> None:
    ensure_schema()
    print("Pulsegrid schema is ready")


if __name__ == "__main__":
    main()
