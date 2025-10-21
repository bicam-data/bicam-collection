import os


def resolve_db_config() -> dict:
    """Resolve DB config from env with common fallbacks.
    Supports POSTGRESQL_USER|USERNAME and POSTGRESQL_DB|DATABASE.
    """
    return {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", 5432)),
        "user": os.getenv("POSTGRESQL_USER") or os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
        "database": os.getenv("POSTGRESQL_DB") or os.getenv("POSTGRESQL_DATABASE"),
    }
