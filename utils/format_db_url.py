"""
Utility functions for formatting database URLs
to be compatible with SQLAlchemy.
"""

from urllib.parse import quote_plus


def encode(value: str) -> str:
    """
    URL-encodes a value so it can be safely used
    inside SQLAlchemy database URLs.
    """
    return quote_plus(value)


def format_postgres_url(
    username: str,
    password: str,
    host: str,
    port: int,
    database: str,
    schema: str,
) -> str:
    """
    Formats a PostgreSQL SQLAlchemy connection URL.

    Example:
    postgresql+psycopg2://user:pass@host:5432/db?options=-csearch_path%3Dschema
    """

    username, password, host, database, schema = map(
        encode,
        [username, password, host, database, schema],
    )

    return (
        f"postgresql+psycopg2://{username}:{password}"
        f"@{host}:{port}/{database}"
        f"?options=-csearch_path%3D{schema}"
    )


def format_oracle_dsn(
    username: str,
    password: str,
    host: str,
    port: int,
    service_name: str,
    schema: str,
) -> str:
    """
    Formats an Oracle SQLAlchemy DSN.

    Example:
    oracle+cx_oracle://user:pass@host:1521/?service_name=ORCL
    """

    username, password, host, service_name, schema = map(
        encode,
        [username, password, host, service_name, schema],
    )

    return (
        f"oracle+cx_oracle://{username}:{password}"
        f"@{host}:{port}/"
        f"?service_name={service_name}"
        f"&encoding=UTF-8"
        f"&nencoding=UTF-8"
        f"&options=-csearch_path%3D{schema}"
    )
