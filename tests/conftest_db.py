# tests/conftest_db.py
import os
import pytest
from psycopg import sql

# Must be set in the environment before running tests
# e.g., export DATABASE_URL=postgresql://ingestion_user:ingestion_pass@postgres:5432/ingestion_test


@pytest.fixture(scope="session")
def test_database_url() -> str:
    """Return DATABASE_URL from environment."""
    return os.environ["DATABASE_URL"]


@pytest.fixture
def clean_vectors_table(test_database_url):
    """
    Ensure a clean 'vectors' table in 'ingestion_service' schema for each test.
    Drops the table if exists, then recreates it.
    """
    from psycopg import connect

    schema = "ingestion_service"
    table = "vectors"

    create_table_sql = sql.SQL("""
        CREATE TABLE {schema}.{table} (
            id SERIAL PRIMARY KEY,
            vector vector(3) NOT NULL,
            ingestion_id TEXT NOT NULL,
            chunk_id TEXT NOT NULL,
            chunk_index INT NOT NULL,
            chunk_strategy TEXT NOT NULL
        )
    """).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )

    with connect(test_database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                )
            )
            cur.execute(create_table_sql)

    yield  # test runs here

    # Optional cleanup
    with connect(test_database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                )
            )
