from __future__ import annotations

from typing import Sequence, Iterable, List
import psycopg
from psycopg import sql

from .base import VectorStore, VectorRecord, VectorMetadata


class PgVectorStore(VectorStore):
    """PostgreSQL + pgvector vector store (psycopg, raw SQL, no ORM)."""

    TABLE_NAME = "vectors"
    SCHEMA = "ingestion_service"

    def __init__(self, dsn: str, dimension: int):
        self._dsn = dsn
        self._dimension = dimension
        self._validate_table()  # 🔥 migrations own schema

    @property
    def dimension(self) -> int:
        return self._dimension

    def add(self, records: Iterable[VectorRecord]) -> None:
        insert_sql = sql.SQL("""
            INSERT INTO {schema}.{table}
            (vector, ingestion_id, chunk_id, chunk_index, chunk_strategy)
            VALUES (%s, %s, %s, %s, %s)
        """).format(
            schema=sql.Identifier(self.SCHEMA),
            table=sql.Identifier(self.TABLE_NAME),
        )

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                for record in records:
                    cur.execute(
                        insert_sql,
                        (
                            record.vector,
                            record.metadata.ingestion_id,
                            record.metadata.chunk_id,
                            record.metadata.chunk_index,
                            record.metadata.chunk_strategy,
                        ),
                    )

    def similarity_search(
        self,
        query_vector: Sequence[float],
        k: int,
    ) -> List[VectorRecord]:
        search_sql = sql.SQL("""
            SELECT vector, ingestion_id, chunk_id, chunk_index, chunk_strategy
            FROM {schema}.{table}
            ORDER BY vector <-> (%s::vector)
            LIMIT %s
        """).format(
            schema=sql.Identifier(self.SCHEMA),
            table=sql.Identifier(self.TABLE_NAME),
        )

        results: List[VectorRecord] = []
        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(search_sql, (query_vector, k))
                for row in cur.fetchall():
                    vector, ingestion_id, chunk_id, chunk_index, chunk_strategy = row
                    metadata = VectorMetadata(
                        ingestion_id=ingestion_id,
                        chunk_id=chunk_id,
                        chunk_index=chunk_index,
                        chunk_strategy=chunk_strategy,
                    )
                    results.append(VectorRecord(vector=vector, metadata=metadata))
        return results

    def delete_by_ingestion_id(self, ingestion_id: str) -> None:
        delete_sql = sql.SQL("""
            DELETE FROM {schema}.{table}
            WHERE ingestion_id = %s
        """).format(
            schema=sql.Identifier(self.SCHEMA),
            table=sql.Identifier(self.TABLE_NAME),
        )

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(delete_sql, (ingestion_id,))

    def reset(self) -> None:
        truncate_sql = sql.SQL("""
            TRUNCATE TABLE {schema}.{table}
        """).format(
            schema=sql.Identifier(self.SCHEMA),
            table=sql.Identifier(self.TABLE_NAME),
        )

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(truncate_sql)

    def _validate_table(self) -> None:
        """
        Fail-fast via probe query.
        Ensures the vectors table exists and is compatible.
        """
        probe_sql = sql.SQL("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = {schema}
              AND table_name = {table}
              AND column_name = 'vector'
              AND udt_name = 'vector'
        """).format(
            schema=sql.Literal(self.SCHEMA),
            # ✅ Literal fixes the UndefinedColumn error
            table=sql.Literal(self.TABLE_NAME),
        )

        try:
            with psycopg.connect(self._dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(probe_sql)
                    if cur.rowcount == 0:
                        raise RuntimeError("PgVectorStore schema validation failed")
        except Exception as exc:
            raise RuntimeError(
                f"PgVectorStore schema validation failed: "
                f"table '{self.SCHEMA}.{self.TABLE_NAME}' is missing or incompatible. "
                f"Have you run database migrations?"
            ) from exc
