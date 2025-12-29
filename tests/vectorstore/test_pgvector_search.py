import pytest

from ingestion_service.core.vectorstore.base import VectorMetadata, VectorRecord
from ingestion_service.core.vectorstore.pgvector_store import PgVectorStore

pytest_plugins = ["tests.conftest_db"]


@pytest.mark.docker
@pytest.mark.integration
def test_pgvector_similarity_search(clean_vectors_table, test_database_url):
    """
    Test that PgVectorStore similarity_search returns correct top-k vectors
    ordered by distance (<-> operator).
    """
    store = PgVectorStore(dsn=test_database_url, dimension=3)

    records = [
        VectorRecord(
            vector=[1.0, 0.0, 0.0],
            metadata=VectorMetadata(
                ingestion_id="ing-1",
                chunk_id="c1",
                chunk_index=0,
                chunk_strategy="fixed",
            ),
        ),
        VectorRecord(
            vector=[0.0, 1.0, 0.0],
            metadata=VectorMetadata(
                ingestion_id="ing-2",
                chunk_id="c2",
                chunk_index=1,
                chunk_strategy="fixed",
            ),
        ),
        VectorRecord(
            vector=[0.0, 0.0, 1.0],
            metadata=VectorMetadata(
                ingestion_id="ing-3",
                chunk_id="c3",
                chunk_index=2,
                chunk_strategy="fixed",
            ),
        ),
    ]

    store.add(records)

    query = [0.9, 0.1, 0.0]
    results = store.similarity_search(query_vector=query, k=2)

    assert len(results) == 2
    assert results[0].metadata.ingestion_id == "ing-1"
    assert results[1].metadata.ingestion_id == "ing-2"

    store.delete_by_ingestion_id("ing-1")
    remaining = store.similarity_search(query_vector=query, k=3)
    assert all(r.metadata.ingestion_id != "ing-1" for r in remaining)
