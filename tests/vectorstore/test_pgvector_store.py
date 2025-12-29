import uuid
import pytest
from ingestion_service.core.vectorstore.base import VectorMetadata, VectorRecord
from ingestion_service.core.vectorstore.pgvector_store import PgVectorStore

pytest_plugins = ["tests.conftest_db"]


@pytest.mark.docker
@pytest.mark.integration
def test_pgvector_store_add_search_delete(clean_vectors_table, test_database_url):
    store = PgVectorStore(dsn=test_database_url, dimension=3)

    ingestion_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())

    record = VectorRecord(
        vector=[0.1, 0.2, 0.3],
        metadata=VectorMetadata(
            ingestion_id=ingestion_id,
            chunk_id=chunk_id,
            chunk_index=0,
            chunk_strategy="test",
        ),
    )

    store.add([record])

    results = store.similarity_search(query_vector=[0.1, 0.2, 0.31], k=1)
    assert len(results) == 1
    assert results[0].metadata.ingestion_id == ingestion_id
    assert results[0].metadata.chunk_id == chunk_id

    store.delete_by_ingestion_id(ingestion_id)
    results_after_delete = store.similarity_search(query_vector=[0.1, 0.2, 0.3], k=1)
    assert results_after_delete == []
