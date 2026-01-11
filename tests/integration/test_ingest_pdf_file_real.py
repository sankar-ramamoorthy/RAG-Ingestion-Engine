# tests/integration/test_ingest_pdf_file_real.py
import json
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from ingestion_service.main import app
from ingestion_service.core.embedders.factory import get_embedder
from ingestion_service.core.embedders.ollama import OllamaEmbedder
from ingestion_service.core.vectorstore.pgvector_store import PgVectorStore
from ingestion_service.core.chunks import Chunk

client = TestClient(app)
pytest_plugins = ["tests.conftest_db"]  # DB fixtures


@pytest.mark.docker
@pytest.mark.integration
def test_real_pdf_ingest_creates_vectors(test_database_url, clean_vectors_table):
    """
    Uploads a real PDF with text and screenshots, ingests it, and verifies vectors.
    """
    pdf_path = Path("tests/fixtures/pdfs/sample_with_test_andscreenshot.pdf")
    with pdf_path.open("rb") as f:
        files = {"file": ("sample_with_test_andscreenshot.pdf", f, "application/pdf")}
        metadata = {"filename": pdf_path.name}
        response = client.post(
            "/v1/ingest/file",
            files=files,
            data={"metadata": json.dumps(metadata)},
        )

    assert response.status_code == 202
    payload = response.json()
    ingestion_id = payload["ingestion_id"]
    assert payload["status"] == "accepted"
    UUID(ingestion_id)

    # --- Verify persisted vectors ---
    embedder = get_embedder("ollama")
    assert isinstance(embedder, OllamaEmbedder)

    store = PgVectorStore(
        dsn=test_database_url,
        dimension=embedder.dimension,
        provider="ollama",
    )

    # Search using a known text from the PDF
    query_chunk = Chunk(chunk_id="query", content="test", metadata={})
    query_embedding = embedder.embed([query_chunk])[0]

    results = store.similarity_search(query_vector=query_embedding, k=5)
    assert any(r.metadata.ingestion_id == ingestion_id for r in results)
