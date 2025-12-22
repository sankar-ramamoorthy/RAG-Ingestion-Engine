# src/ingestion_service/core/pipeline.py

from __future__ import annotations


class IngestionPipeline:
    def __init__(
        self,
        *,
        validator,
        chunker,
        embedder,
        vector_store,
    ) -> None:
        self._validator = validator
        self._chunker = chunker
        self._embedder = embedder
        self._vector_store = vector_store

    def run(self, *, text: str, ingestion_id: str) -> None:
        """
        Execute the ingestion pipeline.

        This method is intentionally side-effect free except
        for calls into injected collaborators.
        """
        self._validate(text)
        chunks = self._chunk(text)
        embeddings = self._embed(chunks)
        self._persist(chunks, embeddings, ingestion_id)

    # ---- pipeline steps ----

    def _validate(self, text: str) -> None:
        self._validator.validate(text)

    def _chunk(self, text: str):
        chunks = self._chunker.chunk(text)
        return chunks

    def _embed(self, chunks):
        embedded_chunks = self._embedder.embed(chunks)
        return embedded_chunks

    def _persist(self, chunks, embeddings, ingestion_id: str) -> None:
        self._vector_store.persist(
            chunks=chunks,
            embeddings=embeddings,
            ingestion_id=ingestion_id,
        )
