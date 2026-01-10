from __future__ import annotations

import asyncio
from typing import Optional
from ingestion_service.core.pipeline import IngestionPipeline
from ingestion_service.core.vectorstore.base import VectorRecord, VectorMetadata


class AsyncIngestionRunner:
    """
    Async wrapper around the synchronous IngestionPipeline.
    """

    def __init__(
        self,
        pipeline: IngestionPipeline,
        *,
        provider: str = "mock",
        source_type: str,
    ):
        self._pipeline = pipeline
        self._provider = provider
        self._source_type = source_type

    async def ingest(
        self,
        *,
        text: str,
        ingestion_id: str,
        source_metadata: Optional[dict] = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        chunks = self._pipeline._chunk(text, self._source_type, self._provider)

        embeddings = self._pipeline._embed(chunks)

        records = [
            VectorRecord(
                vector=embedding,
                metadata=VectorMetadata(
                    ingestion_id=ingestion_id,
                    chunk_id=chunk.chunk_id,
                    chunk_index=i,
                    chunk_strategy=chunk.metadata.get("chunk_strategy", "unknown"),
                    chunk_text=chunk.content,
                    source_metadata=source_metadata or {},
                    provider=self._provider,
                ),
            )
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings))
        ]

        await loop.run_in_executor(None, self._pipeline._vector_store.add, records)
