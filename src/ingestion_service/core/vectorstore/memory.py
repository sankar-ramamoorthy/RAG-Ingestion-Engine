from __future__ import annotations

from typing import Any, List

from ingestion_service.core.chunks import Chunk


class MemoryVectorStore:
    """
    In-memory vector store for MS2.
    """

    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []

    def persist(
        self,
        *,
        chunks: List[Chunk],
        embeddings: List[Any],
        ingestion_id: str,
    ) -> None:
        for index, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            self._rows.append(
                {
                    "ingestion_id": ingestion_id,
                    "chunk_index": index,
                    "chunk_strategy": chunk.metadata.get("chunk_strategy"),
                    "vector": embedding,
                }
            )

    def dump(self) -> list[dict[str, Any]]:
        return list(self._rows)
