# src/ingestion_service/core/chunk_assembly/pdf_chunk_assembler.py
from __future__ import annotations

from typing import Dict, List, Set

from ingestion_service.core.chunks import Chunk
from ingestion_service.core.document_graph.models import DocumentGraph
from ingestion_service.core.chunkers.selector import ChunkerFactory


class PDFChunkAssembler:
    """
    Converts a DocumentGraph into chunks using real chunkers.

    Rules:
    - Only text artifacts are chunked
    - Chunking is delegated to ChunkerFactory
    - Chunk IDs are deterministic
    - Image → text associations are preserved in metadata
    """

    def assemble(self, graph: DocumentGraph) -> List[Chunk]:
        chunks: List[Chunk] = []

        # ---------------------------------------------------------
        # Precompute image → text associations
        # ---------------------------------------------------------
        images_by_text: Dict[str, Set[str]] = {}

        for edge in graph.edges:
            if edge.relation == "image_to_text":
                images_by_text.setdefault(edge.to_id, set()).add(edge.from_id)

        # ---------------------------------------------------------
        # Chunk text artifacts
        # ---------------------------------------------------------
        for node in graph.nodes.values():
            artifact = node.artifact

            if artifact.type != "text" or not artifact.text:
                continue

            # Choose chunker dynamically
            chunker, chunker_params = ChunkerFactory.choose_strategy(artifact.text)
            chunk_strategy = getattr(chunker, "chunk_strategy", "unknown")
            chunker_name = getattr(chunker, "name", chunker.__class__.__name__)

            produced_chunks = chunker.chunk(artifact.text, **chunker_params)

            for idx, produced_chunk in enumerate(produced_chunks):
                produced_chunk.chunk_id = f"{node.artifact_id}:chunk:{idx}"

                produced_chunk.metadata.update(
                    {
                        "source_file": artifact.source_file,
                        "page_numbers": [artifact.page_number],
                        "artifact_ids": [node.artifact_id],
                        "associated_image_ids": list(
                            images_by_text.get(node.artifact_id, [])
                        ),
                        "chunk_strategy": chunk_strategy,
                        "chunker_name": chunker_name,
                        "chunker_params": dict(chunker_params),
                    }
                )

                chunks.append(produced_chunk)

        return chunks
