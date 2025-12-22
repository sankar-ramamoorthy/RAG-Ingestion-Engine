# tests/core/test_pipeline.py

from unittest.mock import Mock, call
import pytest

from ingestion_service.core.pipeline import IngestionPipeline


def make_pipeline():
    validator = Mock()
    chunker = Mock()
    embedder = Mock()
    vector_store = Mock()

    pipeline = IngestionPipeline(
        validator=validator,
        chunker=chunker,
        embedder=embedder,
        vector_store=vector_store,
    )

    return pipeline, validator, chunker, embedder, vector_store


def test_pipeline_calls_steps_in_order():
    pipeline, validator, chunker, embedder, vector_store = make_pipeline()

    chunker.chunk.return_value = ["chunk1"]
    embedder.embed.return_value = ["embedding1"]

    pipeline.run(text="hello world", ingestion_id="ing-123")

    assert validator.validate.call_count == 1
    assert chunker.chunk.call_count == 1
    assert embedder.embed.call_count == 1
    assert vector_store.persist.call_count == 1

    assert validator.validate.call_args == call("hello world")
    assert chunker.chunk.call_args == call("hello world")
    assert embedder.embed.call_args == call(["chunk1"])
    assert vector_store.persist.call_args == call(
        chunks=["chunk1"],
        embeddings=["embedding1"],
        ingestion_id="ing-123",
    )


def test_pipeline_passes_data_through():
    pipeline, _, chunker, embedder, vector_store = make_pipeline()

    chunks = ["c1", "c2"]
    embeddings = ["e1", "e2"]

    chunker.chunk.return_value = chunks
    embedder.embed.return_value = embeddings

    pipeline.run(text="text", ingestion_id="ing-456")

    embedder.embed.assert_called_once_with(chunks)
    vector_store.persist.assert_called_once_with(
        chunks=chunks,
        embeddings=embeddings,
        ingestion_id="ing-456",
    )


def test_pipeline_stops_on_validation_error():
    pipeline, validator, chunker, embedder, vector_store = make_pipeline()

    validator.validate.side_effect = ValueError("invalid")

    with pytest.raises(ValueError):
        pipeline.run(text="bad", ingestion_id="ing-789")

    chunker.chunk.assert_not_called()
    embedder.embed.assert_not_called()
    vector_store.persist.assert_not_called()
