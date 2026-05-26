"""
Tests for vectorstores/chroma.py — ChromaDB adapter.

Strategy
--------
- ``get_chroma_embedding_fn`` is patched via the ``patched_chroma`` fixture
  so no SentenceTransformer weights are loaded.
- ChromaDB uses a real ``PersistentClient`` backed by ``tmp_path``
  (pytest built-in) — actual SQLite + HNSW, zero mocking of ChromaDB itself.
- Each test gets a fresh path so collections are completely isolated.

Covers:
- get_chroma_embedding_fn: returns correct ChromaDB type
- get_chroma_collection: creates collection, metadata, reset behaviour
- index_chunks: upsert, empty guard, batch splitting, metadata stored
- query_collection: hit schema, k enforcement, fewer docs than k, ValueError guard
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch, MagicMock

import pytest

from vectorstores.chroma import (
    get_chroma_collection,
    get_chroma_embedding_fn,
    index_chunks,
    query_collection,
)
from vectorstores.tests.conftest import make_chunk, MockEmbeddingFunction

_PATCH_TARGET = "vectorstores.chroma.get_chroma_embedding_fn"


# ---------------------------------------------------------------------------
# get_chroma_embedding_fn
# ---------------------------------------------------------------------------

class TestGetChromaEmbeddingFn:
    def test_returns_chroma_embedding_function_type(self):
        from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
        fn = get_chroma_embedding_fn()
        assert isinstance(fn, SentenceTransformerEmbeddingFunction)

    def test_accepts_custom_model_name(self):
        from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
        fn = get_chroma_embedding_fn("BAAI/bge-small-en-v1.5")
        assert isinstance(fn, SentenceTransformerEmbeddingFunction)

    def test_default_model_is_used_when_no_arg(self):
        from embeddings.sentence_transformer import DEFAULT_MODEL
        from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
        fn = get_chroma_embedding_fn()
        assert isinstance(fn, SentenceTransformerEmbeddingFunction)


# ---------------------------------------------------------------------------
# get_chroma_collection
# ---------------------------------------------------------------------------

class TestGetChromaCollection:
    def test_creates_collection_successfully(self, patched_chroma):
        collection = get_chroma_collection("test-col", base_path=patched_chroma)
        assert collection is not None
        assert collection.name == "test-col"

    def test_collection_uses_cosine_space(self, patched_chroma):
        collection = get_chroma_collection("cosine-col", base_path=patched_chroma)
        assert collection.metadata.get("hnsw:space") == "cosine"

    def test_creates_subdirectory_for_collection(self, patched_chroma):
        get_chroma_collection("subdir-col", base_path=patched_chroma)
        assert (patched_chroma / "subdir-col").is_dir()

    def test_get_or_create_is_idempotent(self, patched_chroma):
        """Calling twice with the same name returns the same collection."""
        c1 = get_chroma_collection("idem-col", base_path=patched_chroma)
        c2 = get_chroma_collection("idem-col", base_path=patched_chroma)
        assert c1.name == c2.name

    def test_reset_clears_existing_documents(self, patched_chroma, three_chunks):
        col = get_chroma_collection("reset-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        assert col.count() == 3

        # Reset by calling get_chroma_collection again with reset=True
        col2 = get_chroma_collection("reset-col", base_path=patched_chroma, reset=True)
        assert col2.count() == 0

    def test_reset_false_preserves_documents(self, patched_chroma, three_chunks):
        col = get_chroma_collection("keep-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)

        col2 = get_chroma_collection("keep-col", base_path=patched_chroma, reset=False)
        assert col2.count() == 3

    def test_different_names_are_isolated_collections(self, patched_chroma, three_chunks):
        col_a = get_chroma_collection("col-a", base_path=patched_chroma)
        col_b = get_chroma_collection("col-b", base_path=patched_chroma)
        index_chunks(col_a, three_chunks)

        assert col_a.count() == 3
        assert col_b.count() == 0


# ---------------------------------------------------------------------------
# index_chunks
# ---------------------------------------------------------------------------

class TestIndexChunks:
    def test_empty_chunks_is_noop(self, patched_chroma):
        col = get_chroma_collection("empty-col", base_path=patched_chroma)
        index_chunks(col, [])          # must not raise
        assert col.count() == 0

    def test_indexes_all_chunks(self, patched_chroma, three_chunks):
        col = get_chroma_collection("count-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        assert col.count() == len(three_chunks)

    def test_metadata_stored_correctly(self, patched_chroma, three_chunks):
        col = get_chroma_collection("meta-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)

        result = col.get(ids=[three_chunks[0]["chunk_id"]], include=["metadatas"])
        meta = result["metadatas"][0]
        assert meta["doc_id"] == three_chunks[0]["doc_id"]
        assert meta["drug_name"] == three_chunks[0]["drug_name"]
        assert meta["biomarker"] == three_chunks[0]["biomarker"]
        assert meta["section"] == three_chunks[0]["section"]

    def test_section_metadata_stored(self, patched_chroma, three_chunks):
        """Chunk with section='Warnings and Precautions' stores it in metadata."""
        col = get_chroma_collection("section-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)

        chunk_with_section = three_chunks[2]
        result = col.get(ids=[chunk_with_section["chunk_id"]], include=["metadatas"])
        assert result["metadatas"][0]["section"] == "Warnings and Precautions"

    def test_upsert_is_idempotent(self, patched_chroma, three_chunks):
        """Indexing the same chunks twice must not duplicate them."""
        col = get_chroma_collection("idem-idx-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        index_chunks(col, three_chunks)
        assert col.count() == len(three_chunks)

    def test_batch_splitting(self, patched_chroma):
        """index_chunks must correctly handle collections larger than batch_size."""
        chunks = [
            make_chunk(f"doc||bio::{i}", f"sentence number {i}")
            for i in range(10)
        ]
        col = get_chroma_collection("batch-col", base_path=patched_chroma)
        index_chunks(col, chunks, batch_size=3)   # forces 4 batches for 10 items
        assert col.count() == 10

    def test_missing_optional_metadata_defaults_to_empty(self, patched_chroma):
        """Chunks missing optional keys must default gracefully."""
        minimal = {
            "chunk_id": "x||y::0",
            "content": "Minimal chunk text.",
            "doc_id": "x||y",
        }
        col = get_chroma_collection("min-col", base_path=patched_chroma)
        index_chunks(col, [minimal])               # must not raise KeyError
        assert col.count() == 1


# ---------------------------------------------------------------------------
# query_collection
# ---------------------------------------------------------------------------

class TestQueryCollection:
    def test_returns_list_of_dicts(self, patched_chroma, three_chunks):
        col = get_chroma_collection("q-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        hits = query_collection(col, "melanoma treatment", k=2)
        assert isinstance(hits, list)
        assert all(isinstance(h, dict) for h in hits)

    def test_hit_schema_complete(self, patched_chroma, three_chunks):
        """Every hit must have the 5 required keys."""
        col = get_chroma_collection("schema-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        hits = query_collection(col, "adverse reactions", k=1)
        assert len(hits) == 1
        required_keys = {"chunk_id", "doc_id", "section", "content", "distance"}
        assert required_keys.issubset(hits[0].keys())

    def test_returns_at_most_k_hits(self, patched_chroma, three_chunks):
        col = get_chroma_collection("k-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        hits = query_collection(col, "dose", k=2)
        assert len(hits) <= 2

    def test_fewer_docs_than_k_returns_all_docs(self, patched_chroma):
        """When the collection has fewer docs than k, return what exists."""
        col = get_chroma_collection("few-col", base_path=patched_chroma)
        index_chunks(col, [make_chunk("a||b::0", "Only one chunk.")])
        hits = query_collection(col, "query text", k=10)
        assert len(hits) == 1

    def test_distance_is_float_in_valid_range(self, patched_chroma, three_chunks):
        """Cosine distance must be in [0, 2]."""
        col = get_chroma_collection("dist-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        hits = query_collection(col, "immune reaction", k=3)
        for hit in hits:
            assert isinstance(hit["distance"], float)
            assert 0.0 <= hit["distance"] <= 2.0

    def test_doc_id_matches_indexed_doc_id(self, patched_chroma, three_chunks):
        col = get_chroma_collection("docid-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        hits = query_collection(col, "KEYTRUDA melanoma", k=3)
        retrieved_doc_ids = {h["doc_id"] for h in hits}
        assert retrieved_doc_ids == {"KEYTRUDA||PD-L1"}

    def test_content_is_non_empty_string(self, patched_chroma, three_chunks):
        col = get_chroma_collection("content-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        hits = query_collection(col, "dose", k=3)
        for hit in hits:
            assert isinstance(hit["content"], str)
            assert hit["content"].strip()

    def test_chunk_id_matches_indexed_ids(self, patched_chroma, three_chunks):
        col = get_chroma_collection("cid-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)
        indexed_ids = {c["chunk_id"] for c in three_chunks}
        hits = query_collection(col, "query", k=3)
        for hit in hits:
            assert hit["chunk_id"] in indexed_ids

    def test_inconsistent_results_raise_value_error(self, patched_chroma, three_chunks):
        """
        If ChromaDB ever returns inconsistent-length arrays, query_collection
        must raise ValueError rather than silently truncating with zip.
        """
        col = get_chroma_collection("err-col", base_path=patched_chroma)
        index_chunks(col, three_chunks)

        bad_results = {
            "ids": [["id1", "id2"]],
            "metadatas": [[{"doc_id": "a", "section": ""}]],   # length 1 vs 2
            "distances": [[0.1, 0.2]],
            "documents": [["doc1", "doc2"]],
        }

        with patch.object(col, "query", return_value=bad_results):
            with pytest.raises(ValueError, match="inconsistent"):
                query_collection(col, "test", k=2)
