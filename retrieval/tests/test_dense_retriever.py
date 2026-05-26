"""
Tests for retrieval/dense_retriever.py.

Covers:
- DenseRetriever delegates retrieve() to the injected query_fn
- query_fn is called with the exact (query, k) arguments
- Result is passed through unchanged
- Empty results are handled
- name attribute is set correctly
- __repr__ includes the name
- build_dense_retriever wires vectorstores + embeddings correctly
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from retrieval.dense_retriever import DenseRetriever, build_dense_retriever


# ---------------------------------------------------------------------------
# DenseRetriever — delegation contract
# ---------------------------------------------------------------------------

class TestDenseRetrieverDelegation:

    def test_retrieve_calls_query_fn_with_correct_args(self, mock_query_fn):
        call_log = []

        def tracked_query_fn(query, k):
            call_log.append((query, k))
            return mock_query_fn(query, k)

        retriever = DenseRetriever(query_fn=tracked_query_fn)
        retriever.retrieve("What biomarkers does imatinib require?", k=3)

        assert len(call_log) == 1
        assert call_log[0] == ("What biomarkers does imatinib require?", 3)

    def test_retrieve_returns_query_fn_output_unchanged(self, sample_hits):
        retriever = DenseRetriever(query_fn=lambda q, k: sample_hits[:k])
        result = retriever.retrieve("any query", k=3)
        assert result == sample_hits[:3]

    def test_retrieve_empty_results(self, empty_hits):
        retriever = DenseRetriever(query_fn=lambda q, k: empty_hits)
        assert retriever.retrieve("query", k=5) == []

    def test_retrieve_k_larger_than_available(self, sample_hits):
        retriever = DenseRetriever(query_fn=lambda q, k: sample_hits[:k])
        result = retriever.retrieve("query", k=100)
        assert result == sample_hits

    def test_retrieve_k_equals_one(self, sample_hits):
        retriever = DenseRetriever(query_fn=lambda q, k: sample_hits[:k])
        result = retriever.retrieve("query", k=1)
        assert len(result) == 1
        assert result[0] == sample_hits[0]


# ---------------------------------------------------------------------------
# DenseRetriever — identity and schema
# ---------------------------------------------------------------------------

class TestDenseRetrieverIdentity:

    def test_default_name_is_dense(self, mock_query_fn):
        retriever = DenseRetriever(query_fn=mock_query_fn)
        assert retriever.name == "dense"

    def test_custom_name_is_stored(self, mock_query_fn):
        retriever = DenseRetriever(query_fn=mock_query_fn, name="recursive_512")
        assert retriever.name == "recursive_512"

    def test_repr_contains_name(self, mock_query_fn):
        retriever = DenseRetriever(query_fn=mock_query_fn, name="fixed_512")
        assert "fixed_512" in repr(retriever)

    def test_repr_format(self, mock_query_fn):
        retriever = DenseRetriever(query_fn=mock_query_fn, name="test")
        assert repr(retriever) == "DenseRetriever(name='test')"


# ---------------------------------------------------------------------------
# DenseRetriever — hit dict schema passthrough
# ---------------------------------------------------------------------------

class TestDenseRetrieverHitSchema:
    """DenseRetriever must pass through the full hit dict from query_fn."""

    REQUIRED_KEYS = {"chunk_id", "doc_id", "section", "content", "distance"}

    def test_hits_contain_required_keys(self, dense_retriever):
        hits = dense_retriever.retrieve("query", k=3)
        for hit in hits:
            assert self.REQUIRED_KEYS.issubset(hit.keys()), (
                f"Hit missing keys: {self.REQUIRED_KEYS - hit.keys()}"
            )

    def test_distance_is_float(self, dense_retriever):
        hits = dense_retriever.retrieve("query", k=3)
        for hit in hits:
            assert isinstance(hit["distance"], float)

    def test_doc_id_contains_separator(self, dense_retriever):
        hits = dense_retriever.retrieve("query", k=3)
        for hit in hits:
            assert "||" in hit["doc_id"]


# ---------------------------------------------------------------------------
# build_dense_retriever — factory
# ---------------------------------------------------------------------------

class TestBuildDenseRetriever:
    """
    build_dense_retriever must wire get_chroma_collection + query_collection
    and return a correctly configured DenseRetriever.
    Tests use mocks — no real ChromaDB or disk I/O.
    """

    def test_returns_dense_retriever_instance(self, sample_hits):
        mock_collection = MagicMock()
        with patch("retrieval.dense_retriever.get_chroma_collection",
                   return_value=mock_collection) as mock_get, \
             patch("retrieval.dense_retriever.query_collection",
                   return_value=sample_hits) as _mock_query:

            retriever = build_dense_retriever(
                collection_name="fixed_512",
                base_path=Path("/tmp/chroma"),
            )

        assert isinstance(retriever, DenseRetriever)
        mock_get.assert_called_once_with(
            name="fixed_512",
            base_path=Path("/tmp/chroma"),
            model_name="all-MiniLM-L6-v2",
        )

    def test_name_defaults_to_collection_name(self, sample_hits):
        mock_collection = MagicMock()
        with patch("retrieval.dense_retriever.get_chroma_collection",
                   return_value=mock_collection), \
             patch("retrieval.dense_retriever.query_collection",
                   return_value=sample_hits):

            retriever = build_dense_retriever("recursive_512", Path("/tmp"))

        assert retriever.name == "recursive_512"

    def test_name_override_is_respected(self, sample_hits):
        mock_collection = MagicMock()
        with patch("retrieval.dense_retriever.get_chroma_collection",
                   return_value=mock_collection), \
             patch("retrieval.dense_retriever.query_collection",
                   return_value=sample_hits):

            retriever = build_dense_retriever(
                "fixed_512", Path("/tmp"), name="my_retriever"
            )

        assert retriever.name == "my_retriever"

    def test_retrieve_calls_query_collection(self, sample_hits):
        mock_collection = MagicMock()
        with patch("retrieval.dense_retriever.get_chroma_collection",
                   return_value=mock_collection), \
             patch("retrieval.dense_retriever.query_collection",
                   return_value=sample_hits[:3]) as mock_qc:

            retriever = build_dense_retriever("fixed_512", Path("/tmp"))
            hits = retriever.retrieve("imatinib biomarkers", k=3)

        mock_qc.assert_called_once_with(mock_collection, "imatinib biomarkers", 3)
        assert hits == sample_hits[:3]

    def test_custom_model_name_is_passed_to_collection(self):
        mock_collection = MagicMock()
        with patch("retrieval.dense_retriever.get_chroma_collection",
                   return_value=mock_collection) as mock_get, \
             patch("retrieval.dense_retriever.query_collection", return_value=[]):

            build_dense_retriever(
                "fixed_512",
                Path("/tmp"),
                model_name="BAAI/bge-small-en-v1.5",
            )

        mock_get.assert_called_once_with(
            name="fixed_512",
            base_path=Path("/tmp"),
            model_name="BAAI/bge-small-en-v1.5",
        )
