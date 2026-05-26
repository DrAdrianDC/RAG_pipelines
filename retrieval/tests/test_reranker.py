"""
Tests for retrieval/reranker.py — RerankedRetriever.

Covers:
- retrieve() calls inner.retrieve with fetch_k (not k)
- Results are sorted by rerank_score descending
- Each hit gains a rerank_score field
- Only top-k results are returned
- Empty candidates from inner retriever → empty output
- fetch_k < k is auto-corrected to k
- Invalid fetch_k raises ValueError at construction
- Lazy model loading: cross-encoder is not loaded until first retrieve()
- __repr__ contains model name and fetch_k
- Properties: inner, model_name, fetch_k
"""

from __future__ import annotations

import pytest

from retrieval.dense_retriever import DenseRetriever
from retrieval.reranker import RerankedRetriever, DEFAULT_CROSS_ENCODER, DEFAULT_FETCH_K


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_retriever_with_mock_encoder(
    hits: list[dict],
    mock_cross_encoder,
    fetch_k: int = 20,
) -> RerankedRetriever:
    """Build a RerankedRetriever whose cross-encoder is replaced by a mock."""
    inner = DenseRetriever(query_fn=lambda q, k: hits[:k], name="inner")
    retriever = RerankedRetriever(inner=inner, fetch_k=fetch_k)
    retriever._model = mock_cross_encoder
    return retriever


# ---------------------------------------------------------------------------
# Core behaviour
# ---------------------------------------------------------------------------

class TestRerankedRetrieverCore:

    def test_returns_list(self, sample_hits, mock_cross_encoder):
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        result = retriever.retrieve("query", k=3)
        assert isinstance(result, list)

    def test_returns_at_most_k_results(self, sample_hits, mock_cross_encoder):
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        result = retriever.retrieve("query", k=3)
        assert len(result) == 3

    def test_returns_all_when_fewer_candidates_than_k(
        self, single_hit, mock_cross_encoder
    ):
        retriever = _make_retriever_with_mock_encoder(
            single_hit, mock_cross_encoder, fetch_k=20
        )
        result = retriever.retrieve("query", k=5)
        assert len(result) == 1

    def test_empty_inner_returns_empty(self, empty_hits, mock_cross_encoder):
        retriever = _make_retriever_with_mock_encoder(empty_hits, mock_cross_encoder)
        assert retriever.retrieve("query", k=5) == []


# ---------------------------------------------------------------------------
# Reranking order
# ---------------------------------------------------------------------------

class TestRerankedRetrieverOrdering:
    """
    MockCrossEncoder assigns score = 10 - rank_number.
    Original hits are ordered rank 0, 1, 2, 3, 4 (distance ascending).
    After reranking: rank 0 → score 10.0 (best), rank 4 → score 6.0 (worst).
    So the order should be preserved (rank 0 first), but the key point is that
    rerank_score controls the final ordering.
    """

    def test_results_sorted_by_rerank_score_descending(
        self, sample_hits, mock_cross_encoder
    ):
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        result = retriever.retrieve("query", k=5)
        scores = [hit["rerank_score"] for hit in result]
        assert scores == sorted(scores, reverse=True), (
            f"Results are not sorted by rerank_score descending: {scores}"
        )

    def test_rerank_score_is_float(self, sample_hits, mock_cross_encoder):
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        for hit in retriever.retrieve("query", k=3):
            assert isinstance(hit["rerank_score"], float)

    def test_best_hit_has_highest_rerank_score(self, sample_hits, mock_cross_encoder):
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        result = retriever.retrieve("query", k=5)
        assert result[0]["rerank_score"] >= result[-1]["rerank_score"]

    def test_reranking_can_change_original_order(self, mock_cross_encoder):
        """
        Build hits where distance order is rank 4, 3, 2, 1, 0 (worst first).
        MockCrossEncoder should reorder them to rank 0 first (score 10.0).
        """
        hits_reversed = [
            {
                "chunk_id": f"doc::000{4-i}",
                "doc_id": "DRUG||BM",
                "section": "",
                "content": f"This is chunk number {4-i} about x.",
                "distance": 0.05 * i,
            }
            for i in range(5)
        ]
        inner = DenseRetriever(
            query_fn=lambda q, k: hits_reversed[:k], name="inner"
        )
        retriever = RerankedRetriever(inner=inner, fetch_k=20)
        retriever._model = mock_cross_encoder

        result = retriever.retrieve("query", k=5)
        # rank 0 should come first (score 10.0)
        assert "chunk number 0" in result[0]["content"]


# ---------------------------------------------------------------------------
# rerank_score field added to hits
# ---------------------------------------------------------------------------

class TestRerankedRetrieverHitSchema:

    def test_rerank_score_added_to_every_hit(self, sample_hits, mock_cross_encoder):
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        for hit in retriever.retrieve("query", k=3):
            assert "rerank_score" in hit

    def test_original_hit_fields_preserved(self, sample_hits, mock_cross_encoder):
        required_keys = {"chunk_id", "doc_id", "section", "content", "distance"}
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        for hit in retriever.retrieve("query", k=3):
            assert required_keys.issubset(hit.keys())

    def test_original_hits_not_mutated(self, sample_hits, mock_cross_encoder):
        """retrieve() must not add rerank_score to the original hit dicts."""
        originals = [dict(h) for h in sample_hits]
        retriever = _make_retriever_with_mock_encoder(sample_hits, mock_cross_encoder)
        retriever.retrieve("query", k=3)
        for original, current in zip(originals, sample_hits):
            assert original == current, "Original hit dict was mutated"


# ---------------------------------------------------------------------------
# fetch_k behaviour
# ---------------------------------------------------------------------------

class TestRerankedRetrieverFetchK:

    def test_inner_retrieve_called_with_fetch_k(self, mock_cross_encoder):
        call_log = []

        def tracked_query(q, k):
            call_log.append(k)
            return [
                {
                    "chunk_id": f"x::{i}",
                    "doc_id": "D||B",
                    "section": "",
                    "content": f"This is chunk number {i} about x.",
                    "distance": 0.1 * i,
                }
                for i in range(min(k, 20))
            ]

        inner = DenseRetriever(query_fn=tracked_query, name="inner")
        retriever = RerankedRetriever(inner=inner, fetch_k=15)
        retriever._model = mock_cross_encoder

        retriever.retrieve("query", k=5)
        assert call_log[0] == 15, (
            f"Expected inner.retrieve called with k=15, got k={call_log[0]}"
        )

    def test_fetch_k_auto_corrected_when_less_than_k(self, mock_cross_encoder):
        """If fetch_k < k, retrieve must still request at least k candidates."""
        call_log = []

        def tracked_query(q, k):
            call_log.append(k)
            return []

        inner = DenseRetriever(query_fn=tracked_query)
        retriever = RerankedRetriever(inner=inner, fetch_k=3)
        retriever._model = mock_cross_encoder

        retriever.retrieve("query", k=10)
        assert call_log[0] >= 10, (
            f"fetch_k=3 < k=10: inner must be called with at least k=10, got {call_log[0]}"
        )

    def test_invalid_fetch_k_raises_on_construction(self):
        inner = DenseRetriever(query_fn=lambda q, k: [])
        with pytest.raises(ValueError, match="fetch_k"):
            RerankedRetriever(inner=inner, fetch_k=0)


# ---------------------------------------------------------------------------
# Identity and properties
# ---------------------------------------------------------------------------

class TestRerankedRetrieverIdentity:

    def test_default_name_is_reranked(self):
        inner = DenseRetriever(query_fn=lambda q, k: [])
        retriever = RerankedRetriever(inner=inner)
        assert retriever.name == "reranked"

    def test_default_model_name(self):
        inner = DenseRetriever(query_fn=lambda q, k: [])
        retriever = RerankedRetriever(inner=inner)
        assert retriever.model_name == DEFAULT_CROSS_ENCODER

    def test_default_fetch_k(self):
        inner = DenseRetriever(query_fn=lambda q, k: [])
        retriever = RerankedRetriever(inner=inner)
        assert retriever.fetch_k == DEFAULT_FETCH_K

    def test_inner_property_returns_inner_retriever(self):
        inner = DenseRetriever(query_fn=lambda q, k: [], name="inner")
        retriever = RerankedRetriever(inner=inner)
        assert retriever.inner is inner

    def test_repr_contains_model_and_fetch_k(self):
        inner = DenseRetriever(query_fn=lambda q, k: [])
        retriever = RerankedRetriever(inner=inner, fetch_k=15)
        r = repr(retriever)
        assert "fetch_k=15" in r
        assert DEFAULT_CROSS_ENCODER in r

    def test_model_is_not_loaded_at_construction(self):
        """Cross-encoder must be lazy-loaded (no model at construction time)."""
        inner = DenseRetriever(query_fn=lambda q, k: [])
        retriever = RerankedRetriever(inner=inner)
        assert retriever._model is None


# ---------------------------------------------------------------------------
# Default constants
# ---------------------------------------------------------------------------

class TestRerankerConstants:

    def test_default_cross_encoder_is_ms_marco(self):
        assert "ms-marco" in DEFAULT_CROSS_ENCODER

    def test_default_fetch_k_is_positive(self):
        assert DEFAULT_FETCH_K > 0

    def test_default_fetch_k_is_reasonable_multiple_of_k(self):
        typical_k = 5
        assert DEFAULT_FETCH_K >= 3 * typical_k, (
            f"DEFAULT_FETCH_K={DEFAULT_FETCH_K} should be >= 3× typical k={typical_k} "
            "(Weaviate recommendation)"
        )
