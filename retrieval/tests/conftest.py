"""
Shared pytest fixtures for the retrieval test suite.

Design principles
-----------------
- No real ChromaDB collections, model weights, or network calls in any test.
- MockCrossEncoder returns deterministic scores so reranking order is
  predictable and tests are reproducible on any machine.
- All fixtures produce hit dicts that conform to the vectorstores.chroma
  output schema so tests validate the full contract.
"""

from __future__ import annotations

import pytest

from retrieval.dense_retriever import DenseRetriever


# ---------------------------------------------------------------------------
# Hit dict fixtures
# ---------------------------------------------------------------------------

def _make_hit(rank: int, doc_id: str = "KEYTRUDA||PD-L1") -> dict:
    """Return a hit dict that conforms to the vectorstores.chroma schema."""
    return {
        "chunk_id": f"{doc_id}::{rank:04d}",
        "doc_id":   doc_id,
        "section":  "Indications and Usage",
        "content":  f"This is chunk number {rank} about {doc_id}.",
        "distance": round(0.05 * rank, 4),
    }


@pytest.fixture
def sample_hits() -> list[dict]:
    """Five hits ordered by ascending distance (as ChromaDB returns them)."""
    return [_make_hit(i) for i in range(5)]


@pytest.fixture
def single_hit() -> list[dict]:
    """Exactly one hit — tests edge cases for k=1."""
    return [_make_hit(0)]


@pytest.fixture
def empty_hits() -> list[dict]:
    """Empty result — collection has no matching documents."""
    return []


# ---------------------------------------------------------------------------
# Mock query function and DenseRetriever
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_query_fn(sample_hits):
    """
    A callable that mimics vectorstores.chroma.query_collection.
    Returns the first min(k, len(sample_hits)) hits regardless of query text.
    """
    def _query(query: str, k: int) -> list[dict]:
        return sample_hits[:k]
    return _query


@pytest.fixture
def dense_retriever(mock_query_fn) -> DenseRetriever:
    """A DenseRetriever wired to mock_query_fn."""
    return DenseRetriever(query_fn=mock_query_fn, name="test_dense")


# ---------------------------------------------------------------------------
# Mock CrossEncoder — deterministic, no model weights loaded
# ---------------------------------------------------------------------------

class MockCrossEncoder:
    """
    Deterministic drop-in for sentence_transformers.CrossEncoder.

    Assigns scores based on the rank embedded in chunk content:
    higher-numbered chunks get lower scores so that reranking can reverse
    the original ordering in a predictable way.
    """

    def __init__(self, model_name: str = "mock") -> None:
        self.model_name = model_name

    def predict(self, pairs: list[tuple[str, str]]) -> list[float]:
        """
        Return a score per (query, passage) pair.
        Score = 10.0 - rank_number extracted from the passage text.
        Passage "chunk number 0" → score 10.0 (most relevant).
        Passage "chunk number 4" → score  6.0 (least relevant).
        Falls back to 5.0 if parsing fails.
        """
        scores = []
        for _query, passage in pairs:
            try:
                rank = int(passage.split("chunk number ")[1].split(" ")[0])
                scores.append(10.0 - rank)
            except (IndexError, ValueError):
                scores.append(5.0)
        return scores


@pytest.fixture
def mock_cross_encoder() -> MockCrossEncoder:
    """Default MockCrossEncoder instance."""
    return MockCrossEncoder()
