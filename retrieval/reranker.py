"""
Cross-encoder reranking layer for the retrieval pipeline.

Architecture
------------
Reranking is a two-stage process:

    Stage 1 — Candidate retrieval (fast, approximate)
        DenseRetriever.retrieve(query, k=fetch_k)
        → top-``fetch_k`` candidates via vector similarity (ANN)

    Stage 2 — Reranking (slower, precise)
        CrossEncoder.predict([(query, chunk_content), ...])
        → relevance scores for each (query, candidate) pair
        → sort descending → return top-k

Why two stages?
---------------
Dense retrieval (stage 1) is fast but uses separate query/document embeddings
— the model never sees them together.  A cross-encoder (stage 2) jointly
encodes the query and each candidate, capturing fine-grained relevance signals
that bi-encoders miss.  The cost is O(fetch_k) forward passes per query, which
is acceptable when fetch_k ≤ 50 and the cross-encoder is small.

Default model
-------------
``cross-encoder/ms-marco-MiniLM-L-6-v2`` is a 22 M-parameter model trained on
MS MARCO passage ranking.  It runs on CPU in ~50–200 ms for fetch_k=20 and
requires no API key.  It is the standard choice for offline RAG reranking
benchmarks (Weaviate, LlamaIndex, and LangChain all use it as their default
example).

fetch_k guideline
-----------------
Weaviate recommends fetch_k ≥ 3× the final k.  Default here is 20 for k=5,
which gives the reranker enough diversity to make a meaningful difference.
Setting fetch_k too low wastes the reranker; too high slows retrieval.

Boundary rule
-------------
This module imports from ``retrieval.base_retriever`` only.  The cross-encoder
model is lazy-loaded to avoid paying startup cost when only ``DenseRetriever``
is used.
"""

from __future__ import annotations

from typing import Any

from retrieval.base_retriever import BaseRetriever

DEFAULT_CROSS_ENCODER = "cross-encoder/ms-marco-MiniLM-L-6-v2"
DEFAULT_FETCH_K = 20


class RerankedRetriever(BaseRetriever):
    """
    Two-stage retriever: dense ANN candidate recall + cross-encoder reranking.

    Parameters
    ----------
    inner:
        Any ``BaseRetriever`` used for stage-1 candidate recall.
        Typically a ``DenseRetriever``.
    model_name:
        HuggingFace cross-encoder model identifier.
        Defaults to ``cross-encoder/ms-marco-MiniLM-L-6-v2``.
    fetch_k:
        Number of candidates to retrieve in stage 1 before reranking.
        Must be ≥ k (the final number of results requested by the caller).
        Weaviate recommends fetch_k ≥ 3× k.
    name:
        Human-readable identifier used in logs and experiment results.

    Notes
    -----
    - The cross-encoder model is lazy-loaded on the first call to ``retrieve``
      so that importing this module has no startup cost.
    - Each hit dict returned by ``retrieve`` gains a ``rerank_score`` field
      (float) that records the cross-encoder relevance score.
    - Results are sorted by ``rerank_score`` descending (higher = more relevant).

    Example
    -------
        from retrieval import DenseRetriever, RerankedRetriever, build_dense_retriever

        dense = build_dense_retriever("recursive_512", base_path=CHROMA_BASE_PATH)
        reranked = RerankedRetriever(inner=dense, fetch_k=20)

        hits = reranked.retrieve("BRAF mutation in melanoma", k=5)
        for hit in hits:
            print(f"{hit['rerank_score']:.4f}  {hit['doc_id']}  {hit['content'][:60]}")
    """

    def __init__(
        self,
        inner: BaseRetriever,
        model_name: str = DEFAULT_CROSS_ENCODER,
        fetch_k: int = DEFAULT_FETCH_K,
        name: str = "reranked",
    ) -> None:
        if fetch_k < 1:
            raise ValueError(f"fetch_k must be >= 1, got {fetch_k}")
        self._inner = inner
        self._model_name = model_name
        self._fetch_k = fetch_k
        self.name = name
        self._model = None  # lazy-loaded on first retrieve() call

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def retrieve(self, query: str, k: int) -> list[dict[str, Any]]:
        """
        Return the top-``k`` hits reranked by cross-encoder relevance score.

        Parameters
        ----------
        query:
            Natural-language query string.
        k:
            Final number of results to return after reranking.
            If fewer than ``k`` candidates are available, all are returned.

        Returns
        -------
        list[dict]
            Hit dicts sorted by ``rerank_score`` descending.  Each dict
            contains all fields from the inner retriever plus ``rerank_score``.
        """
        fetch_k = max(self._fetch_k, k)
        candidates = self._inner.retrieve(query, k=fetch_k)
        if not candidates:
            return []

        model = self._get_model()
        pairs = [(query, c["content"]) for c in candidates]
        scores = model.predict(pairs)

        ranked = sorted(
            zip(scores, candidates),
            key=lambda x: float(x[0]),
            reverse=True,
        )
        results = []
        for score, hit in ranked[:k]:
            enriched = dict(hit)
            enriched["rerank_score"] = float(score)
            results.append(enriched)
        return results

    @property
    def inner(self) -> BaseRetriever:
        """The stage-1 retriever used for candidate recall."""
        return self._inner

    @property
    def model_name(self) -> str:
        """HuggingFace identifier of the cross-encoder model."""
        return self._model_name

    @property
    def fetch_k(self) -> int:
        """Number of candidates fetched in stage 1 before reranking."""
        return self._fetch_k

    def __repr__(self) -> str:
        return (
            f"RerankedRetriever("
            f"inner={self._inner!r}, "
            f"model={self._model_name!r}, "
            f"fetch_k={self._fetch_k})"
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_model(self):
        """Lazy-load the cross-encoder model on first use."""
        if self._model is None:
            try:
                from sentence_transformers import CrossEncoder
                self._model = CrossEncoder(self._model_name)
            except Exception as exc:
                raise OSError(
                    f"Failed to load CrossEncoder model '{self._model_name}'. "
                    "Install sentence-transformers and check the model name.\n"
                    f"Original error: {exc}"
                ) from exc
        return self._model
