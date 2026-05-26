"""
retrieval — retrieval pipeline adapters.

Public API
----------
Classes:
    BaseRetriever       Abstract interface every retriever must implement.
    DenseRetriever      Wraps any (query_text, k) -> list[dict] as a BaseRetriever.
    RerankedRetriever   Two-stage: DenseRetriever + cross-encoder reranking.

Factories:
    build_dense_retriever   Wire a ChromaDB collection into a DenseRetriever
                            in one call (no boilerplate).

Constants:
    DEFAULT_CROSS_ENCODER   Default cross-encoder model name.
    DEFAULT_FETCH_K         Default candidate pool size for reranking.

Boundary rule
-------------
This package imports from vectorstores/ and embeddings/ (via the factory
only — lazy imports inside build_dense_retriever).  It does NOT import
from chunking/, evaluation/, or experiments/.

Pipeline position
-----------------
    vectorstores/  →  retrieval/  →  evaluation/
    embeddings/   ↗

Usage
-----
    from retrieval import build_dense_retriever, RerankedRetriever

    dense    = build_dense_retriever("recursive_512", base_path=CHROMA_BASE_PATH)
    reranked = RerankedRetriever(inner=dense, fetch_k=20)

    hits = reranked.retrieve("What biomarkers does imatinib require?", k=5)
"""

from retrieval.base_retriever import BaseRetriever
from retrieval.dense_retriever import DenseRetriever, build_dense_retriever
from retrieval.reranker import RerankedRetriever, DEFAULT_CROSS_ENCODER, DEFAULT_FETCH_K

__all__ = [
    "BaseRetriever",
    "DenseRetriever",
    "RerankedRetriever",
    "build_dense_retriever",
    "DEFAULT_CROSS_ENCODER",
    "DEFAULT_FETCH_K",
]
