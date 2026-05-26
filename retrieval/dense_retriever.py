"""
Dense (vector similarity) retriever backed by any vectorstore backend.

Design notes
------------
- ``query_fn`` is injected so the retriever does not import or depend on any
  specific vector database.  This makes it trivial to swap ChromaDB for FAISS,
  Weaviate, or any other backend without touching the retrieval layer.
- ``evaluation/metrics.py`` accepts any callable with the same signature, so
  ``DenseRetriever.retrieve`` can be passed directly as ``query_fn`` to
  ``RetrievalEvaluator``.
- ``build_dense_retriever`` is a convenience factory that wires
  ``vectorstores.chroma`` + ``embeddings.sentence_transformer`` in one call,
  eliminating boilerplate in experiment runners.

Usage — manual wiring (maximum control)
----------------------------------------
    from vectorstores.chroma import get_chroma_collection, query_collection
    from retrieval import DenseRetriever

    collection = get_chroma_collection("fixed_512", base_path=CHROMA_BASE_PATH)
    retriever = DenseRetriever(
        query_fn=lambda q, k: query_collection(collection, q, k),
        name="fixed_512",
    )
    hits = retriever.retrieve("What biomarkers does imatinib require?", k=5)

Usage — factory (recommended for experiments)
----------------------------------------------
    from retrieval import build_dense_retriever

    retriever = build_dense_retriever(
        collection_name="fixed_512",
        base_path=CHROMA_BASE_PATH,
    )
    hits = retriever.retrieve("What biomarkers does imatinib require?", k=5)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from embeddings.sentence_transformer import DEFAULT_MODEL
from retrieval.base_retriever import BaseRetriever
from vectorstores.chroma import get_chroma_collection, query_collection


class DenseRetriever(BaseRetriever):
    """
    Wraps any vector-similarity search function as a ``BaseRetriever``.

    Parameters
    ----------
    query_fn:
        Callable ``(query_text: str, k: int) -> list[dict]``.
        Typically a lambda over ``vectorstores.chroma.query_collection``.
    name:
        Human-readable identifier used in logs and experiment results.
    """

    def __init__(
        self,
        query_fn: Callable[[str, int], list[dict[str, Any]]],
        name: str = "dense",
    ) -> None:
        self._query_fn = query_fn
        self.name = name

    def retrieve(self, query: str, k: int) -> list[dict[str, Any]]:
        """Return the top-``k`` hits for ``query`` via vector similarity search."""
        return self._query_fn(query, k)

    def __repr__(self) -> str:
        return f"DenseRetriever(name={self.name!r})"


# ---------------------------------------------------------------------------
# Factory — wires vectorstores + embeddings in one call
# ---------------------------------------------------------------------------

def build_dense_retriever(
    collection_name: str,
    base_path: Path,
    model_name: str = DEFAULT_MODEL,
    name: str | None = None,
) -> DenseRetriever:
    """
    Build a ``DenseRetriever`` connected to a ChromaDB collection.

    This factory eliminates the boilerplate of creating a collection,
    obtaining a query function, and wiring them into a retriever.

    Parameters
    ----------
    collection_name:
        Name of the ChromaDB collection (must already be indexed).
        Also used as the ``DenseRetriever.name`` if ``name`` is not given.
    base_path:
        Root directory where ChromaDB collections are stored.
        Pass ``experiments/chunking_benchmark/chroma_stores`` or equivalent.
    model_name:
        SentenceTransformer model used for query embedding.
        Must match the model used when the collection was indexed.
    name:
        Optional override for ``DenseRetriever.name``.
        Defaults to ``collection_name``.

    Returns
    -------
    DenseRetriever
        Ready-to-use retriever wired to the specified ChromaDB collection.

    Example
    -------
        from pathlib import Path
        from retrieval import build_dense_retriever

        retriever = build_dense_retriever(
            collection_name="recursive_512",
            base_path=Path("experiments/chunking_benchmark/chroma_stores"),
        )
        hits = retriever.retrieve("BRAF mutation in melanoma", k=5)
    """
    collection = get_chroma_collection(
        name=collection_name,
        base_path=base_path,
        model_name=model_name,
    )
    return DenseRetriever(
        query_fn=lambda q, k: query_collection(collection, q, k),
        name=name if name is not None else collection_name,
    )
