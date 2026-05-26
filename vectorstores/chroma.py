"""
ChromaDB vector store adapter.

Design principles
-----------------
**Path-agnostic**: storage directories are injected by the caller.
Experiment-specific paths (e.g. ``experiments/chunking_benchmark/chroma_stores/``)
are never hardcoded here, making this module reusable across all experiments.

**Single responsibility**: this module only knows about ChromaDB.  All
embedding logic lives in ``embeddings/``.  The only coupling to the
embeddings package is the ``get_chroma_embedding_fn`` helper below, which
wraps the SentenceTransformer in ChromaDB's expected interface.

Hit dict schema (returned by ``query_collection``)
--------------------------------------------------
Every hit returned by this module has exactly these keys:

    {
        "chunk_id" : str,   # ChromaDB document ID
        "doc_id"   : str,   # drug_name||biomarker (from metadata)
        "section"  : str,   # canonical FDA section name, or ""
        "content"  : str,   # raw chunk text
        "distance" : float, # cosine distance ∈ [0, 2], lower = more similar
    }

This schema is consumed by ``evaluation/metrics.py`` and ``retrieval/``.

Bug fixes vs. previous version
--------------------------------
1. ``reset``: was O(n) get-then-delete.  Now uses ``delete_collection`` +
   ``create_collection`` — atomic and independent of collection size.
2. ``kwargs`` pattern: collapsed into a single ``embedding_fn`` variable.
3. ``index_chunks``: early-return guard when ``chunks`` is empty.
4. ``query_collection``: uses explicit length validation instead of bare
   ``zip`` to catch inconsistent ChromaDB responses defensively.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import chromadb
from chromadb.utils import embedding_functions

from embeddings.sentence_transformer import DEFAULT_MODEL


# ---------------------------------------------------------------------------
# ChromaDB embedding function (ChromaDB-specific — does not live in embeddings/)
# ---------------------------------------------------------------------------

def get_chroma_embedding_fn(
    model_name: str = DEFAULT_MODEL,
) -> embedding_functions.SentenceTransformerEmbeddingFunction:
    """
    Return a ChromaDB-compatible embedding function backed by ``model_name``.

    This wrapper is intentionally in ``vectorstores/chroma.py`` rather than
    in ``embeddings/`` — it is ChromaDB-specific plumbing, not a general
    embedding utility.

    The returned function is used when creating or querying a ChromaDB
    collection so that indexing and query embeddings use the same model.
    """
    return embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=model_name
    )


# ---------------------------------------------------------------------------
# Collection lifecycle
# ---------------------------------------------------------------------------

def get_chroma_collection(
    name: str,
    base_path: Path,
    reset: bool = False,
    model_name: str = DEFAULT_MODEL,
) -> chromadb.Collection:
    """
    Return (or create) a persistent ChromaDB collection.

    Storage layout::

        base_path/
        └── <name>/          ← one subdirectory per collection
            └── chroma.sqlite3

    Parameters
    ----------
    name:
        Collection name AND subdirectory name under ``base_path``.
    base_path:
        Root directory where collections for this experiment live.
        Injected by the caller — this function never constructs paths.
    reset:
        If ``True``, delete and recreate the collection so it is empty.
        Uses ``delete_collection`` (atomic) rather than iterating over IDs.
    model_name:
        SentenceTransformer model used for indexing and querying.
        Must be consistent across all calls for a given collection.
    """
    collection_path = base_path / name
    collection_path.mkdir(parents=True, exist_ok=True)

    embedding_fn = get_chroma_embedding_fn(model_name)
    client = chromadb.PersistentClient(path=str(collection_path))

    if reset:
        try:
            client.delete_collection(name)
        except Exception:
            pass  # collection did not exist yet — nothing to delete

    collection = client.get_or_create_collection(
        name=name,
        metadata={"hnsw:space": "cosine"},
        embedding_function=embedding_fn,
    )
    return collection


# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------

def index_chunks(
    collection: chromadb.Collection,
    chunks: list[dict[str, Any]],
    batch_size: int = 256,
) -> None:
    """
    Upsert ``chunks`` into ``collection`` in batches.

    Required keys per chunk dict: ``chunk_id``, ``content``, ``doc_id``.
    Optional keys: ``section``, ``drug_name``, ``biomarker``, ``token_count``.

    Parameters
    ----------
    collection:
        Target ChromaDB collection (already initialised with an embedding function).
    chunks:
        List of chunk dicts produced by any ``BaseChunker`` subclass.
    batch_size:
        Number of documents per ChromaDB upsert call.  256 is a safe default
        that balances throughput and memory on a single machine.
    """
    if not chunks:
        return

    for start in range(0, len(chunks), batch_size):
        batch = chunks[start: start + batch_size]
        collection.upsert(
            ids=[c["chunk_id"] for c in batch],
            documents=[c["content"] for c in batch],
            metadatas=[
                {
                    "doc_id":      c["doc_id"],
                    "section":     c.get("section", ""),
                    "drug_name":   c.get("drug_name", ""),
                    "biomarker":   c.get("biomarker", ""),
                    "token_count": c.get("token_count", 0),
                }
                for c in batch
            ],
        )


# ---------------------------------------------------------------------------
# Querying
# ---------------------------------------------------------------------------

def query_collection(
    collection: chromadb.Collection,
    query_text: str,
    k: int,
) -> list[dict[str, Any]]:
    """
    Retrieve the top-``k`` most similar chunks for ``query_text``.

    ChromaDB returns fewer than ``k`` results when the collection has fewer
    than ``k`` documents — this is handled gracefully.

    Parameters
    ----------
    collection:
        ChromaDB collection to query.
    query_text:
        The natural-language query string.
    k:
        Maximum number of results to return.

    Returns
    -------
    list[dict]
        Ordered list of hit dicts (most similar first), each with keys:
        ``chunk_id``, ``doc_id``, ``section``, ``content``, ``distance``.

    Raises
    ------
    ValueError
        If ChromaDB returns inconsistent result lengths (defensive guard).
    """
    results = collection.query(
        query_texts=[query_text],
        n_results=k,
        include=["metadatas", "distances", "documents"],
    )

    ids       = results["ids"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]
    documents = results["documents"][0]

    if not (len(ids) == len(metadatas) == len(distances) == len(documents)):
        raise ValueError(
            f"ChromaDB returned inconsistent result lengths: "
            f"ids={len(ids)}, metadatas={len(metadatas)}, "
            f"distances={len(distances)}, documents={len(documents)}"
        )

    return [
        {
            "chunk_id": ids[i],
            "doc_id":   metadatas[i]["doc_id"],
            "section":  metadatas[i].get("section", ""),
            "content":  documents[i] or "",
            "distance": distances[i],
        }
        for i in range(len(ids))
    ]
