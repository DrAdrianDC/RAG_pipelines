"""
vectorstores — vector database adapters for the RAG_pipelines framework.

Public API
----------
``get_chroma_collection``   — create/open a persistent ChromaDB collection.
``index_chunks``            — upsert chunk dicts into a collection.
``query_collection``        — retrieve top-k hits for a query string.
``get_chroma_embedding_fn`` — ChromaDB-compatible SentenceTransformer wrapper.

Boundary rule
-------------
This package imports from ``embeddings/`` (for the embedding function) but
NOT from ``chunking/``, ``retrieval/``, or ``experiments/``.  Experiment
paths and benchmark configuration are always injected by the caller.

Hit dict schema
---------------
Every function that returns retrieval results uses this schema:

    {
        "chunk_id" : str,    # ChromaDB document ID
        "doc_id"   : str,    # drug_name||biomarker
        "section"  : str,    # canonical FDA section name, or ""
        "content"  : str,    # chunk text
        "distance" : float,  # cosine distance ∈ [0, 2], lower = more similar
    }

Adding a new backend
--------------------
Create ``vectorstores/<backend>.py`` exposing at minimum:
``get_<backend>_collection``, ``index_chunks``, ``query_collection``.
"""

from vectorstores.chroma import (
    get_chroma_collection,
    get_chroma_embedding_fn,
    index_chunks,
    query_collection,
)

__all__ = [
    "get_chroma_collection",
    "get_chroma_embedding_fn",
    "index_chunks",
    "query_collection",
]
