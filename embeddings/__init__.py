"""
embeddings — embedding model adapters for the RAG_pipelines framework.

Public API
----------
``EmbeddingModel``              — structural protocol every adapter satisfies.
``SentenceTransformerAdapter``  — HuggingFace sentence-transformers backend.
``get_sentence_transformer``    — singleton model loader (raw SentenceTransformer).
``embed``                       — one-call convenience function.
``DEFAULT_MODEL``               — default model name ("all-MiniLM-L6-v2").

Boundary rule
-------------
This package does NOT import from ``vectorstores/``, ``chunking/``,
``retrieval/``, or ``experiments/``.  It has no knowledge of ChromaDB
collections, chunk schemas, or experiment paths.  ChromaDB-specific
embedding function wrappers live in ``vectorstores/chroma.py``.

Adding a new backend
--------------------
Create ``embeddings/<provider>.py`` implementing the ``EmbeddingModel``
protocol, then re-export it here.  Example:

    # embeddings/openai.py
    class OpenAIAdapter:
        @property
        def model_name(self) -> str: ...
        def embed(self, texts: list[str]) -> np.ndarray: ...
"""

from embeddings.base import EmbeddingModel
from embeddings.sentence_transformer import (
    DEFAULT_MODEL,
    SentenceTransformerAdapter,
    embed,
    get_sentence_transformer,
)

__all__ = [
    "EmbeddingModel",
    "SentenceTransformerAdapter",
    "DEFAULT_MODEL",
    "embed",
    "get_sentence_transformer",
]
