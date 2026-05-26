"""
SentenceTransformer embedding adapter.

Naming convention
-----------------
This file is ``sentence_transformer.py`` (singular) — not
``sentence_transformers.py``.  The plural name shadows the installed
``sentence_transformers`` library when Python walks ``sys.path``, which
is a latent import bug.  Singular = the adapter; plural = the library.

Responsibilities
----------------
- Own the model singleton lifecycle (one instance per model name, per process).
- Implement the ``EmbeddingModel`` protocol from ``embeddings.base``.
- Provide a simple ``embed(texts)`` function for callers that do not need
  the full model object.

What this module does NOT do
-----------------------------
- Does not import from ``chromadb``.  ChromaDB-specific integration
  (``SentenceTransformerEmbeddingFunction``) lives in ``vectorstores/chroma.py``
  where it belongs.
- Does not know about chunk schemas, collections, or experiments.

Extending
---------
To add a new embedding backend (OpenAI, Cohere, BGE …), create a new
file in this package (e.g. ``embeddings/openai.py``) that implements the
``EmbeddingModel`` protocol from ``embeddings.base``.
"""

from __future__ import annotations

import numpy as np
from sentence_transformers import SentenceTransformer

from embeddings.base import EmbeddingModel  # noqa: F401  (re-exported for type hints)

DEFAULT_MODEL = "all-MiniLM-L6-v2"

_model_cache: dict[str, SentenceTransformer] = {}


# ---------------------------------------------------------------------------
# Singleton loader
# ---------------------------------------------------------------------------

def get_sentence_transformer(model_name: str = DEFAULT_MODEL) -> SentenceTransformer:
    """
    Lazy-load and cache a SentenceTransformer by model name.

    Subsequent calls with the same ``model_name`` return the cached instance
    without reloading weights.  The cache is keyed on the exact string so
    ``"all-MiniLM-L6-v2"`` and ``"sentence-transformers/all-MiniLM-L6-v2"``
    are treated as different entries.

    Raises
    ------
    OSError
        If the model name is invalid or the model cannot be downloaded.
        Wraps the underlying sentence_transformers error with a clearer message.
    """
    if model_name not in _model_cache:
        try:
            _model_cache[model_name] = SentenceTransformer(model_name)
        except Exception as exc:
            raise OSError(
                f"Failed to load SentenceTransformer model '{model_name}'. "
                "Check the model name and your network connection.\n"
                f"Original error: {exc}"
            ) from exc
    return _model_cache[model_name]


# ---------------------------------------------------------------------------
# EmbeddingModel adapter
# ---------------------------------------------------------------------------

class SentenceTransformerAdapter:
    """
    Thin adapter that wraps a SentenceTransformer to satisfy the
    ``EmbeddingModel`` protocol.

    Use this when you need to pass a model object to a function that expects
    ``EmbeddingModel`` (e.g. ``SemanticChunker(model=...)``).

    Example
    -------
    >>> adapter = SentenceTransformerAdapter("BAAI/bge-small-en-v1.5")
    >>> embeddings = adapter.embed(["KEYTRUDA is indicated for melanoma."])
    >>> embeddings.shape
    (1, 384)
    """

    def __init__(self, model_name: str = DEFAULT_MODEL) -> None:
        self._model_name = model_name
        self._model: SentenceTransformer | None = None  # lazy

    @property
    def model_name(self) -> str:
        return self._model_name

    def _get_model(self) -> SentenceTransformer:
        if self._model is None:
            self._model = get_sentence_transformer(self._model_name)
        return self._model

    def embed(self, texts: list[str]) -> np.ndarray:
        """
        Embed ``texts`` and return a float32 array of shape
        ``(len(texts), embedding_dim)``.

        Embeddings are L2-normalised so cosine similarity equals dot product.
        """
        if not texts:
            raise ValueError("texts must be a non-empty list")
        return self._get_model().encode(
            texts,
            batch_size=64,
            show_progress_bar=False,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------

def embed(
    texts: list[str],
    model_name: str = DEFAULT_MODEL,
) -> np.ndarray:
    """
    One-call embedding convenience function.

    Equivalent to ``get_sentence_transformer(model_name).encode(texts, ...)``.
    Uses the shared singleton cache.

    Parameters
    ----------
    texts:
        Non-empty list of strings to embed.
    model_name:
        HuggingFace model identifier. Defaults to ``all-MiniLM-L6-v2``.

    Returns
    -------
    np.ndarray
        Shape ``(len(texts), embedding_dim)``, float32, L2-normalised.
    """
    if not texts:
        raise ValueError("texts must be a non-empty list")
    model = get_sentence_transformer(model_name)
    return model.encode(
        texts,
        batch_size=64,
        show_progress_bar=False,
        normalize_embeddings=True,
        convert_to_numpy=True,
    )
