"""
Abstract base for all embedding model adapters.

Every concrete adapter (sentence_transformer.py, openai.py, cohere.py …)
must satisfy this interface so that vectorstores, chunkers, and retrievers
can consume embeddings without knowing which backend is active.

Design
------
We use ``typing.Protocol`` (structural subtyping) rather than ABC so that
third-party model objects (e.g. a raw SentenceTransformer) can satisfy the
contract without explicitly inheriting from our class.

Minimal contract
----------------
``embed(texts)``  — the only method every consumer actually needs.
``model_name``    — read-only property for logging and collection naming.

The ``@runtime_checkable`` decorator allows ``isinstance(obj, EmbeddingModel)``
checks at runtime — useful in factory functions and tests.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import numpy as np


@runtime_checkable
class EmbeddingModel(Protocol):
    """
    Structural protocol for embedding model adapters.

    Any object that exposes ``model_name: str`` and
    ``embed(texts: list[str]) -> np.ndarray`` satisfies this protocol.
    """

    @property
    def model_name(self) -> str:
        """Canonical identifier for this model (e.g. 'all-MiniLM-L6-v2')."""
        ...

    def embed(self, texts: list[str]) -> np.ndarray:
        """
        Embed a list of texts.

        Parameters
        ----------
        texts:
            Non-empty list of strings to embed.

        Returns
        -------
        np.ndarray
            Shape ``(len(texts), embedding_dim)``, float32, L2-normalised.
        """
        ...
