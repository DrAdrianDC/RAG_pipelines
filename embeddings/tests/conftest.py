"""
Shared fixtures for the embeddings test suite.

Design
------
No real model weights are loaded in any test.  We patch
``sentence_transformers.SentenceTransformer`` with a lightweight stub so
that tests run on any machine without GPU, network, or large downloads.

MockSentenceTransformer contract
---------------------------------
Satisfies the subset of the SentenceTransformer API used by this codebase:
    - ``encode(texts, ...) -> np.ndarray``  shape (N, DIM), float32

It does NOT mock the full HuggingFace model — only what our adapters call.
"""

from __future__ import annotations

import numpy as np
import pytest


DIM = 384  # all-MiniLM-L6-v2 output dimension


class MockSentenceTransformer:
    """
    Deterministic stub for ``sentence_transformers.SentenceTransformer``.

    Returns random float32 vectors seeded from the input text length so
    the same input always yields the same output within a test session.
    """

    def __init__(self, model_name: str = "mock-model") -> None:
        self._model_name = model_name

    def encode(
        self,
        sentences: list[str],
        batch_size: int = 64,
        show_progress_bar: bool = False,
        normalize_embeddings: bool = True,
        convert_to_numpy: bool = True,
    ) -> np.ndarray:
        rng = np.random.default_rng(seed=len(sentences))
        out = rng.random((len(sentences), DIM)).astype(np.float32)
        if normalize_embeddings:
            norms = np.linalg.norm(out, axis=1, keepdims=True)
            out = out / np.maximum(norms, 1e-12)
        return out


@pytest.fixture
def mock_st_model() -> MockSentenceTransformer:
    """Return a single MockSentenceTransformer instance."""
    return MockSentenceTransformer()


@pytest.fixture
def sample_texts() -> list[str]:
    """Realistic FDA-like texts for embedding tests."""
    return [
        "KEYTRUDA is indicated for the treatment of patients with unresectable melanoma.",
        "Administer 200 mg as an intravenous infusion over 30 minutes every 3 weeks.",
        "Monitor patients for immune-mediated adverse reactions including pneumonitis.",
        "PD-L1 expression should be confirmed by an FDA-approved test.",
    ]
