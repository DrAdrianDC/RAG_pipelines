"""
Shared pytest fixtures and helpers for the chunking test suite.

Design principles
-----------------
- No real model weights are loaded in any test.  The MockSentenceTransformer
  returns deterministic embeddings from a fixed RNG seed so tests are
  reproducible on any machine without GPU, network access, or API keys.
- Fixtures provide the minimal JSONL records needed to exercise every code
  path without depending on the real corpus file on disk.
"""

from __future__ import annotations

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# JSONL record fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def short_record() -> dict:
    """A single-sentence record that fits in one chunk under any strategy."""
    return {
        "content": "KEYTRUDA is indicated for the treatment of unresectable melanoma.",
        "drug_name": "KEYTRUDA",
        "biomarker": "PD-L1",
        "labeling_sections": ["Indications and Usage"],
        "source": "fda_biomarkers",
    }


@pytest.fixture
def empty_record() -> dict:
    """A record with empty content — every strategy must return []."""
    return {
        "content": "",
        "drug_name": "KEYTRUDA",
        "biomarker": "PD-L1",
        "labeling_sections": [],
        "source": "fda_biomarkers",
    }


@pytest.fixture
def whitespace_record() -> dict:
    """A record with only whitespace — treated the same as empty."""
    return {
        "content": "   \n\t  ",
        "drug_name": "KEYTRUDA",
        "biomarker": "PD-L1",
        "labeling_sections": [],
        "source": "fda_biomarkers",
    }


@pytest.fixture
def long_record() -> dict:
    """
    A record long enough to produce multiple chunks at 512 tokens.
    Uses a realistic FDA sentence repeated to reach ~1500 tokens.
    """
    sentence = (
        "The recommended dose of KEYTRUDA is 200 mg administered as an "
        "intravenous infusion over 30 minutes every 3 weeks until disease "
        "progression, unacceptable toxicity, or up to 24 months."
    )
    return {
        "content": " ".join([sentence] * 10),
        "drug_name": "KEYTRUDA",
        "biomarker": "PD-L1",
        "labeling_sections": ["Dosage and Administration"],
        "source": "fda_biomarkers",
    }


@pytest.fixture
def structured_record() -> dict:
    """
    A record with realistic FDA ALL-CAPS section headers.
    Used to test StructureAwareChunker section detection.
    """
    return {
        "content": (
            "1 INDICATIONS AND USAGE "
            "KEYTRUDA is indicated for melanoma with confirmed PD-L1 expression. "
            "2 DOSAGE AND ADMINISTRATION "
            "Administer 200 mg as an intravenous infusion over 30 minutes every 3 weeks. "
            "5 WARNINGS AND PRECAUTIONS "
            "Immune-mediated adverse reactions have occurred. Monitor patients carefully. "
            "6 ADVERSE REACTIONS "
            "Most common adverse reactions include fatigue, rash, and nausea."
        ),
        "drug_name": "KEYTRUDA",
        "biomarker": "PD-L1",
        "labeling_sections": [
            "Indications and Usage",
            "Dosage and Administration",
            "Warnings and Precautions",
            "Adverse Reactions",
        ],
        "source": "fda_biomarkers",
    }


@pytest.fixture
def multi_paragraph_record() -> dict:
    """
    A record with double-newline paragraph breaks so RecursiveChunker
    can split cleanly at paragraph boundaries.
    """
    return {
        "content": (
            "KEYTRUDA is indicated for the treatment of patients with unresectable "
            "or metastatic melanoma.\n\n"
            "The recommended dose is 200 mg administered intravenously every 3 weeks "
            "until disease progression or unacceptable toxicity.\n\n"
            "Immune-mediated adverse reactions including pneumonitis, colitis, "
            "hepatitis, endocrinopathies, and nephritis have been reported."
        ),
        "drug_name": "KEYTRUDA",
        "biomarker": "PD-L1",
        "labeling_sections": ["Warnings and Precautions"],
        "source": "fda_biomarkers",
    }


# ---------------------------------------------------------------------------
# Mock embedding model
# ---------------------------------------------------------------------------

class MockSentenceTransformer:
    """
    Deterministic drop-in for SentenceTransformer.

    Returns embeddings from a fixed RNG so tests are reproducible.
    Accepts an optional ``embeddings`` array to inject specific similarity
    patterns for testing boundary-detection behaviour.
    """

    DIM = 384

    def __init__(self, embeddings: np.ndarray | None = None) -> None:
        self._fixed = embeddings

    def encode(
        self,
        sentences: list[str],
        batch_size: int = 64,
        show_progress_bar: bool = False,
        normalize_embeddings: bool = True,
    ) -> np.ndarray:
        n = len(sentences)
        if self._fixed is not None:
            out = self._fixed[:n]
        else:
            rng = np.random.default_rng(seed=42)
            out = rng.random((n, self.DIM)).astype(np.float32)

        if normalize_embeddings:
            norms = np.linalg.norm(out, axis=1, keepdims=True)
            out = out / np.maximum(norms, 1e-12)
        return out


@pytest.fixture
def mock_model() -> MockSentenceTransformer:
    """Default mock model — random but deterministic embeddings."""
    return MockSentenceTransformer()


@pytest.fixture
def similar_model() -> MockSentenceTransformer:
    """
    Mock model where all sentences have near-identical embeddings.
    The SemanticChunker should produce very few splits (large chunks).
    """
    base = np.ones((100, MockSentenceTransformer.DIM), dtype=np.float32)
    # Add tiny noise so embeddings are not exactly identical.
    rng = np.random.default_rng(seed=0)
    base += rng.random((100, MockSentenceTransformer.DIM)).astype(np.float32) * 0.001
    return MockSentenceTransformer(embeddings=base)


@pytest.fixture
def dissimilar_model() -> MockSentenceTransformer:
    """
    Mock model where each sentence is orthogonal to its neighbours.
    The SemanticChunker should produce many splits (small chunks).
    """
    n, d = 100, MockSentenceTransformer.DIM
    rng = np.random.default_rng(seed=7)
    embs = rng.random((n, d)).astype(np.float32)
    # Make consecutive embeddings dissimilar by alternating signs.
    embs[1::2] *= -1
    return MockSentenceTransformer(embeddings=embs)


# ---------------------------------------------------------------------------
# Schema validation helper (used across multiple test modules)
# ---------------------------------------------------------------------------

REQUIRED_CHUNK_KEYS = {
    "chunk_id", "content", "doc_id", "section",
    "drug_name", "biomarker", "token_count",
}


def assert_valid_chunk_schema(chunk: dict, record: dict) -> None:
    """Assert that *chunk* conforms to the BaseChunker output schema."""
    assert REQUIRED_CHUNK_KEYS.issubset(chunk.keys()), (
        f"Missing keys: {REQUIRED_CHUNK_KEYS - chunk.keys()}"
    )
    assert isinstance(chunk["chunk_id"], str) and "::" in chunk["chunk_id"]
    assert isinstance(chunk["content"], str) and chunk["content"].strip()
    assert chunk["doc_id"] == f"{record['drug_name']}||{record['biomarker']}"
    assert isinstance(chunk["section"], str)
    assert chunk["drug_name"] == record["drug_name"]
    assert chunk["biomarker"] == record["biomarker"]
    assert isinstance(chunk["token_count"], int) and chunk["token_count"] > 0
