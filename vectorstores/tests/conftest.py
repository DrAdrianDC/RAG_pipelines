"""
Shared fixtures for the vectorstores test suite.

Design
------
- ChromaDB is tested with a **real in-memory client** (``chromadb.EphemeralClient``).
  No disk writes, no temp directories, no cleanup needed.
- The SentenceTransformer embedding function is patched with a
  ``MockEmbeddingFunction`` so no model weights are loaded and tests
  are fast and deterministic.
- All fixtures are function-scoped (default) so each test gets a
  fresh, isolated collection.

MockEmbeddingFunction contract
-------------------------------
ChromaDB expects a callable: ``fn(input: list[str]) -> list[list[float]]``
The mock returns deterministic random vectors of dimension 384.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import chromadb
import numpy as np
import pytest


EMBED_DIM = 384
_PATCH_TARGET = "vectorstores.chroma.get_chroma_embedding_fn"


class MockEmbeddingFunction:
    """
    ChromaDB-compatible embedding function stub.

    Returns deterministic float vectors without loading any model.
    Seeded on the number of texts so results are consistent per call.
    """

    def __call__(self, input: list[str]) -> list[list[float]]:  # noqa: A002
        rng = np.random.default_rng(seed=len(input) + 1)
        return rng.random((len(input), EMBED_DIM)).tolist()


@pytest.fixture
def mock_embedding_fn() -> MockEmbeddingFunction:
    return MockEmbeddingFunction()


@pytest.fixture
def patched_chroma(tmp_path, mock_embedding_fn):
    """
    Patch ``get_chroma_embedding_fn`` in ``vectorstores.chroma`` so that every
    call to ``get_chroma_collection`` uses the mock instead of a real model.

    Yields the ``tmp_path`` Path object so tests can call
    ``get_chroma_collection(name, base_path=patched_chroma)``.
    """
    with patch(_PATCH_TARGET, return_value=mock_embedding_fn):
        yield tmp_path


# ---------------------------------------------------------------------------
# Sample chunk dicts (matching BaseChunker output schema)
# ---------------------------------------------------------------------------

def make_chunk(
    chunk_id: str,
    content: str,
    doc_id: str = "KEYTRUDA||PD-L1",
    section: str = "",
    drug_name: str = "KEYTRUDA",
    biomarker: str = "PD-L1",
    token_count: int = 20,
) -> dict[str, Any]:
    return {
        "chunk_id":    chunk_id,
        "content":     content,
        "doc_id":      doc_id,
        "section":     section,
        "drug_name":   drug_name,
        "biomarker":   biomarker,
        "token_count": token_count,
    }


@pytest.fixture
def three_chunks() -> list[dict[str, Any]]:
    """Three minimal chunks from the same document."""
    return [
        make_chunk("KEYTRUDA||PD-L1::0", "KEYTRUDA is indicated for melanoma treatment."),
        make_chunk("KEYTRUDA||PD-L1::1", "Administer 200 mg every 3 weeks intravenously."),
        make_chunk("KEYTRUDA||PD-L1::2", "Monitor for immune-mediated adverse reactions.",
                   section="Warnings and Precautions"),
    ]


@pytest.fixture
def multi_doc_chunks() -> list[dict[str, Any]]:
    """Chunks from two different documents for retrieval precision tests."""
    doc_a = [
        make_chunk("KEYTRUDA||PD-L1::0", "KEYTRUDA is a PD-1 inhibitor for melanoma.",
                   doc_id="KEYTRUDA||PD-L1", drug_name="KEYTRUDA", biomarker="PD-L1"),
        make_chunk("KEYTRUDA||PD-L1::1", "Dose: 200 mg IV every 3 weeks.",
                   doc_id="KEYTRUDA||PD-L1", drug_name="KEYTRUDA", biomarker="PD-L1"),
    ]
    doc_b = [
        make_chunk("HERCEPTIN||HER2::0", "HERCEPTIN targets HER2-positive breast cancer.",
                   doc_id="HERCEPTIN||HER2", drug_name="HERCEPTIN", biomarker="HER2"),
        make_chunk("HERCEPTIN||HER2::1", "Administer 8 mg/kg loading dose then 6 mg/kg.",
                   doc_id="HERCEPTIN||HER2", drug_name="HERCEPTIN", biomarker="HER2"),
    ]
    return doc_a + doc_b
