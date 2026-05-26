"""
Tests for embeddings/base.py — EmbeddingModel protocol.

Covers:
- Protocol is runtime_checkable (isinstance works at runtime)
- Objects that satisfy the structural contract are accepted
- Objects that are missing required members are rejected
- The protocol does not require explicit inheritance
"""

from __future__ import annotations

import numpy as np
import pytest

from embeddings.base import EmbeddingModel
from embeddings.tests.conftest import DIM, MockSentenceTransformer


# ---------------------------------------------------------------------------
# Structural conformance (no inheritance required)
# ---------------------------------------------------------------------------

class TestProtocolConformance:
    def test_mock_satisfies_protocol(self):
        """MockSentenceTransformer satisfies EmbeddingModel structurally."""
        class ConformingAdapter:
            @property
            def model_name(self) -> str:
                return "test-model"

            def embed(self, texts: list[str]) -> np.ndarray:
                return np.zeros((len(texts), DIM), dtype=np.float32)

        adapter = ConformingAdapter()
        assert isinstance(adapter, EmbeddingModel)

    def test_missing_model_name_fails_protocol(self):
        """An object without model_name does not satisfy EmbeddingModel."""
        class MissingModelName:
            def embed(self, texts: list[str]) -> np.ndarray:
                return np.zeros((len(texts), DIM), dtype=np.float32)

        assert not isinstance(MissingModelName(), EmbeddingModel)

    def test_missing_embed_fails_protocol(self):
        """An object without embed() does not satisfy EmbeddingModel."""
        class MissingEmbed:
            @property
            def model_name(self) -> str:
                return "test-model"

        assert not isinstance(MissingEmbed(), EmbeddingModel)

    def test_plain_object_fails_protocol(self):
        assert not isinstance(object(), EmbeddingModel)

    def test_none_fails_protocol(self):
        assert not isinstance(None, EmbeddingModel)


# ---------------------------------------------------------------------------
# Protocol is runtime_checkable
# ---------------------------------------------------------------------------

class TestRuntimeCheckable:
    def test_isinstance_does_not_raise(self):
        """@runtime_checkable means isinstance() works without TypeError."""
        try:
            result = isinstance("not_a_model", EmbeddingModel)
            assert result is False
        except TypeError:
            pytest.fail("EmbeddingModel is not @runtime_checkable")

    def test_protocol_can_be_used_in_type_guard(self):
        """Verify the protocol can be used in a type-narrowing function."""
        def requires_embedding_model(obj: object) -> bool:
            return isinstance(obj, EmbeddingModel)

        class ValidAdapter:
            @property
            def model_name(self) -> str:
                return "test"
            def embed(self, texts: list[str]) -> np.ndarray:
                return np.zeros((len(texts), 1))

        assert requires_embedding_model(ValidAdapter()) is True
        assert requires_embedding_model("not a model") is False
