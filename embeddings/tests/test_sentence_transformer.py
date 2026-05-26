"""
Tests for embeddings/sentence_transformer.py.

No real model weights are loaded — SentenceTransformer is patched with
MockSentenceTransformer throughout via unittest.mock.patch.

Covers:
- get_sentence_transformer: lazy load, cache hit, cache isolation, error handling
- embed(): output shape, dtype, L2 normalisation, empty-list guard
- SentenceTransformerAdapter: model_name property, embed(), lazy load, protocol
- Module-level DEFAULT_MODEL value
"""

from __future__ import annotations

import numpy as np
import pytest
from unittest.mock import MagicMock, patch

from embeddings.base import EmbeddingModel
from embeddings.sentence_transformer import (
    DEFAULT_MODEL,
    SentenceTransformerAdapter,
    embed,
    get_sentence_transformer,
)
from embeddings.tests.conftest import DIM, MockSentenceTransformer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PATCH_TARGET = "embeddings.sentence_transformer.SentenceTransformer"


def _patched_st(mock_cls=None):
    """
    Context manager: patch SentenceTransformer and flush the module-level cache.
    Returns a MockSentenceTransformer instance when instantiated.
    """
    import embeddings.sentence_transformer as _mod
    _mod._model_cache.clear()
    if mock_cls is None:
        mock_cls = MockSentenceTransformer
    return patch(PATCH_TARGET, side_effect=lambda name: mock_cls(name))


# ---------------------------------------------------------------------------
# DEFAULT_MODEL
# ---------------------------------------------------------------------------

class TestDefaultModel:
    def test_default_model_is_non_empty_string(self):
        assert isinstance(DEFAULT_MODEL, str) and DEFAULT_MODEL.strip()

    def test_default_model_is_all_minilm(self):
        assert DEFAULT_MODEL == "all-MiniLM-L6-v2"


# ---------------------------------------------------------------------------
# get_sentence_transformer
# ---------------------------------------------------------------------------

class TestGetSentenceTransformer:
    def test_returns_model_instance(self):
        with _patched_st():
            model = get_sentence_transformer()
            assert model is not None

    def test_cache_hit_returns_same_instance(self):
        with _patched_st():
            m1 = get_sentence_transformer("model-a")
            m2 = get_sentence_transformer("model-a")
            assert m1 is m2

    def test_different_names_return_different_instances(self):
        with _patched_st():
            m1 = get_sentence_transformer("model-a")
            m2 = get_sentence_transformer("model-b")
            assert m1 is not m2

    def test_cache_keyed_on_exact_string(self):
        """'model-a' and 'MODEL-A' are different cache keys."""
        with _patched_st():
            m1 = get_sentence_transformer("model-a")
            m2 = get_sentence_transformer("MODEL-A")
            assert m1 is not m2

    def test_invalid_model_raises_oserror(self):
        """A model that fails to load must raise OSError with a clear message."""
        import embeddings.sentence_transformer as _mod
        _mod._model_cache.clear()

        def _raise(name):
            raise OSError("Connection error")

        with patch(PATCH_TARGET, side_effect=_raise):
            with pytest.raises(OSError, match="Failed to load SentenceTransformer"):
                get_sentence_transformer("invalid-model-xyz")

    def test_error_message_contains_model_name(self):
        import embeddings.sentence_transformer as _mod
        _mod._model_cache.clear()

        with patch(PATCH_TARGET, side_effect=lambda n: (_ for _ in ()).throw(ValueError("bad"))):
            with pytest.raises(OSError) as exc_info:
                get_sentence_transformer("nonexistent/model")
            assert "nonexistent/model" in str(exc_info.value)


# ---------------------------------------------------------------------------
# embed() — module-level convenience function
# ---------------------------------------------------------------------------

class TestEmbedFunction:
    def test_output_shape(self, sample_texts):
        with _patched_st():
            result = embed(sample_texts)
            assert result.shape == (len(sample_texts), DIM)

    def test_output_dtype_is_float32(self, sample_texts):
        with _patched_st():
            result = embed(sample_texts)
            assert result.dtype == np.float32

    def test_single_text_returns_2d_array(self):
        with _patched_st():
            result = embed(["single sentence"])
            assert result.ndim == 2
            assert result.shape[0] == 1

    def test_embeddings_are_l2_normalised(self, sample_texts):
        """Each embedding vector must have unit L2 norm (±0.01 tolerance)."""
        with _patched_st():
            result = embed(sample_texts)
            norms = np.linalg.norm(result, axis=1)
            np.testing.assert_allclose(norms, 1.0, atol=0.01)

    def test_empty_texts_raises_value_error(self):
        with pytest.raises(ValueError, match="non-empty"):
            embed([])

    def test_uses_singleton_cache(self, sample_texts):
        """Two calls with the same model_name must not instantiate SentenceTransformer twice."""
        import embeddings.sentence_transformer as _mod
        _mod._model_cache.clear()

        call_count = 0
        original = MockSentenceTransformer

        class CountingMock(MockSentenceTransformer):
            def __init__(self, name):
                nonlocal call_count
                call_count += 1
                super().__init__(name)

        with patch(PATCH_TARGET, side_effect=CountingMock):
            embed(sample_texts, model_name="counting-model")
            embed(sample_texts, model_name="counting-model")

        assert call_count == 1, "SentenceTransformer instantiated more than once for the same model"


# ---------------------------------------------------------------------------
# SentenceTransformerAdapter
# ---------------------------------------------------------------------------

class TestSentenceTransformerAdapter:
    def test_model_name_property(self):
        adapter = SentenceTransformerAdapter("test-model-name")
        assert adapter.model_name == "test-model-name"

    def test_model_name_defaults_to_default_model(self):
        adapter = SentenceTransformerAdapter()
        assert adapter.model_name == DEFAULT_MODEL

    def test_embed_output_shape(self, sample_texts):
        with _patched_st():
            adapter = SentenceTransformerAdapter()
            result = adapter.embed(sample_texts)
            assert result.shape == (len(sample_texts), DIM)

    def test_embed_output_dtype(self, sample_texts):
        with _patched_st():
            adapter = SentenceTransformerAdapter()
            result = adapter.embed(sample_texts)
            assert result.dtype == np.float32

    def test_embed_empty_list_raises(self):
        adapter = SentenceTransformerAdapter()
        with pytest.raises(ValueError, match="non-empty"):
            adapter.embed([])

    def test_model_loaded_lazily(self):
        """
        The internal SentenceTransformer must not be instantiated until
        embed() is called for the first time.
        """
        import embeddings.sentence_transformer as _mod
        _mod._model_cache.clear()

        with patch(PATCH_TARGET, side_effect=MockSentenceTransformer) as mock_cls:
            adapter = SentenceTransformerAdapter("lazy-model")
            mock_cls.assert_not_called()       # not loaded yet

            adapter.embed(["test"])
            mock_cls.assert_called_once()      # loaded on first embed()

    def test_adapter_satisfies_embedding_model_protocol(self):
        adapter = SentenceTransformerAdapter()
        assert isinstance(adapter, EmbeddingModel)

    def test_deterministic_output_for_same_input(self, sample_texts):
        """Two embed() calls with the same texts must return identical arrays."""
        with _patched_st():
            adapter = SentenceTransformerAdapter()
            r1 = adapter.embed(sample_texts)
            r2 = adapter.embed(sample_texts)
            np.testing.assert_array_equal(r1, r2)
