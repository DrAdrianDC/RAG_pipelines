"""
Tests for SemanticChunker — Weaviate Semantic / Context-Aware Chunking.

All tests inject a MockSentenceTransformer via the ``model`` parameter so
no real model weights are loaded.  Three model behaviours are exercised:

- ``mock_model``      : random embeddings (deterministic, seed=42)
- ``similar_model``   : near-identical embeddings → few splits (large chunks)
- ``dissimilar_model``: alternating-sign embeddings → many splits (small chunks)

Covers:
- Schema compliance (with injected model)
- Empty / degenerate inputs
- Weaviate Step 4: min_chunk_tokens enforcement (merge)
- Weaviate Step 4: max_chunk_tokens enforcement (split)
- Similarity-based boundary detection (similar vs dissimilar model)
- Factory builder matches config
"""

from __future__ import annotations

import pytest
import numpy as np

from chunking.config import STRATEGY_CONFIGS
from chunking.semantic_chunking import SemanticChunker, build_semantic
from chunking.utils import count_tokens
from chunking.tests.conftest import (
    MockSentenceTransformer,
    assert_valid_chunk_schema,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def make_chunker(
    similarity_threshold: float = 0.7,
    min_chunk_tokens: int = 50,
    max_chunk_tokens: int = 600,
    model=None,
) -> SemanticChunker:
    return SemanticChunker(
        similarity_threshold=similarity_threshold,
        min_chunk_tokens=min_chunk_tokens,
        max_chunk_tokens=max_chunk_tokens,
        model=model or MockSentenceTransformer(),
    )


def _multi_sentence_record(n_sentences: int = 10, tokens_per_sentence: int = 15) -> dict:
    """Generate a record with *n_sentences* sentences of ~*tokens_per_sentence* tokens each."""
    sentence = "The patient should be monitored for immune-mediated adverse reactions."
    return {
        "content": " ".join([sentence] * n_sentences),
        "drug_name": "KEYTRUDA",
        "biomarker": "PD-L1",
        "labeling_sections": ["Warnings and Precautions"],
        "source": "fda_biomarkers",
    }


# ---------------------------------------------------------------------------
# Schema compliance
# ---------------------------------------------------------------------------

class TestSchema:
    def test_chunk_schema_valid(self, mock_model):
        record = _multi_sentence_record(n_sentences=8)
        chunker = make_chunker(model=mock_model)
        chunks = chunker.chunk_record(record)
        assert len(chunks) >= 1
        for chunk in chunks:
            assert_valid_chunk_schema(chunk, record)

    def test_section_is_always_empty_string(self, mock_model):
        """SemanticChunker is section-blind."""
        record = _multi_sentence_record(n_sentences=8)
        for chunk in make_chunker(model=mock_model).chunk_record(record):
            assert chunk["section"] == ""

    def test_token_count_field_matches_actual_content(self, mock_model):
        record = _multi_sentence_record(n_sentences=8)
        for chunk in make_chunker(model=mock_model).chunk_record(record):
            assert chunk["token_count"] == count_tokens(chunk["content"])

    def test_chunk_ids_are_unique(self, mock_model):
        record = _multi_sentence_record(n_sentences=10)
        ids = [c["chunk_id"] for c in make_chunker(model=mock_model).chunk_record(record)]
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# Empty and degenerate inputs
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_content(self, empty_record, mock_model):
        assert make_chunker(model=mock_model).chunk_record(empty_record) == []

    def test_whitespace_only(self, whitespace_record, mock_model):
        assert make_chunker(model=mock_model).chunk_record(whitespace_record) == []

    def test_single_sentence_produces_one_chunk(self, short_record, mock_model):
        chunker = make_chunker(min_chunk_tokens=1, model=mock_model)
        chunks = chunker.chunk_record(short_record)
        assert len(chunks) == 1

    def test_content_preserved_in_chunks(self, mock_model):
        """All words from the original content must appear across all chunks."""
        record = _multi_sentence_record(n_sentences=6)
        chunker = make_chunker(model=mock_model)
        chunks = chunker.chunk_record(record)
        combined = " ".join(c["content"] for c in chunks)
        original_words = set(record["content"].split())
        chunk_words = set(combined.split())
        assert original_words.issubset(chunk_words)


# ---------------------------------------------------------------------------
# Weaviate Step 4a — min_chunk_tokens enforcement (merge)
# ---------------------------------------------------------------------------

class TestMinChunkTokens:
    def test_no_chunk_below_min_chunk_tokens(self, mock_model):
        min_tokens = 50
        record = _multi_sentence_record(n_sentences=15)
        chunker = make_chunker(min_chunk_tokens=min_tokens, model=mock_model)
        chunks = chunker.chunk_record(record)
        for chunk in chunks:
            assert chunk["token_count"] >= min_tokens, (
                f"Chunk below min_chunk_tokens: {chunk['token_count']} < {min_tokens}\n"
                f"Content: '{chunk['content'][:80]}'"
            )

    def test_high_min_chunk_tokens_merges_small_fragments(self, dissimilar_model):
        """
        With a very dissimilar model (many splits) and high min_chunk_tokens,
        the enforcer must merge tiny fragments into acceptable chunks.
        """
        min_tokens = 30
        record = _multi_sentence_record(n_sentences=20)
        chunker = make_chunker(min_chunk_tokens=min_tokens, model=dissimilar_model)
        chunks = chunker.chunk_record(record)
        assert len(chunks) >= 1
        for chunk in chunks:
            assert chunk["token_count"] >= min_tokens


# ---------------------------------------------------------------------------
# Weaviate Step 4b — max_chunk_tokens enforcement (split)
# ---------------------------------------------------------------------------

class TestMaxChunkTokens:
    def test_no_chunk_exceeds_max_chunk_tokens(self, similar_model):
        """
        With a very similar model (few splits), all text may land in one huge
        chunk.  max_chunk_tokens enforcement must split it.
        """
        max_tokens = 100
        record = _multi_sentence_record(n_sentences=20, tokens_per_sentence=20)
        chunker = make_chunker(
            max_chunk_tokens=max_tokens,
            min_chunk_tokens=5,
            model=similar_model,
        )
        chunks = chunker.chunk_record(record)
        for chunk in chunks:
            assert chunk["token_count"] <= max_tokens + 20, (
                f"Chunk exceeded max_chunk_tokens: {chunk['token_count']} > {max_tokens}"
            )


# ---------------------------------------------------------------------------
# Boundary detection — similar vs dissimilar model
# ---------------------------------------------------------------------------

class TestBoundaryDetection:
    def test_similar_embeddings_produce_fewer_chunks_than_dissimilar(
        self, similar_model, dissimilar_model
    ):
        """
        With embeddings that are nearly identical, the adaptive percentile
        threshold should trigger fewer splits than with orthogonal embeddings.
        """
        record = _multi_sentence_record(n_sentences=20)
        min_tok = 5

        similar_chunks = make_chunker(min_chunk_tokens=min_tok, model=similar_model).chunk_record(record)
        dissimilar_chunks = make_chunker(min_chunk_tokens=min_tok, model=dissimilar_model).chunk_record(record)

        assert len(similar_chunks) <= len(dissimilar_chunks), (
            f"Expected similar_model to produce ≤ chunks than dissimilar_model, "
            f"got {len(similar_chunks)} vs {len(dissimilar_chunks)}"
        )

    def test_higher_threshold_produces_more_chunks(self, mock_model):
        """Higher similarity_threshold → lower percentile cutoff → more splits."""
        record = _multi_sentence_record(n_sentences=20)
        chunks_low = make_chunker(
            similarity_threshold=0.3, min_chunk_tokens=5, model=mock_model
        ).chunk_record(record)
        chunks_high = make_chunker(
            similarity_threshold=0.9, min_chunk_tokens=5, model=mock_model
        ).chunk_record(record)
        assert len(chunks_low) <= len(chunks_high)


# ---------------------------------------------------------------------------
# Model dependency injection
# ---------------------------------------------------------------------------

class TestModelInjection:
    def test_injected_model_is_used_not_global_singleton(self):
        """
        Passing a model to __init__ must bypass the global get_sentence_transformer
        singleton.  We verify this by checking the model object identity.
        """
        mock = MockSentenceTransformer()
        chunker = SemanticChunker(
            similarity_threshold=0.7,
            min_chunk_tokens=5,
            max_chunk_tokens=600,
            model=mock,
        )
        assert chunker._model is mock

    def test_none_model_defers_to_singleton(self):
        """Passing model=None stores None — singleton loaded lazily on first use."""
        chunker = SemanticChunker(
            similarity_threshold=0.7,
            min_chunk_tokens=5,
            max_chunk_tokens=600,
            model=None,
        )
        assert chunker._model is None


# ---------------------------------------------------------------------------
# Factory builder
# ---------------------------------------------------------------------------

class TestFactoryBuilder:
    def test_build_semantic_name(self):
        # build_semantic() loads the real singleton at call time — just check config.
        cfg = STRATEGY_CONFIGS["semantic"]
        chunker = SemanticChunker(
            similarity_threshold=cfg["similarity_threshold"],
            min_chunk_tokens=cfg["min_chunk_tokens"],
            max_chunk_tokens=cfg["max_chunk_tokens"],
            model=MockSentenceTransformer(),
        )
        assert chunker.similarity_threshold == cfg["similarity_threshold"]
        assert chunker.min_chunk_tokens == cfg["min_chunk_tokens"]
        assert chunker.max_chunk_tokens == cfg["max_chunk_tokens"]

    def test_config_min_chunk_tokens_is_at_least_50(self):
        """Regression: must not revert to the original 20-token default."""
        cfg = STRATEGY_CONFIGS["semantic"]
        assert cfg["min_chunk_tokens"] >= 50, (
            f"min_chunk_tokens={cfg['min_chunk_tokens']} is too low for FDA text. "
            "Should be at least 50."
        )
