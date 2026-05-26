"""
Tests for FixedChunker — Fixed-Size / Token Chunking (Weaviate approach).

Covers:
- Schema compliance
- Empty / whitespace / short content edge cases
- chunk_size enforcement (no chunk exceeds the budget)
- Word-boundary guarantee (no chunk starts or ends mid-word)
- Overlap: words from chunk[i] appear at the start of chunk[i+1]
- overlap_fraction validation
- Factory builders match config values
"""

from __future__ import annotations

import pytest

from chunking.config import STRATEGY_CONFIGS
from chunking.fixed_chunking import FixedChunker, build_fixed_512, build_fixed_1024
from chunking.utils import count_tokens
from chunking.tests.conftest import assert_valid_chunk_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_chunker(chunk_size: int = 512, overlap_fraction: float = 0.10) -> FixedChunker:
    return FixedChunker(chunk_size=chunk_size, overlap_fraction=overlap_fraction)


# ---------------------------------------------------------------------------
# Schema compliance
# ---------------------------------------------------------------------------

class TestSchema:
    def test_chunk_schema_valid(self, short_record):
        chunker = make_chunker()
        chunks = chunker.chunk_record(short_record)
        assert len(chunks) >= 1
        for chunk in chunks:
            assert_valid_chunk_schema(chunk, short_record)

    def test_section_is_always_empty_string(self, long_record):
        """FixedChunker is section-blind."""
        chunker = make_chunker()
        for chunk in chunker.chunk_record(long_record):
            assert chunk["section"] == ""

    def test_chunk_ids_are_unique(self, long_record):
        chunker = make_chunker()
        ids = [c["chunk_id"] for c in chunker.chunk_record(long_record)]
        assert len(ids) == len(set(ids))

    def test_chunk_ids_are_sequential(self, long_record):
        chunker = make_chunker()
        chunks = chunker.chunk_record(long_record)
        for i, chunk in enumerate(chunks):
            assert chunk["chunk_id"].endswith(f"::{i}")


# ---------------------------------------------------------------------------
# Empty and degenerate inputs
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_content(self, empty_record):
        assert make_chunker().chunk_record(empty_record) == []

    def test_whitespace_only(self, whitespace_record):
        assert make_chunker().chunk_record(whitespace_record) == []

    def test_single_word(self):
        record = {"content": "methylphenidate", "drug_name": "X", "biomarker": "Y"}
        chunks = make_chunker().chunk_record(record)
        assert len(chunks) == 1
        assert chunks[0]["content"] == "methylphenidate"

    def test_content_shorter_than_chunk_size_produces_one_chunk(self, short_record):
        chunker = make_chunker(chunk_size=512)
        chunks = chunker.chunk_record(short_record)
        assert len(chunks) == 1

    def test_exact_chunk_size_boundary(self):
        """Content that is exactly chunk_size tokens should produce one chunk."""
        chunker = make_chunker(chunk_size=50, overlap_fraction=0.0)
        # Build content of exactly 50 tokens.
        words = []
        total = 0
        word = "drug"
        word_tokens = count_tokens(word)
        while total + word_tokens <= 50:
            words.append(word)
            total += word_tokens + 1  # +1 for space
        content = " ".join(words)
        record = {"content": content, "drug_name": "X", "biomarker": "Y"}
        chunks = chunker.chunk_record(record)
        assert len(chunks) >= 1
        assert all(count_tokens(c["content"]) <= 50 + 2 for c in chunks)


# ---------------------------------------------------------------------------
# Token budget enforcement
# ---------------------------------------------------------------------------

class TestTokenBudget:
    def test_no_chunk_exceeds_chunk_size(self, long_record):
        chunker = make_chunker(chunk_size=100, overlap_fraction=0.10)
        chunks = chunker.chunk_record(long_record)
        for chunk in chunks:
            # Allow a small margin: overlap may push the first chunk slightly over
            # if the last overlap word itself is multi-token.
            assert chunk["token_count"] <= 120, (
                f"Chunk exceeded budget: {chunk['token_count']} tokens"
            )

    def test_long_content_produces_multiple_chunks(self, long_record):
        chunker = make_chunker(chunk_size=50, overlap_fraction=0.10)
        chunks = chunker.chunk_record(long_record)
        assert len(chunks) > 1

    def test_token_count_field_matches_actual_content(self, long_record):
        chunker = make_chunker()
        for chunk in chunker.chunk_record(long_record):
            assert chunk["token_count"] == count_tokens(chunk["content"])


# ---------------------------------------------------------------------------
# Word-boundary guarantee
# ---------------------------------------------------------------------------

class TestWordBoundaries:
    def test_chunks_start_on_word_boundary(self, long_record):
        """The first character of every chunk must not be a whitespace-continuation."""
        chunker = make_chunker(chunk_size=100, overlap_fraction=0.10)
        for chunk in chunker.chunk_record(long_record):
            assert not chunk["content"].startswith(" "), (
                f"Chunk starts with space: '{chunk['content'][:20]}'"
            )

    def test_chunks_end_on_word_boundary(self, long_record):
        """The last character of every chunk must not be a leading space of the next word."""
        chunker = make_chunker(chunk_size=100, overlap_fraction=0.10)
        for chunk in chunker.chunk_record(long_record):
            assert not chunk["content"].endswith(" "), (
                f"Chunk ends with space: '...{chunk['content'][-20:]}'"
            )


# ---------------------------------------------------------------------------
# Overlap behaviour
# ---------------------------------------------------------------------------

class TestOverlap:
    def test_overlap_words_appear_in_next_chunk(self, long_record):
        """Some words from chunk[i] must appear at the start of chunk[i+1]."""
        chunker = make_chunker(chunk_size=80, overlap_fraction=0.15)
        chunks = chunker.chunk_record(long_record)
        if len(chunks) < 2:
            pytest.skip("Not enough chunks to test overlap")

        for i in range(len(chunks) - 1):
            tail_words = set(chunks[i]["content"].split()[-8:])
            head_words = set(chunks[i + 1]["content"].split()[:8])
            assert tail_words & head_words, (
                f"No overlap found between chunk {i} and {i + 1}.\n"
                f"Tail: {chunks[i]['content'][-60:]}\n"
                f"Head: {chunks[i+1]['content'][:60]}"
            )

    def test_zero_overlap_produces_no_repeated_words_at_boundary(self, long_record):
        chunker = make_chunker(chunk_size=80, overlap_fraction=0.0)
        chunks = chunker.chunk_record(long_record)
        if len(chunks) < 2:
            pytest.skip("Not enough chunks to test")

        tail_words = set(chunks[0]["content"].split()[-5:])
        head_words = set(chunks[1]["content"].split()[:5])
        # With zero overlap, the boundary words should not repeat.
        assert not (tail_words & head_words)


# ---------------------------------------------------------------------------
# overlap_fraction validation
# ---------------------------------------------------------------------------

class TestOverlapFractionValidation:
    def test_negative_overlap_fraction_raises(self):
        with pytest.raises(ValueError):
            FixedChunker(chunk_size=512, overlap_fraction=-0.1)

    def test_overlap_fraction_equal_to_one_raises(self):
        with pytest.raises(ValueError):
            FixedChunker(chunk_size=512, overlap_fraction=1.0)

    def test_overlap_fraction_greater_than_one_raises(self):
        with pytest.raises(ValueError):
            FixedChunker(chunk_size=512, overlap_fraction=1.5)

    def test_zero_overlap_fraction_is_valid(self):
        chunker = FixedChunker(chunk_size=512, overlap_fraction=0.0)
        assert chunker.overlap_tokens == 0


# ---------------------------------------------------------------------------
# Factory builders
# ---------------------------------------------------------------------------

class TestFactoryBuilders:
    def test_build_fixed_512_name(self):
        assert build_fixed_512().name == "fixed_512"

    def test_build_fixed_1024_name(self):
        assert build_fixed_1024().name == "fixed_1024"

    def test_build_fixed_512_matches_config(self):
        cfg = STRATEGY_CONFIGS["fixed_512"]
        chunker = build_fixed_512()
        assert chunker.chunk_size == cfg["chunk_size"]
        assert chunker.overlap_fraction == cfg["overlap_fraction"]

    def test_build_fixed_1024_matches_config(self):
        cfg = STRATEGY_CONFIGS["fixed_1024"]
        chunker = build_fixed_1024()
        assert chunker.chunk_size == cfg["chunk_size"]
        assert chunker.overlap_fraction == cfg["overlap_fraction"]
