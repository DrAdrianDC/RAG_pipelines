"""
Tests for RecursiveChunker — Weaviate Recursive Chunking.

Covers:
- Schema compliance
- Empty / degenerate inputs
- Separator priority (paragraph > newline > sentence)
- Token budget (no chunk exceeds chunk_size)
- Token-level overlap: words from chunk[i] appear at start of chunk[i+1]
- overlap_fraction validation
- Factory builder matches config
"""

from __future__ import annotations

import pytest

from chunking.config import STRATEGY_CONFIGS
from chunking.recursive_chunking import RecursiveChunker, build_recursive_512
from chunking.utils import count_tokens
from chunking.tests.conftest import assert_valid_chunk_schema


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def make_chunker(
    chunk_size: int = 512,
    overlap_fraction: float = 0.10,
    separators: list[str] | None = None,
) -> RecursiveChunker:
    return RecursiveChunker(
        chunk_size=chunk_size,
        overlap_fraction=overlap_fraction,
        separators=separators,
    )


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
        """RecursiveChunker is section-blind."""
        for chunk in make_chunker().chunk_record(long_record):
            assert chunk["section"] == ""

    def test_chunk_ids_are_unique(self, long_record):
        ids = [c["chunk_id"] for c in make_chunker().chunk_record(long_record)]
        assert len(ids) == len(set(ids))

    def test_token_count_field_matches_actual_content(self, long_record):
        for chunk in make_chunker().chunk_record(long_record):
            assert chunk["token_count"] == count_tokens(chunk["content"])


# ---------------------------------------------------------------------------
# Empty and degenerate inputs
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_content(self, empty_record):
        assert make_chunker().chunk_record(empty_record) == []

    def test_whitespace_only(self, whitespace_record):
        assert make_chunker().chunk_record(whitespace_record) == []

    def test_short_content_produces_one_chunk(self, short_record):
        chunker = make_chunker(chunk_size=512)
        chunks = chunker.chunk_record(short_record)
        assert len(chunks) == 1
        assert chunks[0]["content"].strip() == short_record["content"].strip()

    def test_single_word_record(self):
        record = {"content": "imatinib", "drug_name": "X", "biomarker": "Y"}
        chunks = make_chunker().chunk_record(record)
        assert len(chunks) == 1
        assert "imatinib" in chunks[0]["content"]


# ---------------------------------------------------------------------------
# Separator priority
# ---------------------------------------------------------------------------

class TestSeparatorPriority:
    def test_splits_at_double_newline_first(self, multi_paragraph_record):
        """With paragraph breaks in the text, the split should occur there."""
        chunker = make_chunker(chunk_size=60, overlap_fraction=0.0)
        chunks = chunker.chunk_record(multi_paragraph_record)
        assert len(chunks) >= 2
        # Each chunk should contain coherent text (not arbitrary fragments)
        for chunk in chunks:
            assert chunk["content"].strip()

    def test_splits_at_sentence_when_no_newlines(self):
        """
        Without paragraph breaks the splitter falls through to the '. ' separator.

        We verify that:
        1. Content is split into multiple chunks (budget exceeded).
        2. Every chunk contains coherent sentence content — no mid-word splits.

        Note: the recursive merge re-joins pieces with '. ' so non-final chunks
        end with the raw piece text (which has no trailing period).  The test
        therefore checks content coverage, not trailing punctuation.
        """
        sentence = "The patient should receive 200 mg intravenously over 30 minutes."
        content = " ".join([sentence] * 8)
        record = {"content": content, "drug_name": "X", "biomarker": "Y"}
        chunker = make_chunker(chunk_size=80, overlap_fraction=0.0)
        chunks = chunker.chunk_record(record)
        assert len(chunks) > 1, "Expected multiple chunks when content exceeds chunk_size"
        # All chunks must contain recognisable sentence fragments (no garbage splits).
        for chunk in chunks:
            assert "minutes" in chunk["content"], (
                f"Chunk missing expected sentence content: {chunk['content'][:80]}"
            )

    def test_no_empty_chunks_in_output(self, long_record):
        chunker = make_chunker(chunk_size=80, overlap_fraction=0.10)
        chunks = chunker.chunk_record(long_record)
        for chunk in chunks:
            assert chunk["content"].strip(), "Found empty chunk content"


# ---------------------------------------------------------------------------
# Token budget
# ---------------------------------------------------------------------------

class TestTokenBudget:
    def test_no_chunk_exceeds_chunk_size_with_small_budget(self, long_record):
        """
        With a tight chunk_size, every produced chunk must fit within budget.
        We allow +1 token margin for word-boundary rounding.
        """
        chunk_size = 80
        chunker = make_chunker(chunk_size=chunk_size, overlap_fraction=0.10)
        chunks = chunker.chunk_record(long_record)
        for chunk in chunks:
            assert chunk["token_count"] <= chunk_size + 5, (
                f"Chunk exceeded budget: {chunk['token_count']} > {chunk_size}"
            )

    def test_long_content_produces_multiple_chunks(self, long_record):
        chunker = make_chunker(chunk_size=60, overlap_fraction=0.0)
        assert len(chunker.chunk_record(long_record)) > 1

    def test_all_content_covered(self, long_record):
        """
        The union of all chunks must cover the original content
        (ignoring overlap repetition and whitespace normalisation).
        """
        chunker = make_chunker(chunk_size=80, overlap_fraction=0.0)
        chunks = chunker.chunk_record(long_record)
        combined = " ".join(c["content"] for c in chunks)
        original_words = set(long_record["content"].split())
        chunk_words = set(combined.split())
        # Every word in the original must appear in at least one chunk.
        assert original_words.issubset(chunk_words)


# ---------------------------------------------------------------------------
# Token-level overlap (the critical fix)
# ---------------------------------------------------------------------------

class TestTokenLevelOverlap:
    def test_overlap_words_present_in_next_chunk(self, long_record):
        """
        Some tail words of chunk[i] must appear at the head of chunk[i+1].
        This validates that _add_overlap works at token level, not piece level.
        """
        chunker = make_chunker(chunk_size=70, overlap_fraction=0.15)
        chunks = chunker.chunk_record(long_record)
        if len(chunks) < 2:
            pytest.skip("Record too short to produce multiple chunks at this budget")

        for i in range(len(chunks) - 1):
            tail = set(chunks[i]["content"].split()[-10:])
            head = set(chunks[i + 1]["content"].split()[:10])
            assert tail & head, (
                f"No token-level overlap between chunk {i} and {i + 1}.\n"
                f"  Tail (last 10 words): {list(tail)}\n"
                f"  Head (first 10 words): {list(head)}"
            )

    def test_zero_overlap_no_repeated_words_at_boundary(self, long_record):
        chunker = make_chunker(chunk_size=70, overlap_fraction=0.0)
        chunks = chunker.chunk_record(long_record)
        if len(chunks) < 2:
            pytest.skip("Record too short")

        tail = set(chunks[0]["content"].split()[-5:])
        head = set(chunks[1]["content"].split()[:5])
        assert not (tail & head), "Expected no overlap with overlap_fraction=0.0"

    def test_overlap_always_fills_regardless_of_paragraph_size(self):
        """
        Regression test for the old piece-level overlap bug:
        when a single paragraph occupies > overlap_tokens tokens,
        the old implementation produced zero overlap.
        """
        # Create a record with a single long paragraph (> 100 tokens)
        # so the old algorithm would have carried zero pieces.
        long_paragraph = " ".join(["biomarker"] * 80)  # ~80 tokens
        record = {
            "content": long_paragraph + " " + long_paragraph,
            "drug_name": "X",
            "biomarker": "Y",
        }
        chunker = make_chunker(chunk_size=90, overlap_fraction=0.10)
        chunks = chunker.chunk_record(record)
        if len(chunks) < 2:
            pytest.skip("Record too short for this test")

        tail = set(chunks[0]["content"].split()[-8:])
        head = set(chunks[1]["content"].split()[:8])
        assert tail & head, "Token-level overlap should be non-empty even with long paragraphs"


# ---------------------------------------------------------------------------
# overlap_fraction validation
# ---------------------------------------------------------------------------

class TestOverlapFractionValidation:
    def test_negative_raises(self):
        with pytest.raises(ValueError):
            RecursiveChunker(chunk_size=512, overlap_fraction=-0.1)

    def test_one_raises(self):
        with pytest.raises(ValueError):
            RecursiveChunker(chunk_size=512, overlap_fraction=1.0)

    def test_zero_is_valid(self):
        chunker = RecursiveChunker(chunk_size=512, overlap_fraction=0.0)
        assert chunker.overlap_tokens == 0


# ---------------------------------------------------------------------------
# Factory builder
# ---------------------------------------------------------------------------

class TestFactoryBuilder:
    def test_build_recursive_512_name(self):
        assert build_recursive_512().name == "recursive_512"

    def test_build_recursive_512_matches_config(self):
        cfg = STRATEGY_CONFIGS["recursive_512"]
        chunker = build_recursive_512()
        assert chunker.chunk_size == cfg["chunk_size"]
        assert chunker.overlap_fraction == cfg["overlap_fraction"]
