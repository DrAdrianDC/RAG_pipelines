"""
Behavioural tests for chunking/langchain_chunking.py.

These tests exercise the runtime behaviour of the LangChain cross-validation
chunkers (LangChainFixedChunker and LangChainRecursiveChunker) and their
factory helpers.

Design principles
-----------------
- No real corpus file is required: fixtures from conftest.py supply minimal
  JSONL records.
- No real SentenceTransformer is loaded: LangChain chunkers are purely
  text-based and need no embedding model.
- Tests mirror the coverage pattern of test_fixed_chunking.py and
  test_recursive_chunking.py so the three implementations (custom fixed,
  custom recursive, LangChain) can be evaluated side-by-side.

Cross-validation purpose
------------------------
If LangChain and custom implementations agree on retrieval metrics, the custom
code is validated.  Divergences reveal implementation differences worth
investigating (overlap semantics, tokenisation edge cases, separator handling).
"""

from __future__ import annotations

import pytest

from chunking.langchain_chunking import (
    LangChainFixedChunker,
    LangChainRecursiveChunker,
    build_lc_fixed_512,
    build_lc_fixed_1024,
    build_lc_recursive_512,
)
from chunking.utils import count_tokens

from chunking.tests.conftest import REQUIRED_CHUNK_KEYS, assert_valid_chunk_schema


# ---------------------------------------------------------------------------
# LangChainFixedChunker — schema and basic behaviour
# ---------------------------------------------------------------------------

class TestLangChainFixedChunkerSchema:
    """Every chunk dict must satisfy the BaseChunker output contract."""

    def test_short_record_produces_at_least_one_chunk(self, short_record):
        chunker = LangChainFixedChunker()
        chunks = chunker.chunk_record(short_record)
        assert len(chunks) >= 1

    def test_chunk_has_required_keys(self, short_record):
        chunker = LangChainFixedChunker()
        chunks = chunker.chunk_record(short_record)
        for chunk in chunks:
            assert REQUIRED_CHUNK_KEYS.issubset(chunk.keys()), (
                f"Missing keys: {REQUIRED_CHUNK_KEYS - chunk.keys()}"
            )

    def test_chunk_schema_is_valid(self, short_record):
        chunker = LangChainFixedChunker()
        for chunk in chunker.chunk_record(short_record):
            assert_valid_chunk_schema(chunk, short_record)

    def test_empty_record_returns_empty_list(self, empty_record):
        chunker = LangChainFixedChunker()
        assert chunker.chunk_record(empty_record) == []

    def test_whitespace_record_returns_empty_list(self, whitespace_record):
        chunker = LangChainFixedChunker()
        assert chunker.chunk_record(whitespace_record) == []

    def test_chunk_id_contains_doc_id(self, short_record):
        chunker = LangChainFixedChunker()
        chunks = chunker.chunk_record(short_record)
        doc_id = f"{short_record['drug_name']}||{short_record['biomarker']}"
        for chunk in chunks:
            assert doc_id in chunk["chunk_id"]

    def test_chunk_index_is_sequential(self, long_record):
        chunker = LangChainFixedChunker(chunk_size=128, overlap=13)
        chunks = chunker.chunk_record(long_record)
        assert len(chunks) >= 2
        for i, chunk in enumerate(chunks):
            assert chunk["chunk_id"].endswith(f"::{i}")

    def test_doc_id_matches_drug_and_biomarker(self, long_record):
        chunker = LangChainFixedChunker()
        expected_doc_id = f"{long_record['drug_name']}||{long_record['biomarker']}"
        for chunk in chunker.chunk_record(long_record):
            assert chunk["doc_id"] == expected_doc_id


# ---------------------------------------------------------------------------
# LangChainFixedChunker — token budget
# ---------------------------------------------------------------------------

class TestLangChainFixedChunkerTokenBudget:
    """Chunks must respect the token budget (LangChain uses tiktoken cl100k_base)."""

    @pytest.mark.parametrize("chunk_size", [128, 256, 512])
    def test_chunks_do_not_exceed_chunk_size_plus_tolerance(
        self, long_record, chunk_size
    ):
        """
        LangChain's TokenTextSplitter may produce chunks up to chunk_size tokens.
        We allow a small tolerance for boundary effects.
        """
        tolerance = 20
        chunker = LangChainFixedChunker(chunk_size=chunk_size, overlap=0)
        for chunk in chunker.chunk_record(long_record):
            tokens = count_tokens(chunk["content"])
            assert tokens <= chunk_size + tolerance, (
                f"Chunk has {tokens} tokens, expected <= {chunk_size + tolerance}"
            )

    def test_long_record_produces_multiple_chunks(self, long_record):
        chunker = LangChainFixedChunker(chunk_size=128, overlap=13)
        chunks = chunker.chunk_record(long_record)
        assert len(chunks) >= 2, "A ~1500-token record should split at chunk_size=128"

    def test_token_count_field_is_accurate(self, long_record):
        chunker = LangChainFixedChunker(chunk_size=256, overlap=0)
        for chunk in chunker.chunk_record(long_record):
            assert chunk["token_count"] == count_tokens(chunk["content"])

    def test_chunker_name_reflects_chunk_size(self):
        assert LangChainFixedChunker(chunk_size=512).name == "lc_fixed_512"
        assert LangChainFixedChunker(chunk_size=1024).name == "lc_fixed_1024"


# ---------------------------------------------------------------------------
# LangChainRecursiveChunker — schema and basic behaviour
# ---------------------------------------------------------------------------

class TestLangChainRecursiveChunkerSchema:
    """LangChainRecursiveChunker must satisfy the same output contract."""

    def test_short_record_produces_at_least_one_chunk(self, short_record):
        chunker = LangChainRecursiveChunker()
        assert len(chunker.chunk_record(short_record)) >= 1

    def test_chunk_schema_is_valid(self, short_record):
        chunker = LangChainRecursiveChunker()
        for chunk in chunker.chunk_record(short_record):
            assert_valid_chunk_schema(chunk, short_record)

    def test_empty_record_returns_empty_list(self, empty_record):
        chunker = LangChainRecursiveChunker()
        assert chunker.chunk_record(empty_record) == []

    def test_whitespace_record_returns_empty_list(self, whitespace_record):
        chunker = LangChainRecursiveChunker()
        assert chunker.chunk_record(whitespace_record) == []

    def test_doc_id_matches_drug_and_biomarker(self, long_record):
        chunker = LangChainRecursiveChunker()
        expected_doc_id = f"{long_record['drug_name']}||{long_record['biomarker']}"
        for chunk in chunker.chunk_record(long_record):
            assert chunk["doc_id"] == expected_doc_id

    def test_chunker_name_reflects_chunk_size(self):
        assert LangChainRecursiveChunker(chunk_size=512).name == "lc_recursive_512"


# ---------------------------------------------------------------------------
# LangChainRecursiveChunker — separator awareness
# ---------------------------------------------------------------------------

class TestLangChainRecursiveChunkerSeparators:
    """
    RecursiveCharacterTextSplitter should prefer paragraph / sentence boundaries
    over mid-sentence cuts when text structure permits.
    """

    def test_multi_paragraph_record_produces_multiple_chunks(
        self, multi_paragraph_record
    ):
        chunker = LangChainRecursiveChunker(chunk_size=50, overlap=5)
        chunks = chunker.chunk_record(multi_paragraph_record)
        assert len(chunks) >= 2, (
            "Multi-paragraph record should produce >= 2 chunks at chunk_size=50"
        )

    def test_token_count_field_is_accurate(self, long_record):
        chunker = LangChainRecursiveChunker(chunk_size=256, overlap=0)
        for chunk in chunker.chunk_record(long_record):
            assert chunk["token_count"] == count_tokens(chunk["content"])

    def test_long_record_produces_multiple_chunks(self, long_record):
        chunker = LangChainRecursiveChunker(chunk_size=128, overlap=13)
        chunks = chunker.chunk_record(long_record)
        assert len(chunks) >= 2


# ---------------------------------------------------------------------------
# Cross-implementation consistency: Fixed vs LangChain Fixed
# ---------------------------------------------------------------------------

class TestFixedVsLangChainFixed:
    """
    The custom FixedChunker and LangChainFixedChunker implement the same logical
    strategy.  Their chunk counts and approximate token budgets should be
    comparable on the same input.
    """

    def test_both_produce_at_least_one_chunk(self, long_record):
        from chunking.fixed_chunking import FixedChunker

        custom = FixedChunker(chunk_size=512)
        lc = LangChainFixedChunker(chunk_size=512, overlap=51)

        assert len(custom.chunk_record(long_record)) >= 1
        assert len(lc.chunk_record(long_record)) >= 1

    def test_both_produce_similar_chunk_counts(self, long_record):
        """
        Counts may differ by ±2 due to overlap semantics and word-boundary
        handling.  Large divergence signals a hyperparameter mismatch.
        """
        from chunking.fixed_chunking import FixedChunker

        custom_chunks = FixedChunker(chunk_size=512, overlap_fraction=0.10).chunk_record(
            long_record
        )
        lc_chunks = LangChainFixedChunker(chunk_size=512, overlap=51).chunk_record(
            long_record
        )
        diff = abs(len(custom_chunks) - len(lc_chunks))
        assert diff <= 3, (
            f"Custom ({len(custom_chunks)}) and LangChain ({len(lc_chunks)}) "
            f"fixed chunkers differ by {diff} chunks (tolerance: 3). "
            "Investigate overlap semantics or token-boundary handling."
        )


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------

class TestFactoryHelpers:
    """Factory functions must return correctly typed and named instances."""

    def test_build_lc_fixed_512_type(self):
        assert isinstance(build_lc_fixed_512(), LangChainFixedChunker)

    def test_build_lc_fixed_512_name(self):
        assert build_lc_fixed_512().name == "lc_fixed_512"

    def test_build_lc_fixed_1024_type(self):
        assert isinstance(build_lc_fixed_1024(), LangChainFixedChunker)

    def test_build_lc_fixed_1024_name(self):
        assert build_lc_fixed_1024().name == "lc_fixed_1024"

    def test_build_lc_recursive_512_type(self):
        assert isinstance(build_lc_recursive_512(), LangChainRecursiveChunker)

    def test_build_lc_recursive_512_name(self):
        assert build_lc_recursive_512().name == "lc_recursive_512"

    def test_build_lc_fixed_512_produces_chunks(self, long_record):
        chunker = build_lc_fixed_512()
        assert len(chunker.chunk_record(long_record)) >= 1

    def test_build_lc_fixed_1024_produces_chunks(self, long_record):
        chunker = build_lc_fixed_1024()
        assert len(chunker.chunk_record(long_record)) >= 1

    def test_build_lc_recursive_512_produces_chunks(self, long_record):
        chunker = build_lc_recursive_512()
        assert len(chunker.chunk_record(long_record)) >= 1

    def test_build_lc_fixed_512_and_1024_chunk_count_relation(self, long_record):
        """A 1024-token budget should produce fewer chunks than 512 tokens."""
        chunks_512 = build_lc_fixed_512().chunk_record(long_record)
        chunks_1024 = build_lc_fixed_1024().chunk_record(long_record)
        assert len(chunks_1024) <= len(chunks_512), (
            "lc_fixed_1024 should produce at most as many chunks as lc_fixed_512"
        )
