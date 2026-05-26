"""
Tests for BaseChunker — the abstract contract every strategy must satisfy.

We test the shared behaviour via a minimal concrete subclass rather than
instantiating BaseChunker directly (it's abstract).
"""

from __future__ import annotations

import pytest

from chunking.base_chunker import BaseChunker
from chunking.utils import count_tokens
from chunking.tests.conftest import assert_valid_chunk_schema


# ---------------------------------------------------------------------------
# Minimal concrete subclass for testing BaseChunker helpers
# ---------------------------------------------------------------------------

class _PassthroughChunker(BaseChunker):
    """Returns the full record content as a single chunk — no splitting."""
    name = "passthrough"

    def chunk_record(self, record: dict) -> list[dict]:
        content = record.get("content", "").strip()
        if not content:
            return []
        return [
            self._make_chunk(
                text=content,
                doc_id=f"{record['drug_name']}||{record['biomarker']}",
                index=0,
                drug_name=record.get("drug_name", ""),
                biomarker=record.get("biomarker", ""),
                section="",
            )
        ]


class _TinyChunker(BaseChunker):
    """Returns chunks of 1 character each — most will be below MIN_CHUNK_TOKENS."""
    name = "tiny"

    def chunk_record(self, record: dict) -> list[dict]:
        content = record.get("content", "").strip()
        doc_id = f"{record['drug_name']}||{record['biomarker']}"
        return [
            self._make_chunk(
                text=ch,
                doc_id=doc_id,
                index=i,
                drug_name=record.get("drug_name", ""),
                biomarker=record.get("biomarker", ""),
            )
            for i, ch in enumerate(content)
        ]


# ---------------------------------------------------------------------------
# chunk_id and doc_id format
# ---------------------------------------------------------------------------

class TestChunkIdFormat:
    def test_chunk_id_contains_separator(self, short_record):
        chunker = _PassthroughChunker()
        chunks = chunker.chunk_record(short_record)
        assert len(chunks) == 1
        assert "::" in chunks[0]["chunk_id"]

    def test_chunk_id_starts_with_doc_id(self, short_record):
        chunker = _PassthroughChunker()
        chunks = chunker.chunk_record(short_record)
        doc_id = f"{short_record['drug_name']}||{short_record['biomarker']}"
        assert chunks[0]["chunk_id"].startswith(doc_id)

    def test_chunk_id_ends_with_index(self, short_record):
        chunker = _PassthroughChunker()
        chunks = chunker.chunk_record(short_record)
        assert chunks[0]["chunk_id"].endswith("::0")

    def test_doc_id_format(self, short_record):
        chunker = _PassthroughChunker()
        chunks = chunker.chunk_record(short_record)
        expected = f"{short_record['drug_name']}||{short_record['biomarker']}"
        assert chunks[0]["doc_id"] == expected


# ---------------------------------------------------------------------------
# Schema compliance
# ---------------------------------------------------------------------------

class TestChunkSchema:
    def test_all_required_keys_present(self, short_record):
        chunker = _PassthroughChunker()
        chunk = chunker.chunk_record(short_record)[0]
        assert_valid_chunk_schema(chunk, short_record)

    def test_token_count_is_accurate(self, short_record):
        chunker = _PassthroughChunker()
        chunk = chunker.chunk_record(short_record)[0]
        assert chunk["token_count"] == count_tokens(chunk["content"])

    def test_drug_name_and_biomarker_propagated(self, short_record):
        chunker = _PassthroughChunker()
        chunk = chunker.chunk_record(short_record)[0]
        assert chunk["drug_name"] == short_record["drug_name"]
        assert chunk["biomarker"] == short_record["biomarker"]

    def test_default_section_is_empty_string(self, short_record):
        chunker = _PassthroughChunker()
        chunk = chunker.chunk_record(short_record)[0]
        assert chunk["section"] == ""


# ---------------------------------------------------------------------------
# Empty and degenerate inputs
# ---------------------------------------------------------------------------

class TestEmptyInputs:
    def test_empty_content_returns_empty_list(self, empty_record):
        chunker = _PassthroughChunker()
        assert chunker.chunk_record(empty_record) == []

    def test_whitespace_only_content_returns_empty_list(self, whitespace_record):
        chunker = _PassthroughChunker()
        assert chunker.chunk_record(whitespace_record) == []

    def test_missing_content_key_returns_empty_list(self):
        chunker = _PassthroughChunker()
        record = {"drug_name": "X", "biomarker": "Y"}
        assert chunker.chunk_record(record) == []


# ---------------------------------------------------------------------------
# chunk_corpus: MIN_CHUNK_TOKENS filtering
# ---------------------------------------------------------------------------

class TestChunkCorpus:
    def test_chunk_corpus_filters_tiny_chunks(self, short_record):
        chunker = _TinyChunker()
        raw = chunker.chunk_record(short_record)
        # At least some single-character chunks are below MIN_CHUNK_TOKENS
        assert any(c["token_count"] < BaseChunker.MIN_CHUNK_TOKENS for c in raw)

        filtered = chunker.chunk_corpus([short_record])
        assert all(c["token_count"] >= BaseChunker.MIN_CHUNK_TOKENS for c in filtered)

    def test_chunk_corpus_flattens_multiple_records(self, short_record, long_record):
        chunker = _PassthroughChunker()
        chunks = chunker.chunk_corpus([short_record, long_record])
        assert len(chunks) == 2  # one chunk per record (passthrough)

    def test_chunk_corpus_empty_records_list(self):
        chunker = _PassthroughChunker()
        assert chunker.chunk_corpus([]) == []


# ---------------------------------------------------------------------------
# avg_tokens
# ---------------------------------------------------------------------------

class TestAvgTokens:
    def test_avg_tokens_single_chunk(self, short_record):
        chunker = _PassthroughChunker()
        chunks = chunker.chunk_record(short_record)
        avg = chunker.avg_tokens(chunks)
        assert avg == chunks[0]["token_count"]

    def test_avg_tokens_empty_list_returns_zero(self):
        chunker = _PassthroughChunker()
        assert chunker.avg_tokens([]) == 0.0
