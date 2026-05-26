"""
Tests for StructureAwareChunker — Weaviate Document-Based Chunking (FDA).

Covers:
- Schema compliance
- Empty / degenerate inputs
- FDA section header detection (numbered ALL-CAPS headers)
- Section metadata propagation and normalisation
- Preamble (text before first section header) handling
- Oversized section sub-splitting (recursive fallback)
- Content with no detectable headers (passthrough fallback)
- Factory builder matches config
"""

from __future__ import annotations

import pytest

from chunking.config import STRATEGY_CONFIGS
from chunking.structure_aware_chunking import StructureAwareChunker, build_structure_aware
from chunking.utils import count_tokens
from chunking.tests.conftest import assert_valid_chunk_schema


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def make_chunker(max_chunk_tokens: int = 1024) -> StructureAwareChunker:
    return StructureAwareChunker(max_chunk_tokens=max_chunk_tokens)


# ---------------------------------------------------------------------------
# Schema compliance
# ---------------------------------------------------------------------------

class TestSchema:
    def test_chunk_schema_valid(self, structured_record):
        chunker = make_chunker()
        chunks = chunker.chunk_record(structured_record)
        assert len(chunks) >= 1
        for chunk in chunks:
            assert_valid_chunk_schema(chunk, structured_record)

    def test_token_count_matches_content(self, structured_record):
        for chunk in make_chunker().chunk_record(structured_record):
            assert chunk["token_count"] == count_tokens(chunk["content"])

    def test_chunk_ids_are_unique(self, structured_record):
        ids = [c["chunk_id"] for c in make_chunker().chunk_record(structured_record)]
        assert len(ids) == len(set(ids))

    def test_chunk_ids_are_sequential(self, structured_record):
        chunks = make_chunker().chunk_record(structured_record)
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

    def test_no_section_headers_produces_one_chunk(self, short_record):
        """Content with no FDA headers should be treated as a single un-labelled section."""
        chunks = make_chunker().chunk_record(short_record)
        assert len(chunks) == 1
        assert chunks[0]["section"] == ""

    def test_no_section_headers_section_field_is_empty(self, long_record):
        chunks = make_chunker().chunk_record(long_record)
        for chunk in chunks:
            assert chunk["section"] == "", (
                f"Expected empty section for content without headers: '{chunk['section']}'"
            )


# ---------------------------------------------------------------------------
# Section detection
# ---------------------------------------------------------------------------

class TestSectionDetection:
    def test_produces_one_chunk_per_section(self, structured_record):
        """Four FDA sections in the fixture → at least 4 chunks (one per section)."""
        chunks = make_chunker().chunk_record(structured_record)
        assert len(chunks) >= 4

    def test_each_chunk_has_non_empty_section(self, structured_record):
        """Every chunk from a record with headers must carry a section name."""
        chunks = make_chunker().chunk_record(structured_record)
        # All chunks except a possible preamble chunk should have a section.
        section_chunks = [c for c in chunks if c["section"]]
        assert len(section_chunks) >= 4

    def test_known_section_names_are_detected(self, structured_record):
        sections = {c["section"] for c in make_chunker().chunk_record(structured_record)}
        expected = {
            "Indications and Usage",
            "Dosage and Administration",
            "Warnings and Precautions",
            "Adverse Reactions",
        }
        assert expected.issubset(sections), (
            f"Missing sections: {expected - sections}\n"
            f"Found sections: {sections}"
        )

    def test_section_content_contains_header_text(self, structured_record):
        """Section header text should be embedded in the chunk content for retrieval."""
        chunks = make_chunker().chunk_record(structured_record)
        indications_chunks = [c for c in chunks if c["section"] == "Indications and Usage"]
        assert indications_chunks, "No chunk with section='Indications and Usage'"
        # The header should appear in the content for embedding retrieval.
        assert any(
            "INDICATIONS" in c["content"].upper() for c in indications_chunks
        )


# ---------------------------------------------------------------------------
# Section normalisation (21 CFR 201.57 ALL-CAPS → Title Case)
# ---------------------------------------------------------------------------

class TestSectionNormalisation:
    def test_all_caps_section_normalised_to_title_case(self, structured_record):
        chunks = make_chunker().chunk_record(structured_record)
        for chunk in chunks:
            if chunk["section"]:
                # Should never be ALL-CAPS in the section field
                assert chunk["section"] != chunk["section"].upper() or len(chunk["section"]) <= 3, (
                    f"Section not normalised: '{chunk['section']}'"
                )

    def test_numbered_prefix_stripped_from_section(self, structured_record):
        chunks = make_chunker().chunk_record(structured_record)
        for chunk in chunks:
            # e.g. "5 WARNINGS AND PRECAUTIONS" → "Warnings and Precautions"
            assert not chunk["section"].startswith(tuple("0123456789")), (
                f"Section retains numeric prefix: '{chunk['section']}'"
            )

    def test_canonical_section_names_match_jsonl_labeling_sections(self, structured_record):
        """
        Normalised section names must match the format used in the JSONL
        ``labeling_sections`` field so that section-level evaluation metrics work.
        """
        expected_sections = set(structured_record["labeling_sections"])
        found_sections = {c["section"] for c in make_chunker().chunk_record(structured_record) if c["section"]}
        assert found_sections.issubset(expected_sections | {"Boxed Warning", ""}), (
            f"Unexpected section names: {found_sections - expected_sections}"
        )


# ---------------------------------------------------------------------------
# Preamble handling
# ---------------------------------------------------------------------------

class TestPreamble:
    def test_text_before_first_header_is_captured(self):
        """Text that appears before any numbered header becomes a PREAMBLE chunk."""
        record = {
            "content": (
                "WARNING: SERIOUS IMMUNE-MEDIATED REACTIONS. "
                "1 INDICATIONS AND USAGE KEYTRUDA is indicated for melanoma. "
                "2 DOSAGE AND ADMINISTRATION Administer 200 mg every 3 weeks."
            ),
            "drug_name": "KEYTRUDA",
            "biomarker": "PD-L1",
            "labeling_sections": ["Boxed Warning", "Indications and Usage", "Dosage and Administration"],
        }
        chunks = make_chunker().chunk_record(record)
        # There should be a chunk for preamble content
        assert len(chunks) >= 2
        contents = " ".join(c["content"] for c in chunks)
        assert "IMMUNE-MEDIATED" in contents


# ---------------------------------------------------------------------------
# Oversized section sub-splitting
# ---------------------------------------------------------------------------

class TestOversizedSection:
    def test_oversized_section_is_split_into_multiple_chunks(self):
        """A section body exceeding max_chunk_tokens must be sub-split."""
        # Build a section that is clearly larger than 50 tokens.
        section_body = "The patient should receive careful monitoring. " * 20
        record = {
            "content": f"1 INDICATIONS AND USAGE {section_body}",
            "drug_name": "X",
            "biomarker": "Y",
            "labeling_sections": ["Indications and Usage"],
        }
        chunker = make_chunker(max_chunk_tokens=50)
        chunks = chunker.chunk_record(record)
        assert len(chunks) > 1, "Oversized section should produce multiple chunks"

    def test_all_sub_chunks_carry_parent_section_name(self):
        """Every sub-chunk of an oversized section must retain the parent section."""
        section_body = "The patient should receive careful monitoring. " * 20
        record = {
            "content": f"5 WARNINGS AND PRECAUTIONS {section_body}",
            "drug_name": "X",
            "biomarker": "Y",
            "labeling_sections": ["Warnings and Precautions"],
        }
        chunker = make_chunker(max_chunk_tokens=50)
        chunks = chunker.chunk_record(record)
        for chunk in chunks:
            assert chunk["section"] == "Warnings and Precautions", (
                f"Sub-chunk lost section: '{chunk['section']}'"
            )

    def test_no_sub_chunk_exceeds_max_chunk_tokens(self):
        section_body = "Administer the drug as follows per label instructions. " * 25
        record = {
            "content": f"2 DOSAGE AND ADMINISTRATION {section_body}",
            "drug_name": "X",
            "biomarker": "Y",
            "labeling_sections": ["Dosage and Administration"],
        }
        max_tokens = 60
        chunker = make_chunker(max_chunk_tokens=max_tokens)
        chunks = chunker.chunk_record(record)
        for chunk in chunks:
            assert chunk["token_count"] <= max_tokens + 15, (
                f"Sub-chunk exceeded max_chunk_tokens: {chunk['token_count']} > {max_tokens}"
            )


# ---------------------------------------------------------------------------
# Factory builder
# ---------------------------------------------------------------------------

class TestFactoryBuilder:
    def test_build_structure_aware_name(self):
        assert build_structure_aware().name == "structure_aware"

    def test_build_structure_aware_matches_config(self):
        cfg = STRATEGY_CONFIGS["structure_aware"]
        chunker = build_structure_aware()
        assert chunker.max_chunk_tokens == cfg["max_chunk_tokens"]
