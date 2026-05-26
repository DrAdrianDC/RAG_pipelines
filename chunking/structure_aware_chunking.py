"""
Strategy 5 — Document-Based Chunking (Weaviate approach), FDA-specialised.

Weaviate description
--------------------
"Document-based chunking uses the *intrinsic structure of a document*.
Instead of relying on generic separators, it parses the document based
on its format-specific elements … With this method, chunks stay aligned
with the document's logical organisation, which often also correlates
with semantic meaning."
(https://weaviate.io/blog/chunking-strategies-for-rag)

Weaviate recommended use case
------------------------------
"Highly structured documents where the format can easily define logical
separations.  Ideal for Markdown, HTML, source code, or any document
with clear structural markers."

Our data — FDA prescribing information labels
----------------------------------------------
Every FDA label follows the standardised section numbering scheme defined
in 21 CFR 201.57.  Section boundaries are reliably detectable with a
single regex, making this the Weaviate Document-Based approach applied to
regulatory text instead of Markdown.

Detected section headers (pattern: number + ALL-CAPS title)
------------------------------------------------------------
    1  INDICATIONS AND USAGE
    2  DOSAGE AND ADMINISTRATION
    2.1 Recommended Dosage
    4  CONTRAINDICATIONS
    5  WARNINGS AND PRECAUTIONS
    5.1 Hypersensitivity Reactions
    6  ADVERSE REACTIONS
    8  USE IN SPECIFIC POPULATIONS
    11 DESCRIPTION
    12 CLINICAL PHARMACOLOGY
    12.3 Pharmacokinetics
    14 CLINICAL STUDIES

When no numbered header is found the content is treated as a single
un-labelled section and split by the recursive fallback at max_chunk_tokens.

Size enforcement
-----------------
Sections shorter than max_chunk_tokens are kept intact — one section = one
chunk, preserving the complete logical unit (Weaviate's goal).
Sections exceeding max_chunk_tokens are sub-split recursively
(paragraph → newline → sentence) so the section header is preserved in the
metadata ``section`` field of every sub-chunk, enabling section-level
retrieval metrics even after splitting.
"""

from __future__ import annotations

import re
from typing import Any

from chunking.base_chunker import BaseChunker
from chunking.config import STRATEGY_CONFIGS
from chunking.utils import count_tokens, make_doc_id, normalize_section

# Matches FDA-style numbered section headers in flat (no-newline) text.
#
# Pattern explanation:
#   (?<!\d)        — not preceded by a digit (avoids matching inside a year like "2024")
#   \d+(?:\.\d+)* — section number: "1", "12", "5.1", "14.3"
#   \s+            — one or more spaces
#   [A-Z]{2,}      — title must start with at least 2 consecutive uppercase letters
#   [A-Z\s,/\(\)&\-]+ — rest of the ALL-CAPS title (spaces, commas, etc. are allowed)
#   (?=\s)         — lookahead: must be followed by whitespace (not end-of-title guard)
#
# This intentionally matches ONLY all-caps section/subsection titles
# (e.g. "INDICATIONS AND USAGE", "WARNINGS AND PRECAUTIONS") and not
# Title-Case subsections like "1.1 Early Breast Cancer".
_SECTION_PATTERN = re.compile(
    r"(?<!\d)(?P<num>\d+(?:\.\d+)*)\s+(?P<title>[A-Z]{2}[A-Z\s,/\(\)&\-]+?)"
    r"(?=\s+(?:"
    r"\d+(?:\.\d+)*\s+[A-Z]"   # next numbered section  (e.g. "2 DOSAGE")
    r"|[A-Z][a-z]"              # title-case body word   (e.g. "Administer")
    r"|[A-Z]+\s+[a-z]"         # ALL-CAPS brand name followed by lowercase body
    r"|[a-z]"                   # lowercase body word    (e.g. "the")
    r"))"
)

# Separators for oversized section fallback (recursive strategy order).
_FALLBACK_SEPS = ["\n\n", "\n", ". ", " "]


def _recursive_split(text: str, chunk_size: int) -> list[str]:
    """
    Simple recursive split used as a fallback for oversized sections.
    Returns a list of sub-chunks, each ≤ chunk_size tokens.
    """
    if count_tokens(text) <= chunk_size:
        return [text]

    for sep in _FALLBACK_SEPS:
        if sep in text:
            pieces = text.split(sep)
            result: list[str] = []
            current = ""
            for piece in pieces:
                candidate = (current + sep + piece).strip() if current else piece
                if count_tokens(candidate) <= chunk_size:
                    current = candidate
                else:
                    if current:
                        result.append(current)
                    current = piece
            if current:
                result.append(current)
            # If every piece still fits, we're done; otherwise recurse.
            if all(count_tokens(c) <= chunk_size for c in result):
                return result
            flat: list[str] = []
            for r in result:
                flat.extend(_recursive_split(r, chunk_size))
            return flat

    # Character-level last resort.
    tokens_approx = 4  # ~4 chars per token
    char_limit = chunk_size * tokens_approx
    return [text[i : i + char_limit] for i in range(0, len(text), char_limit)]


def _parse_sections(content: str) -> list[tuple[str, str]]:
    """
    Parse *content* into (section_title, section_text) pairs.

    Returns a list of tuples; the text does *not* include the header line
    itself (the header is stored separately in the section_title field).
    """
    matches = list(_SECTION_PATTERN.finditer(content))

    if not matches:
        return [("", content)]

    sections: list[tuple[str, str]] = []

    # Text before the first numbered section (e.g. a Boxed Warning prefix).
    preamble = content[: matches[0].start()].strip()
    if preamble:
        sections.append(("PREAMBLE", preamble))

    for i, match in enumerate(matches):
        title = f"{match.group('num')} {match.group('title').strip()}"
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        body = content[start:end].strip()
        sections.append((title, body))

    return sections


class StructureAwareChunker(BaseChunker):
    """
    FDA section-header aware chunker.

    Parameters
    ----------
    max_chunk_tokens : int
        Maximum tokens per output chunk.  Sections shorter than this are kept
        whole; sections longer are split recursively.
    """

    name = "structure_aware"

    def __init__(self, max_chunk_tokens: int) -> None:
        self.max_chunk_tokens = max_chunk_tokens

    def chunk_record(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """Split one JSONL record on FDA section boundaries."""
        content: str = record.get("content", "").strip()
        if not content:
            return []

        doc_id = make_doc_id(record)
        drug_name = record.get("drug_name", "")
        biomarker = record.get("biomarker", "")

        sections = _parse_sections(content)
        chunks: list[dict[str, Any]] = []
        global_idx = 0

        for section_title, section_text in sections:
            if not section_text.strip():
                continue

            # Normalise the section name to the canonical form used in JSONL
            # "labeling_sections" so that section-level evaluation metrics can
            # compare retrieved sections directly against ground-truth sections.
            canonical_section = normalize_section(section_title) if section_title else ""

            # Include the section header in chunk content so that queries
            # mentioning the section name (e.g. "warnings and precautions")
            # can match the embedding. Without this, the header exists only
            # in metadata and the embedding is computed on body text alone —
            # causing retrieval to miss section-relevant queries.
            header_prefix = f"{section_title}\n\n" if section_title else ""

            if count_tokens(header_prefix + section_text) <= self.max_chunk_tokens:
                chunks.append(
                    self._make_chunk(
                        text=header_prefix + section_text,
                        doc_id=doc_id,
                        index=global_idx,
                        drug_name=drug_name,
                        biomarker=biomarker,
                        section=canonical_section,
                    )
                )
                global_idx += 1
            else:
                sub_texts = _recursive_split(section_text, self.max_chunk_tokens)
                for sub_idx, sub in enumerate(sub_texts):
                    if sub.strip():
                        # Prepend header only to the first sub-chunk of a section
                        # so it appears once in the index without bloating all sub-chunks.
                        text = (header_prefix + sub) if sub_idx == 0 else sub
                        chunks.append(
                            self._make_chunk(
                                text=text,
                                doc_id=doc_id,
                                index=global_idx,
                                drug_name=drug_name,
                                biomarker=biomarker,
                                section=canonical_section,
                            )
                        )
                        global_idx += 1

        return chunks


# ---------------------------------------------------------------------------
# Pre-built instance matching the benchmark configuration
# ---------------------------------------------------------------------------

def build_structure_aware() -> StructureAwareChunker:
    """Strategy 5: structure-aware, max 1024 tokens per chunk."""
    cfg = STRATEGY_CONFIGS["structure_aware"]
    chunker = StructureAwareChunker(max_chunk_tokens=cfg["max_chunk_tokens"])
    chunker.name = "structure_aware"
    return chunker
