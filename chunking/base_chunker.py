"""
Abstract base class that every chunking strategy must implement.

Contract
--------
Every subclass receives a list of JSONL records and must return a list of
chunk dicts, each with these mandatory keys:

    chunk_id    : str   — globally unique ID (e.g. "<doc_id>::<index>")
    content     : str   — the text of the chunk
    doc_id      : str   — parent document identifier (drug_name||biomarker)
    section     : str   — FDA labeling section, or "" if not determinable
    drug_name   : str   — raw drug name from the source record
    biomarker   : str   — raw biomarker from the source record
    token_count : int   — number of tokens in *content*
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import numpy as np

from chunking.utils import count_tokens, make_doc_id


class BaseChunker(ABC):
    """Abstract chunking strategy."""

    #: Human-readable name used in result tables and ChromaDB collection names.
    name: str = "base"

    @abstractmethod
    def chunk_record(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Split a single JSONL record into chunks.

        Parameters
        ----------
        record:
            A single line from the JSONL corpus.

        Returns
        -------
        list[dict]
            List of chunk dicts following the schema described in the module
            docstring.
        """

    # ------------------------------------------------------------------
    # Non-abstract helpers (shared across all strategies)
    # ------------------------------------------------------------------

    #: Minimum token count for a chunk to be kept.
    #: Chunks shorter than this are almost certainly tokenizer artefacts
    #: (stray punctuation, single characters) that add noise without value.
    MIN_CHUNK_TOKENS: int = 5

    def chunk_corpus(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Chunk every record in *records* and return a flat list of chunks.

        Degenerate chunks (token_count < MIN_CHUNK_TOKENS) are silently
        dropped here rather than in individual strategy implementations,
        ensuring consistent behaviour across all strategies.
        """
        all_chunks: list[dict[str, Any]] = []
        for record in records:
            for chunk in self.chunk_record(record):
                if chunk.get("token_count", 0) >= self.MIN_CHUNK_TOKENS:
                    all_chunks.append(chunk)
        return all_chunks

    def avg_tokens(self, chunks: list[dict[str, Any]]) -> float:
        """Mean token count across *chunks*."""
        if not chunks:
            return 0.0
        return float(np.mean([c["token_count"] for c in chunks]))

    # ------------------------------------------------------------------
    # Shared chunk-building helper
    # ------------------------------------------------------------------

    @staticmethod
    def _make_chunk(
        text: str,
        doc_id: str,
        index: int,
        drug_name: str,
        biomarker: str,
        section: str = "",
    ) -> dict[str, Any]:
        """Build a well-formed chunk dict from its constituent parts."""
        return {
            "chunk_id": f"{doc_id}::{index}",
            "content": text,
            "doc_id": doc_id,
            "section": section,
            "drug_name": drug_name,
            "biomarker": biomarker,
            "token_count": count_tokens(text),
        }
