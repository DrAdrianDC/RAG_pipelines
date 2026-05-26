"""
LangChain-backed chunking strategies (cross-validation variants).

Purpose
-------
These chunkers wrap ``langchain_text_splitters`` so the benchmark can
compare the *same logical strategy* (fixed-size, recursive) in two
independent implementations:

1. Custom (fixed_chunking.py, recursive_chunking.py) — word-boundary-aware,
   token-level overlap, full control over every step.
2. LangChain (this file) — battle-tested, industry standard, widely used
   in production RAG systems.

Why keep both?
--------------
If both implementations produce the same retrieval metrics, the custom code
is validated.  If they diverge, the gap reveals a subtle implementation
difference worth investigating (separator handling, overlap semantics,
boundary conditions, tokenisation edge cases).

This cross-validation is the main value of a *research* benchmark vs a
*production* chatbot.

Weaviate and LangChain alignment
----------------------------------
Weaviate's article describes LangChain as the standard library for
"Fixed-Size" and "Recursive" chunking in production RAG pipelines:

  "LangChain: A broad framework for building LLM applications.  Its
   flexible TextSplitters make it easy to integrate chunking as part of
   a larger system … Best for: modular workflows where chunking is just
   one piece of the puzzle."
  (https://weaviate.io/blog/chunking-strategies-for-rag)

LangChain uses *absolute* overlap tokens, not a fraction.  The values
below are set to match the ~10 % overlap_fraction used in the custom
implementations (see STRATEGY_CONFIGS in config.py).

Requirements
------------
    pip install langchain-text-splitters tiktoken

Note: this imports only ``langchain_text_splitters`` (lightweight ~5 MB),
NOT the full LangChain ecosystem (~300 MB).
"""

from __future__ import annotations

from typing import Any

from langchain_text_splitters import (
    RecursiveCharacterTextSplitter,
    TokenTextSplitter,
)

from chunking.base_chunker import BaseChunker
from chunking.config import STRATEGY_CONFIGS
from chunking.utils import count_tokens, make_doc_id


class LangChainFixedChunker(BaseChunker):
    """
    Fixed-size chunker backed by LangChain's ``TokenTextSplitter``.

    Uses tiktoken (cl100k_base) under the hood — same tokeniser as the
    custom FixedChunker.  Differences in results reveal overlap semantics
    or word-boundary handling between the two implementations.
    """

    def __init__(self, chunk_size: int = 512, overlap: int = 51) -> None:
        self.name = f"lc_fixed_{chunk_size}"
        self._splitter = TokenTextSplitter(
            encoding_name="cl100k_base",
            chunk_size=chunk_size,
            chunk_overlap=overlap,
        )

    def chunk_record(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        content: str = record.get("content", "")
        doc_id = make_doc_id(record)
        pieces = self._splitter.split_text(content)
        return [
            self._make_chunk(
                text=piece,
                doc_id=doc_id,
                index=i,
                drug_name=record.get("drug_name", ""),
                biomarker=record.get("biomarker", ""),
            )
            for i, piece in enumerate(pieces)
            if piece.strip()
        ]


class LangChainRecursiveChunker(BaseChunker):
    """
    Recursive character splitter backed by LangChain's
    ``RecursiveCharacterTextSplitter``.

    Uses the standard Weaviate/LangChain separator hierarchy:
    ["\\ n\\ n", "\\ n", ". ", "! ", "? ", "; ", " ", ""]
    with ``count_tokens`` as the length function so that the chunk_size
    budget is measured in tokens, not characters.
    """

    def __init__(self, chunk_size: int = 512, overlap: int = 51) -> None:
        self.name = f"lc_recursive_{chunk_size}"
        self._splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=overlap,
            length_function=count_tokens,
            separators=["\n\n", "\n", ". ", "! ", "? ", "; ", " ", ""],
        )

    def chunk_record(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        content: str = record.get("content", "")
        doc_id = make_doc_id(record)
        pieces = self._splitter.split_text(content)
        return [
            self._make_chunk(
                text=piece,
                doc_id=doc_id,
                index=i,
                drug_name=record.get("drug_name", ""),
                biomarker=record.get("biomarker", ""),
            )
            for i, piece in enumerate(pieces)
            if piece.strip()
        ]


# ---------------------------------------------------------------------------
# Factory helpers — consistent with the rest of the chunking library
# ---------------------------------------------------------------------------

def build_lc_fixed_192() -> LangChainFixedChunker:
    """Model-aligned cross-validation: LangChain TokenTextSplitter at 192 tokens.

    Use alongside ``build_fixed_192`` to validate that both implementations
    produce equivalent results.
    """
    cfg = STRATEGY_CONFIGS["lc_fixed_192"]
    chunker = LangChainFixedChunker(chunk_size=cfg["chunk_size"], overlap=cfg["overlap"])
    chunker.name = "lc_fixed_192"
    return chunker


def build_lc_recursive_192() -> LangChainRecursiveChunker:
    """Model-aligned cross-validation: LangChain RecursiveCharacterTextSplitter at 192 tokens.

    Use alongside ``build_recursive_192`` to validate that both implementations
    produce equivalent results.
    """
    cfg = STRATEGY_CONFIGS["lc_recursive_192"]
    chunker = LangChainRecursiveChunker(chunk_size=cfg["chunk_size"], overlap=cfg["overlap"])
    chunker.name = "lc_recursive_192"
    return chunker


def build_lc_fixed_512() -> LangChainFixedChunker:
    """Legacy: LangChain TokenTextSplitter at 512 tokens. Exceeds model window."""
    cfg = STRATEGY_CONFIGS["lc_fixed_512"]
    return LangChainFixedChunker(chunk_size=cfg["chunk_size"], overlap=cfg["overlap"])


def build_lc_fixed_1024() -> LangChainFixedChunker:
    """Legacy: LangChain TokenTextSplitter at 1024 tokens. Exceeds model window."""
    cfg = STRATEGY_CONFIGS["lc_fixed_1024"]
    return LangChainFixedChunker(chunk_size=cfg["chunk_size"], overlap=cfg["overlap"])


def build_lc_recursive_512() -> LangChainRecursiveChunker:
    """Legacy: LangChain RecursiveCharacterTextSplitter at 512 tokens. Exceeds model window."""
    cfg = STRATEGY_CONFIGS["lc_recursive_512"]
    return LangChainRecursiveChunker(chunk_size=cfg["chunk_size"], overlap=cfg["overlap"])
