"""
Strategy 1 & 2 — Fixed-size chunking (Weaviate approach).

Weaviate description
--------------------
"Fixed-size chunking is the simplest and most straightforward approach.
It splits text into chunks of a predetermined size … A common solution is
chunk overlap, where some tokens from the end of one chunk are repeated
at the beginning of the next.  A typical overlap is between 10 % and 20 %
of the chunk size."
(https://weaviate.io/blog/chunking-strategies-for-rag)

Weaviate adaptation for token-based budgets
--------------------------------------------
Weaviate's reference example uses *words* as atomic units.  We adopt the
same word-boundary principle but count tokens (tiktoken cl100k_base) for
the size budget, because the embedding model's context window is measured
in tokens.

Algorithm
---------
1. Collapse consecutive whitespace and split text into words.
2. Greedily add words to the current chunk until the token count would
   exceed ``chunk_size``.
3. When the budget is full, save the chunk and seed the next chunk with
   the final ``overlap`` tokens' worth of words from the current chunk.
4. This guarantees every chunk starts and ends on a word boundary and
   that consecutive chunks share exactly ``overlap_fraction × chunk_size``
   tokens of context (10 % by default; Weaviate recommends 10–20 %).

Why word-boundary splitting matters
-------------------------------------
The previous implementation encoded text to token IDs and sliced the
array directly.  tiktoken can split words across multiple tokens (e.g.
"biomarker" → ["bio", "marker"]).  Slicing the array mid-word produced
chunks starting with partial words like "marker testing…".  Splitting at
word boundaries first prevents this artefact without sacrificing accurate
token-budget control.

Implemented configurations
---------------------------
- ``FixedChunker(chunk_size=512,  overlap_fraction=0.10)``  → Strategy 1
- ``FixedChunker(chunk_size=1024, overlap_fraction=0.10)``  → Strategy 2
"""

from __future__ import annotations

import re
from typing import Any

from chunking.base_chunker import BaseChunker
from chunking.config import STRATEGY_CONFIGS
from chunking.utils import count_tokens, make_doc_id


def _split_words(text: str) -> list[str]:
    """Split *text* into words, collapsing consecutive whitespace."""
    return re.sub(r"\s+", " ", text.strip()).split(" ")


class FixedChunker(BaseChunker):
    """
    Fixed-size, word-boundary-aware chunker with fraction-based overlap.

    Parameters
    ----------
    chunk_size : int
        Maximum number of tokens per chunk.
    overlap_fraction : float
        Fraction of ``chunk_size`` repeated at each chunk boundary.
        Weaviate recommends 0.10–0.20.  Defaults to 0.10.
    """

    def __init__(self, chunk_size: int, overlap_fraction: float = 0.10) -> None:
        if not 0.0 <= overlap_fraction < 1.0:
            raise ValueError("overlap_fraction must be in [0.0, 1.0)")
        self.chunk_size = chunk_size
        self.overlap_fraction = overlap_fraction
        self.overlap_tokens = int(chunk_size * overlap_fraction)
        self.name = f"fixed_{chunk_size}"

    # ------------------------------------------------------------------
    # Core chunking logic (Weaviate word-based approach, token budget)
    # ------------------------------------------------------------------

    def _chunk_words(self, words: list[str]) -> list[str]:
        """
        Group *words* into chunks respecting ``chunk_size`` (tokens).

        Overlap is built at the token level: after saving a chunk the last
        ``overlap_tokens`` worth of words are carried forward as the seed
        of the next chunk.  This matches Weaviate's overlap semantics while
        keeping accurate token control.
        """
        chunks: list[str] = []
        current: list[str] = []
        current_tokens = 0

        for word in words:
            # +1 for the space that will separate this word from the next
            word_cost = count_tokens(word) + 1

            if current_tokens + word_cost > self.chunk_size and current:
                chunk_text = " ".join(current).strip()
                if chunk_text:
                    chunks.append(chunk_text)

                # Build overlap seed: take words from the tail of current
                # until we have accumulated self.overlap_tokens.
                overlap_seed: list[str] = []
                accumulated = 0
                for w in reversed(current):
                    wt = count_tokens(w) + 1
                    if accumulated + wt > self.overlap_tokens:
                        break
                    overlap_seed.insert(0, w)
                    accumulated += wt

                current = overlap_seed
                current_tokens = accumulated

            current.append(word)
            current_tokens += word_cost

        # Flush the final (possibly shorter) chunk.
        if current:
            chunk_text = " ".join(current).strip()
            if chunk_text:
                chunks.append(chunk_text)

        return chunks

    def chunk_record(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """Split one JSONL record into fixed-size, word-boundary-aware chunks."""
        content: str = record.get("content", "").strip()
        if not content:
            return []

        doc_id = make_doc_id(record)
        drug_name = record.get("drug_name", "")
        biomarker = record.get("biomarker", "")

        words = _split_words(content)
        texts = self._chunk_words(words)

        return [
            self._make_chunk(
                text=text,
                doc_id=doc_id,
                index=idx,
                drug_name=drug_name,
                biomarker=biomarker,
                section="",  # fixed chunking is section-blind
            )
            for idx, text in enumerate(texts)
            if text.strip()
        ]


# ---------------------------------------------------------------------------
# Pre-built instances matching the benchmark configurations
# ---------------------------------------------------------------------------

def build_fixed_192() -> FixedChunker:
    """Model-aligned strategy: 192-token chunks, 10 % overlap.

    192 tiktoken tokens ≈ 249 WordPiece tokens — fits within all-MiniLM-L6-v2's
    256-token context window with a 7-token safety margin.  This is the
    recommended fixed-size strategy when using all-MiniLM-L6-v2.
    """
    cfg = STRATEGY_CONFIGS["fixed_192"]
    chunker = FixedChunker(
        chunk_size=cfg["chunk_size"],
        overlap_fraction=cfg["overlap_fraction"],
    )
    chunker.name = "fixed_192"
    return chunker


def build_fixed_256() -> FixedChunker:
    """Borderline model-aligned strategy: 256-token chunks, 10 % overlap.

    Sits at the edge of all-MiniLM-L6-v2's context window.  Most chunks will
    fit; outliers with denser WordPiece tokenisation may be marginally truncated.
    Prefer ``build_fixed_192`` for a guaranteed fit.
    """
    cfg = STRATEGY_CONFIGS["fixed_256"]
    chunker = FixedChunker(
        chunk_size=cfg["chunk_size"],
        overlap_fraction=cfg["overlap_fraction"],
    )
    chunker.name = "fixed_256"
    return chunker


def build_fixed_512() -> FixedChunker:
    """Legacy strategy: 512-token chunks, 10 % overlap.

    Chunks exceed the all-MiniLM-L6-v2 context window (256 WordPiece tokens).
    The embedding model silently truncates input beyond ~192 tiktoken tokens.
    Kept for historical comparison only — use ``build_fixed_192`` instead.
    """
    cfg = STRATEGY_CONFIGS["fixed_512"]
    chunker = FixedChunker(
        chunk_size=cfg["chunk_size"],
        overlap_fraction=cfg["overlap_fraction"],
    )
    chunker.name = "fixed_512"
    return chunker


def build_fixed_1024() -> FixedChunker:
    """Legacy strategy: 1024-token chunks, 10 % overlap.

    Chunks far exceed the all-MiniLM-L6-v2 context window.  Kept for historical
    comparison only — use ``build_fixed_192`` instead.
    """
    cfg = STRATEGY_CONFIGS["fixed_1024"]
    chunker = FixedChunker(
        chunk_size=cfg["chunk_size"],
        overlap_fraction=cfg["overlap_fraction"],
    )
    chunker.name = "fixed_1024"
    return chunker
