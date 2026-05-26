"""
Strategy 3 — Recursive chunking (Weaviate approach).

Weaviate description
--------------------
"Recursive chunking is a more nuanced approach.  It splits text using a
prioritised list of common separators, such as double newlines (for
paragraphs) or single newlines (for sentences).  It first tries to split
the text by the highest-priority separator (paragraphs).  If any resulting
chunk is still too large, the algorithm *recursively* applies the next
separator (sentences) to that specific chunk."
(https://weaviate.io/blog/chunking-strategies-for-rag)

Algorithm (Weaviate recursive + token-level overlap)
-----------------------------------------------------
Step 1 — _split_recursive(text, separators):
    Base case: text already fits within chunk_size → return as-is.
    Otherwise: find the first separator that appears in text.
    Split by that separator and greedily merge pieces back into chunks
    up to chunk_size.  Any individual piece that is still too large is
    recursively split with the remaining separators.

Step 2 — _add_overlap(chunks):
    For each consecutive pair of chunks, prepend the last
    ``overlap_tokens`` worth of *words* from chunk[i-1] to chunk[i].
    This step is intentionally separate from Step 1 so that overlap is
    applied at the token level across the final chunk list, not at the
    piece-merging level.

Why Step 2 is separate (and why the old implementation was buggy)
-----------------------------------------------------------------
The previous implementation tried to build overlap inside _merge_pieces
by carrying pieces (whole paragraphs) from the previous chunk.  Because
a single FDA paragraph can easily exceed the overlap budget (50 tokens),
the carry loop broke immediately and produced *zero* overlap for the
majority of consecutive chunks.  Separating split from overlap-injection
guarantees that the overlap budget is always filled from actual word
content regardless of how large the underlying pieces are.

Separator hierarchy (Weaviate default)
---------------------------------------
    ["\\ n\\ n", "\\ n", ". ", "! ", "? ", " ", ""]

The empty string "" is the character-level last resort; it ensures the
algorithm never gets stuck on a pathologically long word-free string.

Overlap convention
------------------
Uses ``overlap_fraction`` (Weaviate: 10–20 %) rather than an absolute
token count, consistent with fixed_chunking.py.
"""

from __future__ import annotations

from typing import Any

from chunking.base_chunker import BaseChunker
from chunking.config import STRATEGY_CONFIGS
from chunking.utils import count_tokens, make_doc_id

# Weaviate separator hierarchy — highest structural priority first.
_DEFAULT_SEPARATORS: list[str] = ["\n\n", "\n", ". ", "! ", "? ", " ", ""]


class RecursiveChunker(BaseChunker):
    """
    Recursive character-based text splitter (Weaviate approach).

    Parameters
    ----------
    chunk_size : int
        Maximum number of tokens per output chunk.
    overlap_fraction : float
        Fraction of ``chunk_size`` prepended from the previous chunk to
        each new chunk (Weaviate: 10–20 %).
    separators : list[str] | None
        Ordered separator list.  Defaults to Weaviate's hierarchy:
        paragraph → newline → sentence end → space → character.
    """

    def __init__(
        self,
        chunk_size: int,
        overlap_fraction: float = 0.10,
        separators: list[str] | None = None,
    ) -> None:
        if not 0.0 <= overlap_fraction < 1.0:
            raise ValueError("overlap_fraction must be in [0.0, 1.0)")
        self.chunk_size = chunk_size
        self.overlap_fraction = overlap_fraction
        self.overlap_tokens = int(chunk_size * overlap_fraction)
        self.separators = separators if separators is not None else _DEFAULT_SEPARATORS
        self.name = f"recursive_{chunk_size}"

    # ------------------------------------------------------------------
    # Step 1: Weaviate recursive split
    # ------------------------------------------------------------------

    def _split_recursive(self, text: str, separators: list[str]) -> list[str]:
        """
        Recursively split *text* until every piece fits within chunk_size.

        Follows Weaviate's algorithm exactly:
        1. Base case: fits → return as singleton.
        2. Find first separator present in text.
        3. Greedily merge pieces into chunks ≤ chunk_size.
        4. Any individual oversized piece is recursively split with the
           remaining separators.
        """
        # Base case
        if count_tokens(text) <= self.chunk_size:
            stripped = text.strip()
            return [stripped] if stripped else []

        # Find the first separator that exists in text.
        chosen_sep = ""
        remaining_seps: list[str] = []
        for idx, sep in enumerate(separators):
            if sep == "" or sep in text:
                chosen_sep = sep
                remaining_seps = separators[idx + 1:]
                break

        # Split by chosen separator (character fallback for empty sep).
        if chosen_sep == "":
            # Hard character split — last resort, avoids infinite recursion.
            char_limit = self.chunk_size * 4  # ~4 chars per token heuristic
            return [text[i: i + char_limit] for i in range(0, len(text), char_limit)]

        raw_parts = text.split(chosen_sep)

        # Greedily merge parts into chunks of at most chunk_size tokens.
        chunks: list[str] = []
        current = ""
        for part in raw_parts:
            if not part:
                continue
            candidate = (current + chosen_sep + part) if current else part
            if count_tokens(candidate) <= self.chunk_size:
                current = candidate
            else:
                # Save what we have so far.
                if current:
                    chunks.append(current.strip())
                # Part itself may exceed chunk_size — recurse on it.
                if count_tokens(part) > self.chunk_size:
                    sub = self._split_recursive(part, remaining_seps)
                    if sub:
                        # Carry the last sub-chunk forward for merging.
                        chunks.extend(sub[:-1])
                        current = sub[-1]
                    else:
                        current = ""
                else:
                    current = part

        if current:
            chunks.append(current.strip())

        return [c for c in chunks if c.strip()]

    # ------------------------------------------------------------------
    # Step 2: token-level overlap injection
    # ------------------------------------------------------------------

    def _add_overlap(self, chunks: list[str]) -> list[str]:
        """
        Prepend the last ``overlap_tokens`` worth of words from chunk[i-1]
        to chunk[i].

        Works at the word level to avoid cutting mid-word, but controls
        the budget in tokens — guaranteeing the overlap is always filled
        regardless of how large the underlying pieces were in Step 1.
        """
        if self.overlap_tokens <= 0 or len(chunks) <= 1:
            return chunks

        result: list[str] = [chunks[0]]
        for i in range(1, len(chunks)):
            prev_words = chunks[i - 1].split()
            overlap_words: list[str] = []
            accumulated = 0
            for word in reversed(prev_words):
                wt = count_tokens(word) + 1  # +1 for separator space
                if accumulated + wt > self.overlap_tokens:
                    break
                overlap_words.insert(0, word)
                accumulated += wt

            if overlap_words:
                result.append(" ".join(overlap_words) + " " + chunks[i])
            else:
                result.append(chunks[i])

        return result

    # ------------------------------------------------------------------
    # BaseChunker interface
    # ------------------------------------------------------------------

    def chunk_record(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """Split one JSONL record using Weaviate's recursive algorithm."""
        content: str = record.get("content", "").strip()
        if not content:
            return []

        doc_id = make_doc_id(record)
        drug_name = record.get("drug_name", "")
        biomarker = record.get("biomarker", "")

        # Step 1: recursive split
        chunks = self._split_recursive(content, self.separators)
        # Step 2: inject token-level overlap
        chunks = self._add_overlap(chunks)

        return [
            self._make_chunk(
                text=text,
                doc_id=doc_id,
                index=idx,
                drug_name=drug_name,
                biomarker=biomarker,
                section="",  # recursive chunking is section-blind
            )
            for idx, text in enumerate(chunks)
            if text.strip()
        ]


# ---------------------------------------------------------------------------
# Pre-built instance matching the benchmark configuration
# ---------------------------------------------------------------------------

def build_recursive_192() -> RecursiveChunker:
    """Model-aligned strategy: recursive splitting at 192 tokens, 10 % token-level overlap.

    192 tiktoken tokens ≈ 249 WordPiece tokens — fits within all-MiniLM-L6-v2's
    256-token context window with a 7-token safety margin.  This is the
    recommended recursive strategy when using all-MiniLM-L6-v2.
    """
    cfg = STRATEGY_CONFIGS["recursive_192"]
    chunker = RecursiveChunker(
        chunk_size=cfg["chunk_size"],
        overlap_fraction=cfg["overlap_fraction"],
    )
    chunker.name = "recursive_192"
    return chunker


def build_recursive_512() -> RecursiveChunker:
    """Legacy strategy: recursive splitting at 512 tokens, 10 % token-level overlap.

    Chunks exceed the all-MiniLM-L6-v2 context window (256 WordPiece tokens).
    Kept for historical comparison only — use ``build_recursive_192`` instead.
    """
    cfg = STRATEGY_CONFIGS["recursive_512"]
    chunker = RecursiveChunker(
        chunk_size=cfg["chunk_size"],
        overlap_fraction=cfg["overlap_fraction"],
    )
    chunker.name = "recursive_512"
    return chunker
