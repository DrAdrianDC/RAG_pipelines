"""
Strategy 4 — Semantic chunking / Context-Aware Chunking (Weaviate approach).

Weaviate description
--------------------
"Semantic chunking shifts from traditional rule-based splitting to
meaning-based segmentation.  Instead of relying on character counts or
document structure, this more advanced technique divides text based on
its semantic similarity.  The process involves:

  1. Sentence Segmentation  — breaking the text into individual sentences
  2. Embedding Generation   — converting each sentence into a vector embedding
  3. Similarity Analysis    — comparing embeddings to detect semantic breakpoints
  4. Chunk Formation        — creating new chunks between these breakpoints"
(https://weaviate.io/blog/chunking-strategies-for-rag)

Our implementation of each Weaviate step
-----------------------------------------
Step 1 — _split_into_sentences():
    NLTK punkt tokeniser (falls back to period-split on failure).

Step 2 — SemanticChunker._find_split_points() → model.encode():
    SentenceTransformer (default: all-MiniLM-L6-v2).
    Batched encoding with normalised embeddings for stable cosine values.

Step 3 — Sliding-window similarity + adaptive percentile threshold:
    Instead of comparing sentence[i-1] vs sentence[i] directly (noisy),
    we compare the mean embedding of the previous W sentences against the
    mean of the next W sentences (W=2).  This smooths outlier sentences.

    The similarity_threshold config value is interpreted as a *percentile*
    of the per-document similarity distribution, not a fixed global cutoff.
    This is the adaptive variant that prevents over- or under-splitting
    when the corpus contains both dense technical prose (Clinical
    Pharmacology) and narrative sections (Adverse Reactions).

    similarity_threshold=0.7 → split at (1-0.7)*100 = 30th-percentile of
    THIS document's pairwise-similarity distribution → conservative (30% of
    sentence pairs are split points).  Lower threshold → higher percentile
    cutoff → more splits.  Higher threshold → fewer splits (larger chunks).

Step 4 — _enforce_limits():
    - Merge chunks shorter than min_chunk_tokens with their neighbour.
    - Recursively split chunks exceeding max_chunk_tokens on "\\ n\\ n".

Why min_chunk_tokens was raised from 20 to 50
----------------------------------------------
FDA regulatory text is dense.  A 20-token chunk (≈ 2–3 words of drug
dosing information) lacks enough context for the embedding to be
informative.  The Vecta benchmark found that semantic chunking with
threshold 0.7 produced 43-token average chunks on academic text,
collapsing document-level F1 to 0.42.  Raising min_chunk_tokens to 50
forces merging of sub-sentence fragments into coherent units before
they are indexed.

Weaviate recommended use case
------------------------------
"Technical, academic, or narrative documents … works well for academic
papers, legal documents, or long stories.  These texts do not always use
clear separators like paragraphs to show topic changes."
FDA prescribing information labels match this description exactly.
"""

from __future__ import annotations

from typing import Any

import nltk
import numpy as np
from numpy.linalg import norm

from chunking.base_chunker import BaseChunker
from chunking.config import EMBEDDING_MODEL, STRATEGY_CONFIGS
from chunking.utils import count_tokens, get_sentence_transformer, make_doc_id

# Download punkt data on first use (silent if already present).
for _resource in ("tokenizers/punkt_tab", "tokenizers/punkt"):
    try:
        nltk.data.find(_resource)
    except LookupError:
        nltk.download(_resource.split("/")[-1], quiet=True)


# ---------------------------------------------------------------------------
# Step 1 helper — sentence segmentation
# ---------------------------------------------------------------------------

def _split_into_sentences(text: str) -> list[str]:
    """
    Weaviate Step 1: break *text* into individual sentences.

    Uses NLTK punkt; falls back to period-split on any failure.
    Empty strings are filtered before returning.
    """
    try:
        sentences = nltk.sent_tokenize(text)
    except Exception:
        sentences = [s.strip() for s in text.split(". ") if s.strip()]
    return [s.strip() for s in sentences if s.strip()]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two normalised 1-D vectors."""
    denom = norm(a) * norm(b)
    return float(np.dot(a, b) / denom) if denom > 0 else 0.0


# ---------------------------------------------------------------------------
# SemanticChunker
# ---------------------------------------------------------------------------

class SemanticChunker(BaseChunker):
    """
    Embedding-based semantic boundary chunker — Weaviate 4-step process.

    Parameters
    ----------
    similarity_threshold : float
        Controls how aggressively to split.  Interpreted as a *percentile*:
        threshold=0.7 → split at the bottom-30th percentile of this document's
        pairwise-similarity distribution (adaptive, formula: (1-threshold)*100).
        Lower → more splits (smaller, focused chunks) — looser cutoff.
        Higher → fewer splits (larger, broader chunks) — stricter cutoff.
    min_chunk_tokens : int
        Minimum acceptable chunk size.  Chunks below this are merged with
        their right neighbour (Step 4).  Default 50 tokens for FDA text.
    max_chunk_tokens : int
        Maximum acceptable chunk size.  Chunks above this are split on
        "\\ n\\ n" (Step 4).
    model : SentenceTransformer | None
        Embedding model used in Step 2.  ``None`` → shared lazy singleton.
        Injecting a model decouples the class from the global singleton,
        making it testable and experiment-friendly.
    """

    name = "semantic"

    def __init__(
        self,
        similarity_threshold: float,
        min_chunk_tokens: int,
        max_chunk_tokens: int,
        model=None,
    ) -> None:
        self.similarity_threshold = similarity_threshold
        self.min_chunk_tokens = min_chunk_tokens
        self.max_chunk_tokens = max_chunk_tokens
        self._model = model

    # ------------------------------------------------------------------
    # Steps 2 & 3 — embed + similarity analysis
    # ------------------------------------------------------------------

    def _find_split_points(self, sentences: list[str]) -> list[int]:
        """
        Weaviate Steps 2 & 3: embed sentences and detect semantic breakpoints.

        Returns indices in *sentences* where a new chunk should begin.
        Index 0 is always included (the first chunk starts at sentence 0).

        Sliding window (W=2):
            Compare mean(embeddings[i-W : i]) vs mean(embeddings[i : i+W]).
            Smoother than adjacent-pair comparison; reduces noise from
            individual outlier sentences without masking real topic changes.

        Adaptive percentile threshold:
            The config value is treated as a percentile of THIS document's
            own similarity distribution.  A document where all sections are
            naturally similar gets a higher absolute cutoff, preventing
            over-splitting.  A document with abrupt topic changes gets a
            lower cutoff, capturing real boundaries.
        """
        if len(sentences) <= 1:
            return [0]

        # Step 2: embed all sentences at once (batch for speed).
        model = self._model if self._model is not None else get_sentence_transformer()
        embeddings: np.ndarray = model.encode(
            sentences,
            batch_size=64,
            show_progress_bar=False,
            normalize_embeddings=True,
        )

        # Step 3: compute sliding-window cosine similarities.
        window = 2
        similarities: list[float] = []
        for i in range(1, len(sentences)):
            left = embeddings[max(0, i - window): i].mean(axis=0)
            right = embeddings[i: min(len(sentences), i + window)].mean(axis=0)
            similarities.append(_cosine(left, right))

        if not similarities:
            return [0]

        # Adaptive cutoff: split at the bottom (1 - threshold) fraction.
        percentile = (1.0 - self.similarity_threshold) * 100
        cutoff = float(np.percentile(similarities, percentile))

        split_starts = [0]
        for i, sim in enumerate(similarities):
            if sim < cutoff:
                split_starts.append(i + 1)

        return split_starts

    # ------------------------------------------------------------------
    # Step 4 — size enforcement
    # ------------------------------------------------------------------

    def _enforce_limits(self, raw_chunks: list[str]) -> list[str]:
        """
        Weaviate Step 4 post-processing:

        a) Merge chunks shorter than min_chunk_tokens into their right neighbour.
        b) Split chunks exceeding max_chunk_tokens on double-newlines.
        """
        # --- (a) Merge too-small chunks ---
        # Accumulate into `carry` until combined size reaches min_chunk_tokens,
        # then flush the *combined* text — never the carry alone.
        merged: list[str] = []
        carry = ""
        for chunk in raw_chunks:
            combined = (carry + " " + chunk).strip() if carry else chunk
            if count_tokens(combined) < self.min_chunk_tokens:
                carry = combined
            else:
                merged.append(combined)
                carry = ""
        if carry:
            if merged:
                merged[-1] = (merged[-1] + " " + carry).strip()
            else:
                merged.append(carry)

        # --- (b) Split too-large chunks ---
        final: list[str] = []
        for chunk in merged:
            if count_tokens(chunk) <= self.max_chunk_tokens:
                final.append(chunk)
            else:
                acc = ""
                for piece in chunk.split("\n\n"):
                    candidate = (acc + "\n\n" + piece).strip() if acc else piece
                    if count_tokens(candidate) <= self.max_chunk_tokens:
                        acc = candidate
                    else:
                        if acc:
                            final.append(acc)
                        acc = piece
                if acc:
                    final.append(acc)

        return [c for c in final if c.strip()]

    # ------------------------------------------------------------------
    # BaseChunker interface
    # ------------------------------------------------------------------

    def chunk_record(self, record: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Apply Weaviate's 4-step semantic chunking to one JSONL record.

        Step 1: sentence segmentation
        Step 2: embedding generation
        Step 3: similarity analysis & breakpoint detection
        Step 4: chunk formation + size enforcement
        """
        content: str = record.get("content", "").strip()
        if not content:
            return []

        doc_id = make_doc_id(record)
        drug_name = record.get("drug_name", "")
        biomarker = record.get("biomarker", "")

        # Steps 1–3
        sentences = _split_into_sentences(content)
        if not sentences:
            return []

        split_starts = self._find_split_points(sentences)

        # Build raw chunks from the detected breakpoints.
        raw_chunks: list[str] = []
        for i, start in enumerate(split_starts):
            end = split_starts[i + 1] if i + 1 < len(split_starts) else len(sentences)
            chunk_text = " ".join(sentences[start:end]).strip()
            if chunk_text:
                raw_chunks.append(chunk_text)

        # Step 4
        final_chunks = self._enforce_limits(raw_chunks)

        return [
            self._make_chunk(
                text=text,
                doc_id=doc_id,
                index=idx,
                drug_name=drug_name,
                biomarker=biomarker,
                section="",  # semantic chunking is section-blind
            )
            for idx, text in enumerate(final_chunks)
        ]


# ---------------------------------------------------------------------------
# Pre-built instance matching the benchmark configuration
# ---------------------------------------------------------------------------

def build_semantic() -> SemanticChunker:
    """Strategy 4: semantic chunking — Weaviate 4-step, adaptive threshold."""
    cfg = STRATEGY_CONFIGS["semantic"]
    chunker = SemanticChunker(
        similarity_threshold=cfg["similarity_threshold"],
        min_chunk_tokens=cfg["min_chunk_tokens"],
        max_chunk_tokens=cfg["max_chunk_tokens"],
    )
    chunker.name = "semantic"
    return chunker
