"""
Library-level configuration for the chunking package.

Scope
-----
Only chunking-strategy parameters, the embedding model name used
by the semantic chunker, and the dataset path live here.

Experiment-level concerns (ChromaDB paths, results paths, context
budget, sample size) live in experiments/chunking_benchmark/config.py.

Rule: if you delete experiments/ entirely, this file must still be
importable and correct.

Embedding model context-window alignment
-----------------------------------------
``all-MiniLM-L6-v2`` was trained with a hard limit of 256 WordPiece tokens.
Any input longer than this is silently truncated — the embedding represents
only the first 256 WordPiece tokens of the chunk text, and the rest is lost.

tiktoken ``cl100k_base`` (BPE) and WordPiece tokenise the same text
differently.  For English biomedical prose the empirical ratio is roughly
1.15–1.30 WordPiece tokens per cl100k token, so:

    256 WordPiece ≈ 197–222 cl100k tokens

``SAFE_CHUNK_TOKENS = 192`` provides a ~30-token margin below the lowest
end of that range.  All model-aligned strategies use this constant so the
constraint is enforced from a single source of truth.

Strategies labelled "legacy / oversize" below use chunk sizes that exceed
``SAFE_CHUNK_TOKENS``.  They are kept for historical comparison but are
NOT recommended for production use with ``all-MiniLM-L6-v2``.  Switching
to a model with a larger context window (e.g. ``all-mpnet-base-v2`` at
512 tokens, or ``BAAI/bge-base-en-v1.5`` at 512 tokens) removes this
constraint and allows the oversize strategies to perform as intended.

Weaviate overlap convention
----------------------------
Weaviate recommends expressing overlap as a *fraction* of chunk_size
(10–20 %) rather than an absolute token count.  All strategies below
use ``overlap_fraction`` so the parameter scales automatically when
chunk_size is changed.

  overlap_fraction = 0.10  →  19 tokens for fixed_192
                           →  26 tokens for fixed_256
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Dataset location (shared across all experiments)
# ---------------------------------------------------------------------------
DATASET_PATH = (
    PROJECT_ROOT / "datasets" / "fda_biomarkers" / "benchmark" / "fda_biomarkers.jsonl"
)

# ---------------------------------------------------------------------------
# Embedding model for semantic chunker
# SemanticChunker supports dependency injection; this is the default.
# ---------------------------------------------------------------------------
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# ---------------------------------------------------------------------------
# Maximum safe chunk size (tiktoken cl100k_base tokens) for EMBEDDING_MODEL.
#
# all-MiniLM-L6-v2: max_seq_length = 256 WordPiece tokens.
# Empirical ratio for English biomedical text: 1 cl100k token ≈ 1.15–1.30 WordPiece.
# SAFE_CHUNK_TOKENS = 192  →  192 × 1.30 = 249 WordPiece  (7-token margin below 256).
#
# Update this constant when EMBEDDING_MODEL is changed to a model with a
# different context window.  All model-aligned strategies reference it.
# ---------------------------------------------------------------------------
SAFE_CHUNK_TOKENS: int = 192

# ---------------------------------------------------------------------------
# Chunking strategy hyperparameters
# ---------------------------------------------------------------------------
STRATEGY_CONFIGS: dict = {
    # ==================================================================
    # MODEL-ALIGNED STRATEGIES  (chunk_size <= SAFE_CHUNK_TOKENS)
    # Recommended for use with all-MiniLM-L6-v2 (256 WordPiece limit).
    # Every chunk fits within the embedding model's context window.
    # ==================================================================

    # ------------------------------------------------------------------
    # Fixed-Size / Token Chunking — model-aligned
    # ------------------------------------------------------------------
    "fixed_192": {
        "chunk_size": SAFE_CHUNK_TOKENS,        # 192 ≤ 256 WordPiece → no truncation
        "overlap_fraction": 0.10,               # 10 % ≈ 19 tokens
        "description": (
            "Fixed-size 192 tokens, 10 % overlap, word-boundary-aware "
            "[model-aligned: fits all-MiniLM-L6-v2 256-token window]"
        ),
    },
    "fixed_256": {
        "chunk_size": 256,                      # borderline — most chunks fit, outliers may truncate
        "overlap_fraction": 0.10,               # 10 % ≈ 26 tokens
        "description": (
            "Fixed-size 256 tokens, 10 % overlap, word-boundary-aware "
            "[borderline: near all-MiniLM-L6-v2 limit — prefer fixed_192]"
        ),
    },

    # ------------------------------------------------------------------
    # Recursive Chunking — model-aligned
    # ------------------------------------------------------------------
    "recursive_192": {
        "chunk_size": SAFE_CHUNK_TOKENS,        # 192 ≤ 256 WordPiece → no truncation
        "overlap_fraction": 0.10,               # 10 % ≈ 19 tokens
        "description": (
            "Recursive character splitting, 192 tokens, 10 % token-level overlap "
            "[model-aligned: fits all-MiniLM-L6-v2 256-token window]"
        ),
    },

    # ------------------------------------------------------------------
    # Semantic / Context-Aware Chunking — model-aligned
    # max_chunk_tokens caps the output to SAFE_CHUNK_TOKENS so the
    # adaptive threshold cannot produce chunks that exceed the window.
    # ------------------------------------------------------------------
    "semantic": {
        "similarity_threshold": 0.7,
        # Interpreted as a percentile: 0.7 → split at bottom-30th percentile
        # of pairwise similarities for THIS document (adaptive, not global).
        # Formula: percentile = (1 − threshold) × 100.
        # Lower values = more splits (smaller, focused chunks) — looser cutoff.
        # Higher values = fewer splits (larger, broader chunks) — stricter cutoff.
        "min_chunk_tokens": 50,                 # FDA text is not meaningful below 50 tokens
        "max_chunk_tokens": SAFE_CHUNK_TOKENS,  # hard cap at 192 — no chunk exceeds model window
        "description": (
            "Semantic chunking (Weaviate 4-step), adaptive percentile threshold "
            "[model-aligned: max_chunk_tokens capped at 192]"
        ),
    },

    # ------------------------------------------------------------------
    # Document-Based Chunking (FDA sections) — model-aligned
    # max_chunk_tokens controls recursive sub-splitting of long sections.
    # ------------------------------------------------------------------
    "structure_aware": {
        "max_chunk_tokens": SAFE_CHUNK_TOKENS,  # sub-split FDA sections at 192 tokens
        "description": (
            "Document-based chunking on FDA 21 CFR 201.57 section headers "
            "[model-aligned: max_chunk_tokens capped at 192]"
        ),
    },

    # ==================================================================
    # LEGACY / OVERSIZE STRATEGIES  (chunk_size > SAFE_CHUNK_TOKENS)
    # Kept for historical comparison only.
    # Chunks exceed the all-MiniLM-L6-v2 context window (256 WordPiece).
    # The embedding silently truncates: only the first ~192 tokens are
    # represented.  NOT recommended for production.
    # ==================================================================

    "fixed_512": {
        "chunk_size": 512,
        "overlap_fraction": 0.10,
        "description": (
            "Fixed-size 512 tokens, 10 % overlap [LEGACY: exceeds model window — "
            "embedding truncates at ~192 tokens]"
        ),
    },
    "fixed_1024": {
        "chunk_size": 1024,
        "overlap_fraction": 0.10,
        "description": (
            "Fixed-size 1024 tokens, 10 % overlap [LEGACY: exceeds model window — "
            "embedding truncates at ~192 tokens]"
        ),
    },
    "recursive_512": {
        "chunk_size": 512,
        "overlap_fraction": 0.10,
        "description": (
            "Recursive splitting, 512 tokens, 10 % overlap [LEGACY: exceeds model window — "
            "embedding truncates at ~192 tokens]"
        ),
    },

    # ==================================================================
    # LANGCHAIN CROSS-VALIDATION VARIANTS
    # Run alongside custom strategies to validate implementation parity.
    # Model-aligned variants use SAFE_CHUNK_TOKENS.
    # LangChain uses absolute overlap; kept here for comparison parity.
    # ==================================================================

    "lc_fixed_192": {
        "chunk_size": SAFE_CHUNK_TOKENS,
        "overlap": 19,              # ≈ 10 % of 192
        "description": "LangChain TokenTextSplitter, 192 tokens, ~10 % overlap [model-aligned]",
    },
    "lc_recursive_192": {
        "chunk_size": SAFE_CHUNK_TOKENS,
        "overlap": 19,              # ≈ 10 % of 192
        "description": "LangChain RecursiveCharacterTextSplitter, 192 tokens, ~10 % overlap [model-aligned]",
    },
    "lc_fixed_512": {
        "chunk_size": 512,
        "overlap": 51,              # ≈ 10 % of 512
        "description": "LangChain TokenTextSplitter, 512 tokens, ~10 % overlap [LEGACY]",
    },
    "lc_fixed_1024": {
        "chunk_size": 1024,
        "overlap": 102,             # ≈ 10 % of 1024
        "description": "LangChain TokenTextSplitter, 1024 tokens, ~10 % overlap [LEGACY]",
    },
    "lc_recursive_512": {
        "chunk_size": 512,
        "overlap": 51,              # ≈ 10 % of 512
        "description": "LangChain RecursiveCharacterTextSplitter, 512 tokens, ~10 % overlap [LEGACY]",
    },
}
