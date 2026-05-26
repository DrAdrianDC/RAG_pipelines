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

Weaviate overlap convention
----------------------------
Weaviate recommends expressing overlap as a *fraction* of chunk_size
(10–20 %) rather than an absolute token count.  All strategies below
use ``overlap_fraction`` so the parameter scales automatically when
chunk_size is changed.

  overlap_fraction = 0.10  →  51 tokens for fixed_512
                           →  102 tokens for fixed_1024
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
# Chunking strategy hyperparameters
# ---------------------------------------------------------------------------
STRATEGY_CONFIGS: dict = {
    # ------------------------------------------------------------------
    # Simple strategies (Weaviate: Fixed-Size / Token Chunking)
    # ------------------------------------------------------------------
    "fixed_512": {
        "chunk_size": 512,
        "overlap_fraction": 0.10,   # 10 % ≈ 51 tokens
        "description": "Fixed-size 512 tokens, 10 % overlap, word-boundary-aware",
    },
    "fixed_1024": {
        "chunk_size": 1024,
        "overlap_fraction": 0.10,   # 10 % ≈ 102 tokens
        "description": "Fixed-size 1024 tokens, 10 % overlap, word-boundary-aware",
    },
    # ------------------------------------------------------------------
    # Simple strategies (Weaviate: Recursive Chunking)
    # ------------------------------------------------------------------
    "recursive_512": {
        "chunk_size": 512,
        "overlap_fraction": 0.10,   # 10 % ≈ 51 tokens
        "description": "Recursive character splitting, 512 tokens, 10 % token-level overlap",
    },
    # ------------------------------------------------------------------
    # Advanced strategy (Weaviate: Semantic / Context-Aware Chunking)
    # ------------------------------------------------------------------
    "semantic": {
        "similarity_threshold": 0.7,
        # Interpreted as a percentile: 0.7 → split at bottom-30th percentile
        # of pairwise similarities for THIS document (adaptive, not global).
        # Formula: percentile = (1 − threshold) × 100.
        # Lower values = more splits (smaller, focused chunks) — looser cutoff.
        # Higher values = fewer splits (larger, broader chunks) — stricter cutoff.
        "min_chunk_tokens": 50,     # Raised from 20 — FDA text is never meaningful at 20 tokens
        "max_chunk_tokens": 600,
        "description": "Semantic chunking (Weaviate 4-step), adaptive percentile threshold",
    },
    # ------------------------------------------------------------------
    # Advanced strategy (Weaviate: Document-Based Chunking)
    # ------------------------------------------------------------------
    "structure_aware": {
        "max_chunk_tokens": 1024,
        "description": "Document-based chunking on FDA 21 CFR 201.57 section headers",
    },
    # ------------------------------------------------------------------
    # LangChain cross-validation variants
    # LangChain uses absolute overlap; we keep it here for parity.
    # ------------------------------------------------------------------
    "lc_fixed_512": {
        "chunk_size": 512,
        "overlap": 51,              # ≈ 10 % of 512
        "description": "LangChain TokenTextSplitter, 512 tokens, ~10 % overlap",
    },
    "lc_fixed_1024": {
        "chunk_size": 1024,
        "overlap": 102,             # ≈ 10 % of 1024
        "description": "LangChain TokenTextSplitter, 1024 tokens, ~10 % overlap",
    },
    "lc_recursive_512": {
        "chunk_size": 512,
        "overlap": 51,              # ≈ 10 % of 512
        "description": "LangChain RecursiveCharacterTextSplitter, 512 tokens, ~10 % overlap",
    },
}
