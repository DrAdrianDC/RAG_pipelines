"""
Experiment-level configuration for the chunking benchmark.

Scope
-----
Everything here is specific to *this experiment*.  Library-level settings
(chunking strategy params, embedding model name) live in chunking/config.py.

Separation principle: if you delete this entire experiments/ folder, the
chunking/ library should still be importable and usable independently.
"""

from pathlib import Path

# Project root is two levels up from this file.
PROJECT_ROOT = Path(__file__).parent.parent.parent

# ---------------------------------------------------------------------------
# Paths owned by this experiment
# ---------------------------------------------------------------------------
CHROMA_BASE_PATH = PROJECT_ROOT / "experiments" / "chunking_benchmark" / "chroma_stores"
RESULTS_PATH     = PROJECT_ROOT / "experiments" / "chunking_benchmark" / "results"

# ---------------------------------------------------------------------------
# Retrieval budget
# ---------------------------------------------------------------------------
# Every strategy retrieves approx. this many tokens of context per query.
# k = round(TARGET_CONTEXT_TOKENS / avg_tokens_per_chunk)
TARGET_CONTEXT_TOKENS = 2000

# ---------------------------------------------------------------------------
# Benchmark sampling defaults
# ---------------------------------------------------------------------------
DEFAULT_SAMPLE_SIZE = 100   # number of queries drawn from the 597-record corpus
DEFAULT_SEED        = 42    # random seed for reproducible sampling
