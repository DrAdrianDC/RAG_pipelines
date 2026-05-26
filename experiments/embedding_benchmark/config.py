"""
Experiment-level configuration for the embedding benchmark.

Holds embedding model list and paths.  Chunking is fixed (recursive_512)
so only the embedding model varies.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent

CHROMA_BASE_PATH = PROJECT_ROOT / "experiments" / "embedding_benchmark" / "chroma_stores"
RESULTS_PATH = PROJECT_ROOT / "experiments" / "embedding_benchmark" / "results"

TARGET_CONTEXT_TOKENS = 2000
DEFAULT_SAMPLE_SIZE = 100
DEFAULT_SEED = 42

# Models to compare — all must work with SentenceTransformer backend.
# Add/remove models here; each gets its own ChromaDB subdirectory.
EMBEDDING_MODELS: list[dict[str, str]] = [
    {
        "name": "all-MiniLM-L6-v2",
        "description": "General-purpose baseline (384d, fast)",
    },
    {
        "name": "BAAI/bge-small-en-v1.5",
        "description": "Strong general retrieval model (384d)",
    },
    {
        "name": "NeuML/pubmedbert-base-embeddings",
        "description": "Biomedical PubMedBERT embeddings",
    },
]

# Fixed chunking strategy for fair comparison
FIXED_CHUNKER = "recursive_512"
