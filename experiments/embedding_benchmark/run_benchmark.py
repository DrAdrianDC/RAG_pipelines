"""
Embedding Benchmark — compare embedding models with fixed chunking.

Only the embedding model varies; chunking (recursive_512), ChromaDB,
adaptive k, and queries are identical across runs.

Usage (from the project root)
------------------------------
    python -m experiments.embedding_benchmark.run_benchmark
    python -m experiments.embedding_benchmark.run_benchmark --sample 30
    python -m experiments.embedding_benchmark.run_benchmark --models all-MiniLM-L6-v2 BAAI/bge-small-en-v1.5
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — set env vars manually or via shell

from chunking.recursive_chunking import build_recursive_512
from chunking.utils import load_jsonl, normalize_section
from evaluation.metrics import RetrievalEvaluator
from experiments.chunking_benchmark.benchmark_utils import (
    compute_adaptive_k,
    generate_benchmark_queries,
    index_chunks,
    query_collection,
)
from experiments.embedding_benchmark.config import (
    CHROMA_BASE_PATH,
    DEFAULT_SAMPLE_SIZE,
    DEFAULT_SEED,
    EMBEDDING_MODELS,
    RESULTS_PATH,
)
from vectorstores.chroma import get_chroma_collection

RESULTS_PATH.mkdir(parents=True, exist_ok=True)
CHROMA_BASE_PATH.mkdir(parents=True, exist_ok=True)

_PRIMARY_METRICS = ["doc_hit_at_k", "mrr", "ndcg_at_k", "context_recall"]
_PRIMARY_LABELS = ["Doc\nHit@k", "MRR", "nDCG@k", "Context\nRecall"]


def run_embedding_model(
    model_name: str,
    description: str,
    records: list[dict[str, Any]],
    queries: list[dict[str, Any]],
    reset_db: bool = True,
) -> dict[str, Any]:
    slug = model_name.replace("/", "_")
    print(f"\n{'─' * 60}")
    print(f"  Embedding: {model_name}")
    print(f"  {description}")
    print(f"{'─' * 60}")

    chunker = build_recursive_512()
    t0 = time.time()
    chunks = chunker.chunk_corpus(records)
    chunk_time = time.time() - t0
    adaptive_k = compute_adaptive_k(chunker.avg_tokens(chunks))

    collection = get_chroma_collection(
        name=f"recursive_512__{slug}",
        base_path=CHROMA_BASE_PATH,
        reset=reset_db,
        model_name=model_name,
    )
    t0 = time.time()
    index_chunks(collection, chunks)
    index_time = time.time() - t0

    evaluator = RetrievalEvaluator(
        query_fn=lambda q, k: query_collection(collection, q, k),
        adaptive_k=adaptive_k,
        normalize_section_fn=normalize_section,
    )
    eval_results = evaluator.run(queries, verbose=True)
    summary = eval_results.summary()
    eval_results.pretty_print(model_name)

    return {
        "embedding_model": model_name,
        "description": description,
        "chunker": chunker.name,
        "n_chunks": len(chunks),
        "avg_tokens": round(chunker.avg_tokens(chunks), 1),
        "adaptive_k": adaptive_k,
        **summary,
        "chunk_time_s": round(chunk_time, 1),
        "index_time_s": round(index_time, 1),
    }


def plot_embedding_heatmap(df: pd.DataFrame, save_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(9, max(3, len(df) * 0.8)))
    sns.heatmap(
        df[_PRIMARY_METRICS].values.astype(float),
        annot=True, fmt=".2f", cmap="RdYlGn", vmin=0, vmax=1,
        xticklabels=_PRIMARY_LABELS,
        yticklabels=df["embedding_model"],
        linewidths=0.5, ax=ax,
    )
    ax.set_title("Embedding Model Comparison\n(recursive_512 chunking · FDA Biomarker Corpus)", pad=14)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Embedding model benchmark")
    p.add_argument("--sample", type=int, default=DEFAULT_SAMPLE_SIZE)
    p.add_argument("--reset-db", action="store_true", default=True)
    p.add_argument("--no-reset-db", action="store_false", dest="reset_db")
    p.add_argument("--no-plots", action="store_true", default=False)
    p.add_argument("--models", nargs="*", default=None,
                   help="Subset of embedding model names to run")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    sample_size = args.sample if args.sample > 0 else None

    print("=" * 60)
    print("  Embedding Model Benchmark (fixed: recursive_512)")
    print("=" * 60)

    models = EMBEDDING_MODELS
    if args.models:
        models = [m for m in EMBEDDING_MODELS if m["name"] in args.models]
        if not models:
            print(f"ERROR: no matching models in {args.models}", file=sys.stderr)
            sys.exit(1)

    records = load_jsonl()
    queries = generate_benchmark_queries(records, sample_size=sample_size, seed=DEFAULT_SEED)
    print(f"\n  {len(records)} records | {len(queries)} queries | {len(models)} models")

    results: list[dict[str, Any]] = []
    for spec in models:
        results.append(run_embedding_model(
            spec["name"], spec["description"],
            records, queries, reset_db=args.reset_db,
        ))

    df = pd.DataFrame(results)
    df.to_csv(RESULTS_PATH / "embedding_benchmark_results.csv", index=False)
    with open(RESULTS_PATH / "embedding_benchmark_results.json", "w") as fh:
        json.dump(results, fh, indent=2)

    print("\n" + "=" * 60)
    print("  RESULTS SUMMARY")
    print("=" * 60)
    cols = ["embedding_model", "doc_hit_at_k", "mrr", "context_recall"]
    print(df[cols].to_string(index=False, float_format="{:.3f}".format))

    if not args.no_plots:
        plot_embedding_heatmap(df, RESULTS_PATH / "embedding_heatmap.png")

    print("\nDone.")


if __name__ == "__main__":
    main()
