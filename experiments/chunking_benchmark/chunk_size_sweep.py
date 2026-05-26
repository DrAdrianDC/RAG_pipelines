"""
Chunk Size Sweep — metric vs chunk size curve.

Usage (from the project root)
------------------------------
    python -m experiments.chunking_benchmark.chunk_size_sweep
    python -m experiments.chunking_benchmark.chunk_size_sweep --sample 30
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

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from chunking.fixed_chunking import FixedChunker
from chunking.utils import load_jsonl, normalize_section
from evaluation.metrics import EvaluationResults, RetrievalEvaluator
from experiments.chunking_benchmark.benchmark_utils import (
    compute_adaptive_k,
    generate_benchmark_queries,
    get_chroma_collection,
    index_chunks,
    query_collection,
)
from experiments.chunking_benchmark.config import (
    DEFAULT_SAMPLE_SIZE,
    DEFAULT_SEED,
    RESULTS_PATH,
)

RESULTS_PATH.mkdir(parents=True, exist_ok=True)

SWEEP_SIZES = [64, 128, 256, 384, 512, 640, 768, 896, 1024, 1280, 1536]
OVERLAP_FRACTION = 0.10


def run_single_size(
    chunk_size: int,
    records: list[dict[str, Any]],
    queries: list[dict[str, Any]],
    reset_db: bool = False,
) -> dict[str, Any]:
    # overlap_tokens kept for result reporting only — FixedChunker uses overlap_fraction
    overlap_tokens = max(1, int(chunk_size * OVERLAP_FRACTION))
    name = f"sweep_{chunk_size}"
    chunker = FixedChunker(chunk_size=chunk_size, overlap_fraction=OVERLAP_FRACTION)
    chunker.name = name

    chunks = chunker.chunk_corpus(records)
    avg_tok = chunker.avg_tokens(chunks)
    adaptive_k = compute_adaptive_k(avg_tok)

    collection = get_chroma_collection(name, reset=reset_db)
    index_chunks(collection, chunks)

    query_fn = lambda q, k: query_collection(collection, q, k)
    evaluator = RetrievalEvaluator(query_fn, adaptive_k, normalize_section_fn=normalize_section)
    summary = evaluator.run(queries).summary()

    return {
        "chunk_size": chunk_size, "overlap_tokens": overlap_tokens,
        "n_chunks": len(chunks), "avg_tokens": round(avg_tok, 1),
        "adaptive_k": adaptive_k, **summary,
    }


def plot_sweep_curves(df: pd.DataFrame, save_path: Path) -> None:
    """Chunk size (X) vs context recall (Y) — standard RAG sweep curve."""
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(
        df["chunk_size"], df["context_recall"],
        "o-", lw=2.5, ms=8, color="#2E86AB", label="Context Recall",
    )
    best = df.loc[df["context_recall"].idxmax()]
    ax.annotate(
        f"Peak={best['context_recall']:.3f}\n@ {int(best['chunk_size'])} tok",
        xy=(best["chunk_size"], best["context_recall"]),
        xytext=(20, -28), textcoords="offset points",
        arrowprops={"arrowstyle": "->", "color": "#2E86AB"},
        fontsize=9, color="#2E86AB",
    )
    ax.set_xlabel("Chunk Size (tokens)", fontsize=12)
    ax.set_ylabel("Context Recall (filtered)", fontsize=12)
    ax.set_title(
        f"Chunk Size vs Retrieval Quality\n"
        f"(FDA Biomarker Corpus, overlap={int(OVERLAP_FRACTION * 100)}%)",
        fontsize=11,
    )
    ax.set_ylim(0, min(1.05, df["context_recall"].max() + 0.12))
    ax.grid(alpha=0.3)
    ax.set_xticks(df["chunk_size"])
    ax.set_xticklabels(df["chunk_size"], rotation=30, ha="right")
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_sweep_k_and_nchunks(df: pd.DataFrame, save_path: Path) -> None:
    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax1.plot(df["chunk_size"], df["adaptive_k"], "o-", color="#4C72B0", lw=2, label="Adaptive k")
    ax1.set_xlabel("Chunk Size (tokens)", fontsize=11)
    ax1.set_ylabel("Adaptive k", color="#4C72B0", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#4C72B0")
    ax2 = ax1.twinx()
    ax2.plot(df["chunk_size"], df["n_chunks"], "s--", color="#DD8452", lw=1.8, label="Total chunks")
    ax2.set_ylabel("Total chunks in index", color="#DD8452", fontsize=11)
    ax2.tick_params(axis="y", labelcolor="#DD8452")
    l1, n1 = ax1.get_legend_handles_labels()
    l2, n2 = ax2.get_legend_handles_labels()
    ax1.legend(l1 + l2, n1 + n2, loc="upper right")
    ax1.set_title("Adaptive k and Index Size vs Chunk Size\n(FDA Biomarker Corpus)", fontsize=11)
    ax1.set_xticks(df["chunk_size"])
    ax1.set_xticklabels(df["chunk_size"], rotation=30, ha="right")
    ax1.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_ranking_by_size(df: pd.DataFrame, save_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 6))
    cmap = plt.cm.viridis
    norm = plt.Normalize(df["chunk_size"].min(), df["chunk_size"].max())
    for i in range(len(df) - 1):
        a, b = df.iloc[i], df.iloc[i + 1]
        ax.annotate("", xy=(b["doc_hit_at_k"], b["mrr"]),
                    xytext=(a["doc_hit_at_k"], a["mrr"]),
                    arrowprops={"arrowstyle": "->",
                                "color": cmap(norm((a["chunk_size"] + b["chunk_size"]) / 2)),
                                "lw": 1.5})
    sc = ax.scatter(df["doc_hit_at_k"], df["mrr"],
                    c=df["chunk_size"], cmap=cmap, s=90, zorder=5)
    for _, row in df.iterrows():
        ax.annotate(str(int(row["chunk_size"])),
                    xy=(row["doc_hit_at_k"], row["mrr"]),
                    xytext=(5, 3), textcoords="offset points", fontsize=8)
    plt.colorbar(sc, ax=ax, label="Chunk size (tokens)")
    ax.set_xlabel("Doc Hit@k", fontsize=11)
    ax.set_ylabel("MRR", fontsize=11)
    ax.set_title("Hit Rate vs MRR Across Chunk Sizes\nArrows = increasing chunk size", fontsize=11)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Chunk size sweep")
    p.add_argument("--sample", type=int, default=DEFAULT_SAMPLE_SIZE)
    p.add_argument("--reset-db", action="store_true", default=True)
    p.add_argument("--no-reset-db", action="store_false", dest="reset_db")
    p.add_argument("--no-plots", action="store_true", default=False)
    p.add_argument("--overlap-pct", type=float, default=10.0)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    global OVERLAP_FRACTION
    OVERLAP_FRACTION = args.overlap_pct / 100.0

    print("=" * 60)
    print(f"  Chunk Size Sweep  |  sizes: {SWEEP_SIZES}")
    print("=" * 60)

    records = load_jsonl()
    queries = generate_benchmark_queries(
        records,
        sample_size=args.sample if args.sample > 0 else None,
        seed=DEFAULT_SEED,
    )
    print(f"  {len(records)} records | {len(queries)} queries\n")

    all_results: list[dict[str, Any]] = []
    for chunk_size in SWEEP_SIZES:
        t0 = time.time()
        print(f"  chunk_size={chunk_size} …", end=" ", flush=True)
        result = run_single_size(chunk_size, records, queries, reset_db=args.reset_db)
        print(f"n_chunks={result['n_chunks']:,}  k={result['adaptive_k']}  "
              f"Context Recall={result['context_recall']:.3f}  ({time.time()-t0:.0f}s)")
        all_results.append(result)

    df = pd.DataFrame(all_results)
    df.to_csv(RESULTS_PATH / "sweep_results.csv", index=False)
    with open(RESULTS_PATH / "sweep_results.json", "w") as fh:
        json.dump(all_results, fh, indent=2)

    best = df.loc[df["context_recall"].idxmax()]
    print(f"\nBest Context Recall: {best['context_recall']:.3f} @ {int(best['chunk_size'])} tokens")

    if not args.no_plots:
        print("\nGenerating sweep plots …")
        plot_sweep_curves(df, RESULTS_PATH / "chunk_size_vs_quality.png")
        plot_sweep_k_and_nchunks(df, RESULTS_PATH / "sweep_k_and_nchunks.png")
        plot_ranking_by_size(df, RESULTS_PATH / "sweep_ranking_scatter.png")

    print("\nDone.")


if __name__ == "__main__":
    main()
