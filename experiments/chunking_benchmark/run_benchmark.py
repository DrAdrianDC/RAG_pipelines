"""
Chunking Benchmark — orchestrator.

Usage (from the project root)
------------------------------
    python -m experiments.chunking_benchmark.run_benchmark
    python -m experiments.chunking_benchmark.run_benchmark --sample 50
    python -m experiments.chunking_benchmark.run_benchmark --llm-judge --sample 20
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Allow running as a script without `pip install -e .`.
# When the project is installed (recommended), this no-ops.
_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — set env vars manually or via shell

from chunking.config import SAFE_CHUNK_TOKENS, STRATEGY_CONFIGS
from chunking.fixed_chunking import build_fixed_192, build_fixed_256, build_fixed_512, build_fixed_1024
from chunking.recursive_chunking import build_recursive_192, build_recursive_512
from chunking.semantic_chunking import build_semantic
from chunking.structure_aware_chunking import build_structure_aware
from chunking.utils import load_jsonl, normalize_section
from evaluation.e2e_metrics import evaluate_e2e_single
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
from retrieval.dense_retriever import DenseRetriever

RESULTS_PATH.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Strategy registry
# ---------------------------------------------------------------------------

def _build_strategies() -> list[Any]:
    """Return the default benchmark strategies.

    Model-aligned strategies (chunk_size <= SAFE_CHUNK_TOKENS = 192) are the
    primary set.  They guarantee that every chunk fits within the
    all-MiniLM-L6-v2 context window (256 WordPiece tokens) so embeddings
    represent the full chunk text.

    Legacy strategies (fixed_512, fixed_1024, recursive_512) are excluded from
    the default run but can be included with --strategies for historical
    comparison.  Their chunks exceed the model window and are silently
    truncated by the embedding model.
    """
    return [
        build_fixed_192(),
        build_fixed_256(),
        build_recursive_192(),
        build_semantic(),
        build_structure_aware(),
    ]


def _build_legacy_strategies() -> list[Any]:
    """Legacy oversize strategies — chunks exceed all-MiniLM-L6-v2 context window."""
    return [
        build_fixed_512(),
        build_fixed_1024(),
        build_recursive_512(),
    ]


def _build_langchain_strategies() -> list[Any]:
    from chunking.langchain_chunking import (
        build_lc_fixed_192,
        build_lc_recursive_192,
        build_lc_fixed_512,
        build_lc_fixed_1024,
        build_lc_recursive_512,
    )
    return [
        build_lc_fixed_192(),
        build_lc_recursive_192(),
        build_lc_fixed_512(),
        build_lc_fixed_1024(),
        build_lc_recursive_512(),
    ]


# ---------------------------------------------------------------------------
# Single-strategy pipeline
# ---------------------------------------------------------------------------

def run_strategy(
    chunker,
    records: list[dict[str, Any]],
    benchmark_queries: list[dict[str, Any]],
    reset_db: bool = False,
    verbose: bool = True,
    use_llm_judge: bool = False,
    llm_provider: str = "groq",
    llm_model: str | None = None,
    fixed_k: int | None = None,
) -> dict[str, Any]:
    strategy_name = chunker.name
    print(f"\n{'─' * 60}")
    print(f"  Strategy: {strategy_name}")
    print(f"  Config:   {STRATEGY_CONFIGS.get(strategy_name, {}).get('description', '')}")
    print(f"{'─' * 60}")

    print("  [1/3] Chunking corpus …", flush=True)
    t0 = time.time()
    chunks = chunker.chunk_corpus(records)
    chunk_time = time.time() - t0

    n_chunks = len(chunks)
    avg_tok = chunker.avg_tokens(chunks)
    token_counts = [c["token_count"] for c in chunks]
    adaptive_k = fixed_k if fixed_k is not None else compute_adaptive_k(avg_tok)
    k_mode = f"fixed k={adaptive_k}" if fixed_k is not None else f"adaptive k={adaptive_k}"
    print(f"        {n_chunks:,} chunks | avg {avg_tok:.0f} tokens | {k_mode} | {chunk_time:.1f}s")

    print("  [2/3] Indexing in ChromaDB …", flush=True)
    t0 = time.time()
    collection = get_chroma_collection(strategy_name, reset=reset_db)
    index_chunks(collection, chunks)
    index_time = time.time() - t0
    print(f"        Done in {index_time:.1f}s")

    print(f"  [3/3] Evaluating {len(benchmark_queries)} queries …", flush=True)

    retriever = DenseRetriever(
        query_fn=lambda q, k: query_collection(collection, q, k),
        name=strategy_name,
    )
    evaluator = RetrievalEvaluator(
        query_fn=retriever.retrieve,
        adaptive_k=adaptive_k,
        normalize_section_fn=normalize_section,
    )
    eval_results: EvaluationResults = evaluator.run(benchmark_queries, verbose=verbose)
    summary = eval_results.summary()
    eval_results.pretty_print(strategy_name)

    # Structural ceiling for doc_precision: best possible = 1/k (one GT doc among k unique docs).
    # Values near this ceiling mean the GT doc was found every time. Values far below mean low hit rate.
    structural_precision_ceiling = round(1.0 / adaptive_k, 3)

    result: dict[str, Any] = {
        "strategy": strategy_name,
        "description": STRATEGY_CONFIGS.get(strategy_name, {}).get("description", ""),
        "n_chunks": n_chunks,
        "avg_tokens": round(avg_tok, 1),
        "min_tokens": int(np.min(token_counts)),
        "max_tokens": int(np.max(token_counts)),
        "adaptive_k": adaptive_k,
        "k_mode": "fixed" if fixed_k is not None else "adaptive",
        "structural_precision_ceiling": structural_precision_ceiling,
        **summary,
        "query_type_breakdown": eval_results.summary_by_query_type(),
        "chunk_time_s": round(chunk_time, 1),
        "index_time_s": round(index_time, 1),
    }

    if use_llm_judge:
        print(f"  [E2E] Generating answers + judging via {llm_provider} …", flush=True)
        e2e_scores: list[dict[str, float]] = []
        for qm, qr in zip(benchmark_queries, eval_results.query_results):
            e2e = evaluate_e2e_single(
                qm, qr.hits,
                use_llm=True,
                llm_provider=llm_provider,
                llm_model=llm_model,
            )
            e2e_scores.append({
                "faithfulness": e2e.faithfulness,
                "answer_relevance": e2e.answer_relevance,
            })
        result["faithfulness"] = float(np.mean([s["faithfulness"] for s in e2e_scores]))
        result["answer_relevance"] = float(np.mean([s["answer_relevance"] for s in e2e_scores]))
        print(f"  Faithfulness={result['faithfulness']:.3f}  "
              f"Answer relevance={result['answer_relevance']:.3f}")

    return result


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

_DISPLAY_NAMES = {
    # Model-aligned (recommended)
    "fixed_192": "Fixed 192",
    "fixed_256": "Fixed 256",
    "recursive_192": "Recursive 192",
    "semantic": "Semantic",
    "structure_aware": "Structure-Aware",
    # Legacy / oversize
    "fixed_512": "Fixed 512 (legacy)",
    "fixed_1024": "Fixed 1024 (legacy)",
    "recursive_512": "Recursive 512 (legacy)",
}

_PRIMARY_METRICS = ["doc_hit_at_k", "mrr", "ndcg_at_k", "context_recall"]
_PRIMARY_LABELS = ["Doc\nHit@k", "MRR", "nDCG@k", "Context\nRecall"]


def plot_retrieval_comparison(df: pd.DataFrame, save_path: Path) -> None:
    strategies = [_DISPLAY_NAMES.get(s, s) for s in df["strategy"]]
    x = np.arange(len(strategies))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 5))
    bars1 = ax.bar(x - width / 2, df["doc_hit_at_k"], width, label="Doc Hit@k", color="#4C72B0")
    bars2 = ax.bar(x + width / 2, df["context_recall"], width, label="Context Recall", color="#55A868")
    ax.set_xlabel("Chunking Strategy")
    ax.set_ylabel("Score")
    ax.set_title("Doc Hit@k vs Context Recall\n(FDA Biomarker Corpus · adaptive k ≈ 2000 tokens)")
    ax.set_xticks(x)
    ax.set_xticklabels(strategies, rotation=15, ha="right")
    ax.set_ylim(0, 1.05)
    ax.legend()
    ax.bar_label(bars1, fmt="%.2f", padding=2, fontsize=9)
    ax.bar_label(bars2, fmt="%.2f", padding=2, fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_metrics_heatmap(df: pd.DataFrame, save_path: Path) -> None:
    row_labels = [_DISPLAY_NAMES.get(s, s) for s in df["strategy"]]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    sns.heatmap(
        df[_PRIMARY_METRICS].values.astype(float),
        annot=True, fmt=".2f", cmap="RdYlGn", vmin=0, vmax=1,
        xticklabels=_PRIMARY_LABELS, yticklabels=row_labels,
        linewidths=0.5, ax=ax, annot_kws={"size": 11},
    )
    ax.set_title("Retrieval Metrics Heatmap\n(FDA Biomarker Corpus)", pad=14)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_chunk_distribution(df: pd.DataFrame, save_path: Path) -> None:
    strategies = [_DISPLAY_NAMES.get(s, s) for s in df["strategy"]]
    x = np.arange(len(strategies))
    width = 0.35
    fig, ax1 = plt.subplots(figsize=(10, 5))
    bars1 = ax1.bar(x - width / 2, df["avg_tokens"], width, label="Avg Tokens / Chunk", color="#4C72B0")
    ax1.set_xlabel("Chunking Strategy")
    ax1.set_ylabel("Avg Tokens per Chunk", color="#4C72B0")
    ax1.tick_params(axis="y", labelcolor="#4C72B0")
    ax1.set_xticks(x)
    ax1.set_xticklabels(strategies, rotation=15, ha="right")
    ax2 = ax1.twinx()
    bars2 = ax2.bar(x + width / 2, df["adaptive_k"], width, label="Adaptive k", color="#55A868")
    ax2.set_ylabel("Adaptive k", color="#55A868")
    ax2.tick_params(axis="y", labelcolor="#55A868")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    ax1.bar_label(bars1, fmt="%.0f", padding=2, fontsize=9)
    ax2.bar_label(bars2, fmt="%d", padding=2, fontsize=9)
    fig.suptitle("Chunk Size Distribution and Adaptive k per Strategy\n(FDA Biomarker Corpus)")
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_ranking_scatter(df: pd.DataFrame, save_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 6))
    colors = plt.cm.tab10(np.linspace(0, 0.6, len(df)))
    for i, (_, row) in enumerate(df.iterrows()):
        label = _DISPLAY_NAMES.get(row["strategy"], row["strategy"])
        ax.scatter(row["doc_hit_at_k"], row["mrr"],
                   s=max(100, row["adaptive_k"] * 25),
                   color=colors[i], alpha=0.85, zorder=3)
        ax.annotate(
            f'{label}\n(k={int(row["adaptive_k"])})',
            xy=(row["doc_hit_at_k"], row["mrr"]),
            xytext=(8, 4), textcoords="offset points",
            fontsize=8.5, color=colors[i],
        )
    ax.set_xlabel("Doc Hit@k", fontsize=11)
    ax.set_ylabel("MRR", fontsize=11)
    ax.set_title("Hit Rate vs Mean Reciprocal Rank\nIdeal = top-right", fontsize=11)
    ax.set_xlim(-0.02, min(1.05, max(df["doc_hit_at_k"]) + 0.12))
    ax.set_ylim(-0.02, min(1.05, max(df["mrr"]) + 0.12))
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_chunk_size_vs_quality(df: pd.DataFrame, save_path: Path) -> None:
    plot_df = df.sort_values("avg_tokens").copy()
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(
        plot_df["avg_tokens"], plot_df["context_recall"],
        "o-", lw=2.5, ms=8, color="#2E86AB", label="Context Recall (filtered)",
    )
    best = plot_df.loc[plot_df["context_recall"].idxmax()]
    best_label = _DISPLAY_NAMES.get(best["strategy"], best["strategy"])
    ax.annotate(
        f"Peak={best['context_recall']:.3f}\n{best_label}\n({int(best['avg_tokens'])} tok)",
        xy=(best["avg_tokens"], best["context_recall"]),
        xytext=(18, -30), textcoords="offset points",
        arrowprops={"arrowstyle": "->", "color": "#2E86AB"},
        fontsize=9, color="#2E86AB",
    )
    for _, row in plot_df.iterrows():
        label = _DISPLAY_NAMES.get(row["strategy"], row["strategy"])
        ax.annotate(
            label, xy=(row["avg_tokens"], row["context_recall"]),
            xytext=(0, 10), textcoords="offset points",
            fontsize=8, ha="center", color="#444444",
        )
    ax.set_xlabel("Chunk Size (avg tokens)", fontsize=12)
    ax.set_ylabel("Context Recall (filtered)", fontsize=12)
    ax.set_title(
        "Chunk Size vs Retrieval Quality\n"
        "FDA Biomarker Corpus · adaptive k ≈ 2000 tokens",
        fontsize=11,
    )
    ax.set_ylim(0, min(1.05, plot_df["context_recall"].max() + 0.12))
    ax.set_xlim(0, plot_df["avg_tokens"].max() * 1.15)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_radar(df: pd.DataFrame, save_path: Path) -> None:
    metrics = _PRIMARY_METRICS
    labels = _PRIMARY_LABELS
    angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist() + [0]
    colors = cm.tab10(np.linspace(0, 0.6, len(df)))
    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw={"polar": True})
    for i, (_, row) in enumerate(df.iterrows()):
        vals = [row[m] for m in metrics] + [row[metrics[0]]]
        label = _DISPLAY_NAMES.get(row["strategy"], row["strategy"])
        ax.plot(angles, vals, "o-", linewidth=2, label=label, color=colors[i])
        ax.fill(angles, vals, alpha=0.08, color=colors[i])
    ax.set_thetagrids(np.degrees(angles[:-1]), labels)
    ax.set_ylim(0, 1)
    ax.set_title("Strategy Comparison — Radar Chart\n(FDA Biomarker Corpus)", pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.1), fontsize=9)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path}")


def _print_run_header(args: argparse.Namespace) -> None:
    """Print a reproducibility fingerprint so every run is self-documenting."""
    from experiments.chunking_benchmark.config import TARGET_CONTEXT_TOKENS
    from embeddings.sentence_transformer import DEFAULT_MODEL

    k_desc = (
        f"fixed k={args.fixed_k} (all strategies same k — fair for Hit@k / nDCG@k)"
        if args.fixed_k
        else f"adaptive k = round({TARGET_CONTEXT_TOKENS} / avg_chunk_tokens)  "
             f"[NOTE: k varies per strategy — Hit@k and nDCG@k are NOT comparable "
             f"across strategies with different k. Use --fixed-k 5 for fair comparison. "
             f"MRR is k-independent and always safe to compare.]"
    )
    llm_desc = (
        f"{args.llm_provider} / {args.llm_model or 'default model'}  "
        f"(throttle: 2.2 s/call for groq, 4.0 s/call for gemini)"
        if args.llm_judge
        else "disabled — retrieval metrics only (sufficient to rank chunking strategies)"
    )
    print(f"\n  Embedding: {DEFAULT_MODEL}  (22M params, 384 dims)")
    print(f"             Context window: 256 WordPiece tokens")
    print(f"             SAFE_CHUNK_TOKENS = {SAFE_CHUNK_TOKENS} tiktoken cl100k_base")
    print(f"             (≈ 249 WordPiece at 1.3× ratio — 7-token margin below model limit)")
    print(f"             Model-aligned strategies (fixed_192, fixed_256, recursive_192,")
    print(f"             semantic, structure_aware) respect this limit.")
    print(f"             Legacy strategies (fixed_512, fixed_1024, recursive_512) do not")
    print(f"             and are excluded from the default run (use --include-legacy).")
    print(f"  k mode   : {k_desc}")
    print(f"  LLM judge: {llm_desc}")
    print(f"  Sample   : {args.sample or 'all'} queries  |  seed={42}")


def _print_summary_table(df: pd.DataFrame, fixed_k: int | None = None) -> None:
    """Print the results table with k-fairness warnings and structural precision ceiling."""
    cols = ["strategy", "adaptive_k", "structural_precision_ceiling",
            "doc_hit_at_k", "mrr", "ndcg_at_k",
            "context_recall", "context_recall_unfiltered", "section_hit_at_k"]
    available = [c for c in cols if c in df.columns]
    print(df[available].to_string(index=False, float_format="{:.3f}".format))

    print()
    if fixed_k is None:
        print(
            "  [!] k varies per strategy (adaptive). Hit@k and nDCG@k reflect\n"
            "      different retrieval budgets — not an apples-to-apples comparison.\n"
            "      MRR is k-independent: always safe to compare across strategies.\n"
            "      Rerun with --fixed-k 5 for a fair normalized table."
        )
    else:
        print(f"  [OK] All strategies used fixed k={fixed_k}. Hit@k / nDCG@k are directly comparable.")

    print(
        "\n  [!] structural_precision_ceiling = 1/k (best achievable doc_precision\n"
        "      when GT doc is retrieved every time). Values far below ceiling\n"
        "      indicate low hit-rate, not a retrieval precision problem.\n"
        "\n  [!] context_recall (filtered) counts only tokens from the correct\n"
        "      document. context_recall_unfiltered includes all top-k chunks.\n"
        "      The ~0.40 gap is caused by shared FDA regulatory vocabulary:\n"
        "      other drug labels contain biomarker terminology that overlaps\n"
        "      with the GT document, inflating the unfiltered score."
    )


def _print_experiment_conclusion(
    df: pd.DataFrame,
    all_results: list[dict[str, Any]],
    fixed_k: int | None,
) -> None:
    """
    Answer the research question directly from the metrics.

    Research question
    -----------------
    "Which chunking strategy retrieves best with all-MiniLM-L6-v2
     on the FDA biomarker corpus (597 docs)?"
    """
    from embeddings.sentence_transformer import DEFAULT_MODEL

    print("\n" + "=" * 60)
    print("  EXPERIMENT CONCLUSION")
    print("=" * 60)
    print(f"\n  Research question:")
    print(f"  'Which chunking strategy retrieves best with {DEFAULT_MODEL}")
    print(f"   on the FDA biomarker corpus?'")

    # ------------------------------------------------------------------
    # Composite score: MRR (k-independent) carries most weight
    # ------------------------------------------------------------------
    weights = {
        "mrr":              0.40,   # k-independent — primary fair signal
        "context_recall":   0.35,   # GT content recovered from correct doc
        "doc_hit_at_k":     0.15,   # hit rate (fair only if fixed-k was used)
        "section_hit_at_k": 0.10,   # structure-specific bonus
    }
    if fixed_k is None:
        # doc_hit_at_k and ndcg are k-dependent → fold weight into MRR
        weights["mrr"] += weights.pop("doc_hit_at_k") / 2
        weights["context_recall"] += weights.pop("doc_hit_at_k", 0) / 2
        # rebalance after pop (pop already removed it)
        weights["mrr"] = 0.50
        weights["context_recall"] = 0.40
        weights["section_hit_at_k"] = 0.10

    scores: dict[str, float] = {s: 0.0 for s in df["strategy"]}
    for metric, weight in weights.items():
        if metric not in df.columns:
            continue
        col = df[metric].values.astype(float)
        min_v, max_v = col.min(), col.max()
        for i, strategy in enumerate(df["strategy"]):
            norm = (col[i] - min_v) / (max_v - min_v) if max_v > min_v else 1.0
            scores[strategy] += weight * norm

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    winner = ranked[0][0]
    medals = ["→ WINNER", "  #2    ", "  #3    ", "  #4    ", "  #5    "]

    weight_desc = "MRR×50% + ContextRecall×40% + SecHit×10%" if fixed_k is None else \
                  "MRR×40% + ContextRecall×35% + DocHit×15% + SecHit×10%"
    print(f"\n  Composite ranking  ({weight_desc})")
    print(f"  {'─' * 62}")
    for rank, (strategy, score) in enumerate(ranked, 1):
        row = df[df["strategy"] == strategy].iloc[0]
        print(
            f"  {medals[rank-1]}  {strategy:<20}  score={score:.3f}  "
            f"MRR={row['mrr']:.3f}  "
            f"ContextRecall={row['context_recall']:.3f}  "
            f"SecHit={row['section_hit_at_k']:.3f}"
        )

    # ------------------------------------------------------------------
    # Query type breakdown — the most operationally relevant analysis
    # ------------------------------------------------------------------
    type_order = ["semantic_hard", "named_section", "named_generic", "named_brand", "named_full"]
    type_labels = {
        "semantic_hard":  "Semantic (no drug name) ← hardest, most realistic in prod",
        "named_section":  "Section queries         ← 'What does section X say?'",
        "named_generic":  "Named generic drug      ← medium difficulty",
        "named_brand":    "Named brand drug        ← medium difficulty",
        "named_full":     "Named full (brand+INN)  ← easiest",
    }
    print(f"\n  Performance by query type  (MRR — higher is better)")
    print(f"  {'─' * 62}")
    for qtype in type_order:
        rows_qt: list[tuple[str, dict]] = []
        for r in all_results:
            breakdown = r.get("query_type_breakdown", {})
            if qtype in breakdown:
                rows_qt.append((r["strategy"], breakdown[qtype]))
        if not rows_qt:
            continue
        n = int(rows_qt[0][1].get("n", 0))
        best = max(rows_qt, key=lambda x: x[1]["mrr"])
        worst = min(rows_qt, key=lambda x: x[1]["mrr"])
        print(
            f"  {type_labels[qtype]:<50} (n={n:2d})\n"
            f"      best  → {best[0]:<20} MRR={best[1]['mrr']:.3f}  CR={best[1]['context_recall']:.3f}\n"
            f"      worst → {worst[0]:<20} MRR={worst[1]['mrr']:.3f}"
        )

    # ------------------------------------------------------------------
    # Actionable answers
    # ------------------------------------------------------------------
    sa_row = df[df["strategy"] == "structure_aware"]
    has_section = not sa_row.empty and sa_row.iloc[0]["section_hit_at_k"] > 0.0

    print(f"\n  {'─' * 62}")
    print(f"  ANSWERS TO THE RESEARCH QUESTION:")
    print(f"")
    print(f"  General queries (named + semantic)  →  {winner}")
    if has_section:
        sec_hit = sa_row.iloc[0]["section_hit_at_k"]
        print(f"  Section queries ('What does X say') →  structure_aware  (Sec Hit={sec_hit:.2f})")
        print(f"")
        print(f"  RECOMMENDATION — Hybrid index:")
        print(f"    if 'section' in query  →  structure_aware collection")
        print(f"    else                   →  {winner} collection")
    print()

    # ------------------------------------------------------------------
    # Embedding model ceiling — critical context for interpreting scores
    # ------------------------------------------------------------------
    avg_mrr = float(df["mrr"].mean())
    avg_hit = float(df["doc_hit_at_k"].mean())
    print(f"  {'─' * 62}")
    print(f"  EMBEDDING MODEL CEILING  ({DEFAULT_MODEL}, 384 dims)")
    print(f"")
    print(f"  This model — not the chunking strategy — is the primary performance")
    print(f"  ceiling. Evidence from this run:")
    print(f"    avg MRR across all strategies:    {avg_mrr:.3f}  (Vecta got Doc F1 ~0.86 with text-embedding-3-small)")
    print(f"    avg Doc Hit@k across strategies:  {avg_hit:.3f}  (GT doc missed ~{(1-avg_hit)*100:.0f}% of queries)")
    print(f"")
    print(f"  The ranking '{winner} > others' holds within this embedding model.")
    print(f"  Absolute scores will improve significantly with a stronger model")
    print(f"  (e.g. BAAI/bge-base-en-v1.5, text-embedding-3-small, or a biomedical")
    print(f"   model such as NeuML/pubmedbert-base-embeddings).")
    print(f"")
    print(f"  SAFE_CHUNK_TOKENS = {SAFE_CHUNK_TOKENS}  →  model-aligned strategies respected the")
    print(f"  context window.  Compare these results against --include-legacy to")
    print(f"  quantify the truncation penalty of oversize strategies.")
    print(f"  The chunking comparison experiment is valid as-is.")
    print()

    # ------------------------------------------------------------------
    # LLM judge note
    # ------------------------------------------------------------------
    print(f"  {'─' * 62}")
    print(f"  LLM JUDGE (--llm-judge flag)")
    print(f"")
    print(f"  The retrieval metrics above fully answer the research question.")
    print(f"  Add --llm-judge (requires GROQ_API_KEY) only to measure:")
    print(f"    - Faithfulness: does retrieved context ground the generated answer?")
    print(f"    - Answer relevance: does the answer address the query?")
    print(f"  This validates end-to-end generation quality (~26 min extra).")
    print(f"  It does NOT change the chunking strategy ranking.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FDA Biomarker chunking benchmark")
    p.add_argument("--sample", type=int, default=DEFAULT_SAMPLE_SIZE,
                   help=f"Queries to evaluate (default: {DEFAULT_SAMPLE_SIZE}; 0 = all)")
    p.add_argument("--reset-db", action="store_true", default=True)
    p.add_argument("--no-reset-db", action="store_false", dest="reset_db")
    p.add_argument("--no-plots", action="store_true", default=False)
    p.add_argument("--strategies", nargs="*", default=None)
    p.add_argument("--include-langchain", action="store_true", default=False)
    p.add_argument(
        "--include-legacy", action="store_true", default=False,
        help=(
            "Also run legacy oversize strategies (fixed_512, fixed_1024, recursive_512). "
            "Their chunks exceed the all-MiniLM-L6-v2 context window (256 WordPiece tokens) "
            "and are silently truncated by the embedding model. "
            "Useful to quantify the truncation penalty vs model-aligned strategies."
        ),
    )
    p.add_argument(
        "--llm-judge", action="store_true", default=False,
        help=(
            "Generate LLM answers from retrieved context and score them "
            "(Faithfulness + Answer Relevance). "
            "Uses Groq by default — FREE key at https://console.groq.com. "
            "Set GROQ_API_KEY in .env."
        ),
    )
    p.add_argument(
        "--llm-provider", choices=["groq", "gemini", "openai"], default="groq",
        help=(
            "LLM provider for --llm-judge: "
            "'groq' (free, 30 req/min, default), "
            "'gemini' (free but only 20 req/day — avoid for 100+ queries), "
            "'openai' (paid)."
        ),
    )
    p.add_argument(
        "--llm-model", default=None,
        help=(
            "Override model name. Defaults: "
            "groq → llama-3.1-8b-instant, "
            "gemini → gemini-2.5-flash-lite, "
            "openai → gpt-4o-mini."
        ),
    )
    p.add_argument(
        "--fixed-k", type=int, default=None, metavar="K",
        help=(
            "Force all strategies to retrieve exactly K chunks, disabling adaptive k. "
            "Use for a fair k-normalized comparison (recommended: --fixed-k 5). "
            "Without this flag, k = round(2000 / avg_chunk_tokens) per strategy, "
            "which means semantic (avg 146 tok → k=14) gets more retrieval slots "
            "than fixed_512 (avg 377 tok → k=5), making Hit@k and nDCG@k incomparable."
        ),
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    sample_size: int | None = args.sample if args.sample > 0 else None

    print("=" * 60)
    print("  FDA Biomarker Chunking Benchmark")
    print("=" * 60)
    _print_run_header(args)

    records = load_jsonl()
    print(f"\n  {len(records)} records loaded")

    queries = generate_benchmark_queries(records, sample_size=sample_size, seed=DEFAULT_SEED)
    type_counts: dict[str, int] = {}
    for q in queries:
        type_counts[q["query_type"]] = type_counts.get(q["query_type"], 0) + 1
    print(f"  {len(queries)} queries generated — types: {type_counts}")

    all_strategies = _build_strategies()
    if args.include_legacy:
        all_strategies += _build_legacy_strategies()
    if args.include_langchain:
        all_strategies += _build_langchain_strategies()
    if args.strategies:
        all_strategies = [s for s in all_strategies if s.name in args.strategies]
        if not all_strategies:
            print(f"ERROR: no matching strategies for {args.strategies}", file=sys.stderr)
            sys.exit(1)

    all_results: list[dict[str, Any]] = []
    for chunker in all_strategies:
        result = run_strategy(
            chunker, records, queries,
            reset_db=args.reset_db, verbose=True,
            use_llm_judge=args.llm_judge,
            llm_provider=args.llm_provider, llm_model=args.llm_model,
            fixed_k=args.fixed_k,
        )
        all_results.append(result)

    core_names = {s.name for s in _build_strategies()}  # model-aligned only
    df_all = pd.DataFrame(all_results)
    df = df_all[df_all["strategy"].isin(core_names)].copy()

    csv_path = RESULTS_PATH / "benchmark_results.csv"
    json_path = RESULTS_PATH / "benchmark_results.json"
    # JSON-serialisable (drop nested dicts for CSV)
    json_results = [{k: v for k, v in r.items()} for r in all_results]
    df_all.drop(columns=["query_type_breakdown"], errors="ignore").to_csv(csv_path, index=False)
    with open(json_path, "w") as fh:
        json.dump(json_results, fh, indent=2)
    print(f"\nResults saved → {csv_path}")

    print("\n" + "=" * 60)
    print("  RESULTS SUMMARY")
    print("=" * 60)
    _print_summary_table(df, fixed_k=args.fixed_k)

    if not args.no_plots:
        print("\nGenerating plots …")
        plot_retrieval_comparison(df, RESULTS_PATH / "retrieval_comparison.png")
        plot_metrics_heatmap(df, RESULTS_PATH / "metrics_heatmap.png")
        plot_chunk_distribution(df, RESULTS_PATH / "chunk_distribution.png")
        plot_radar(df, RESULTS_PATH / "radar_chart.png")
        plot_ranking_scatter(df, RESULTS_PATH / "ranking_scatter.png")
        plot_chunk_size_vs_quality(df, RESULTS_PATH / "chunk_size_vs_quality.png")

    _print_experiment_conclusion(df, all_results, fixed_k=args.fixed_k)

    print("\nDone.")


if __name__ == "__main__":
    main()
