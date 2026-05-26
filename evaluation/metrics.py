"""
Retrieval evaluation metrics — Hit@k, MRR, nDCG, Context Recall.

Design principle
----------------
Pure module: no ChromaDB, no embeddings, no file I/O.

Injected query function::

    query_fn(query_text: str, k: int) -> list[dict]

Each hit dict must include at minimum:
    - ``doc_id``   : str
    - ``section``  : str
    - ``content``  : str  (required for context-recall metrics)

Metric glossary (FDA biomarker RAG)
------------------------------------
doc_hit_at_k     : fraction of queries where the correct drug+biomarker
                   record appears in the top-k retrieved chunks.
mrr              : mean reciprocal rank of the first correct document.
ndcg_at_k        : ranking quality with binary relevance (correct doc = 1).
context_recall   : token recall of GT document text in retrieved chunks
                   **from the correct document only** (filtered).
context_recall_unfiltered : same overlap but includes all top-k chunks
                   (inflated by shared regulatory vocabulary — diagnostic only).
doc_precision    : |correct docs in hits| / k — structurally low when k > 1.
doc_f1           : harmonic mean of doc_precision and doc_hit_at_k (legacy).
section_hit_at_k : fraction of queries where at least one correct FDA
                   labeling section appears in retrieved chunks.
"""

from __future__ import annotations

import math
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import tiktoken

_TOKENIZER = tiktoken.get_encoding("cl100k_base")


# ---------------------------------------------------------------------------
# Low-level metric primitives
# ---------------------------------------------------------------------------

def precision(retrieved: set[str], ground_truth: set[str]) -> float:
    if not retrieved:
        return 0.0
    return len(retrieved & ground_truth) / len(retrieved)


def recall(retrieved: set[str], ground_truth: set[str]) -> float:
    if not ground_truth:
        return 1.0
    return len(retrieved & ground_truth) / len(ground_truth)


def f1(p: float, r: float) -> float:
    denom = p + r
    return 0.0 if denom == 0 else 2 * p * r / denom


def reciprocal_rank(hits: list[dict[str, Any]], gt_doc_id: str) -> float:
    """Reciprocal rank of the first hit from the ground-truth document."""
    for i, hit in enumerate(hits):
        if hit.get("doc_id") == gt_doc_id:
            return 1.0 / (i + 1)
    return 0.0


def ndcg_at_k(hits: list[dict[str, Any]], gt_doc_id: str, k: int) -> float:
    """nDCG@k with binary relevance (correct parent document = 1)."""
    if k <= 0:
        return 0.0
    dcg = 0.0
    for i, hit in enumerate(hits[:k]):
        rel = 1.0 if hit.get("doc_id") == gt_doc_id else 0.0
        dcg += rel / math.log2(i + 2)
    idcg = 1.0 / math.log2(2)  # single relevant item at rank 1
    return dcg / idcg if idcg > 0 else 0.0


def _token_recall(hits: list[dict[str, Any]], gt_content: str) -> float:
    if not gt_content.strip():
        return 1.0
    gt_tokens = _TOKENIZER.encode(gt_content)
    if not gt_tokens:
        return 1.0

    retrieved_text = " ".join(h.get("content", "") for h in hits if h.get("content"))
    if not retrieved_text.strip():
        return 0.0

    gt_counter = Counter(gt_tokens)
    ret_counter = Counter(_TOKENIZER.encode(retrieved_text))
    overlap = sum((gt_counter & ret_counter).values())
    return overlap / len(gt_tokens)


def context_recall(
    hits: list[dict[str, Any]],
    gt_content: str,
    gt_doc_id: str | None = None,
) -> float:
    """
    Token-level recall of GT document in retrieved context.

    When *gt_doc_id* is provided, only chunks from the correct document
    are counted (recommended for FDA RAG evaluation).
    """
    if gt_doc_id is not None:
        hits = [h for h in hits if h.get("doc_id") == gt_doc_id]
    return _token_recall(hits, gt_content)


def context_recall_unfiltered(hits: list[dict[str, Any]], gt_content: str) -> float:
    """Token recall over all top-k chunks (diagnostic — often inflated)."""
    return _token_recall(hits, gt_content)


def compute_metrics(
    retrieved: set[str],
    ground_truth: set[str],
) -> dict[str, float]:
    """Return precision, recall, and F1 for one query."""
    p = precision(retrieved, ground_truth)
    r = recall(retrieved, ground_truth)
    return {"precision": p, "recall": r, "f1": f1(p, r)}


# ---------------------------------------------------------------------------
# Per-query result container
# ---------------------------------------------------------------------------

@dataclass
class QueryResult:
    query: str
    gt_doc_id: str
    gt_sections: list[str]
    query_type: str = "unknown"

    hits: list[dict[str, Any]] = field(default_factory=list)
    retrieved_doc_ids: set[str] = field(default_factory=set)
    retrieved_sections: set[str] = field(default_factory=set)

    doc_metrics: dict[str, float] = field(default_factory=dict)
    section_metrics: dict[str, float] = field(default_factory=dict)
    mrr: float = 0.0
    ndcg_at_k: float = 0.0
    context_recall: float = 0.0
    context_recall_unfiltered: float = 0.0
    section_keyword_coverage: float = 0.0


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

class RetrievalEvaluator:
    """
    Run retrieval queries against any backend and compute metrics.

    Parameters
    ----------
    query_fn :
        ``(query_text, k) -> list[dict]`` with doc_id, section, content.
    adaptive_k :
        Number of chunks retrieved per query.
    normalize_section_fn :
        Optional section normaliser (e.g. ``chunking.utils.normalize_section``).
    """

    def __init__(
        self,
        query_fn: Callable[[str, int], list[dict[str, Any]]],
        adaptive_k: int,
        normalize_section_fn: Callable[[str], str] | None = None,
    ) -> None:
        self.query_fn = query_fn
        self.adaptive_k = adaptive_k
        self._norm = normalize_section_fn or (lambda x: x)

    def evaluate_single(self, query_meta: dict[str, Any]) -> QueryResult:
        qr = QueryResult(
            query=query_meta["query"],
            gt_doc_id=query_meta["gt_doc_id"],
            gt_sections=query_meta.get("gt_sections", []),
            query_type=query_meta.get("query_type", "unknown"),
        )

        qr.hits = self.query_fn(qr.query, self.adaptive_k)
        qr.retrieved_doc_ids = {h["doc_id"] for h in qr.hits}
        qr.retrieved_sections = {
            self._norm(h["section"])
            for h in qr.hits
            if h.get("section")
        }

        qr.doc_metrics = compute_metrics(qr.retrieved_doc_ids, {qr.gt_doc_id})
        qr.section_metrics = compute_metrics(
            qr.retrieved_sections, set(qr.gt_sections)
        )
        qr.mrr = reciprocal_rank(qr.hits, qr.gt_doc_id)
        qr.ndcg_at_k = ndcg_at_k(qr.hits, qr.gt_doc_id, self.adaptive_k)

        gt_content = query_meta.get("gt_content", "")
        qr.context_recall = context_recall(qr.hits, gt_content, qr.gt_doc_id)
        qr.context_recall_unfiltered = context_recall_unfiltered(qr.hits, gt_content)
        qr.section_keyword_coverage = _section_keyword_coverage(
            qr.hits, qr.gt_doc_id, qr.gt_sections
        )
        return qr

    def run(
        self,
        benchmark_queries: list[dict[str, Any]],
        verbose: bool = False,
    ) -> "EvaluationResults":
        results: list[QueryResult] = []
        t0 = time.time()
        for i, qm in enumerate(benchmark_queries):
            results.append(self.evaluate_single(qm))
            if verbose and (i + 1) % 10 == 0:
                print(f"  ... {i + 1}/{len(benchmark_queries)} queries done", flush=True)
        return EvaluationResults(
            query_results=results,
            adaptive_k=self.adaptive_k,
            elapsed_seconds=time.time() - t0,
        )


def _section_keyword_coverage(
    hits: list[dict[str, Any]],
    gt_doc_id: str,
    gt_sections: list[str],
) -> float:
    """Fraction of GT section names found in text from the correct document."""
    if not gt_sections:
        return 1.0
    relevant = [
        h.get("content", "")
        for h in hits
        if h.get("doc_id") == gt_doc_id and h.get("content")
    ]
    if not relevant:
        return 0.0
    text = " ".join(relevant).lower()
    found = 0
    for section in gt_sections:
        tokens = [t for t in section.lower().split() if len(t) > 3]
        if section.lower() in text or any(t in text for t in tokens):
            found += 1
    return found / len(gt_sections)


# ---------------------------------------------------------------------------
# Aggregated results
# ---------------------------------------------------------------------------

@dataclass
class EvaluationResults:
    query_results: list[QueryResult]
    adaptive_k: int
    elapsed_seconds: float

    def _mean(self, attr: str) -> float:
        return float(np.mean([getattr(q, attr) for q in self.query_results]))

    def _agg(self, key: str) -> dict[str, float]:
        return {
            f"doc_{key}": float(np.mean([q.doc_metrics.get(key, 0.0) for q in self.query_results])),
            f"section_{key}": float(np.mean([q.section_metrics.get(key, 0.0) for q in self.query_results])),
        }

    def summary(self) -> dict[str, float | int]:
        out: dict[str, float | int] = {"adaptive_k": self.adaptive_k}
        for m in ("precision", "recall", "f1"):
            out.update(self._agg(m))

        # Primary retrieval metrics (clear names)
        out["doc_hit_at_k"] = out["doc_recall"]
        out["section_hit_at_k"] = out["section_recall"]
        out["mrr"] = self._mean("mrr")
        out["ndcg_at_k"] = self._mean("ndcg_at_k")
        out["context_recall"] = self._mean("context_recall")
        out["context_recall_unfiltered"] = self._mean("context_recall_unfiltered")
        out["section_keyword_coverage"] = self._mean("section_keyword_coverage")
        out["elapsed_seconds"] = round(self.elapsed_seconds, 1)
        return out

    def summary_by_query_type(self) -> dict[str, dict[str, float]]:
        """Break down doc_hit_at_k and MRR by query difficulty/type."""
        by_type: dict[str, list[QueryResult]] = {}
        for q in self.query_results:
            by_type.setdefault(q.query_type, []).append(q)
        out: dict[str, dict[str, float]] = {}
        for qtype, qs in sorted(by_type.items()):
            out[qtype] = {
                "n": float(len(qs)),
                "doc_hit_at_k": float(np.mean([q.doc_metrics["recall"] for q in qs])),
                "mrr": float(np.mean([q.mrr for q in qs])),
                "context_recall": float(np.mean([q.context_recall for q in qs])),
            }
        return out

    def pretty_print(self, strategy_name: str = "") -> None:
        s = self.summary()
        max_precision = round(1.0 / self.adaptive_k, 3)
        gap = s["context_recall_unfiltered"] - s["context_recall"]

        print(f"=== {strategy_name} | k={self.adaptive_k} ===")
        print(
            f"  Doc Hit@{self.adaptive_k}={s['doc_hit_at_k']:.3f}  "
            f"MRR={s['mrr']:.3f}  nDCG@{self.adaptive_k}={s['ndcg_at_k']:.3f}"
        )
        print(
            f"  Context recall (filtered)={s['context_recall']:.3f}  "
            f"(unfiltered)={s['context_recall_unfiltered']:.3f}  "
            f"[gap={gap:+.3f} — shared FDA vocabulary inflating unfiltered]"
        )
        print(
            f"  Sec Hit@{self.adaptive_k}={s['section_hit_at_k']:.3f}  "
            f"Sec keyword coverage={s['section_keyword_coverage']:.3f}"
        )
        print(
            f"  Doc precision ceiling=1/{self.adaptive_k}={max_precision:.3f}  "
            f"(structural max when GT doc always retrieved)"
        )
        print(f"  Queries: {len(self.query_results)} | Time: {s['elapsed_seconds']}s")
