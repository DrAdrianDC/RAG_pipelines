"""
evaluation — retrieval and end-to-end evaluation metrics.

Public API — retrieval metrics (evaluation.metrics)
----------------------------------------------------
RetrievalEvaluator    Run queries, compute all metrics, return EvaluationResults.
EvaluationResults     Aggregate container with summary() and pretty_print().
QueryResult           Per-query result dataclass.
compute_metrics       Precision / Recall / F1 for one query.
precision             |retrieved ∩ GT| / |retrieved|
recall                |retrieved ∩ GT| / |GT|
f1                    Harmonic mean of precision and recall.
reciprocal_rank       1 / rank_of_first_correct_hit  (0 if not found).
ndcg_at_k             Normalised Discounted Cumulative Gain, binary relevance.
context_recall        Token recall of GT text in correct-doc chunks (filtered).
context_recall_unfiltered  Same but over all top-k chunks (diagnostic).

Public API — end-to-end metrics (evaluation.e2e_metrics)
---------------------------------------------------------
evaluate_e2e_single   Score one query: faithfulness + answer_relevance.
E2EQueryResult        Per-query E2E result dataclass.
E2EResults            Aggregate E2E container.
faithfulness_proxy    Extractive faithfulness proxy (no LLM required).
answer_relevance_proxy Extractive relevance proxy (no LLM required).
build_retrieved_context Concatenate chunk text for a query's context.

Boundary rule
-------------
Pure metrics — no ChromaDB, no embeddings, no file I/O, no experiment paths.
Receives data via injected callables and plain dicts.
"""

from evaluation.e2e_metrics import (
    E2EQueryResult,
    E2EResults,
    answer_relevance_proxy,
    build_retrieved_context,
    evaluate_e2e_single,
    faithfulness_proxy,
    LLMProvider,
)
from evaluation.metrics import (
    EvaluationResults,
    QueryResult,
    RetrievalEvaluator,
    compute_metrics,
    context_recall,
    context_recall_unfiltered,
    f1,
    ndcg_at_k,
    precision,
    recall,
    reciprocal_rank,
)

__all__ = [
    # retrieval metrics
    "RetrievalEvaluator",
    "EvaluationResults",
    "QueryResult",
    "compute_metrics",
    "precision",
    "recall",
    "f1",
    "reciprocal_rank",
    "ndcg_at_k",
    "context_recall",
    "context_recall_unfiltered",
    # e2e metrics
    "evaluate_e2e_single",
    "E2EQueryResult",
    "E2EResults",
    "faithfulness_proxy",
    "answer_relevance_proxy",
    "build_retrieved_context",
    "LLMProvider",
]
