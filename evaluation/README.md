# `evaluation` — Retrieval & End-to-End Metrics

A self-contained, pure-Python library of evaluation metrics for the `RAG_pipelines` framework. Covers retrieval quality (offline, no LLM required) and optional end-to-end faithfulness scoring (extractive proxy or LLM-as-judge).

---

## Module structure

```
evaluation/
├── metrics.py      # Retrieval metrics: Hit@k, MRR, nDCG, Context Recall, Precision, Recall, F1
├── e2e_metrics.py  # End-to-end: faithfulness, answer relevance (proxy + optional LLM judge)
└── README.md
```

> **Boundary rule**: pure metrics module — no ChromaDB, no embeddings, no file I/O, no experiment paths. Data arrives via injected callables and plain Python dicts.

---

## Quick start

```python
from evaluation import RetrievalEvaluator

# query_fn: (query_text, k) -> list[hit_dict]
evaluator = RetrievalEvaluator(
    query_fn=retriever.retrieve,
    adaptive_k=4,
    normalize_section_fn=normalize_section,   # optional, from chunking.utils
)

results = evaluator.run(benchmark_queries, verbose=True)
results.pretty_print("recursive_512")
print(results.summary())
```

---

## Retrieval metrics (`metrics.py`)

### Metric catalogue

| Metric | Key | Description |
|---|---|---|
| **Doc Hit@k** | `doc_hit_at_k` | Fraction of queries where the correct `drug+biomarker` document appears in top-k chunks. The primary benchmark metric. |
| **MRR** | `mrr` | Mean Reciprocal Rank of the first correct hit. Penalises low ranks. |
| **nDCG@k** | `ndcg_at_k` | Normalised Discounted Cumulative Gain with binary relevance. Accounts for rank quality across all hits. |
| **Context Recall (filtered)** | `context_recall` | Token recall of the ground-truth document text in retrieved chunks **from the correct document only**. The recommended primary content quality metric. |
| **Context Recall (unfiltered)** | `context_recall_unfiltered` | Same recall but over all top-k chunks. Often inflated by shared regulatory vocabulary — use for diagnostics only. |
| **Section Hit@k** | `section_hit_at_k` | Fraction of queries where at least one correct FDA labeling section appears in retrieved chunks. Mainly useful for `structure_aware` evaluation. |
| **Section Keyword Coverage** | `section_keyword_coverage` | Fraction of GT section names found as text in correct-document chunks. Complementary to Section Hit@k. |
| Precision | `doc_precision` | `|correct docs ∩ hits| / k`. Structurally low when k > 1 — secondary metric. |
| F1 | `doc_f1` | Harmonic mean of precision and recall. Legacy metric. |

### `RetrievalEvaluator`

```python
evaluator = RetrievalEvaluator(
    query_fn=Callable[[str, int], list[dict]],   # injected retrieval function
    adaptive_k=int,                               # from compute_adaptive_k()
    normalize_section_fn=Optional[Callable],      # section name normaliser
)
results: EvaluationResults = evaluator.run(benchmark_queries)
```

Each query dict in `benchmark_queries` must contain:

```python
{
    "query":       str,          # natural language question
    "gt_doc_id":   str,          # "drug_name||biomarker"
    "gt_content":  str,          # ground-truth document text (for context recall)
    "gt_sections": list[str],    # canonical section names (for section metrics)
    "query_type":  str,          # "named_full" | "semantic_hard" | … (for breakdown)
}
```

### `EvaluationResults`

```python
results.summary()                  # -> dict[str, float|int]  — all metrics aggregated
results.summary_by_query_type()    # -> dict[str, dict]       — broken down by query_type
results.pretty_print("strategy")   # prints a concise metric table
```

---

## End-to-end metrics (`e2e_metrics.py`)

### Extractive proxies (no API key required)

| Function | Description |
|---|---|
| `faithfulness_proxy` | Token recall of GT text in correct-doc chunks. Same as filtered context recall. |
| `answer_relevance_proxy` | Weighted overlap of query words and GT tokens in the generated answer. |

These run on every machine without any API key and provide a lower-bound signal on E2E quality.

### LLM-as-judge (optional, requires `OPENAI_API_KEY`)

```python
from evaluation import evaluate_e2e_single

result = evaluate_e2e_single(
    query_meta=query_dict,
    hits=retrieved_hits,
    generated_answer="The recommended dose is 200 mg every 3 weeks.",
    use_llm=True,         # set False to use extractive proxies
)
print(result.faithfulness, result.answer_relevance)
```

When `use_llm=True` and `OPENAI_API_KEY` is set and a `generated_answer` is provided, the judge calls `gpt-4o-mini` with a structured prompt returning `{"faithfulness": 0-1, "answer_relevance": 0-1}`. If the LLM call fails for any reason, it falls back to the extractive proxies and logs a warning.

---

## Benchmark pipeline position

```
FDA Corpus (.jsonl)
        ↓
[CHUNKING: varies]          ← chunking/
        ↓
Embedding (fixed)           ← embeddings/
        ↓
ChromaDB (fixed)            ← vectorstores/
        ↓
Retrieval                   ← retrieval/
        ↓
[EVALUATION]                ← evaluation/  ← you are here
```

---

## Design principles

- **Pure module**: zero side effects at import time. No file writes, no model loads, no network calls.
- **Injected query function**: `RetrievalEvaluator` receives `query_fn` rather than a `DenseRetriever` or a ChromaDB collection. This decouples metrics from any specific retrieval backend.
- **Adaptive k**: the evaluator does not fix k — it uses `adaptive_k` computed by the experiment runner so that all strategies receive the same ~2000-token context budget. See `experiments/chunking_benchmark/benchmark_utils.py`.
- **Filtered context recall is the primary metric**: unfiltered recall is inflated by shared regulatory vocabulary (e.g., "FDA-approved" appears in every label). Always prefer the filtered variant.
- **Fail loudly on LLM judge errors**: bare `except` suppression replaced by a `logging.warning` so failures are observable in logs.
