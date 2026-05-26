# Embedding Benchmark — FDA Biomarker Corpus

Fair comparison of **3 embedding models** with fixed chunking, directly addressing the *"one pipeline, one result"* limitation from [Vecta's chunking study (Feb 2026)](https://www.runvecta.com/blog/we-benchmarked-7-chunking-strategies-most-advice-was-wrong):

> *"Running the same experiment across multiple embedding models would be valuable follow-up work, and it's on our roadmap."*
> — Vecta Team

We already do this.

---

## Design

| Component | Value |
|---|---|
| **Varies** | Embedding model |
| Corpus | 597 FDA biomarker JSONL records |
| Chunking | `recursive_512` (fixed — winner or strong runner-up in chunking benchmark) |
| Vector store | ChromaDB (cosine HNSW) |
| Retriever | Dense cosine similarity |
| Queries | 100 synthetic (seed=42), 9 template types |
| Context budget | Adaptive k ≈ 2000 tokens |

---

## Embedding models compared

| Model | Dim | Notes |
|---|---|---|
| `all-MiniLM-L6-v2` | 384 | General-purpose baseline (fast) |
| `BAAI/bge-small-en-v1.5` | 384 | Strong retrieval model, same speed |
| `NeuML/pubmedbert-base-embeddings` | 768 | Biomedical PubMedBERT — domain match for FDA text |

All models run locally — no API key required, no cost per query.

**Why these three?** The corpus is FDA regulatory text (drug labels). A general-purpose model may underperform a biomedical model because clinical vocabulary ("biomarker", "prescribing information", "adverse reactions") is denser in the medical embedding space. This benchmark answers whether domain-specific embeddings justify the slower inference speed.

---

## Why this matters (Vecta's finding)

Vecta's benchmark used only `text-embedding-3-small` (OpenAI, paid). Their own conclusion:

> *"Swap any of those components and the rankings could shift."*

Our embedding benchmark directly tests this hypothesis on FDA text. If `NeuML/pubmedbert-base-embeddings` scores significantly higher on Doc Hit@k and Context Recall, it confirms that embedding choice matters and domain-specific models are worth the extra latency.

---

## Primary metrics

| Metric | Meaning |
|---|---|
| **Doc Hit@k** | % queries where the correct drug+biomarker doc appears in top-k |
| **MRR** | Mean reciprocal rank of first correct document |
| **nDCG@k** | Ranking quality (binary relevance) |
| **Context Recall** | Token recall of GT document in correct-doc chunks only |

---

## Run

```bash
# All 3 models (recommended — takes ~15 min with local models)
python -m experiments.embedding_benchmark.run_benchmark

# Quick test with fewer queries
python -m experiments.embedding_benchmark.run_benchmark --sample 30

# Specific models only
python -m experiments.embedding_benchmark.run_benchmark --models all-MiniLM-L6-v2 BAAI/bge-small-en-v1.5

# Keep existing indexes (skip re-embedding)
python -m experiments.embedding_benchmark.run_benchmark --no-reset-db
```

---

## Outputs

```
results/
├── embedding_benchmark_results.csv   ← one row per model, all metrics
├── embedding_benchmark_results.json  ← same + metadata
└── embedding_heatmap.png             ← Doc Hit@k, MRR, nDCG, Context Recall heatmap
```

---

## Intended workflow with chunking benchmark

```
Step 1: python -m experiments.chunking_benchmark.run_benchmark
        → identifies best chunking strategy (e.g. recursive_512 or structure_aware)

Step 2: python -m experiments.embedding_benchmark.run_benchmark
        → identifies best embedding model for that chunking strategy

Step 3 (optional): Re-run chunking benchmark with the winning embedding model
        to confirm rankings hold across embedding choices.
```

This two-stage design breaks the "one pipeline, one result" constraint systematically: first you optimise the chunking axis, then the embedding axis, rather than treating both as fixed and unknown.
