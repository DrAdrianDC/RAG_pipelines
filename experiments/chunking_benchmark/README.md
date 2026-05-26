# Chunking Benchmark — FDA Biomarker Corpus

Fair comparison of **5 model-aligned chunking strategies** on FDA regulatory text. All strategies respect `SAFE_CHUNK_TOKENS = 192` so every chunk fits within the `all-MiniLM-L6-v2` context window (256 WordPiece tokens).

Legacy oversize strategies (`fixed_512`, `fixed_1024`, `recursive_512`) are available via `--include-legacy` for historical comparison.

---

## Fixed components

| Component | Value |
|---|---|
| Corpus | 597 FDA biomarker JSONL records |
| Embedding | `all-MiniLM-L6-v2` (256 WordPiece max, 384 dims) |
| Vector store | ChromaDB, `hnsw:space=cosine` |
| Retriever | Dense cosine similarity |
| Queries | 100 synthetic (seed=42), 5 query types |
| k | Fixed at 5 (fair comparison across strategies) |
| Chunk size cap | `SAFE_CHUNK_TOKENS = 192` (tiktoken cl100k_base) |

---

## Strategies compared (default run)

| Strategy | Avg tokens | Max tokens | Weaviate type |
|---|---|---|---|
| `fixed_192` | ~108 | ~177 | Fixed-size |
| `fixed_256` | ~141 | ~231 | Fixed-size (borderline) |
| `recursive_192` | ~157 | ~208 | Recursive |
| `semantic` | ~192 | ≤192 | Semantic / Context-Aware |
| `structure_aware` | ~135 | ~202 | Document-Based |

---

## Latest results (fixed k=5, 100 queries)

| Strategy | Doc Hit@5 | MRR | nDCG@5 | Context Recall | Section Hit@5 |
|---|---|---|---|---|---|
| `fixed_192` | **0.430** | **0.322** | **0.521** | 0.207 | 0.000 |
| `semantic` | **0.430** | 0.300 | 0.442 | 0.204 | 0.000 |
| `structure_aware` | 0.420 | 0.306 | 0.475 | 0.221 | **0.539** |
| `recursive_192` | 0.400 | 0.297 | 0.484 | **0.258** | 0.000 |
| `fixed_256` | 0.370 | 0.284 | 0.452 | 0.213 | 0.000 |

**Recommendation — hybrid index:**
- Section queries → `structure_aware` collection
- General queries → `fixed_192` or `recursive_192`

Full results: [`results/benchmark_results.csv`](results/benchmark_results.csv) · [`results/benchmark_results.json`](results/benchmark_results.json)

---

## Query types

| Type | Description | Difficulty |
|---|---|---|
| `named_full` | Brand + generic + biomarker | Easy |
| `named_generic` | Generic name + biomarker | Medium |
| `named_brand` | Brand name + biomarker | Medium |
| `named_section` | Section + generic + biomarker | Medium |
| `semantic_hard` | Biomarker + therapeutic area, **no drug name** | Hard |

---

## Primary metrics

| Metric | Meaning |
|---|---|
| **Doc Hit@k** | % queries where correct drug+biomarker doc appears in top-k |
| **MRR** | Mean reciprocal rank of first correct document |
| **nDCG@k** | Ranking quality (binary relevance) |
| **Context Recall (filtered)** | Token recall of GT document in **correct-doc chunks only** |
| Context Recall (unfiltered) | Same but all top-k chunks — diagnostic, often inflated |
| Section Hit@k | Correct FDA labeling section retrieved |

---

## Run

```bash
# Default — 5 model-aligned strategies, fixed k=5, 100 queries
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --reset-db

# Include legacy oversize strategies for truncation comparison
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --include-legacy --reset-db

# Include LangChain cross-validation variants
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --include-langchain --reset-db

# LLM judge (requires GROQ_API_KEY in .env)
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --llm-judge

# Quick validation — 20 queries, no plots
python -m experiments.chunking_benchmark.run_benchmark --sample 20 --no-plots

# Specific strategies only
python -m experiments.chunking_benchmark.run_benchmark --strategies fixed_192 semantic --fixed-k 5
```

---

## Outputs

```
results/
├── benchmark_results.csv / .json     ← one row per strategy, all metrics
├── retrieval_comparison.png          ← Doc Hit@k vs Context Recall (bar)
├── metrics_heatmap.png               ← Hit@k, MRR, nDCG, Context Recall (heatmap)
├── ranking_scatter.png               ← Hit@k vs MRR scatter
├── chunk_size_vs_quality.png         ← avg tokens vs Context Recall
├── chunk_distribution.png            ← avg tokens + adaptive k per strategy
└── radar_chart.png                   ← all metrics per strategy (radar)
```

Query-type breakdowns are stored in `benchmark_results.json` under `query_type_breakdown`.

---

## Next step

After identifying the best chunking strategy, run the embedding benchmark to break the "one pipeline, one result" constraint:

```bash
python -m experiments.embedding_benchmark.run_benchmark
```

See `experiments/embedding_benchmark/README.md` for details.
