# Chunking Benchmark — FDA Biomarker Corpus

Fair comparison of **5 chunking strategies** on FDA regulatory text, implementing the experimental design from [Vecta's chunking benchmark (Feb 2026)](https://www.runvecta.com/blog/we-benchmarked-7-chunking-strategies-most-advice-was-wrong): fixed embedding, fixed vector store, **adaptive k ≈ 2000 tokens**.

---

## Alignment with Vecta's methodology

| Vecta design decision | Our implementation |
|---|---|
| Adaptive k = round(2000 / avg_tokens) | `compute_adaptive_k()` — identical formula |
| Fixed embedding model | `all-MiniLM-L6-v2` fixed across all strategies |
| Fixed vector store | ChromaDB, cosine HNSW |
| Varied chunking strategy | 5 strategies (see table below) |
| Retrieval metrics | Doc Hit@k (≈ Doc Recall), MRR, nDCG@k, Context Recall |
| E2E metrics (Accuracy, Groundedness) | Available via `--e2e` flag (extractive proxies) or `--e2e --llm-judge` (requires `OPENAI_API_KEY`) |

**What Vecta has that we do not (yet):**
- LLM-generated answers evaluated for Accuracy/Groundedness by default (we use extractive proxies)
- `page-per-chunk` strategy (trivial to add)
- `proposition chunking` (requires LLM decomposition)

**What we have that Vecta does not:**
- Medical domain corpus (FDA regulatory text) — harder than academic papers due to shared vocabulary
- Section-level metrics (Section Hit@k, Section Keyword Coverage) — specific to FDA 21 CFR 201.57
- Brand/generic drug name enrichment in query generation (reduces metadata–content mismatch)
- MRR and nDCG@k (ranking quality, not reported by Vecta)
- A companion `embedding_benchmark` — directly addresses Vecta's "one pipeline, one result" limitation

---

## Fixed components

| Component | Value |
|---|---|
| Corpus | 597 FDA biomarker JSONL records |
| Embedding | `all-MiniLM-L6-v2` (local, free) |
| Vector store | ChromaDB, `hnsw:space=cosine` |
| Retriever | Dense cosine similarity |
| Queries | 100 synthetic (seed=42), 9 template types |
| Context budget | Adaptive k ≈ 2000 tokens per query |

---

## Strategies compared

| Strategy | Avg tokens | Adaptive k | Weaviate type |
|---|---|---|---|
| `fixed_512` | ~487 | ~4 | Fixed-size |
| `fixed_1024` | ~900 | ~2 | Fixed-size |
| `recursive_512` | ~487 | ~4 | Recursive |
| `semantic` | ~300–600 | ~4–6 | Semantic / Context-Aware |
| `structure_aware` | ~700–900 | ~2–3 | Document-Based |

---

## Query types

| Type | Description | Difficulty |
|---|---|---|
| `named_full` | Brand + generic + biomarker | Easy |
| `named_generic` | Generic name + biomarker | Medium |
| `named_brand` | Brand name + biomarker | Medium |
| `named_section` | Section + generic + biomarker | Medium |
| `semantic_hard` | Biomarker + therapeutic area, **no drug name** | Hard |

Queries enrich records with brand names extracted from label text (`DRUG®`) to reduce metadata–content mismatch — a domain-specific fix not present in Vecta's academic paper benchmark.

---

## Primary metrics

| Metric | Meaning | Vecta equivalent |
|---|---|---|
| **Doc Hit@k** | % queries where correct drug+biomarker doc appears in top-k | Doc Recall |
| **MRR** | Mean reciprocal rank of first correct document | — |
| **nDCG@k** | Ranking quality (binary relevance) | — |
| **Context Recall (filtered)** | Token recall of GT document in **correct-doc chunks only** | Page Recall (approximate) |
| Context Recall (unfiltered) | Same but all top-k chunks — diagnostic, often inflated | — |
| Section Hit@k | Correct FDA labeling section retrieved | — (FDA-specific) |

> **Accuracy / Groundedness**: run `--e2e` for extractive proxies or `--e2e --llm-judge` for LLM scoring (analogous to Vecta's Accuracy and Groundedness). Requires `OPENAI_API_KEY` for LLM judge.

---

## Run

```bash
# Install dependencies
pip install -r chunking/requirements.txt "numpy>=1.26.0,<2.0" python-dotenv

# Full benchmark — retrieval metrics only (no API key needed)
python -m experiments.chunking_benchmark.run_benchmark

# Full benchmark + LLM generation & judging (Vecta-style Faithfulness + Answer Relevance)
# FREE — uses Gemini. Set GEMINI_API_KEY in .env (key at https://aistudio.google.com)
python -m experiments.chunking_benchmark.run_benchmark --llm-judge

# Higher quality LLM judge (still free)
python -m experiments.chunking_benchmark.run_benchmark --llm-judge --llm-model gemini-2.5-flash

# Quick validation — 20 queries, no plots
python -m experiments.chunking_benchmark.run_benchmark --sample 20 --no-plots

# Specific strategies only
python -m experiments.chunking_benchmark.run_benchmark --strategies fixed_512 recursive_512

# Include LangChain cross-validation variants
python -m experiments.chunking_benchmark.run_benchmark --include-langchain

# Chunk size sweep (fixed strategy, vary chunk size 64–1536)
python -m experiments.chunking_benchmark.chunk_size_sweep
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
├── chunk_distribution.png           ← avg tokens + adaptive k per strategy
└── radar_chart.png                   ← all metrics per strategy (radar)
```

Query-type breakdowns (easy vs hard queries) are stored in `benchmark_results.json` under `query_type_breakdown`. Use these to assess whether a strategy fails specifically on `semantic_hard` queries — the main discriminator for real-world robustness.

---

## Next step

After identifying the best chunking strategy, run the embedding benchmark to break the "one pipeline, one result" constraint:

```bash
python -m experiments.embedding_benchmark.run_benchmark
```

See `experiments/embedding_benchmark/README.md` for details.
