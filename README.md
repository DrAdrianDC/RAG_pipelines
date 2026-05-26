# RAG Pipelines

[![CI](https://github.com/DrAdrianDC/RAG_pipelines/actions/workflows/ci.yml/badge.svg)](https://github.com/DrAdrianDC/RAG_pipelines/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A modular experimental framework for evaluating and optimizing Retrieval-Augmented Generation (RAG) systems through reproducible offline evaluation workflows.

The project focuses on understanding how architectural decisions impact retrieval quality, context relevance, and downstream generation performance — **not** on building a single end-user chatbot application.

---

## Goal

The main objective of this repository is to:

- Understand how different RAG components affect performance
- Experiment with chunking and retrieval strategies on a real-world domain corpus (FDA biomarker labeling)
- Compare embeddings and vector database options
- Build a foundation for production-ready RAG systems

---

## Pipeline Architecture

```
Data Ingestion  →  Chunking  →  Embeddings  →  Vector Store
                                                     ↓
              Evaluation  ←  Generation  ←  Retrieval + Reranking
```

---

## Components

### Data Ingestion (`data_ingestion/`)

Modular ingestion pipelines for acquiring, parsing, and preprocessing document collections.

- PDF acquisition and OCR-based extraction (Marker)
- FDA biomarker structured text preprocessing
- JSONL corpus generation (597-record benchmark corpus)

### Datasets (`datasets/`)

Benchmark-ready corpora consumed by all downstream pipeline stages.

- **`datasets/fda_biomarkers/benchmark/fda_biomarkers.jsonl`** — 597 FDA drug-biomarker records used for chunking, embedding, retrieval, and evaluation experiments

### Chunking (`chunking/`)

13 chunking strategies — **5 model-aligned** (recommended) + 3 legacy oversize + 5 LangChain cross-validation variants.

All model-aligned strategies respect `SAFE_CHUNK_TOKENS = 192`, ensuring every chunk fits within the `all-MiniLM-L6-v2` context window (256 WordPiece tokens). See [`chunking/README.md`](chunking/README.md) for the full rationale.

| Strategy | Implementation | Max tokens | Status |
|---|---|---|---|
| `fixed_192` | Custom word-boundary, token-budget | 192 | **Recommended** |
| `fixed_256` | Custom word-boundary, token-budget | 256 | Borderline |
| `recursive_192` | Custom Weaviate separator hierarchy | 192 | **Recommended** |
| `semantic` | Adaptive percentile threshold + NLTK | ≤192 | **Recommended** |
| `structure_aware` | FDA 21 CFR section detection | ≤192 | **Recommended** |
| `fixed_512` / `fixed_1024` / `recursive_512` | Legacy oversize | 512–1024 | Truncated by embedding model |
| `lc_fixed_192` / `lc_recursive_192` | LangChain cross-validation | 192 | Model-aligned |
| `lc_fixed_512` / `lc_fixed_1024` / `lc_recursive_512` | LangChain cross-validation | 512–1024 | Legacy |

LangChain variants serve as cross-validation: if custom and LangChain implementations agree on retrieval metrics, the custom code is validated.

Tests: `pytest chunking/tests/ -v` (no API keys or GPU required)

### Embeddings (`embeddings/`)

`EmbeddingModel` protocol with a `SentenceTransformerAdapter` backed by a lazy-loaded singleton cache. Supports swapping models without changing downstream code.

Tested models: `all-MiniLM-L6-v2`, `BAAI/bge-small-en-v1.5`, `NeuML/pubmedbert-base-embeddings`

Tests: `pytest embeddings/tests/ -v`

### Vector Store (`vectorstores/`)

ChromaDB collection lifecycle adapter: `get_chroma_collection`, `index_chunks`, `query_collection`. Designed to be extended with FAISS, Weaviate, or Pinecone backends.

Tests: `pytest vectorstores/tests/ -v`

### Retrieval (`retrieval/`)

Two-stage retrieval pipeline built on top of `vectorstores/` and `embeddings/`:

- `DenseRetriever` — dependency-injection wrapper around any `(query, k) → list[dict]` function
- `build_dense_retriever` — factory that wires a ChromaDB collection into a retriever in one call
- `RerankedRetriever` — cross-encoder reranking on top of dense retrieval (`cross-encoder/ms-marco-MiniLM-L-6-v2`, CPU, no API key)

Tests: `pytest retrieval/tests/ -v` (no API keys, no model weights loaded)

### Evaluation (`evaluation/`)

- **`evaluation/metrics.py`** — `RetrievalEvaluator` computing Hit@K, MRR, nDCG@K, context recall (filtered + unfiltered), and section coverage. Pure module: no vector store, no model weights.
- **`evaluation/e2e_metrics.py`** — End-to-end generation evaluation with extractive proxies (no API key) or LLM-as-judge via Groq (free), Gemini, or OpenAI.

### Experiments (`experiments/`)

- **`experiments/chunking_benchmark/`** — Runnable benchmark comparing 5 model-aligned chunking strategies (legacy oversize strategies available via `--include-legacy`). Produces CSV/JSON results and 6 publication-ready plots.
- **`experiments/embedding_benchmark/`** — Compares embedding models with fixed chunking strategy.

---

## Installation

```bash
git clone https://github.com/DrAdrianDC/RAG_pipelines.git
cd RAG_pipelines

python -m venv venv
source venv/bin/activate

# Editable install (recommended — no sys.path hacks)
pip install -e ".[dev]"
```

For LLM judge evaluation, copy `.env.example` to `.env` and add your API key:

```bash
cp .env.example .env
# edit .env and set GROQ_API_KEY (free at console.groq.com) or OPENAI_API_KEY
```

---

## Running Tests

```bash
# Full test suite — 294 tests, no API keys or GPU required (~5 s)
pytest

# Individual modules
pytest chunking/tests/ -v
pytest embeddings/tests/ -v
pytest vectorstores/tests/ -v
pytest retrieval/tests/ -v
```

All 294 unit tests use mock models and in-memory ChromaDB — no network access, no GPU, no API keys.

---

## Running the Benchmark

```bash
# Default — 5 model-aligned strategies, 100 queries, fixed k=5 (fair comparison)
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --reset-db

# Include legacy oversize strategies (fixed_512, fixed_1024, recursive_512)
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --include-legacy --reset-db

# Quick validation — 20 queries, no plots
python -m experiments.chunking_benchmark.run_benchmark --sample 20 --no-plots

# Full run + Groq LLM judge (requires GROQ_API_KEY in .env)
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --llm-judge --llm-provider groq

# Embedding benchmark — compare MiniLM, BGE, PubMedBERT
python -m experiments.embedding_benchmark.run_benchmark --sample 50
```

Results are saved to `experiments/chunking_benchmark/results/` (CSV, JSON, and six plots). These artifacts are tracked in git so benchmark numbers are visible on GitHub without re-running the pipeline.

---

## Benchmark Results

**Corpus:** 597 FDA drug-biomarker records · **Embedding:** `all-MiniLM-L6-v2` (384 dims, 256 WordPiece max) · **k:** fixed at 5 (fair comparison) · **Queries:** 100 synthetic (seed=42)

| Strategy | Chunks | Avg tokens | Max tokens | MRR | nDCG@5 | Doc Hit@5 | Context Recall | Section Hit@5 |
|---|---|---|---|---|---|---|---|---|
| `fixed_192` | 4 113 | 108 | 177 | **0.322** | **0.521** | **0.430** | 0.207 | 0.000 |
| `semantic` | 3 315 | 122 | ≤192 | 0.300 | 0.442 | **0.430** | 0.204 | 0.000 |
| `structure_aware` | 2 991 | 135 | 202 | 0.306 | 0.475 | 0.420 | 0.221 | **0.539** |
| `recursive_192` | 2 707 | 157 | 208 | 0.297 | 0.484 | 0.400 | **0.258** | 0.000 |
| `fixed_256` | 3 127 | 141 | 231 | 0.284 | 0.452 | 0.370 | 0.213 | 0.000 |

> All strategies use **model-aligned chunk sizes** (`SAFE_CHUNK_TOKENS = 192`) so embeddings represent the full chunk text — no silent truncation by `all-MiniLM-L6-v2`.
> Fixed k=5 makes Hit@5 and nDCG@5 directly comparable across strategies.

**Key findings:**

- **`fixed_192` leads on MRR (0.322) and nDCG@5 (0.521)** — best ranking quality; simplest baseline to implement.
- **`structure_aware` is the only strategy with section retrieval (0.539)** — route section-specific queries to this collection.
- **`recursive_192` leads on context recall (0.258)** — best at recovering GT document tokens from correct-doc chunks.
- **Two strategies tie on Doc Hit@5 (0.430):** `fixed_192` and `semantic`; `structure_aware` is close (0.420).
- **Recommended hybrid index:** section keywords → `structure_aware`; general queries → `fixed_192` or `recursive_192`.

**Embedding model — the main bottleneck (prototype scope):**

These numbers are a valid baseline for comparing *chunking strategies*, but they are not production-ready retrieval scores. The pipeline uses `all-MiniLM-L6-v2`, a general-purpose model trained on web text (Wikipedia, Reddit, news). It was not trained on biomedical or regulatory corpora and does not treat FDA-specific entities — drug INN names, biomarker codes, 21 CFR section headers — as first-class semantic signals.

Consequences visible in the results:

- **Average Doc Hit@5 is ~0.41** — the correct document is missed in ~59% of queries, even with chunking correctly aligned to the model window.
- **Context recall stays low (0.20–0.26)** — when the right document is found, only ~20–26% of its relevant tokens appear in the retrieved chunks.
- **Shared regulatory vocabulary inflates unfiltered metrics** — words like "pharmacokinetics" or "adverse reactions" appear across most of the 597 labels, so dense retrieval confuses documents that share terminology but refer to different drugs.

As a **research prototype**, `all-MiniLM-L6-v2` is a reasonable starting point: it is fast, free, runs locally, and makes chunking comparisons reproducible. For a **clinical or regulatory RAG system**, the next step is a domain-specific embedding (`NeuML/pubmedbert-base-embeddings`, `pritamdeka/BioBert-Pubmed-Sentence-Similarity`, or `BAAI/bge-base-en-v1.5`) — not further chunk size tuning alone.

Plots: [`retrieval_comparison.png`](experiments/chunking_benchmark/results/retrieval_comparison.png) · [`metrics_heatmap.png`](experiments/chunking_benchmark/results/metrics_heatmap.png) · [`radar_chart.png`](experiments/chunking_benchmark/results/radar_chart.png)

---

## Known Limitations

**Benchmark queries are synthetic.** Queries are generated from the same JSONL records that are indexed, using 9 fixed templates rotated by record position. Named queries (which include the drug name and biomarker directly) are easier than real user queries — they test lexical match more than semantic understanding. The `semantic_hard` query type (no drug name) better approximates production difficulty.

**General-purpose embedding on a biomedical corpus.** All chunking comparisons use `all-MiniLM-L6-v2` (384 dims), which was not trained for FDA regulatory text, drug nomenclature, or clinical terminology. The chunking *ranking* is valid within this model; absolute retrieval scores are not representative of what a domain-tuned system would achieve. Swapping to a biomedical embedding (PubMedBERT, BioBERT) or a stronger general model (BGE-base) is expected to raise the ceiling for all strategies — the relative ranking may shift.

**LLM self-evaluation bias.** The end-to-end judge asks the LLM to generate an answer and score it in the same call. Self-scored faithfulness/relevance tends to be optimistic. Separating generation from evaluation with a stronger judge model is recommended for production measurement.

**Context recall is bag-of-words.** Token-level overlap between ground-truth and retrieved chunks is a useful proxy but misses semantically equivalent content expressed with different vocabulary.

**ChromaDB at 597 documents.** At this scale, brute-force exact search (e.g., FAISS flat) would be faster with no approximation error. ChromaDB HNSW is correct here but adds overhead that only pays off at >10K documents.

---

## Next Steps

Components planned:

- **Context Construction** — strategies for assembling the final context window from retrieved chunks
- **LLM Generation** — generation layer with prompt templates and LLM judge evaluation
- **Embedding benchmark results** — publish comparison of MiniLM vs BGE vs PubMedBERT on FDA text
- **Fixed-k benchmark run** — completed with `--fixed-k 5` on model-aligned strategies (see Benchmark Results above)

---

## License

MIT — see [LICENSE](LICENSE)
