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

8 chunking strategies benchmarked against the same FDA corpus:

| Strategy | Implementation | Tokens |
|---|---|---|
| `fixed_512` | Custom word-boundary, token-budget | 512 |
| `fixed_1024` | Custom word-boundary, token-budget | 1024 |
| `recursive_512` | Custom Weaviate separator hierarchy | 512 |
| `semantic` | Adaptive percentile threshold + NLTK | variable |
| `structure_aware` | FDA 21 CFR section detection | ≤1024 |
| `lc_fixed_512` | LangChain `TokenTextSplitter` | 512 |
| `lc_fixed_1024` | LangChain `TokenTextSplitter` | 1024 |
| `lc_recursive_512` | LangChain `RecursiveCharacterTextSplitter` | 512 |

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

- **`experiments/chunking_benchmark/`** — Runnable benchmark comparing all 8 chunking strategies. Produces CSV results and 6 publication-ready plots.
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
# Full test suite — 262 tests, no API keys or GPU required (~5 s)
pytest

# Individual modules
pytest chunking/tests/ -v
pytest embeddings/tests/ -v
pytest vectorstores/tests/ -v
pytest retrieval/tests/ -v
```

All 262 unit tests use mock models and in-memory ChromaDB — no network access, no GPU, no API keys.

---

## Running the Benchmark

```bash
# Quick run — 50 queries, adaptive k, no LLM judge
python -m experiments.chunking_benchmark.run_benchmark --sample 50

# Full run — all 597 queries, fixed k=5 (fair comparison), Groq LLM judge
python -m experiments.chunking_benchmark.run_benchmark --fixed-k 5 --llm-judge --llm-provider groq

# Embedding benchmark — compare MiniLM, BGE, PubMedBERT
python -m experiments.embedding_benchmark.run_benchmark --sample 50
```

Results are saved to `experiments/chunking_benchmark/results/benchmark_results.csv` and six plots (heatmap, radar, scatter, etc.).

---

## Benchmark Results

**Corpus:** 597 FDA drug-biomarker records · **Embedding:** `all-MiniLM-L6-v2` (384 dims) · **k:** adaptive (≈ 2000 token context budget per query)

| Strategy | Chunks | Avg tokens | k | MRR | Doc Hit@k | Context Recall | Section Hit@k |
|---|---|---|---|---|---|---|---|
| `semantic` | 2 584 | 157 | 13 | **0.326** | **0.51** | 0.315 | 0.00 |
| `fixed_1024` | 1 016 | 424 | 5 | 0.294 | 0.45 | **0.379** | 0.00 |
| `recursive_512` | 1 133 | 371 | 5 | 0.294 | 0.45 | 0.368 | 0.00 |
| `fixed_512` | 1 683 | 260 | 8 | 0.284 | 0.50 | 0.374 | 0.00 |
| `structure_aware` | 1 511 | 267 | 7 | 0.267 | 0.38 | 0.292 | **0.540** |

> **k is adaptive** (k = round(2000 / avg\_tokens)) to equalise the total context budget across strategies.
> MRR is k-independent and the primary fair ranking signal.
> Hit@k and nDCG@k are not directly comparable across rows with different k — rerun with `--fixed-k 5` for a normalised table.

**Key findings:**

- **`semantic` leads on MRR (0.326)** — the embedding-based boundary detection captures topic transitions better than fixed-size splitting, particularly on named queries.
- **`fixed_1024` leads on context recall (0.379)** — larger chunks preserve more of the GT document text in the retrieved context window, useful when the answer spans multiple sentences.
- **`structure_aware` is the only strategy with non-zero section retrieval (0.540)** — it is the correct choice whenever the user query references a specific FDA labeling section (e.g. "What does the Warnings and Precautions section say about…").
- **Embedding model is the primary performance ceiling.** Average MRR across all strategies is ~0.29. Literature with `text-embedding-3-small` on similar corpora reports Doc F1 ~0.86. Chunking strategy rankings are valid within `all-MiniLM-L6-v2`; absolute scores will improve significantly with a stronger model.
- **Recommended hybrid index:** route queries containing section keywords → `structure_aware` collection; all other queries → `semantic` collection.

Plots: [`retrieval_comparison.png`](experiments/chunking_benchmark/results/retrieval_comparison.png) · [`metrics_heatmap.png`](experiments/chunking_benchmark/results/metrics_heatmap.png) · [`radar_chart.png`](experiments/chunking_benchmark/results/radar_chart.png)

---

## Known Limitations

**Benchmark queries are synthetic.** Queries are generated from the same JSONL records that are indexed, using 9 fixed templates rotated by record position. Named queries (which include the drug name and biomarker directly) are easier than real user queries — they test lexical match more than semantic understanding. The `semantic_hard` query type (no drug name) better approximates production difficulty.

**Single embedding model ceiling.** All chunking comparisons use `all-MiniLM-L6-v2` (384 dims). Absolute retrieval scores are bounded by this model. The chunking *ranking* is valid within this model. Stronger embeddings (BGE-base, text-embedding-3-small) will raise the ceiling for all strategies uniformly — the relative ranking may shift.

**LLM self-evaluation bias.** The end-to-end judge asks the LLM to generate an answer and score it in the same call. Self-scored faithfulness/relevance tends to be optimistic. Separating generation from evaluation with a stronger judge model is recommended for production measurement.

**Context recall is bag-of-words.** Token-level overlap between ground-truth and retrieved chunks is a useful proxy but misses semantically equivalent content expressed with different vocabulary.

**ChromaDB at 597 documents.** At this scale, brute-force exact search (e.g., FAISS flat) would be faster with no approximation error. ChromaDB HNSW is correct here but adds overhead that only pays off at >10K documents.

---

## Next Steps

Components planned:

- **Context Construction** — strategies for assembling the final context window from retrieved chunks
- **LLM Generation** — generation layer with prompt templates and LLM judge evaluation
- **Embedding benchmark results** — publish comparison of MiniLM vs BGE vs PubMedBERT on FDA text
- **Fixed-k benchmark run** — rerun with `--fixed-k 5` for a normalised Hit@k / nDCG table

---

## License

MIT — see [LICENSE](LICENSE)
