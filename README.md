# RAG Pipelines

A modular experimental framework for evaluating and optimizing Retrieval-Augmented Generation (RAG) systems through reproducible offline evaluation workflows.

The project focuses on understanding how architectural decisions impact retrieval quality, context relevance, and downstream generation performance — **not** on building a single end-user chatbot application.

---

## Goal

The main objective of this repository is to:

- Understand how different RAG components affect performance
- Experiment with chunking and retrieval strategies
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

Dense retriever built on top of the vector store and embedding layers.

### Evaluation (`evaluation/`)

Retrieval quality metrics (Recall@K, MRR, NDCG) and end-to-end generation metrics.

### Experiments (`experiments/`)

Runnable benchmarks that wire all components together:

- `experiments/chunking_benchmark/` — compares all 8 chunking strategies on retrieval quality
- `experiments/embedding_benchmark/` — compares embedding models on the same retrieval task

Results (CSV, JSON, plots) are gitignored and regenerated locally.

---

## Installation

```bash
git clone https://github.com/DrAdrianDC/RAG_pipelines.git
cd RAG_pipelines

python -m venv venv
source venv/bin/activate

# Install dependencies for the module you want to run, e.g.:
pip install -r chunking/requirements.txt
```

For LLM judge evaluation, copy `.env.example` to `.env` and add your API key:

```bash
cp .env.example .env
# edit .env and set GEMINI_API_KEY or OPENAI_API_KEY
```

---

## Running Tests

```bash
# All chunking tests (no API keys or GPU required)
pytest chunking/tests/ -v

# Embeddings tests
pytest embeddings/tests/ -v

# Vector store tests
pytest vectorstores/tests/ -v

# Full suite
pytest chunking/tests/ embeddings/tests/ vectorstores/tests/ -v
```

---

## Next Steps

Components already built locally, pending publication:

- **Retrieval** — dense retriever pipeline on top of ChromaDB + embeddings
- **Evaluation** — Recall@K, MRR, NDCG metrics and end-to-end generation evaluation
- **Experiments** — runnable chunking and embedding benchmarks with result artifacts (CSV, plots)

Components planned:

- **Reranking** — cross-encoder reranking layer (e.g. `cross-encoder/ms-marco-MiniLM-L-6-v2`) on top of dense retrieval
- **Context Construction** — strategies for assembling the final context window from retrieved chunks
- **LLM Generation** — generation layer with prompt templates and LLM judge evaluation (Gemini / Groq)
- **End-to-end benchmark** — full pipeline evaluation combining retrieval quality and generation quality metrics

---

## License

MIT — see [LICENSE](LICENSE)
