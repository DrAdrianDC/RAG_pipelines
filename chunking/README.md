# `chunking` — Modular Text Chunking Library

A self-contained Python library that implements, benchmarks, and cross-validates text chunking strategies for RAG pipelines. Designed as a standalone module within the `RAG_pipelines` framework — it has no dependency on the experiment runner, the vector store, or the evaluation layer.

---

## Overview

Chunking is the step that determines what text reaches the LLM. A well-formed chunk is **small enough for precise retrieval** and **complete enough to give the LLM full context**. This module implements the strategies described in the [Weaviate chunking guide](https://weaviate.io/blog/chunking-strategies-for-rag) and validated against the methodology of the [Vecta benchmark (2026)](https://www.runvecta.com/blog/we-benchmarked-7-chunking-strategies-most-advice-was-wrong).

---

## Embedding model context-window alignment

> **Critical constraint** — read before choosing a chunk size.

`all-MiniLM-L6-v2` (the default embedding model) was trained with a hard limit of **256 WordPiece tokens**. Any chunk that exceeds this limit is **silently truncated** by the model — the embedding only represents the first 256 WordPiece tokens, and the rest of the chunk text is invisible to the retrieval system.

tiktoken `cl100k_base` (used for token counting in this library) and WordPiece tokenise the same text differently. For English biomedical prose the empirical ratio is approximately **1.15–1.30 WordPiece tokens per cl100k token**, so:

```
256 WordPiece  ≈  197–222 cl100k tokens
```

`SAFE_CHUNK_TOKENS = 192` (defined in `config.py`) provides a ~30-token margin below the lower end of that range:

```
192 cl100k  ×  1.30  =  249 WordPiece  →  7-token margin below the 256 limit
```

**All model-aligned strategies set `chunk_size = SAFE_CHUNK_TOKENS`.** If you change `EMBEDDING_MODEL` to a model with a different context window, update `SAFE_CHUNK_TOKENS` accordingly — all strategies scale automatically.

| Model | Context window | Recommended `SAFE_CHUNK_TOKENS` |
|---|---|---|
| `all-MiniLM-L6-v2` (current) | 256 WordPiece | **192** |
| `all-mpnet-base-v2` | 512 WordPiece | 384 |
| `BAAI/bge-base-en-v1.5` | 512 WordPiece | 384 |
| `NeuML/pubmedbert-base-embeddings` | 512 WordPiece | 384 |

### Corpus

All strategies are evaluated against the **FDA Biomarker JSONL corpus** (`datasets/fda_biomarkers/benchmark/fda_biomarkers.jsonl`): 597 records of drug–biomarker pairs extracted from FDA prescribing information labels. Each record is a JSON object with the fields described in [`datasets/fda_biomarkers/README.md`](../datasets/fda_biomarkers/README.md).

---

## Module structure

```
chunking/
├── config.py                    # Hyperparameters for all strategies
├── base_chunker.py              # Abstract base class — defines the chunk schema
├── utils.py                     # Token counting, section normalisation, JSONL loader
├── fixed_chunking.py            # Strategy 1 & 2 — Fixed-Size / Token Chunking
├── recursive_chunking.py        # Strategy 3   — Recursive Chunking
├── semantic_chunking.py         # Strategy 4   — Semantic / Context-Aware Chunking
├── structure_aware_chunking.py  # Strategy 5   — Document-Based Chunking (FDA)
├── langchain_chunking.py        # LangChain cross-validation variants
├── tests/
│   ├── conftest.py              # Shared fixtures and mock embedding model
│   ├── test_base_chunker.py
│   ├── test_fixed_chunking.py
│   ├── test_recursive_chunking.py
│   ├── test_semantic_chunking.py
│   ├── test_structure_aware_chunking.py
│   └── test_config.py
└── requirements.txt
```

> **Boundary rule**: `chunking/` imports nothing from `experiments/`, `vectorstores/`, or `evaluation/`. It only imports from `embeddings/` (for the sentence transformer singleton). If you delete `experiments/` the module must still import and work correctly.

---

## Installation

```bash
# From the project root
pip install -r chunking/requirements.txt
```

| Package | Purpose |
|---|---|
| `tiktoken` | Token counting — `cl100k_base`, same tokenizer as OpenAI |
| `sentence-transformers==2.7.0` | Local embedding model for `SemanticChunker` |
| `transformers==4.44.2` | Pinned for ABI compatibility with sentence-transformers 2.7.x |
| `langchain-text-splitters` | TextSplitters for LangChain cross-validation variants (~5 MB, not full LangChain) |
| `nltk` | Sentence tokenizer for `SemanticChunker` |
| `chromadb` | Required by `embeddings/sentence_transformers.py` (shared module) |
| `numpy<2.0` | Pinned below 2.0 for PyTorch ABI compatibility |

---

## Quick start

```python
from chunking.utils import load_jsonl
from chunking.fixed_chunking import build_fixed_192
from chunking.recursive_chunking import build_recursive_192
from chunking.structure_aware_chunking import build_structure_aware

# Load corpus
records = load_jsonl()                       # 597 FDA biomarker records

# Instantiate a model-aligned strategy (fits all-MiniLM-L6-v2 context window)
chunker = build_recursive_192()

# Chunk a single record
record = records[0]
chunks = chunker.chunk_record(record)

# Chunk the full corpus
all_chunks = chunker.chunk_corpus(records)

# Inspect a chunk
print(chunks[0])
# {
#   "chunk_id":    "KEYTRUDA||PD-L1::0",
#   "content":     "KEYTRUDA is indicated for ...",
#   "doc_id":      "KEYTRUDA||PD-L1",
#   "section":     "",
#   "drug_name":   "KEYTRUDA",
#   "biomarker":   "PD-L1",
#   "token_count": 487
# }

# Average tokens per chunk (used for adaptive-k calculation)
avg = chunker.avg_tokens(all_chunks)
print(f"{chunker.name}: {len(all_chunks)} chunks, avg {avg:.0f} tokens")
```

---

## Chunk schema

Every chunker must produce dicts that conform to this schema (enforced by `BaseChunker._make_chunk`):

| Field | Type | Description |
|---|---|---|
| `chunk_id` | `str` | Globally unique: `"<doc_id>::<index>"` |
| `content` | `str` | The text of the chunk |
| `doc_id` | `str` | Parent document identifier: `"drug_name\|\|biomarker"` |
| `section` | `str` | Normalised FDA section name, or `""` if section-blind |
| `drug_name` | `str` | From the source JSONL record |
| `biomarker` | `str` | From the source JSONL record |
| `token_count` | `int` | `tiktoken cl100k_base` token count of `content` |

Chunks with `token_count < 5` are silently filtered by `BaseChunker.chunk_corpus`.

---

## Strategy catalogue

### Model-aligned strategies (recommended — `chunk_size` ≤ `SAFE_CHUNK_TOKENS = 192`)

Every chunk produced by these strategies fits within the `all-MiniLM-L6-v2` context window. No silent truncation.

| ID | Class | Weaviate category | `chunk_size` | `overlap_fraction` |
|---|---|---|---|---|
| `fixed_192` | `FixedChunker` | Fixed-Size / Token | 192 (`SAFE_CHUNK_TOKENS`) | 0.10 |
| `fixed_256` | `FixedChunker` | Fixed-Size / Token | 256 (borderline) | 0.10 |
| `recursive_192` | `RecursiveChunker` | Recursive | 192 (`SAFE_CHUNK_TOKENS`) | 0.10 |
| `semantic` | `SemanticChunker` | Semantic / Context-Aware | adaptive, capped at 192 | — |
| `structure_aware` | `StructureAwareChunker` | Document-Based | max 192 (`SAFE_CHUNK_TOKENS`) | — |
| `lc_fixed_192` | `LangChainFixedChunker` | Fixed-Size (LangChain) | 192 | ~0.10 |
| `lc_recursive_192` | `LangChainRecursiveChunker` | Recursive (LangChain) | 192 | ~0.10 |

### Legacy / oversize strategies (historical comparison only)

These strategies use chunk sizes that exceed `SAFE_CHUNK_TOKENS`. The embedding model silently truncates chunks beyond ~192 tokens. **Not recommended for production with `all-MiniLM-L6-v2`.**

| ID | Class | `chunk_size` | Status |
|---|---|---|---|
| `fixed_512` | `FixedChunker` | 512 | Truncated at embedding — avg chunk > model window |
| `fixed_1024` | `FixedChunker` | 1024 | Truncated at embedding — avg chunk >> model window |
| `recursive_512` | `RecursiveChunker` | 512 | Truncated at embedding — avg chunk > model window |
| `lc_fixed_512` | `LangChainFixedChunker` | 512 | Legacy cross-validation |
| `lc_fixed_1024` | `LangChainFixedChunker` | 1024 | Legacy cross-validation |
| `lc_recursive_512` | `LangChainRecursiveChunker` | 512 | Legacy cross-validation |

### `fixed_chunking.py` — Fixed-Size / Token Chunking

Splits at word boundaries and accumulates words until the token budget is reached. Overlap is a **fraction** of `chunk_size` (Weaviate: 10–20 %).

```
[word_1 … word_N  ≤ chunk_size tokens]
              [overlap_words … word_N+M]   ← ~overlap_fraction × chunk_size tokens repeated
```

**Weaviate**: *"The simplest approach … A typical overlap is between 10 % and 20 % of the chunk size."*

**Use when**: getting a quick, reproducible baseline. Also the standard comparison point for all other strategies.

### `recursive_chunking.py` — Recursive Chunking

Two-step process:

1. **`_split_recursive`** — attempts separators in priority order (`\n\n` → `\n` → `". "` → `"! "` → `"? "` → `" "` → char). Greedily merges pieces into chunks ≤ `chunk_size`. Oversized pieces are recursively split with remaining separators.
2. **`_add_overlap`** — injects token-level overlap on the final chunk list by prepending the last `overlap_tokens` words from `chunk[i-1]` to `chunk[i]`. This step is **separate** from Step 1 to guarantee the overlap budget is always filled regardless of paragraph size.

**Weaviate**: *"Recommended for unstructured text documents, such as articles, blog posts, and research papers."*

**Use when**: the corpus has no consistent structure. Consistently strong performer (Vecta: 69 % accuracy, 0.92 page F1).

### `semantic_chunking.py` — Semantic / Context-Aware Chunking

Weaviate's 4-step process:

1. **Sentence segmentation** — NLTK `sent_tokenize`
2. **Embedding generation** — SentenceTransformer, batched, normalised
3. **Similarity analysis** — sliding-window cosine (W=2); adaptive percentile threshold (threshold=0.7 → split at 30th percentile of *this document's* similarity distribution)
4. **Chunk formation** — merge chunks < `min_chunk_tokens`; split chunks > `max_chunk_tokens` on `\n\n`

**Weaviate**: *"Recommended for dense, unstructured text to preserve the complete semantic context of an idea."*

**Use when**: topic boundaries do not align with paragraph breaks (e.g. dense clinical pharmacology sections).

### `structure_aware_chunking.py` — Document-Based Chunking (FDA)

Detects FDA 21 CFR 201.57 numbered section headers using a regex, then keeps each section as an intact chunk. Sections exceeding `max_chunk_tokens` are sub-split recursively. Every chunk carries a normalised `section` field (e.g. `"Warnings and Precautions"`), enabling section-level retrieval metrics.

**Weaviate**: *"Ideal for Markdown, HTML, source code, or any document with clear structural markers."*

**Use when**: the corpus has reliable structural markers and section-level retrieval matters.

### `langchain_chunking.py` — LangChain cross-validation

Wraps `langchain_text_splitters.TokenTextSplitter` and `RecursiveCharacterTextSplitter` with `count_tokens` as the length function. Run these alongside the custom strategies: matching F1 scores validate the custom implementations; diverging scores signal implementation differences worth investigating.

---

## Configuration reference

All hyperparameters live in `config.py` under `STRATEGY_CONFIGS`:

```python
STRATEGY_CONFIGS = {
    "fixed_512": {
        "chunk_size": 512,
        "overlap_fraction": 0.10,   # 10 % of chunk_size ≈ 51 tokens
    },
    "recursive_512": {
        "chunk_size": 512,
        "overlap_fraction": 0.10,
    },
    "semantic": {
        "similarity_threshold": 0.7,    # percentile-based, per-document
        "min_chunk_tokens": 50,         # merge chunks smaller than this
        "max_chunk_tokens": 600,        # split chunks larger than this
    },
    "structure_aware": {
        "max_chunk_tokens": 1024,
    },
    ...
}
```

**`overlap_fraction`**: expressed as a fraction (Weaviate: 10–20 %) rather than absolute tokens, so it scales automatically when `chunk_size` changes.

**`similarity_threshold`** (semantic): not a global cosine cutoff — it is the percentile below which a split is inserted in the *current document's* similarity distribution. Lower → fewer splits (larger chunks). Higher → more splits (smaller chunks).

**`min_chunk_tokens`** (semantic): raised to 50 for FDA regulatory text. At 20 tokens (previous default), sub-sentence fragments were indexed as independent chunks with no informative embedding.

---

## Running tests

```bash
# From the project root
pytest chunking/tests/ -v

# With coverage
pytest chunking/tests/ -v --cov=chunking --cov-report=term-missing

# One strategy only
pytest chunking/tests/test_recursive_chunking.py -v
```

Tests use a `MockSentenceTransformer` that returns deterministic embeddings without loading model weights. No GPU, no network access, no API keys required.

---

## Extending with a new strategy

1. **Create the module** — inherit from `BaseChunker`, implement `chunk_record`:

```python
# chunking/my_strategy.py
from chunking.base_chunker import BaseChunker
from chunking.utils import count_tokens, make_doc_id

class MyChunker(BaseChunker):
    name = "my_strategy"

    def __init__(self, chunk_size: int) -> None:
        self.chunk_size = chunk_size

    def chunk_record(self, record: dict) -> list[dict]:
        content = record.get("content", "").strip()
        if not content:
            return []
        doc_id = make_doc_id(record)
        # ... your splitting logic ...
        return [
            self._make_chunk(
                text=piece,
                doc_id=doc_id,
                index=i,
                drug_name=record.get("drug_name", ""),
                biomarker=record.get("biomarker", ""),
                section="",
            )
            for i, piece in enumerate(pieces)
        ]
```

2. **Register in `config.py`** (use `SAFE_CHUNK_TOKENS` to respect the model window):

```python
from chunking.config import SAFE_CHUNK_TOKENS

"my_strategy": {
    "chunk_size": SAFE_CHUNK_TOKENS,   # 192 — fits all-MiniLM-L6-v2
    "description": "My custom strategy [model-aligned]",
}
```

3. **Register in the experiment runner** — add to `_build_strategies()` in `experiments/chunking_benchmark/run_benchmark.py`.

4. **Write tests** — add `chunking/tests/test_my_strategy.py` following the existing test pattern.

---

## Design principles

- **Pure library**: `chunking/` does not import from `experiments/`, `vectorstores/`, or `evaluation/`. It can be used, tested, and shipped independently.
- **Deterministic by default**: all strategies produce the same output for the same input. No randomness except in `SemanticChunker` where it is controlled via the model weights (fixed model = deterministic output).
- **Token-accurate**: all size limits are enforced in `tiktoken cl100k_base` tokens, not characters or words. This guarantees chunk sizes are accurate relative to the embedding model's context window.
- **Model-window aligned**: `SAFE_CHUNK_TOKENS = 192` is the single source of truth for the maximum safe chunk size given the current embedding model. All model-aligned strategies reference it so that updating the model requires changing only one constant.
- **Schema contract**: every chunker produces dicts validated against the same schema via `BaseChunker._make_chunk`. Downstream modules (`vectorstores/`, `evaluation/`) depend on this contract.
- **Overlap fraction, not absolute**: overlap is expressed as a fraction of `chunk_size` (Weaviate: 10–20 %). This means hyperparameters in `config.py` remain meaningful when `chunk_size` is changed.
