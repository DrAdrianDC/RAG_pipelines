# `embeddings` — Embedding Model Adapters

A self-contained library of embedding model adapters for the `RAG_pipelines` framework. Provides a unified interface for converting text into dense vectors, backed by local HuggingFace models (no API key required).

---

## Module structure

```
embeddings/
├── base.py                  # EmbeddingModel protocol — the contract every adapter must satisfy
├── sentence_transformer.py  # HuggingFace sentence-transformers adapter (local, free)
├── tests/
│   ├── conftest.py          # MockSentenceTransformer — no real weights loaded in tests
│   ├── test_base.py         # Protocol conformance and runtime_checkable tests
│   └── test_sentence_transformer.py  # Adapter, singleton cache, embed(), error handling
└── README.md
```

> **Boundary rule**: this package does **not** import from `vectorstores/`, `chunking/`, `retrieval/`, or `experiments/`. It has no knowledge of ChromaDB collections, chunk schemas, or experiment paths. ChromaDB-specific wrappers live in `vectorstores/chroma.py`.

---

## Quick start

```python
from embeddings import embed, SentenceTransformerAdapter, DEFAULT_MODEL

# One-call convenience function — uses the shared singleton cache
vectors = embed(["KEYTRUDA is indicated for melanoma.", "Dose: 200 mg IV every 3 weeks."])
# vectors: np.ndarray shape (2, 384), float32, L2-normalised

# Adapter object — implements the EmbeddingModel protocol
adapter = SentenceTransformerAdapter("BAAI/bge-small-en-v1.5")
vectors = adapter.embed(["FDA label text..."])

# Access the raw SentenceTransformer (for advanced use)
from embeddings import get_sentence_transformer
model = get_sentence_transformer("all-MiniLM-L6-v2")  # cached singleton
```

---

## `EmbeddingModel` protocol (`base.py`)

Every adapter in this package satisfies the `EmbeddingModel` structural protocol:

```python
from embeddings.base import EmbeddingModel

class EmbeddingModel(Protocol):
    @property
    def model_name(self) -> str: ...

    def embed(self, texts: list[str]) -> np.ndarray: ...
    # Returns: shape (len(texts), dim), float32, L2-normalised
```

The protocol is `@runtime_checkable` — `isinstance(obj, EmbeddingModel)` works at runtime without explicit inheritance:

```python
adapter = SentenceTransformerAdapter()
assert isinstance(adapter, EmbeddingModel)  # True — structural check
```

---

## Available adapters

### `sentence_transformer.py` — HuggingFace backend

| Item | Detail |
|---|---|
| **Provider** | HuggingFace `sentence-transformers` library |
| **Cost** | Free — runs locally, no API key |
| **Models** | Any HuggingFace sentence-transformer checkpoint |
| **Default** | `all-MiniLM-L6-v2` (fast, 384-dim, general purpose) |

**Naming note**: the file is `sentence_transformer.py` (singular), not `sentence_transformers.py`. The plural name would shadow the installed `sentence_transformers` library in Python's import resolution — a latent bug avoided by using the singular.

#### Singleton cache

Models are loaded once per process and cached by model name:

```python
m1 = get_sentence_transformer("all-MiniLM-L6-v2")
m2 = get_sentence_transformer("all-MiniLM-L6-v2")
assert m1 is m2  # same object — no double loading
```

#### Recommended models for this corpus

| Model | Dim | Notes |
|---|---|---|
| `all-MiniLM-L6-v2` | 384 | Fast baseline — default |
| `BAAI/bge-small-en-v1.5` | 384 | Better retrieval on benchmarks, same speed |
| `NeuML/pubmedbert-base-embeddings` | 768 | Trained on PubMed — best for FDA regulatory text |

Compare them using `experiments/embedding_benchmark/run_benchmark.py`.

---

## Adding a new backend

Create `embeddings/<provider>.py` and implement the `EmbeddingModel` protocol:

```python
# embeddings/openai.py
import numpy as np
from openai import OpenAI

class OpenAIAdapter:
    def __init__(self, model_name: str = "text-embedding-3-small") -> None:
        self._model_name = model_name
        self._client = OpenAI()

    @property
    def model_name(self) -> str:
        return self._model_name

    def embed(self, texts: list[str]) -> np.ndarray:
        if not texts:
            raise ValueError("texts must be a non-empty list")
        response = self._client.embeddings.create(input=texts, model=self._model_name)
        vectors = [d.embedding for d in response.data]
        return np.array(vectors, dtype=np.float32)
```

Then re-export from `__init__.py` and add tests in `embeddings/tests/`.

No changes are needed in `vectorstores/`, `chunking/`, or `evaluation/` — they all consume the `EmbeddingModel` protocol, not the concrete adapter.

---

## Running tests

```bash
# From the project root
pytest embeddings/tests/ -v

# With coverage
pytest embeddings/tests/ -v --cov=embeddings --cov-report=term-missing
```

Tests use `MockSentenceTransformer` — no real model weights, no network, no GPU required.

---

## Design principles

- **No upward coupling**: `embeddings/` does not import from `vectorstores/`, `chunking/`, or `experiments/`. ChromaDB-specific wrappers (`SentenceTransformerEmbeddingFunction`) live in `vectorstores/chroma.py`.
- **One file per provider, not per model**: `sentence_transformer.py` supports any HuggingFace checkpoint via the `model_name` parameter. A file per model would be code duplication.
- **Singleton cache**: models are expensive to load (~100–500 MB). The process-level cache in `_model_cache` ensures a model is loaded exactly once, regardless of how many components request it.
- **L2-normalised output**: all `embed()` calls return unit-norm vectors so that cosine similarity equals dot product — required by ChromaDB's HNSW index with `hnsw:space=cosine`.
- **Clear error messages**: an invalid `model_name` raises `OSError` with the model name and a human-readable explanation, not a raw traceback from inside HuggingFace.
