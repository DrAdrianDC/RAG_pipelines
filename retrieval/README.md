# `retrieval` — Retrieval Strategy Adapters

A self-contained library of retrieval adapters built on top of the `vectorstores/` and `embeddings/` layers. Provides a uniform interface for all retrieval backends so that the evaluation and experiment layers never depend on a specific vector database.

---

## Module structure

```
retrieval/
├── base_retriever.py   # BaseRetriever — abstract interface every retriever must implement
├── dense_retriever.py  # DenseRetriever + build_dense_retriever factory
├── reranker.py         # RerankedRetriever — two-stage: dense ANN + cross-encoder reranking
└── tests/
    ├── conftest.py
    ├── test_dense_retriever.py
    └── test_reranker.py
```

> **Boundary rule**: this package imports from `vectorstores/` and `embeddings/` (lazy, inside the factory only). It does **not** import from `chunking/`, `evaluation/`, or `experiments/`.

---

## Pipeline position

```
embeddings/  ─┐
               ├──► vectorstores/  ──►  retrieval/  ──►  evaluation/
chunking/   ──┘
```

---

## Quick start

### Dense retrieval

```python
from pathlib import Path
from retrieval import build_dense_retriever

# Wire a ChromaDB collection in one call
retriever = build_dense_retriever(
    collection_name="recursive_512",
    base_path=Path("experiments/chunking_benchmark/chroma_stores"),
)

hits = retriever.retrieve("What biomarkers does imatinib require testing for?", k=5)
for hit in hits:
    print(f"{hit['distance']:.4f}  {hit['doc_id']}  {hit['content'][:80]}")
```

### Two-stage retrieval with reranking

```python
from retrieval import build_dense_retriever, RerankedRetriever

dense    = build_dense_retriever("recursive_512", base_path=CHROMA_BASE_PATH)
reranked = RerankedRetriever(inner=dense, fetch_k=20)

hits = reranked.retrieve("BRAF mutation in melanoma", k=5)
for hit in hits:
    print(f"rerank={hit['rerank_score']:.4f}  dense_dist={hit['distance']:.4f}  {hit['content'][:60]}")
```

---

## `BaseRetriever` — the interface

All strategies must subclass `BaseRetriever` and implement `retrieve`:

```python
class BaseRetriever(ABC):
    @abstractmethod
    def retrieve(self, query: str, k: int) -> list[dict[str, Any]]:
        """Return the top-k hits for query."""
        ...
```

Hit dict contract (same schema as `vectorstores.chroma.query_collection`):

```python
{
    "chunk_id":     str,    # unique chunk identifier
    "doc_id":       str,    # "drug_name||biomarker"
    "section":      str,    # FDA labeling section or ""
    "content":      str,    # raw chunk text
    "distance":     float,  # cosine distance ∈ [0, 2], lower = more similar
    # added by RerankedRetriever only:
    "rerank_score": float,  # cross-encoder relevance score, higher = more relevant
}
```

---

## `DenseRetriever` — dependency injection pattern

`DenseRetriever` wraps any callable `(query_text: str, k: int) -> list[dict]` as a `BaseRetriever`. The vector database is **never imported** inside the retriever — it is injected at construction time.

```python
# Manual wiring — maximum control
from vectorstores.chroma import get_chroma_collection, query_collection
from retrieval import DenseRetriever

collection = get_chroma_collection("fixed_512", base_path=CHROMA_BASE_PATH)
retriever = DenseRetriever(
    query_fn=lambda q, k: query_collection(collection, q, k),
    name="fixed_512",
)
```

```python
# Factory — recommended for experiment runners
from retrieval import build_dense_retriever

retriever = build_dense_retriever(
    collection_name="fixed_512",
    base_path=CHROMA_BASE_PATH,
    model_name="all-MiniLM-L6-v2",  # must match indexing model
)
```

Benefits of the DI pattern:
- Swapping ChromaDB for FAISS requires only changing the `query_fn` lambda
- The evaluation and experiment code never needs updating when the backend changes
- `DenseRetriever.retrieve` can be passed directly as `query_fn` to `RetrievalEvaluator`

---

## `RerankedRetriever` — two-stage retrieval

Reranking is a standard technique to improve precision: stage 1 retrieves a large candidate pool fast (approximate); stage 2 reranks with a cross-encoder that jointly encodes query and passage.

```
Stage 1: DenseRetriever.retrieve(query, k=fetch_k)  → top-fetch_k candidates (fast, ~1 ms)
Stage 2: CrossEncoder.predict([(query, content), ...])  → relevance scores (slower, ~100 ms)
         sort descending by score → return top-k
```

**Default model**: `cross-encoder/ms-marco-MiniLM-L-6-v2` — 22 M parameters, CPU-friendly, no API key required, standard benchmark reference model used by Weaviate, LlamaIndex, and LangChain.

**fetch_k guideline**: Weaviate recommends `fetch_k ≥ 3× k`. Default is 20 (for k=5).

```python
from retrieval import build_dense_retriever, RerankedRetriever

dense = build_dense_retriever("recursive_512", base_path=CHROMA_BASE_PATH)

# Retrieve 20 candidates, rerank, return top 5
reranked = RerankedRetriever(
    inner=dense,
    model_name="cross-encoder/ms-marco-MiniLM-L-6-v2",
    fetch_k=20,
)
hits = reranked.retrieve("HER2 amplification in breast cancer", k=5)
```

The cross-encoder model is **lazy-loaded** on the first `retrieve()` call.

---

## Running tests

```bash
pytest retrieval/tests/ -v
```

No API keys, no ChromaDB collections, and no model weights are loaded in any test. `MockCrossEncoder` provides deterministic scores for reranking tests.

---

## Adding a new retrieval strategy

1. Create `retrieval/<strategy>.py` and subclass `BaseRetriever`.
2. Implement `retrieve(self, query: str, k: int) -> list[dict]`.
3. Ensure every returned hit dict includes `chunk_id`, `doc_id`, `section`, `content`, and `distance`.
4. Re-export from `retrieval/__init__.py`.
5. Add tests under `retrieval/tests/`.

Candidate strategies:
- `SparseRetriever` — BM25 / TF-IDF using `rank_bm25` library.
- `HybridRetriever` — weighted combination of dense + sparse scores (Reciprocal Rank Fusion).
