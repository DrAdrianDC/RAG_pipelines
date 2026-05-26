# `vectorstores` — Vector Database Adapters

A self-contained library of vector database adapters for the `RAG_pipelines` framework. Handles the full lifecycle of ChromaDB collections: creation, indexing, and similarity search — without leaking database details into the rest of the pipeline.

---

## Module structure

```
vectorstores/
├── chroma.py        # ChromaDB adapter — collection lifecycle, indexing, querying
├── tests/
│   ├── conftest.py  # MockEmbeddingFunction, sample chunk fixtures
│   └── test_chroma.py  # Collection management, index_chunks, query_collection tests
└── README.md
```

> **Boundary rule**: this package imports from `embeddings/` (only to obtain the `SentenceTransformerEmbeddingFunction` wrapper for ChromaDB) but **not** from `chunking/`, `retrieval/`, or `experiments/`. It receives chunk dicts as plain Python dicts — it has no dependency on `BaseChunker` or any chunk class.

---

## Quick start

```python
from vectorstores.chroma import get_chroma_collection, index_chunks, query_collection

# 1. Get (or create) a ChromaDB collection
collection = get_chroma_collection("keytruda-fixed-256", base_path="./chroma_db")

# 2. Index a list of chunk dicts (output of any chunking strategy)
index_chunks(collection, chunks)

# 3. Query — returns a ranked list of hit dicts
hits = query_collection(collection, query_text="What is the recommended dose?", k=5)
for hit in hits:
    print(hit["distance"], hit["doc_id"], hit["content"][:80])
```

---

## API reference

### `get_chroma_collection`

```python
get_chroma_collection(
    collection_name: str,
    base_path: str | Path = "./chroma_db",
    model_name: str = DEFAULT_MODEL,
    reset: bool = False,
) -> chromadb.Collection
```

Creates or retrieves a persistent ChromaDB collection.

| Parameter | Default | Description |
|---|---|---|
| `collection_name` | — | Unique name for the collection (e.g. `"keytruda-fixed-256"`) |
| `base_path` | `"./chroma_db"` | Root directory for ChromaDB storage |
| `model_name` | `"all-MiniLM-L6-v2"` | Sentence-transformer model used for embedding |
| `reset` | `False` | If `True`, atomically deletes and recreates the collection |

The collection always uses `hnsw:space=cosine` so distances are in `[0, 2]` (0 = identical, 2 = opposite).

**Reset behaviour**: uses `client.delete_collection()` followed by `client.create_collection()`. This is atomic at the client level — no partial state.

---

### `index_chunks`

```python
index_chunks(
    collection: chromadb.Collection,
    chunks: list[dict],
    batch_size: int = 512,
) -> None
```

Upserts chunks into the collection in batches.

- **Empty guard**: if `chunks` is empty, returns immediately (no-op).
- **Upsert semantics**: re-indexing the same `chunk_id` overwrites the existing entry — safe to call multiple times without duplication.
- **Batch size**: defaults to 512, which ChromaDB handles efficiently. Reduce if you hit memory limits.

Expected chunk dict schema:

```python
{
    "chunk_id":    str,    # unique — e.g. "KEYTRUDA||PD-L1::42"
    "content":     str,    # raw text to embed and store
    "doc_id":      str,    # "drug_name||biomarker"
    "section":     str,    # "" if document root, else header text
    "drug_name":   str,    # e.g. "KEYTRUDA"
    "biomarker":   str,    # e.g. "PD-L1"
    "token_count": int,    # from tiktoken cl100k_base
}
```

Optional keys (`section`, `drug_name`, `biomarker`, `token_count`) default to `""` / `0` if missing, so minimal dicts with only `chunk_id`, `content`, and `doc_id` are accepted without raising `KeyError`.

---

### `query_collection`

```python
query_collection(
    collection: chromadb.Collection,
    query_text: str,
    k: int = 5,
) -> list[dict]
```

Queries the collection and returns the top-k most similar chunks.

Returns a ranked list of **hit dicts**:

```python
{
    "chunk_id": str,    # matches the chunk_id used at index time
    "doc_id":   str,    # "drug_name||biomarker"
    "section":  str,    # section header or ""
    "content":  str,    # original text of the chunk
    "distance": float,  # cosine distance — lower is more similar
}
```

**Defensive validation**: ChromaDB can return result lists of inconsistent lengths if the internal store is corrupted. `query_collection` explicitly checks that all four result arrays (`ids`, `metadatas`, `distances`, `documents`) have the same length and raises `ValueError("inconsistent ChromaDB result lengths")` rather than silently dropping rows via `zip`.

**Fewer docs than k**: if the collection contains fewer documents than `k`, ChromaDB returns all available documents (no padding or error).

---

### `get_chroma_embedding_fn`

```python
get_chroma_embedding_fn(
    model_name: str = DEFAULT_MODEL,
) -> SentenceTransformerEmbeddingFunction
```

Returns a ChromaDB-compatible embedding function wrapping the given sentence-transformer model. Called internally by `get_chroma_collection` — exposed for testing and custom collection setup.

---

## Running tests

```bash
# From the project root
pytest vectorstores/tests/ -v

# With coverage
pytest vectorstores/tests/ -v --cov=vectorstores --cov-report=term-missing
```

Tests use:
- `chromadb.PersistentClient` with `tmp_path` — real HNSW index, isolated per test.
- `MockEmbeddingFunction` — returns deterministic random vectors without loading any model.

---

## Adding a new backend

Create `vectorstores/<provider>.py` exposing at minimum:

| Function | Signature | Notes |
|---|---|---|
| `get_collection` | `(name, ...) -> Collection` | Handles creation and reset |
| `index_chunks` | `(collection, chunks) -> None` | Upsert with batch support |
| `query_collection` | `(collection, text, k) -> list[dict]` | Returns hit dicts with schema above |

The retrieval and evaluation modules consume the **hit dict schema** — they do not call ChromaDB directly. As long as your new backend returns dicts with `chunk_id`, `doc_id`, `section`, `content`, and `distance`, no other module needs to change.

Candidate backends: FAISS (`vectorstores/faiss.py`), Weaviate (`vectorstores/weaviate.py`), Pinecone (`vectorstores/pinecone.py`).

---

## Design principles

- **Atomic reset**: `reset=True` uses `delete_collection()` rather than iterating over IDs. This is O(1) and leaves no orphaned data.
- **Upsert, not insert**: `collection.upsert()` makes re-indexing safe and idempotent — running `index_chunks` twice on the same data is correct behaviour.
- **Defensive result validation**: ChromaDB returns results as parallel lists. We validate their lengths explicitly before zipping, so a corrupted internal state produces a clear `ValueError` rather than a silent data loss bug.
- **Embedding function isolation**: `get_chroma_embedding_fn` is intentionally separated from `get_chroma_collection` so it can be patched in tests independently, and so different collections can use different models without refactoring.
- **No chunk objects**: this module receives and returns plain Python dicts. Coupling to `BaseChunker` or any chunk class would create a circular dependency with `chunking/`.
