

# 📚 RAG Pipelines

A modular exploration of Retrieval-Augmented Generation (RAG) pipelines focused on experimenting with different components such as ingestion, chunking, embeddings, and retrieval strategies.

This project is built as a **modular experimental framework for RAG systems design**.



---

# 🎯 Goal

The main objective of this repository is to:

- Understand how different RAG components affect performance
- Experiment with chunking and retrieval strategies
- Compare embeddings and vector database options
- Build a foundation for production-ready RAG systems

---

# 🧩 Current Components

## 📥 Data Ingestion

Modular ingestion pipelines for acquiring, parsing, and preprocessing
document collections used in retrieval experiments.

Current ingestion workflows include:

* PDF acquisition and document collection
* OCR-based document extraction
* Structured text preprocessing
* JSONL corpus generation
* Preparation of benchmark-ready datasets for downstream RAG evaluation

The ingestion layer is designed to support reproducible dataset construction
for chunking, retrieval, and retrieval-quality benchmarking experiments.

# Next steps

#### ✂️ Chunking Strategies
Experiments with different ways of splitting text:
- Fixed-size chunking
- Recursive splitting
- (More strategies in progress)

#### 🔢 Embeddings
Interface for testing different embedding models.

#### 🧱 Vector Storage
Initial integrations with vector databases for similarity search.

#### 🔍 Retrieval
Basic retrieval pipelines using vector similarity search.

---


# ⚙️ Installation

```bash
git clone https://github.com/DrAdrianDC/RAG_pipelines.git
cd RAG_pipelines

python -m venv venv
source venv/bin/activate
```

Install dependencies as needed per module.
